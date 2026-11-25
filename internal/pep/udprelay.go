package pep

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"sync"
	"time"

	"github.com/bojieli/queqiao/internal/identity"
	"github.com/bojieli/queqiao/internal/session"
)

// A UDP association's relay is a socket on the server, and its source address
// is what the destination sees. Recreating it on a lane failure changes that
// address, which for a destination that pinned the flow -- a NAT, a game
// server, a QUIC peer -- is a different client. So a rescue used to preserve
// the local SOCKS socket and silently break the far end of the association.
//
// The relay is now retained for a bounded grace period and named by a token,
// and the replacement association reclaims it. What is deliberately not
// preserved is the datagrams in flight when the lane died: those are lost, and
// over the datagram substrate that is what a UDP packet is allowed to be.
type retainedRelay struct {
	conn      serverUDPRelay
	expires   time.Time
	principal identity.Principal
	// lease, in owned admission, is the ended association's session and
	// account slots, kept with the relay so that its replacement is not
	// refused for the capacity it is coming back to. It is handed to the
	// replacement that takes the relay over, or released when the relay is
	// closed instead.
	lease *flowLease
	// also is a second token the same relay answers to: the one its
	// association was opened with, while the client may not yet hold the
	// association's own grant. Taking the relay under either removes both.
	also    [session.UDPResumeTokenSize]byte
	hasAlso bool
}

// drop closes a retained relay and releases whatever it held.
func (r retainedRelay) drop() {
	_ = r.conn.Close()
	r.lease.release()
}

// udpRelayStore holds relays waiting to be reclaimed.
//
// Everything here is bounded because the entries are created by a peer. A
// relay holds a file descriptor and a kernel receive buffer, so the count is
// capped and the grace is short: the point is to survive a lane replacement,
// which takes seconds, not to keep a socket for a client that has gone.
type udpRelayStore struct {
	// grace and maximum are fixed at construction and read without the lock.
	// They were fields a test could reach in and change, and the sweeper
	// running concurrently read one of them -- a race the detector found, and
	// a shape worth not having: nothing about this store's policy changes
	// while it is running.
	grace   time.Duration
	maximum int

	mu     sync.Mutex
	relays map[[session.UDPResumeTokenSize]byte]retainedRelay
	// count is the number of distinct relays in relays: one parked under two
	// tokens is one socket, and maximum bounds sockets.
	count int
	// live names relays still held by a running association, under the
	// token that association granted (owned admission only). An entry is
	// bounded by the association that published it, which is bounded by
	// the session limit.
	live map[[session.UDPResumeTokenSize]byte]*liveRelay
	// sweeping is true while a goroutine is expiring entries. Retain and
	// claim each sweep as they go, which is enough while an association is
	// failing; this covers the case where they stop. It starts on the first
	// retain and exits once the store is empty, so a server that has never
	// lost a UDP lane runs no timer at all.
	sweeping bool
}

const (
	// udpRelayGrace is how long a relay outlives the lane that was using it.
	// A rescue is bounded exponential backoff over a handful of attempts, so
	// this only has to cover that; anything longer is holding a socket for a
	// client that is not coming back.
	udpRelayGrace = 30 * time.Second
	// udpRelaysRetained bounds the descriptors a peer can make the server
	// hold by failing associations.
	udpRelaysRetained = 256
)

func newUDPRelayStore() *udpRelayStore {
	return newUDPRelayStoreWith(udpRelayGrace, udpRelaysRetained)
}

// newUDPRelayStoreWith is for tests that need a shorter grace or a smaller
// bound than a server would use.
func newUDPRelayStoreWith(grace time.Duration, maximum int) *udpRelayStore {
	return &udpRelayStore{
		relays:  map[[session.UDPResumeTokenSize]byte]retainedRelay{},
		live:    map[[session.UDPResumeTokenSize]byte]*liveRelay{},
		grace:   grace,
		maximum: maximum,
	}
}

func newUDPResumeToken() ([session.UDPResumeTokenSize]byte, error) {
	var token [session.UDPResumeTokenSize]byte
	_, err := rand.Read(token[:])
	return token, err
}

// retain parks a relay under its token. It sweeps expired entries first, so a
// store that is never claimed from still does not grow, and drops the whole
// request rather than an arbitrary victim when it is full: refusing to retain
// degrades to today's behaviour, while evicting someone else's live relay
// breaks an association that was working.
func (s *udpRelayStore) retain(token [session.UDPResumeTokenSize]byte, principal identity.Principal, conn serverUDPRelay) {
	if s == nil || conn == nil {
		return
	}
	s.mu.Lock()
	start, _ := s.retainLocked(token, principal, conn, nil, nil)
	s.mu.Unlock()
	if start {
		go s.sweep()
	}
}

// retainLocked parks a relay, with the lease it keeps, and reports whether
// the caller must start the sweeper and whether the relay was kept. A full
// store closes the relay and releases the lease.
//
// also, when not nil, is a second token the relay answers to. A token
// already in use names a relay that cannot be reclaimed through it any
// more; that relay is dropped, with its lease, rather than overwritten.
func (s *udpRelayStore) retainLocked(token [session.UDPResumeTokenSize]byte, principal identity.Principal, conn serverUDPRelay, lease *flowLease, also *[session.UDPResumeTokenSize]byte) (start, kept bool) {
	now := time.Now()
	s.sweepLocked(now)
	keys := [][session.UDPResumeTokenSize]byte{token}
	if also != nil {
		keys = append(keys, *also)
	}
	for _, k := range keys {
		if held, ok := s.relays[k]; ok {
			s.removeLocked(k, held)
			held.drop()
		}
	}
	if s.count >= s.maximum {
		retainedRelay{conn: conn, lease: lease}.drop()
		return false, false
	}
	s.count++
	entry := retainedRelay{conn: conn, expires: now.Add(s.grace), principal: principal, lease: lease}
	if also != nil && *also != token {
		paired := entry
		paired.also, paired.hasAlso = token, true
		s.relays[*also] = paired
		entry.also, entry.hasAlso = *also, true
	}
	s.relays[token] = entry
	start = !s.sweeping
	s.sweeping = true
	return start, true
}

// sweep expires entries until there are none left, then stops. A relay nobody
// reclaims must not hold its descriptor for the life of the process merely
// because no other association failed after it.
func (s *udpRelayStore) sweep() {
	// Clamped rather than taken as given: the grace is a field so a test can
	// shorten it, and half of a short enough one is zero, which is not an
	// interval a ticker accepts.
	interval := s.grace / 2
	if interval < time.Millisecond {
		interval = time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		s.mu.Lock()
		s.sweepLocked(time.Now())
		if len(s.relays) == 0 {
			s.sweeping = false
			s.mu.Unlock()
			return
		}
		s.mu.Unlock()
	}
}

// claim takes the relay a token names, if it is still there. The token is
// consumed either way: one is issued per open, so a second attempt on the same
// token is a replay rather than a retry.
//
// The comparison is constant time. The map lookup that finds the entry is not,
// and cannot be, but a peer that has to guess sixteen random bytes learns
// nothing useful from either.
func (s *udpRelayStore) claim(token []byte, principal identity.Principal) serverUDPRelay {
	if s == nil || len(token) != session.UDPResumeTokenSize {
		return nil
	}
	var key [session.UDPResumeTokenSize]byte
	copy(key[:], token)
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sweepLocked(now)
	held, ok := s.relays[key]
	if !ok {
		return nil
	}
	s.removeLocked(key, held)
	if subtle.ConstantTimeCompare(key[:], token) != 1 || now.After(held.expires) || !samePrincipal(held.principal, principal) {
		held.drop()
		return nil
	}
	// The claimer was charged for itself; anything the ended association
	// kept is released.
	held.lease.release()
	return held.conn
}

// removeLocked withdraws a retained relay under key and under the other
// token it answers to, if any.
func (s *udpRelayStore) removeLocked(key [session.UDPResumeTokenSize]byte, held retainedRelay) {
	if _, ok := s.relays[key]; !ok {
		return
	}
	s.count--
	delete(s.relays, key)
	if held.hasAlso {
		if other, ok := s.relays[held.also]; ok && other.conn == held.conn {
			delete(s.relays, held.also)
		}
	}
}

func (s *udpRelayStore) sweepLocked(now time.Time) {
	for token, held := range s.relays {
		if now.After(held.expires) {
			s.removeLocked(token, held)
			held.drop()
		}
	}
}

// closeAll drops every retained relay, for server shutdown.
func (s *udpRelayStore) closeAll() {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for token, held := range s.relays {
		s.removeLocked(token, held)
		held.drop()
	}
}

// retained reports how many relays are waiting, for tests and for the metrics
// that make a leak visible.
func (s *udpRelayStore) retained() int {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.count
}

// liveRelay is a relay its association still holds, published so that the
// association's replacement can take it over (design §11.2). The client
// always closes the old association's lane before it sends the resume OPEN,
// but that close may never arrive, and even when it does the old handler
// parks the relay only after its workers have been joined. A replacement
// that finds the relay live therefore fences the old handler: it stops the
// association, waits for the handler's deferred cleanup to join its workers,
// and is handed the relay and the association's lease instead of the relay
// being parked or closed.
type liveRelay struct {
	principal identity.Principal
	// stop ends the owning association.
	stop context.CancelFunc
	// claimed is set, under the store lock, by the replacement that fenced
	// the owner; abandoned, by one that gave up waiting.
	claimed   bool
	abandoned bool
	// handed is closed once conn and lease belong to the replacement.
	handed chan struct{}
	conn   serverUDPRelay
	lease  *flowLease
	// alias is the token this association was opened with, when it is a
	// replacement. Until the client is known to hold the new grant, the
	// client still holds only the alias: a retry after a lost OPEN_OK
	// presents it, so the entry answers to it, and an unconfirmed
	// association is parked under it.
	alias     [session.UDPResumeTokenSize]byte
	hasAlias  bool
	confirmed bool
}

// aliasLive also publishes a live relay under the token its association was
// opened with, until confirmLive.
func (s *udpRelayStore) aliasLive(entry *liveRelay, alias []byte) {
	if s == nil || entry == nil || len(alias) != session.UDPResumeTokenSize {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	copy(entry.alias[:], alias)
	entry.hasAlias = true
	if !entry.claimed {
		s.live[entry.alias] = entry
	}
}

// confirmLive records that the client holds the association's grant: it has
// sent a frame on the lane, which it does only after reading the OPEN_OK.
func (s *udpRelayStore) confirmLive(entry *liveRelay) {
	if s == nil || entry == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	entry.confirmed = true
	if entry.hasAlias && s.live[entry.alias] == entry {
		delete(s.live, entry.alias)
	}
}

// publishLive records a running association's relay under the token it
// granted.
func (s *udpRelayStore) publishLive(token [session.UDPResumeTokenSize]byte, principal identity.Principal, stop context.CancelFunc) *liveRelay {
	if s == nil {
		return nil
	}
	entry := &liveRelay{principal: principal, stop: stop, handed: make(chan struct{})}
	s.mu.Lock()
	if s.live == nil {
		s.live = map[[session.UDPResumeTokenSize]byte]*liveRelay{}
	}
	s.live[token] = entry
	s.mu.Unlock()
	return entry
}

// liveEnd is what became of a live relay when its association ended.
type liveEnd int

const (
	liveClosed liveEnd = iota
	liveHanded
	liveParked
	// liveDropped: retention was wanted but the store was full; the relay
	// is already closed and the lease released.
	liveDropped
)

// endLive withdraws a live relay as its association ends, in one step under
// the store lock, so the token is claimable throughout: a claim either sees
// the live entry, or finds the relay already parked with its lease, or finds
// it gone because the association ended for good.
//
//   - A replacement had claimed it: the relay and the lease become the
//     replacement's (liveHanded).
//   - retain: the relay is parked under the token with the lease, which the
//     relay keeps until it is taken over or closed (liveParked).
//   - Otherwise nothing is done and the caller closes the relay (liveClosed).
func (s *udpRelayStore) endLive(token [session.UDPResumeTokenSize]byte, entry *liveRelay, conn serverUDPRelay, lease *flowLease, retain bool) liveEnd {
	if s == nil || entry == nil || conn == nil {
		return liveClosed
	}
	s.mu.Lock()
	if s.live[token] == entry {
		delete(s.live, token)
	}
	if entry.hasAlias && s.live[entry.alias] == entry {
		delete(s.live, entry.alias)
	}
	// Until the client has shown it holds the new grant, it may retry with
	// either token: the one it opened with, if the OPEN_OK was lost, or the
	// new one, if it was read but nothing followed it. The relay answers to
	// both.
	var also *[session.UDPResumeTokenSize]byte
	if !entry.confirmed && entry.hasAlias {
		also = &entry.alias
	}
	if entry.claimed && !entry.abandoned && !retain {
		// The association ended for good -- its relay failed, or it was
		// closed -- while a replacement was waiting. Wake it with nothing,
		// so it opens a fresh relay instead of resuming a broken one.
		entry.abandoned = true
		close(entry.handed)
		s.mu.Unlock()
		return liveClosed
	}
	if entry.claimed && !entry.abandoned {
		entry.conn = conn
		entry.lease = lease.detach()
		close(entry.handed)
		s.mu.Unlock()
		return liveHanded
	}
	if !retain {
		s.mu.Unlock()
		return liveClosed
	}
	// The slots are kept with the relay only for a client known to be able
	// to come back for them: one that has sent on the lane, so holds the
	// grant, or a replacement, which still holds the token it presented. A
	// fresh association whose OPEN_OK may never have been read leaves its
	// socket reclaimable but gives its slots back, so the client's retry
	// without a token is not refused by them.
	var kept *flowLease
	if entry.confirmed || entry.hasAlias {
		kept = lease.detach()
	} else {
		// Released before the parked relay becomes visible, so a
		// replacement that finds it is admitted against the freed slots.
		lease.detach().release()
	}
	start, retained := s.retainLocked(token, entry.principal, conn, kept, also)
	s.mu.Unlock()
	if start {
		go s.sweep()
	}
	if !retained {
		return liveDropped
	}
	return liveParked
}

// transfer takes over the relay a live association of the same principal
// holds under token, waiting at most wait for that association to stop. It
// returns nil when no such association exists, or when it did not stop in
// time; the caller then opens as before, which also finds a relay parked
// under the same token. The token is consumed by the attempt.
func (s *udpRelayStore) transfer(ctx context.Context, token []byte, principal identity.Principal, wait time.Duration) (serverUDPRelay, *flowLease) {
	if s == nil || len(token) != session.UDPResumeTokenSize {
		return nil, nil
	}
	var key [session.UDPResumeTokenSize]byte
	copy(key[:], token)
	s.mu.Lock()
	if held, ok := s.relays[key]; ok && held.lease != nil {
		// Parked by an association that ended in owned admission: the relay
		// comes back with the slots it kept for this replacement.
		if subtle.ConstantTimeCompare(key[:], token) != 1 || !samePrincipal(held.principal, principal) {
			s.mu.Unlock()
			return nil, nil
		}
		if time.Now().After(held.expires) {
			// Expired, if not yet swept: its slots go back before the
			// caller is admitted on its own.
			s.removeLocked(key, held)
			s.mu.Unlock()
			held.drop()
			return nil, nil
		}
		s.removeLocked(key, held)
		s.mu.Unlock()
		return held.conn, held.lease
	}
	entry := s.live[key]
	if entry == nil || entry.claimed || subtle.ConstantTimeCompare(key[:], token) != 1 || !samePrincipal(entry.principal, principal) {
		s.mu.Unlock()
		return nil, nil
	}
	entry.claimed = true
	delete(s.live, key)
	s.mu.Unlock()
	entry.stop()
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-entry.handed:
		if entry.conn == nil {
			return nil, nil
		}
		return entry.conn, entry.lease
	case <-timer.C:
	case <-ctx.Done():
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	select {
	case <-entry.handed:
		if entry.conn == nil {
			return nil, nil
		}
		return entry.conn, entry.lease
	default:
	}
	entry.abandoned = true
	return nil, nil
}
