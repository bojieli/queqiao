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
	now := time.Now()
	s.mu.Lock()
	s.sweepLocked(now)
	if len(s.relays) >= s.maximum {
		s.mu.Unlock()
		_ = conn.Close()
		return
	}
	s.relays[token] = retainedRelay{conn: conn, expires: now.Add(s.grace), principal: principal}
	start := !s.sweeping
	s.sweeping = true
	s.mu.Unlock()
	if start {
		go s.sweep()
	}
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
	delete(s.relays, key)
	if subtle.ConstantTimeCompare(key[:], token) != 1 || now.After(held.expires) || !samePrincipal(held.principal, principal) {
		_ = held.conn.Close()
		return nil
	}
	return held.conn
}

func (s *udpRelayStore) sweepLocked(now time.Time) {
	for token, held := range s.relays {
		if now.After(held.expires) {
			delete(s.relays, token)
			_ = held.conn.Close()
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
		delete(s.relays, token)
		_ = held.conn.Close()
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
	return len(s.relays)
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

// endLive withdraws a live relay as its association ends. It reports true
// when a replacement had claimed it: the relay and the lease are then the
// replacement's, and the caller must neither park nor close the relay. Under
// the store lock, so a claim either sees the entry and is handed the relay,
// or misses it and finds the relay parked (or gone) instead.
func (s *udpRelayStore) endLive(token [session.UDPResumeTokenSize]byte, entry *liveRelay, conn serverUDPRelay, lease *flowLease) bool {
	if s == nil || entry == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.live[token] == entry {
		delete(s.live, token)
	}
	if !entry.claimed || entry.abandoned || conn == nil {
		return false
	}
	entry.conn = conn
	entry.lease = lease.detach()
	close(entry.handed)
	return true
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
		return entry.conn, entry.lease
	case <-timer.C:
	case <-ctx.Done():
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	select {
	case <-entry.handed:
		return entry.conn, entry.lease
	default:
	}
	entry.abandoned = true
	return nil, nil
}
