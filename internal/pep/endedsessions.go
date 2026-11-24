package pep

import (
	"sync"
	"time"

	"github.com/bojieli/queqiao/internal/identity"
)

// endedSessionMemory is how long a gateway remembers that it held a session.
// It comfortably covers a client's whole open window -- an unconfirmed OPEN is
// abandoned after one handshake bound -- and a rescue's replacement grace.
const endedSessionMemory = 2 * time.Minute

// endedSessionCapacity bounds the memory against a storm of short flows. An
// entry pushed out early is only a JOIN answered "unknown session" instead of
// "session ended", or a very late duplicate OPEN no longer refused.
const endedSessionCapacity = 1 << 16

// endedSessions is a bounded, expiring set of session identifiers this
// gateway has held. It lets a JOIN for one be told the session ended rather
// than that it is unknown, and refuses a late copy of its OPEN. The zero value
// is ready to use.
type endedSessions struct {
	mu      sync.Mutex
	entries map[[16]byte]endedEntry
	order   []endedEntry
	head    int
}

type endedEntry struct {
	id      [16]byte
	expires time.Time
	// The flow and device the session belonged to. Only a JOIN naming both
	// is told the session ended; anyone else learns nothing about it.
	flowID    uint64
	principal identity.Principal
}

func (e *endedSessions) add(id [16]byte, flowID uint64, principal identity.Principal, now time.Time) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.pruneLocked(now)
	if e.entries == nil {
		e.entries = make(map[[16]byte]endedEntry)
	}
	if len(e.order)-e.head >= endedSessionCapacity {
		e.dropOldestLocked()
	}
	entry := endedEntry{id: id, expires: now.Add(endedSessionMemory), flowID: flowID, principal: principal}
	e.entries[id] = entry
	e.order = append(e.order, entry)
	if e.head > len(e.order)/2 {
		e.order = append(e.order[:0:0], e.order[e.head:]...)
		e.head = 0
	}
}

// contains reports whether the identifier belonged to a recently ended
// session, whoever owned it. It is what refuses a duplicate OPEN.
func (e *endedSessions) contains(id [16]byte, now time.Time) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	entry, ok := e.entries[id]
	return ok && now.Before(entry.expires)
}

// heldBy reports whether a recently ended session was this flow of this
// device's, which is when a JOIN may be told it ended.
func (e *endedSessions) heldBy(id [16]byte, flowID uint64, principal identity.Principal, now time.Time) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	entry, ok := e.entries[id]
	return ok && now.Before(entry.expires) && entry.flowID == flowID && samePrincipal(entry.principal, principal)
}

func (e *endedSessions) pruneLocked(now time.Time) {
	for e.head < len(e.order) && !now.Before(e.order[e.head].expires) {
		e.dropOldestLocked()
	}
}

func (e *endedSessions) dropOldestLocked() {
	oldest := e.order[e.head]
	e.head++
	if current, ok := e.entries[oldest.id]; ok && current.expires.Equal(oldest.expires) {
		delete(e.entries, oldest.id)
	}
}
