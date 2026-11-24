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
	// openFailed marks a session whose OPEN failed before it was admitted.
	openFailed bool
}

func (e *endedSessions) add(id [16]byte, flowID uint64, principal identity.Principal, now time.Time) {
	e.record(endedEntry{id: id, flowID: flowID, principal: principal}, now)
}

// addFailedOpen remembers a session whose OPEN failed before admission.
func (e *endedSessions) addFailedOpen(id [16]byte, flowID uint64, principal identity.Principal, now time.Time) {
	e.record(endedEntry{id: id, flowID: flowID, principal: principal, openFailed: true}, now)
}

func (e *endedSessions) record(entry endedEntry, now time.Time) {
	id := entry.id
	e.mu.Lock()
	defer e.mu.Unlock()
	e.pruneLocked(now)
	if e.entries == nil {
		e.entries = make(map[[16]byte]endedEntry)
	}
	if len(e.order)-e.head >= endedSessionCapacity {
		e.dropOldestLocked()
	}
	entry.expires = now.Add(endedSessionMemory)
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
// device's, which is when a JOIN may be told it ended, and whether it ended
// because its OPEN failed.
func (e *endedSessions) heldBy(id [16]byte, flowID uint64, principal identity.Principal, now time.Time) (held, openFailed bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	entry, ok := e.entries[id]
	if !ok || !now.Before(entry.expires) || entry.flowID != flowID || !samePrincipal(entry.principal, principal) {
		return false, false
	}
	return true, entry.openFailed
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
