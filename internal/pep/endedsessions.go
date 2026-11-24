package pep

import (
	"sync"
	"time"
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
	expires map[[16]byte]time.Time
	order   []endedEntry
	head    int
}

type endedEntry struct {
	id      [16]byte
	expires time.Time
}

func (e *endedSessions) add(id [16]byte, now time.Time) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.pruneLocked(now)
	if e.expires == nil {
		e.expires = make(map[[16]byte]time.Time)
	}
	if len(e.order)-e.head >= endedSessionCapacity {
		e.dropOldestLocked()
	}
	expires := now.Add(endedSessionMemory)
	e.expires[id] = expires
	e.order = append(e.order, endedEntry{id: id, expires: expires})
	if e.head > len(e.order)/2 {
		e.order = append(e.order[:0:0], e.order[e.head:]...)
		e.head = 0
	}
}

func (e *endedSessions) contains(id [16]byte, now time.Time) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	expires, ok := e.expires[id]
	return ok && now.Before(expires)
}

func (e *endedSessions) pruneLocked(now time.Time) {
	for e.head < len(e.order) && !now.Before(e.order[e.head].expires) {
		e.dropOldestLocked()
	}
}

func (e *endedSessions) dropOldestLocked() {
	oldest := e.order[e.head]
	e.head++
	if e.expires[oldest.id] == oldest.expires {
		delete(e.expires, oldest.id)
	}
}
