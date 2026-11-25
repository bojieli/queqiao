// Package memlimit provides hard byte admission for memory retained by many
// concurrent flows. It accounts ownership rather than sampling the Go heap:
// a caller must acquire bytes before allocating or retaining them and release
// exactly those bytes when the data is no longer reachable from the data path.
package memlimit

import (
	"container/list"
	"context"
	"errors"
	"sync"
)

var ErrRequestTooLarge = errors.New("memory request exceeds budget capacity")

// Snapshot is a race-free view of a Budget.
type Snapshot struct {
	Capacity int64 `json:"capacity_bytes"`
	Used     int64 `json:"used_bytes"`
	Peak     int64 `json:"peak_bytes"`
	Waiters  int64 `json:"waiters"`
}

// Budget is a fixed-capacity, concurrency-safe byte semaphore. All flows of a
// client share one Budget, so opening more flows changes scheduling and
// backpressure, never the maximum retained payload memory.
//
// Waiters use a broadcast-style generation channel. A release closes the old
// generation and creates a new one, waking every waiter without a goroutine per
// request. Acquisition is deliberately not strictly FIFO: data-path requests
// are uniformly bounded chunks, and allowing a small request through prevents
// an oversized request at the head from blocking control progress.
//
// A Budget made by NewFair grants in arrival order instead (see NewFair).
type Budget struct {
	mu       sync.Mutex
	capacity int64
	used     int64
	peak     int64
	waiters  int64
	changed  chan struct{}

	// fair selects explicit, first-come grants. queue holds *grant values
	// for requests that could not be satisfied on arrival.
	fair  bool
	queue list.List
}

// grant is one queued request of a fair Budget. Release hands it bytes by
// charging them to used and closing ready, so a woken waiter already owns
// its reservation and never re-races for it.
type grant struct {
	n       int64
	ready   chan struct{}
	granted bool
}

func New(capacity int64) *Budget {
	if capacity <= 0 {
		return nil
	}
	return &Budget{capacity: capacity, changed: make(chan struct{})}
}

// NewFair returns a Budget whose waiters are granted in the order they began
// waiting. Released bytes go to the head of the queue; a request never
// overtakes one that was waiting before it, and a non-blocking TryAcquire
// fails while anyone waits.
//
// The send path asks for at most one chunk per flow at a time and asks again
// only after its previous request was granted, so arrival order is a
// round-robin of one chunk per waiting flow: no waiter can be overtaken
// indefinitely, which the broadcast race of New cannot promise.
func NewFair(capacity int64) *Budget {
	b := New(capacity)
	if b != nil {
		b.fair = true
	}
	return b
}

// Acquire blocks until n bytes are available or ctx ends. A nil Budget is an
// intentionally unbounded configuration and succeeds immediately.
func (b *Budget) Acquire(ctx context.Context, n int) error {
	if b == nil || n <= 0 {
		return nil
	}
	request := int64(n)
	b.mu.Lock()
	if request > b.capacity {
		b.mu.Unlock()
		return ErrRequestTooLarge
	}
	if b.fair {
		return b.acquireFairLocked(ctx, request)
	}
	for {
		// Cancellation wins even when capacity is currently available. Callers
		// use cancellation to tear down a flow; admitting a final allocation
		// after that point needlessly extends the lifetime of its payload.
		if err := ctx.Err(); err != nil {
			b.mu.Unlock()
			return err
		}
		if request <= b.capacity-b.used {
			b.used += request
			if b.used > b.peak {
				b.peak = b.used
			}
			b.mu.Unlock()
			return nil
		}
		generation := b.changed
		b.waiters++
		b.mu.Unlock()
		select {
		case <-generation:
		case <-ctx.Done():
			b.mu.Lock()
			b.waiters--
			b.mu.Unlock()
			return ctx.Err()
		}
		b.mu.Lock()
		b.waiters--
	}
}

// acquireFairLocked queues a request behind every earlier waiter. It is
// called with b.mu held and returns with it released.
func (b *Budget) acquireFairLocked(ctx context.Context, request int64) error {
	if err := ctx.Err(); err != nil {
		b.mu.Unlock()
		return err
	}
	if b.queue.Len() == 0 && request <= b.capacity-b.used {
		b.chargeLocked(request)
		b.mu.Unlock()
		return nil
	}
	g := &grant{n: request, ready: make(chan struct{})}
	element := b.queue.PushBack(g)
	b.waiters++
	b.mu.Unlock()
	select {
	case <-g.ready:
		return nil
	case <-ctx.Done():
	}
	b.mu.Lock()
	if g.granted {
		// The grant and the cancellation raced. Cancellation wins, as in
		// the unfair path, so the bytes go straight to the next waiter.
		b.used -= g.n
	} else {
		b.queue.Remove(element)
		b.waiters--
	}
	// Either way the head may have changed: a smaller request behind a
	// cancelled one can fit now.
	b.grantLocked()
	b.mu.Unlock()
	return ctx.Err()
}

// grantLocked satisfies queued requests from the head while they fit.
func (b *Budget) grantLocked() {
	for front := b.queue.Front(); front != nil; front = b.queue.Front() {
		g := front.Value.(*grant)
		if g.n > b.capacity-b.used {
			return
		}
		b.queue.Remove(front)
		b.waiters--
		b.chargeLocked(g.n)
		g.granted = true
		close(g.ready)
	}
}

func (b *Budget) chargeLocked(request int64) {
	b.used += request
	if b.used > b.peak {
		b.peak = b.used
	}
}

// TryAcquire reserves n bytes without waiting. On a fair Budget it fails
// while any request is queued, so it cannot overtake a waiter.
func (b *Budget) TryAcquire(n int) bool {
	if b == nil || n <= 0 {
		return true
	}
	request := int64(n)
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.fair && b.queue.Len() > 0 {
		return false
	}
	if request > b.capacity-b.used {
		return false
	}
	b.used += request
	if b.used > b.peak {
		b.peak = b.used
	}
	return true
}

// Release returns n bytes. Releasing bytes that were not acquired is a
// programming error: silently saturating at zero would hide double releases
// and destroy the hard-limit invariant.
func (b *Budget) Release(n int) {
	if b == nil || n <= 0 {
		return
	}
	released := int64(n)
	b.mu.Lock()
	if released > b.used {
		b.mu.Unlock()
		panic("memlimit: release exceeds acquired bytes")
	}
	b.used -= released
	if b.fair {
		b.grantLocked()
		b.mu.Unlock()
		return
	}
	old := b.changed
	b.changed = make(chan struct{})
	close(old)
	b.mu.Unlock()
}

func (b *Budget) Snapshot() Snapshot {
	if b == nil {
		return Snapshot{}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return Snapshot{Capacity: b.capacity, Used: b.used, Peak: b.peak, Waiters: b.waiters}
}
