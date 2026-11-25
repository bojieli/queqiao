package memlimit

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func waitWaiters(t *testing.T, b *Budget, n int64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for b.Snapshot().Waiters != n {
		if time.Now().After(deadline) {
			t.Fatalf("waiters = %d, want %d", b.Snapshot().Waiters, n)
		}
		time.Sleep(time.Millisecond)
	}
}

func acquireAsync(b *Budget, ctx context.Context, n int) chan error {
	done := make(chan error, 1)
	go func() { done <- b.Acquire(ctx, n) }()
	return done
}

func granted(done chan error) bool {
	select {
	case err := <-done:
		if err != nil {
			panic(err)
		}
		return true
	case <-time.After(20 * time.Millisecond):
		return false
	}
}

// A fair Budget grants in arrival order: released bytes go to the head of the
// queue, and a request that would fit never overtakes an earlier one.
func TestFairBudgetGrantsInArrivalOrder(t *testing.T) {
	b := NewFair(4)
	ctx := context.Background()
	if err := b.Acquire(ctx, 4); err != nil {
		t.Fatal(err)
	}
	first := acquireAsync(b, ctx, 2)
	waitWaiters(t, b, 1)
	second := acquireAsync(b, ctx, 1)
	waitWaiters(t, b, 2)
	third := acquireAsync(b, ctx, 2)
	waitWaiters(t, b, 3)

	b.Release(1)
	if granted(second) {
		t.Fatal("a later small request overtook the head of the queue")
	}
	if b.TryAcquire(1) {
		t.Fatal("TryAcquire overtook queued waiters")
	}
	b.Release(1)
	if !granted(first) {
		t.Fatal("the head was not granted once its bytes were free")
	}
	b.Release(2)
	if !granted(second) {
		t.Fatal("the second waiter was not granted")
	}
	if granted(third) {
		t.Fatal("granted beyond capacity")
	}
	if got := b.Snapshot(); got.Used != 3 || got.Waiters != 1 || got.Peak != 4 {
		t.Fatalf("snapshot = %+v", got)
	}
	b.Release(1)
	if !granted(third) {
		t.Fatal("the third waiter was not granted")
	}
	b.Release(4)
	if got := b.Snapshot(); got.Used != 0 || got.Waiters != 0 {
		t.Fatalf("after release = %+v", got)
	}
}

// A cancelled head leaves the queue and hands its turn on; it does not hold
// up the requests behind it.
func TestFairBudgetCancelledHeadPassesItsTurn(t *testing.T) {
	b := NewFair(2)
	if err := b.Acquire(context.Background(), 2); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	head := acquireAsync(b, ctx, 2)
	waitWaiters(t, b, 1)
	next := acquireAsync(b, context.Background(), 1)
	waitWaiters(t, b, 2)
	b.Release(1)
	if granted(next) {
		t.Fatal("overtook a live head")
	}
	cancel()
	if err := <-head; err == nil {
		t.Fatal("cancelled acquire succeeded")
	}
	if !granted(next) {
		t.Fatal("cancelled head kept its turn")
	}
	if got := b.Snapshot(); got.Used != 2 || got.Waiters != 0 {
		t.Fatalf("snapshot = %+v", got)
	}
}

// Under sustained contention every flow is granted in turn: none is starved,
// and none gets more than its share. Each worker is one flow's producer,
// asking for one chunk at a time.
func TestFairBudgetStarvesNoWaiter(t *testing.T) {
	const (
		chunk   = 16
		flows   = 24
		holding = 4 // chunks the budget holds
		rounds  = 40
	)
	b := NewFair(holding * chunk)
	var counts [flows]atomic.Int64
	var total atomic.Int64
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for i := range flows {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for {
				if err := b.Acquire(ctx, chunk); err != nil {
					return
				}
				counts[i].Add(1)
				if total.Add(1) >= flows*rounds {
					cancel()
				}
				time.Sleep(50 * time.Microsecond)
				b.Release(chunk)
			}
		}(i)
	}
	wg.Wait()
	cancel()
	lo, hi := counts[0].Load(), counts[0].Load()
	for i := range flows {
		c := counts[i].Load()
		lo, hi = min(lo, c), max(hi, c)
	}
	// Round-robin of one chunk per waiting flow: counts differ by at most the
	// grants in flight when the run stopped.
	if lo == 0 || hi-lo > holding+1 {
		t.Fatalf("grants per flow range %d..%d over %d flows", lo, hi, flows)
	}
	if got := b.Snapshot(); got.Used != 0 || got.Peak > got.Capacity {
		t.Fatalf("snapshot = %+v", got)
	}
}

// A large request at the head is not starved by a stream of small ones that
// would each fit sooner.
func TestFairBudgetLargeRequestIsNotStarvedBySmallOnes(t *testing.T) {
	b := NewFair(8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				if err := b.Acquire(ctx, 2); err != nil {
					return
				}
				time.Sleep(100 * time.Microsecond)
				b.Release(2)
			}
		}()
	}
	time.Sleep(5 * time.Millisecond)
	large := acquireAsync(b, context.Background(), 8)
	select {
	case err := <-large:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("large request starved behind small ones")
	}
	b.Release(8)
	cancel()
	wg.Wait()
}
