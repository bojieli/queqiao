package stripe

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/bojieli/queqiao/internal/memlimit"
)

// writesReader hands the scheduler one queued write per Read, and blocks
// between them, which is what an application socket does.
type writesReader struct{ writes chan []byte }

func (r *writesReader) Read(p []byte) (int, error) {
	w, ok := <-r.writes
	if !ok {
		return 0, io.EOF
	}
	return copy(p, w), nil
}

// A small write that finds nothing READY is marked sparse, if the flow admits
// it, and its dispatch asks ReliableSparse. A large write, or a small one
// behind READY bytes, is not.
func TestSparseWritesAreMarkedOnlyOntoAnEmptyQueue(t *testing.T) {
	src := &writesReader{writes: make(chan []byte, 8)}
	defer close(src.writes)
	admitted := 0
	s := New(src, Config{
		ChunkSize: 32 << 10, LaneWindow: 64, MaxOutstanding: 64, MaxOutstandingBytes: 1 << 20,
		SparseBytes:    2800,
		Sparse:         func(n int) bool { admitted += n; return true },
		Reliable:       func(uint64) bool { return true },
		ReliableSparse: func(uint64) bool { return false },
	})
	defer s.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	waitReady := func(n uint64) {
		t.Helper()
		for s.ReadyBytes() != n {
			if ctx.Err() != nil {
				t.Fatalf("ready bytes %d, want %d", s.ReadyBytes(), n)
			}
			time.Sleep(time.Millisecond)
		}
	}

	src.writes <- make([]byte, 36)
	waitReady(36)
	c, err := s.Next(ctx, 1, 0)
	if err != nil || c == nil {
		t.Fatal(err)
	}
	if !c.Sparse || c.Reliable {
		t.Fatalf("a keystroke onto an empty queue: sparse=%v reliable=%v, want sparse and unreliable (coded)", c.Sparse, c.Reliable)
	}

	src.writes <- make([]byte, 16<<10)
	waitReady(16 << 10)
	src.writes <- make([]byte, 36)
	waitReady(16<<10 + 36)
	big, _ := s.Next(ctx, 1, 0)
	behind, _ := s.Next(ctx, 1, 0)
	if big.Sparse || !big.Reliable {
		t.Fatalf("a 16 KiB read was marked sparse=%v reliable=%v", big.Sparse, big.Reliable)
	}
	if behind.Sparse || !behind.Reliable {
		t.Fatalf("a small write behind READY bytes was marked sparse=%v reliable=%v", behind.Sparse, behind.Reliable)
	}
	if admitted != 36 {
		t.Fatalf("the flow was asked to admit %d sparse bytes, want 36", admitted)
	}
	if ref := c.Ref(); !ref.Sparse {
		t.Fatal("a chunk's reference lost its sparse mark")
	}
}

// A retry waiting in the ready set is a non-empty send queue, although
// readyBytes counts only bytes never sent.
func TestASmallWriteBehindAQueuedRetryIsNotSparse(t *testing.T) {
	src := &writesReader{writes: make(chan []byte, 8)}
	defer close(src.writes)
	s := New(src, Config{
		ChunkSize: 32 << 10, LaneWindow: 64, MaxOutstanding: 64, MaxOutstandingBytes: 1 << 20,
		SparseBytes: 2800, Sparse: func(int) bool { return true },
	})
	defer s.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	waitReady := func(n uint64) {
		t.Helper()
		for s.ReadyBytes() != n {
			if ctx.Err() != nil {
				t.Fatalf("ready bytes %d, want %d", s.ReadyBytes(), n)
			}
			time.Sleep(time.Millisecond)
		}
	}
	src.writes <- make([]byte, 16<<10)
	waitReady(16 << 10)
	c, err := s.Next(ctx, 1, 0)
	if err != nil || c == nil {
		t.Fatalf("first dispatch: %v", err)
	}
	s.Fail(1, c) // requeued for another attempt
	src.writes <- make([]byte, 36)
	waitReady(36)
	retry, err := s.Next(ctx, 2, 0)
	if err != nil || retry == nil {
		t.Fatalf("retry dispatch: %v", err)
	}
	small, err := s.Next(ctx, 2, 0)
	if err != nil || small == nil {
		t.Fatalf("small write dispatch: %v", err)
	}
	if retry.Offset != 0 || small.Sparse {
		t.Fatalf("retry offset %d, small write sparse=%v; want the retry first and the write not sparse", retry.Offset, small.Sparse)
	}
}

// A read that filled what the scheduler offered was limited by the scheduler,
// not by the application, however small: a tiny chunk size is not a stream of
// sparse writes.
func TestAFullReadIsNotSparse(t *testing.T) {
	src := &writesReader{writes: make(chan []byte, 8)}
	defer close(src.writes)
	s := New(src, Config{
		ChunkSize: 1024, LaneWindow: 64, MaxOutstanding: 64, MaxOutstandingBytes: 1 << 20,
		SparseBytes: 2800, Sparse: func(int) bool { return true },
	})
	defer s.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	src.writes <- make([]byte, 1024)
	full, err := s.Next(ctx, 1, 0)
	if err != nil || full == nil {
		t.Fatal(err)
	}
	src.writes <- make([]byte, 36)
	short, err := s.Next(ctx, 1, 0)
	if err != nil || short == nil {
		t.Fatal(err)
	}
	if full.Sparse || !short.Sparse {
		t.Fatalf("full 1 KiB read sparse=%v, short read sparse=%v; want false and true", full.Sparse, short.Sparse)
	}
}

// The retransmit delay is asked for with the chunk's own mark, so a caller can
// give a chunk that arrived alone the short deadline it needs while one with
// data behind it keeps the long one (#282).
func TestTheRetransmitDelayIsAskedPerChunk(t *testing.T) {
	src := &writesReader{writes: make(chan []byte, 2)}
	defer close(src.writes)
	now := time.Now()
	type asked struct {
		sparse bool
		size   int
	}
	var calls []asked
	s := New(src, Config{
		ChunkSize: 32 << 10, LaneWindow: 64, MaxOutstanding: 64, MaxOutstandingBytes: 1 << 20,
		Reliable:    func(uint64) bool { return false },
		SparseBytes: 512, Sparse: func(int) bool { return true },
		RetransmitAfter: func(sparse bool, size int) time.Duration {
			calls = append(calls, asked{sparse: sparse, size: size})
			if sparse || size <= 512 {
				return time.Millisecond
			}
			return time.Second
		},
		Now: func() time.Time { return now },
	})
	defer s.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	src.writes <- make([]byte, 1<<10)
	full, err := s.Next(ctx, 1, 0)
	if err != nil || full == nil {
		t.Fatal(err)
	}
	src.writes <- make([]byte, 36)
	short, err := s.Next(ctx, 1, 0)
	if err != nil || short == nil {
		t.Fatal(err)
	}
	s.WroteAs(1, full, false)
	s.WroteAs(1, short, false)
	// The caller is told what the chunk is: its mark and its size. Size is
	// the load-independent half of the answer, which is why it is asked for
	// beside the mark (#282).
	if len(calls) != 2 || calls[0] != (asked{size: 1 << 10}) || calls[1] != (asked{sparse: true, size: 36}) {
		t.Fatalf("the delay was asked with %v; want the full read's size and the short read's mark and size", calls)
	}
	// The short chunk's deadline is the one that comes first, so a supervisor
	// waking on NextDeadline catches it without polling at its interval.
	if want := now.Add(time.Millisecond); !s.NextDeadline().Equal(want) {
		t.Fatalf("NextDeadline %v, want %v (the short chunk's)", s.NextDeadline(), want)
	}
}

// A chunk whose coded attempt expired and is waiting for its retry is still
// outstanding coded work: the stall watchdog samples once per episode and
// must not miss it in that gap.
func TestCodedLossIsRememberedWhileTheRetryWaits(t *testing.T) {
	src := &writesReader{writes: make(chan []byte, 2)}
	defer close(src.writes)
	now := time.Now()
	s := New(src, Config{
		ChunkSize: 32 << 10, LaneWindow: 64, MaxOutstanding: 64, MaxOutstandingBytes: 1 << 20,
		Reliable:        func(uint64) bool { return false },
		RetransmitAfter: func(bool, int) time.Duration { return time.Millisecond },
		Now:             func() time.Time { return now },
	})
	defer s.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	src.writes <- make([]byte, 36)
	c, err := s.Next(ctx, 0, 0)
	if err != nil || c == nil {
		t.Fatal(err)
	}
	s.WroteAs(0, c, false)
	now = now.Add(time.Second)
	if s.ReissueExpired() != 1 {
		t.Fatal("the coded attempt did not expire")
	}
	if !s.UnreliableOutstanding() {
		t.Fatal("a requeued chunk whose coded attempt was lost is not outstanding coded work")
	}
}

// noDeadlineSource is an application socket with bytes already buffered that
// cannot take read deadlines, so the scheduler's staged fill is unsupported.
type noDeadlineSource struct {
	mu      sync.Mutex
	pending []byte
	more    chan struct{}
}

func (s *noDeadlineSource) write(p []byte) {
	s.mu.Lock()
	s.pending = append(s.pending, p...)
	s.mu.Unlock()
	s.more <- struct{}{}
}

func (s *noDeadlineSource) Read(p []byte) (int, error) {
	for {
		s.mu.Lock()
		if len(s.pending) > 0 {
			n := copy(p, s.pending)
			s.pending = s.pending[n:]
			s.mu.Unlock()
			return n, nil
		}
		s.mu.Unlock()
		if _, ok := <-s.more; !ok {
			return 0, io.EOF
		}
	}
}

func (s *noDeadlineSource) ReadAvailable([]byte) (int, error) { return 0, ErrUnsupported }

// On the staged path a read that filled the staging buffer says more may be
// waiting. When the source cannot do the fill that would find out, the chunk
// is short of its reservation only because of the reservation, and it is not
// a sparse write. A small write that leaves the staging buffer part-empty
// still is.
func TestAnUnsupportedStagedFillIsNotSparse(t *testing.T) {
	src := &noDeadlineSource{more: make(chan struct{}, 8)}
	defer close(src.more)
	s := New(src, Config{
		ChunkSize: 32 << 10, LaneWindow: 64, MaxOutstanding: 64, MaxOutstandingBytes: 1 << 20,
		Memory: memlimit.New(1 << 20), StagingBytes: 1024,
		SparseBytes: 2800, Sparse: func(int) bool { return true },
	})
	defer s.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	src.write(make([]byte, 64<<10)) // a backlogged transfer
	first, err := s.Next(ctx, 1, 0)
	if err != nil || first == nil {
		t.Fatal(err)
	}
	if len(first.Data) != 1024 {
		t.Fatalf("staged chunk of %d bytes, want the 1 KiB staging read", len(first.Data))
	}
	if first.Sparse {
		t.Fatal("a full staging read whose fill was unsupported was marked sparse")
	}
	// Drain the transfer, then a keystroke.
	for got := len(first.Data); got < 64<<10; {
		c, err := s.Next(ctx, 1, 0)
		if err != nil || c == nil {
			t.Fatal(err)
		}
		got += len(c.Data)
		s.Complete(1, c)
	}
	s.Complete(1, first)
	src.write(make([]byte, 36))
	key, err := s.Next(ctx, 1, 0)
	if err != nil || key == nil {
		t.Fatal(err)
	}
	if !key.Sparse {
		t.Fatal("a 36-byte write on the staged path was not marked sparse")
	}
}

// ReadDone reports each completed read once, full only if it filled what it
// was offered: a staged read whose fill the source cannot do stands as full,
// and a short read is not.
func TestReadDoneReportsTheCompletedRead(t *testing.T) {
	src := &noDeadlineSource{more: make(chan struct{}, 8)}
	defer close(src.more)
	var done []bool
	var mu sync.Mutex
	s := New(src, Config{
		ChunkSize: 32 << 10, LaneWindow: 64, MaxOutstanding: 64, MaxOutstandingBytes: 1 << 20,
		Memory: memlimit.New(1 << 20), StagingBytes: 1024,
		ReadDone: func(full bool) { mu.Lock(); done = append(done, full); mu.Unlock() },
	})
	defer s.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	src.write(make([]byte, 1024)) // fills staging; the fill is unsupported
	if c, err := s.Next(ctx, 1, 0); err != nil || c == nil {
		t.Fatal(err)
	}
	src.write(make([]byte, 36)) // short
	if c, err := s.Next(ctx, 1, 0); err != nil || c == nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(done) != 2 || !done[0] || done[1] {
		t.Fatalf("ReadDone reported %v, want [true false]", done)
	}
}

// An unextended staged read is reported complete before the scheduler waits
// for shared memory to hold it: the wait says nothing about the source.
func TestReadDoneComesBeforeTheMemoryWait(t *testing.T) {
	src := &writesReader{writes: make(chan []byte, 2)}
	defer close(src.writes)
	memory := memlimit.New(2048)
	if !memory.TryAcquire(1536) { // leave 512 bytes: a 1 KiB read must wait
		t.Fatal("setup")
	}
	done := make(chan bool, 1)
	s := New(src, Config{
		ChunkSize: 32 << 10, LaneWindow: 64, MaxOutstanding: 64, MaxOutstandingBytes: 1 << 20,
		Memory: memory, StagingBytes: 1024,
		ReadDone: func(full bool) { done <- full },
	})
	defer s.Close()
	src.writes <- make([]byte, 1024)
	select {
	case full := <-done:
		if !full {
			t.Fatal("a staging read that filled its buffer was reported short")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the read was not reported while the scheduler waited for memory")
	}
	memory.Release(1536)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if c, err := s.Next(ctx, 1, 0); err != nil || c == nil || len(c.Data) != 1024 {
		t.Fatalf("chunk after the wait: %v %v", c, err)
	}
}
