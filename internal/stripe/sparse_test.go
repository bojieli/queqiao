package stripe

import (
	"context"
	"io"
	"testing"
	"time"
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
	src.writes <- make([]byte, 16<<10)
	for s.ReadyBytes() == 0 {
		time.Sleep(time.Millisecond)
	}
	c, err := s.Next(ctx, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	s.Fail(1, c) // requeued for another attempt
	src.writes <- make([]byte, 36)
	for s.ReadyBytes() == 0 {
		time.Sleep(time.Millisecond)
	}
	retry, _ := s.Next(ctx, 2, 0)
	small, _ := s.Next(ctx, 2, 0)
	if retry.Offset != 0 || small.Sparse {
		t.Fatalf("retry offset %d, small write sparse=%v; want the retry first and the write not sparse", retry.Offset, small.Sparse)
	}
}
