package stripe

import (
	"bytes"
	"context"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bojieli/queqiao/internal/memlimit"
)

// availableSource is an application socket: Read blocks until the
// application writes, and ReadAvailable returns only what it already wrote.
type availableSource struct {
	mu     sync.Mutex
	cond   sync.Cond
	buf    []byte
	eof    bool
	reads  int
	onRead func()
}

func newAvailableSource() *availableSource {
	s := &availableSource{}
	s.cond.L = &s.mu
	return s
}

func (s *availableSource) write(p []byte) {
	s.mu.Lock()
	s.buf = append(s.buf, p...)
	s.mu.Unlock()
	s.cond.Broadcast()
}

func (s *availableSource) close() {
	s.mu.Lock()
	s.eof = true
	s.mu.Unlock()
	s.cond.Broadcast()
}

func (s *availableSource) Read(p []byte) (int, error) {
	if s.onRead != nil {
		s.onRead()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reads++
	for len(s.buf) == 0 && !s.eof {
		s.cond.Wait()
	}
	return s.takeLocked(p)
}

func (s *availableSource) ReadAvailable(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.takeLocked(p)
}

func (s *availableSource) takeLocked(p []byte) (int, error) {
	n := copy(p, s.buf)
	s.buf = s.buf[n:]
	if len(s.buf) == 0 && s.eof {
		return n, io.EOF
	}
	return n, nil
}

func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// drainAll takes and completes chunks until the stream ends, returning the
// bytes in order and whether the final marker arrived.
func drainAll(t *testing.T, s *Scheduler) ([]byte, bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var out []byte
	final := false
	for {
		chunk, err := s.Next(ctx, 1, 0)
		if err != nil {
			t.Fatalf("active flow: %v", err)
		}
		if chunk == nil {
			return out, final
		}
		if chunk.Offset != uint64(len(out)) {
			t.Fatalf("chunk at %d, want %d", chunk.Offset, len(out))
		}
		out = append(out, chunk.Data...)
		final = final || chunk.Final
		s.Complete(1, chunk)
	}
}

// Issue #169 at the scheduler: with iOS's 3 MiB budget and 16 KiB chunks, 192
// idle readers used to hold the whole budget while blocked in Read, so a
// request behind them was never read -- and neither was an EOF.
func TestIdleProducersHoldNoSharedMemory(t *testing.T) {
	for _, fair := range []bool{false, true} {
		name := "broadcast"
		if fair {
			name = "fair"
		}
		t.Run(name, func(t *testing.T) {
			const (
				chunkSize = 16 * 1024
				budget    = 3 * 1024 * 1024
				idle      = budget/chunkSize + 8
			)
			memory := memlimit.New(budget)
			if fair {
				memory = memlimit.NewFair(budget)
			}
			cfg := Config{ChunkSize: chunkSize, MaxOutstanding: 64, MaxOutstandingBytes: 1024 * 1024, Memory: memory}
			var idleSources []*availableSource
			for range idle {
				src := newAvailableSource()
				idleSources = append(idleSources, src)
				s := New(src, cfg)
				defer s.Close()
			}
			waitUntil(t, "idle producers to block on their applications", func() bool {
				for _, src := range idleSources {
					src.mu.Lock()
					reads := src.reads
					src.mu.Unlock()
					if reads == 0 {
						return false
					}
				}
				return true
			})
			if used := memory.Snapshot().Used; used != 0 {
				t.Fatalf("%d idle producers hold %d shared bytes", idle, used)
			}

			// An active request, written in one go and then half-closed.
			request := bytes.Repeat([]byte("request-"), 6*1024)
			src := newAvailableSource()
			src.write(request)
			src.close()
			active := New(src, cfg)
			defer active.Close()
			got, final := drainAll(t, active)
			if !bytes.Equal(got, request) || !final {
				t.Fatalf("active request: %d/%d bytes, final=%v", len(got), len(request), final)
			}

			// EOF on a socket that never sent anything.
			late := newAvailableSource()
			lateSched := New(late, cfg)
			defer lateSched.Close()
			late.close()
			if got, final := drainAll(t, lateSched); len(got) != 0 || !final {
				t.Fatalf("EOF behind idle producers: %d bytes, final=%v", len(got), final)
			}
			if snap := memory.Snapshot(); snap.Used != 0 || snap.Peak > snap.Capacity {
				t.Fatalf("shared memory after completion = %+v", snap)
			}
		})
	}
}

// A reserved chunk is filled from what the application already wrote, so
// staging costs no throughput: a bulk source still yields whole chunks, each
// reserved at its allocation.
func TestStagedChunksFillFromAvailableBytes(t *testing.T) {
	const chunkSize = 16 * 1024
	memory := memlimit.New(1024 * 1024)
	src := newAvailableSource()
	payload := bytes.Repeat([]byte{7}, 5*chunkSize+100)
	src.write(payload)
	src.close()
	s := New(src, Config{ChunkSize: chunkSize, MaxOutstanding: 64, MaxOutstandingBytes: 1024 * 1024, Memory: memory})
	defer s.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var got []byte
	for {
		chunk, err := s.Next(ctx, 1, 0)
		if err != nil {
			t.Fatal(err)
		}
		if chunk == nil {
			break
		}
		if len(got)+chunkSize <= len(payload) && len(chunk.Data) != chunkSize {
			t.Fatalf("chunk at %d is %d bytes; staging should not shrink bulk chunks", chunk.Offset, len(chunk.Data))
		}
		if used := memory.Snapshot().Used; used < int64(len(chunk.Data)) {
			t.Fatalf("chunk of %d bytes reserved only %d", len(chunk.Data), used)
		}
		got = append(got, chunk.Data...)
		s.Complete(1, chunk)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("got %d bytes, want %d", len(got), len(payload))
	}
	if used := memory.Snapshot().Used; used != 0 {
		t.Fatalf("%d bytes still reserved", used)
	}
}

// blockingReader returns its data in one Read and then blocks forever, like
// a socket with no read deadlines whose application went quiet.
type blockingReader struct {
	data    []byte
	blocked chan struct{}
	once    sync.Once
	check   func()
}

func (r *blockingReader) Read(p []byte) (int, error) {
	if r.check != nil {
		r.check()
	}
	if len(r.data) > 0 {
		n := copy(p, r.data)
		r.data = r.data[n:]
		return n, nil
	}
	r.once.Do(func() { close(r.blocked) })
	select {}
}

// A source without AvailableReader publishes what one ordinary Read returned
// and never issues a second Read while holding a reservation.
func TestSourceWithoutAvailableReadNeverBlocksHoldingAReservation(t *testing.T) {
	const chunkSize = 16 * 1024
	memory := memlimit.New(1024 * 1024)
	var current atomic.Pointer[Scheduler]
	src := &blockingReader{data: bytes.Repeat([]byte{1}, 5000), blocked: make(chan struct{})}
	src.check = func() {
		s := current.Load()
		if s == nil {
			return
		}
		// Every reserved byte belongs to a published chunk when Read is
		// entered: the producer holds nothing of its own.
		s.mu.Lock()
		var published int64
		for _, c := range s.pending {
			published += int64(c.reservation)
		}
		for _, out := range s.live {
			published += int64(out.chunk.reservation)
		}
		s.mu.Unlock()
		if used := memory.Snapshot().Used; used != published {
			t.Errorf("Read entered holding %d unpublished reserved bytes", used-published)
		}
	}
	s := New(src, Config{ChunkSize: chunkSize, MaxOutstanding: 64, MaxOutstandingBytes: 1024 * 1024, Memory: memory})
	current.Store(s)
	defer s.Close()
	<-src.blocked
	s.mu.Lock()
	var total int
	for _, c := range s.pending {
		if len(c.Data) > DefaultStagingBytes || c.reservation != len(c.Data) {
			t.Errorf("short chunk of %d bytes reserved %d", len(c.Data), c.reservation)
		}
		total += len(c.Data)
	}
	s.mu.Unlock()
	if total != 5000 {
		t.Fatalf("published %d of 5000 bytes", total)
	}
}

// Replay safety (design §8.4): a staged chunk's reservation survives the death
// of the lane carrying it, and the same bytes are offered to another lane. It
// is released only when the chunk is acknowledged.
func TestStagedReservationIsReleasedOnlyByAcknowledgement(t *testing.T) {
	for _, owned := range []bool{false, true} {
		cfg := Config{ChunkSize: 16 * 1024, MaxOutstanding: 64, MaxOutstandingBytes: 1024 * 1024}
		cfg.Memory = memlimit.New(1024 * 1024)
		if owned {
			cfg.Memory = memlimit.NewFair(1024 * 1024)
			cfg.Autotune = &Autotune{}
		}
		src := newAvailableSource()
		src.write(bytes.Repeat([]byte{3}, 4000))
		s := New(src, cfg)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		first, err := s.Next(ctx, 1, 0)
		if err != nil {
			t.Fatal(err)
		}
		reserved := cfg.Memory.Snapshot().Used
		if reserved == 0 {
			t.Fatal("dispatched chunk holds no reservation")
		}
		s.RetireLane(1)
		if used := cfg.Memory.Snapshot().Used; used != reserved {
			t.Fatalf("lane death changed the reservation from %d to %d", reserved, used)
		}
		replay, err := s.Next(ctx, 2, 0)
		if err != nil {
			t.Fatal(err)
		}
		if replay.Offset != first.Offset || !bytes.Equal(replay.Data, first.Data) {
			t.Fatalf("replay carries [%d,+%d), want [%d,+%d)", replay.Offset, len(replay.Data), first.Offset, len(first.Data))
		}
		s.Wrote(2, replay)
		if used := cfg.Memory.Snapshot().Used; used != reserved {
			t.Fatalf("a transport write released memory: %d -> %d", reserved, used)
		}
		s.Complete(2, replay)
		if used := cfg.Memory.Snapshot().Used; used != 0 {
			t.Fatalf("acknowledged chunk still holds %d bytes", used)
		}
		cancel()
		s.Close()
	}
}

// fakeClock is a settable clock for the autotune's credit rounds.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

// infiniteSource never runs dry.
type infiniteSource struct{}

func (infiniteSource) Read(p []byte) (int, error) { return len(p), nil }

// laneWorker takes every chunk offered and holds it, unacknowledged, until
// the test completes it. It is idle in Next whenever nothing is ready.
type laneWorker struct {
	mu    sync.Mutex
	taken []*Chunk
	done  chan struct{}
}

func startLane(ctx context.Context, s *Scheduler) *laneWorker {
	w := &laneWorker{done: make(chan struct{})}
	go func() {
		defer close(w.done)
		for {
			chunk, err := s.Next(ctx, 1, 0)
			if err != nil || chunk == nil {
				return
			}
			s.Wrote(1, chunk)
			w.mu.Lock()
			w.taken = append(w.taken, chunk)
			w.mu.Unlock()
		}
	}()
	return w
}

func (w *laneWorker) completeAll(s *Scheduler) int {
	w.mu.Lock()
	taken := w.taken
	w.taken = nil
	w.mu.Unlock()
	for _, c := range taken {
		s.Complete(1, c)
	}
	return len(taken)
}

const (
	tuneChunk = 16 * 1024
	tuneMax   = 1024 * 1024
	tuneRound = 50 * time.Millisecond
)

func newTunedScheduler(t *testing.T, clock *fakeClock) *Scheduler {
	t.Helper()
	s := New(infiniteSource{}, Config{
		ChunkSize: tuneChunk, LaneWindow: 1024, MaxOutstanding: 1024, MaxOutstandingBytes: tuneMax,
		Now: clock.Now, Autotune: &Autotune{Round: func() time.Duration { return tuneRound }},
	})
	t.Cleanup(s.Close)
	return s
}

// waitBlockedOnBound waits until the producer has filled the bound and the
// lane has taken all of it.
func waitBlockedOnBound(t *testing.T, s *Scheduler) {
	t.Helper()
	waitUntil(t, "the producer to fill the bound", func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return len(s.pending) == 0 && s.tune.waiting && s.idleLanes > 0 && s.retainedBytes() >= uint64(s.tune.bound)
	})
}

// The retention bound never grows while flow ACKs are withheld, however long
// the producer waits on it with a lane idle beside it.
func TestRetentionBoundDoesNotGrowWithoutCredit(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1000, 0)}
	s := newTunedScheduler(t, clock)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startLane(ctx, s)
	startup := DefaultAutotuneStartupChunks * tuneChunk
	for range 20 {
		waitBlockedOnBound(t, s)
		clock.advance(tuneRound)
		if bound := s.RetentionBound(); bound != startup {
			t.Fatalf("bound moved to %d without credit", bound)
		}
		if _, retained := s.Pending(); retained > uint64(startup) {
			t.Fatalf("retained %d over the bound %d", retained, startup)
		}
	}
}

// Credit that arrives while the bound is what holds the flow back doubles it,
// once per credit round, up to the per-flow maximum.
func TestRetentionBoundDoublesWhileCreditArrivesAndItBinds(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1000, 0)}
	s := newTunedScheduler(t, clock)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	lane := startLane(ctx, s)
	startup := DefaultAutotuneStartupChunks * tuneChunk
	var seen []int
	for range 12 {
		waitBlockedOnBound(t, s)
		bound := s.RetentionBound()
		seen = append(seen, bound)
		if _, retained := s.Pending(); retained > uint64(bound) {
			t.Fatalf("retained %d over the bound %d", retained, bound)
		}
		clock.advance(tuneRound)
		lane.completeAll(s)
	}
	// Round 0 has no credit; every round after it doubles.
	for i, bound := range seen {
		want := startup
		if i > 0 {
			want = min(startup<<(i-1), tuneMax)
		}
		if bound != want {
			t.Fatalf("round %d bound %d, want %d (all: %v)", i, bound, want, seen)
		}
	}
}

// Once the flow stops using what it was granted, the bound falls back toward
// twice what it uses -- and to the startup allowance when it uses nothing --
// rather than keeping the peak forever.
func TestRetentionBoundDecaysTowardUse(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1000, 0)}
	s := newTunedScheduler(t, clock)
	ctx, cancel := context.WithCancel(context.Background())
	lane := startLane(ctx, s)
	for range 8 {
		waitBlockedOnBound(t, s)
		clock.advance(tuneRound)
		lane.completeAll(s)
	}
	peak := s.RetentionBound()
	if peak != tuneMax {
		t.Fatalf("bound reached only %d", peak)
	}
	// Stop the lane: nothing more is dispatched, so U falls to zero once the
	// last credit arrives.
	cancel()
	<-lane.done
	lane.completeAll(s)
	previous := peak
	for range 40 {
		clock.advance(tuneRound)
		bound := s.RetentionBound()
		if bound > previous {
			t.Fatalf("bound rose from %d to %d without credit", previous, bound)
		}
		previous = bound
	}
	if startup := DefaultAutotuneStartupChunks * tuneChunk; previous != startup {
		t.Fatalf("idle bound settled at %d, want the startup allowance %d", previous, startup)
	}
}

// READY bytes are limited by the write-ahead bound, separately from the
// retention bound: a flow with no lane reads at most that much.
func TestReadyBytesAreLimitedByTheWriteAheadBound(t *testing.T) {
	const limit = 3 * tuneChunk
	s := New(infiniteSource{}, Config{
		ChunkSize: tuneChunk, LaneWindow: 1024, MaxOutstanding: 1024, MaxOutstandingBytes: tuneMax,
		Autotune: &Autotune{Startup: tuneMax, ReadyLimit: func() int { return limit }},
	})
	defer s.Close()
	waitUntil(t, "READY bytes", func() bool { return s.ReadyBytes() >= limit })
	time.Sleep(20 * time.Millisecond)
	if ready := s.ReadyBytes(); ready != limit {
		t.Fatalf("READY bytes %d, want %d", ready, limit)
	}
}
