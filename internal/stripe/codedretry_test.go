package stripe

import (
	"bytes"
	"context"
	"math/rand"
	"testing"
	"time"
)

// codedRetryFixture is a one-lane scheduler whose lane carries data on coded
// datagrams, on a clock the test moves.
type codedRetryFixture struct {
	t     *testing.T
	sched *Scheduler
	now   time.Time
	ctx   context.Context
}

const codedRetryAfter = 300 * time.Millisecond

func newCodedRetryFixture(t *testing.T, sticky bool, payload []byte, chunk int) *codedRetryFixture {
	t.Helper()
	f := &codedRetryFixture{t: t, now: time.Unix(1000, 0)}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	f.ctx = ctx
	f.sched = New(bytes.NewReader(payload), Config{
		ChunkSize:       chunk,
		Reliable:        func(uint64) bool { return false },
		RetransmitAfter: func() time.Duration { return codedRetryAfter },
		CodedRetry:      sticky,
		Now:             func() time.Time { return f.now },
	})
	t.Cleanup(f.sched.Close)
	return f
}

func (f *codedRetryFixture) next(lane uint64) *Chunk {
	f.t.Helper()
	chunk, err := f.sched.Next(f.ctx, lane, 0)
	if err != nil || chunk == nil {
		f.t.Fatalf("Next = %v, %v", chunk, err)
	}
	return chunk
}

// A coded attempt's deadline starts when its datagrams are registered in a
// packet, not when the scheduler hands it out: a chunk delayed in the queues
// below is not retried early, however long it waits there (design §8.1, §15
// test 15).
func TestCodedDeadlineStartsAtRegistration(t *testing.T) {
	f := newCodedRetryFixture(t, true, []byte("response"), 64)
	first := f.next(0)
	if first.Reliable {
		t.Fatal("first attempt must go to the code")
	}
	f.sched.WroteAs(0, first, false)
	// Ten deadlines pass while the datagram waits in the connection's queue.
	f.now = f.now.Add(10 * codedRetryAfter)
	if n := f.sched.ReissueExpired(); n != 0 {
		t.Fatalf("an unregistered coded attempt was retried after %d queue waits", n)
	}
	f.sched.Registered(0, first)
	f.now = f.now.Add(codedRetryAfter - time.Millisecond)
	if n := f.sched.ReissueExpired(); n != 0 {
		t.Fatal("retried before the deadline measured from registration")
	}
	f.now = f.now.Add(time.Millisecond)
	if n := f.sched.ReissueExpired(); n != 1 {
		t.Fatalf("ReissueExpired = %d at the registration deadline, want 1", n)
	}
	retry := f.next(0)
	if !retry.Reliable {
		t.Fatal("a coded chunk past its deadline was retried on the code; want the stream")
	}
}

// The deadline counts from the packet, not from when the report about it
// reached the scheduler.
func TestCodedDeadlineCountsFromThePacketNotTheReport(t *testing.T) {
	f := newCodedRetryFixture(t, true, []byte("response"), 64)
	first := f.next(0)
	f.sched.WroteAs(0, first, false)
	f.sched.RegisteredAgo(0, first, codedRetryAfter/2)
	f.now = f.now.Add(codedRetryAfter / 2)
	if n := f.sched.ReissueExpired(); n != 1 {
		t.Fatalf("ReissueExpired = %d a deadline after the packet, want 1", n)
	}
}

// Gap evidence the receiver reported before a coded attempt left cannot
// show that attempt lost; only evidence that advances afterwards can.
func TestStaleGapEvidenceDoesNotRetryAFreshCodedAttempt(t *testing.T) {
	f := newCodedRetryFixture(t, true, bytes.Repeat([]byte("x"), 64), 16)
	a := f.next(0)
	f.sched.WroteAs(0, a, false)
	if n := f.sched.ReissueUnacknowledgedBelow(48); n != 0 {
		t.Fatal("gap evidence retried an unregistered attempt")
	}
	f.sched.Registered(0, a)
	if n := f.sched.ReissueUnacknowledgedBelow(48); n != 0 {
		t.Fatal("gap evidence older than the attempt retried it")
	}
	if n := f.sched.ReissueUnacknowledgedBelow(64); n != 1 {
		t.Fatalf("fresh gap evidence retried %d attempts, want 1", n)
	}
}

// A stream safety write that completes after the code's registration gets
// the stream's own deadline, from the write, not the code's.
func TestStreamWriteAfterRegistrationGetsTheStreamsDeadline(t *testing.T) {
	f := newCodedRetryFixture(t, true, []byte("response"), 64)
	a := f.next(0)
	f.sched.Registered(0, a)
	f.now = f.now.Add(2 * codedRetryAfter)
	f.sched.WroteAs(0, a, true)
	if n := f.sched.ReissueExpired(); n != 0 {
		t.Fatal("a stream write inherited the code's expired deadline")
	}
}

// Registration can be reported before the lane writer reports the write: the
// connection may build the packet while the writer is still returning.
func TestCodedRegistrationBeforeTheWriteReport(t *testing.T) {
	f := newCodedRetryFixture(t, true, []byte("response"), 64)
	first := f.next(0)
	f.sched.Registered(0, first)
	f.sched.WroteAs(0, first, false)
	f.now = f.now.Add(codedRetryAfter)
	if n := f.sched.ReissueExpired(); n != 1 {
		t.Fatalf("ReissueExpired = %d, want 1: the write report erased the registration", n)
	}
}

// Without the switch the deadline counts from dispatch and a retry goes back
// to the code, exactly as on main.
func TestCodedRetryOffKeepsTheLegacyLifecycle(t *testing.T) {
	f := newCodedRetryFixture(t, false, []byte("response"), 64)
	first := f.next(0)
	f.sched.WroteAs(0, first, false)
	// Reports the legacy scheduler is never given are ignored if they come.
	f.sched.Registered(0, first)
	f.sched.Abandoned(0, first)
	f.now = f.now.Add(codedRetryAfter)
	if n := f.sched.ReissueExpired(); n != 1 {
		t.Fatalf("legacy ReissueExpired = %d, want 1 at the dispatch deadline", n)
	}
	retry := f.next(0)
	if retry.Reliable {
		t.Fatal("legacy retry was pinned to the stream without the frontier")
	}
}

// A discarded datagram retires its attempt at once, and the retry is on the
// stream. One reported before the write is settled by the write report: if
// the write also went to the stream (the pre-OPEN_OK safety copy) the stream
// owns the bytes and nothing is retried.
func TestAbandonedCodedAttemptIsRetriedOnTheStream(t *testing.T) {
	f := newCodedRetryFixture(t, true, []byte("response"), 64)
	first := f.next(0)
	f.sched.WroteAs(0, first, false)
	f.sched.Abandoned(0, first)
	retry := f.next(0)
	if !retry.Reliable || retry.Offset != first.Offset {
		t.Fatalf("abandoned chunk retried as %+v; want the same bytes on the stream", retry)
	}
	if got := f.sched.Stats().CodedAbandoned; got != 1 {
		t.Fatalf("CodedAbandoned = %d, want 1", got)
	}
	// A late report for the retired attempt changes nothing.
	f.sched.Abandoned(0, first)
	f.sched.Registered(0, first)
	if ready, _ := f.sched.Pending(); ready != 0 {
		t.Fatal("a late report for a retired attempt requeued the chunk again")
	}

	early := newCodedRetryFixture(t, true, []byte("response"), 64)
	a := early.next(0)
	early.sched.Abandoned(0, a)
	if ready, _ := early.sched.Pending(); ready != 0 {
		t.Fatal("abandonment before the write report retried bytes whose substrate is not known yet")
	}
	early.sched.WroteAs(0, a, false)
	if !early.next(0).Reliable {
		t.Fatal("abandoned, unreported write was not retried on the stream")
	}

	safety := newCodedRetryFixture(t, true, []byte("response"), 64)
	b := safety.next(0)
	safety.sched.Abandoned(0, b)
	safety.sched.WroteAs(0, b, true)
	if ready, _ := safety.sched.Pending(); ready != 0 {
		t.Fatal("bytes the stream also carried were copied again")
	}
}

// Receiver gap evidence proves a sent chunk missing, but says nothing about
// one still waiting to leave: retrying that would give it two owners.
func TestGapEvidenceRetiresOnlyRegisteredCodedAttempts(t *testing.T) {
	f := newCodedRetryFixture(t, true, bytes.Repeat([]byte("x"), 64), 16)
	a := f.next(0)
	f.sched.WroteAs(0, a, false)
	if n := f.sched.ReissueUnacknowledgedBelow(48); n != 0 {
		t.Fatal("gap evidence retried a coded attempt that was never registered")
	}
	f.sched.Registered(0, a)
	if n := f.sched.ReissueUnacknowledgedBelow(64); n != 1 {
		t.Fatalf("gap evidence retried %d registered coded attempts, want 1", n)
	}
	if !f.next(0).Reliable {
		t.Fatal("a chunk proven missing was retried on the code")
	}
}

// I6 (design §2): every byte has one repair owner at a time. Under random
// interleavings of every event the scheduler hears, no chunk ever holds an
// active coded attempt beside any other attempt, and every attempt after a
// chunk's first is on the stream.
func TestEveryChunkHasOneRepairOwner(t *testing.T) {
	for seed := int64(1); seed <= 200; seed++ {
		rng := rand.New(rand.NewSource(seed))
		f := newCodedRetryFixture(t, true, bytes.Repeat([]byte("y"), 8*16), 16)
		type issued struct {
			lane  uint64
			chunk *Chunk
		}
		var live []issued
		seen := map[uint64]bool{}
		for step := 0; step < 200; step++ {
			switch op := rng.Intn(9); {
			case op <= 1:
				lane := uint64(rng.Intn(2))
				ctx, cancel := context.WithTimeout(f.ctx, time.Millisecond)
				chunk, err := f.sched.Next(ctx, lane, 0)
				cancel()
				if err != nil || chunk == nil {
					continue
				}
				if seen[chunk.Offset] && !chunk.Reliable {
					t.Fatalf("seed %d: chunk %d retried on the code", seed, chunk.Offset)
				}
				seen[chunk.Offset] = true
				live = append(live, issued{lane, chunk})
			case len(live) == 0:
				continue
			default:
				pick := live[rng.Intn(len(live))]
				switch op {
				case 2:
					f.sched.WroteAs(pick.lane, pick.chunk, pick.chunk.Reliable || rng.Intn(4) == 0)
				case 3:
					f.sched.Registered(pick.lane, pick.chunk)
				case 4:
					f.sched.Abandoned(pick.lane, pick.chunk)
				case 5:
					f.now = f.now.Add(time.Duration(rng.Intn(400)) * time.Millisecond)
					f.sched.ReissueExpired()
				case 6:
					f.sched.ReissueUnacknowledgedBelow(uint64(rng.Intn(9) * 16))
				case 7:
					f.sched.Fail(pick.lane, pick.chunk)
				case 8:
					f.sched.Complete(pick.lane, pick.chunk)
				}
			}
			f.sched.mu.Lock()
			for offset, out := range f.sched.live {
				coded := 0
				for _, a := range out.attempts {
					if !a.reliable {
						coded++
					}
				}
				if coded > 0 && len(out.attempts) > 1 {
					f.sched.mu.Unlock()
					t.Fatalf("seed %d step %d: chunk %d has a coded attempt and %d other owners", seed, step, offset, len(out.attempts)-1)
				}
				if coded > 0 && f.sched.pendingHas(offset) {
					f.sched.mu.Unlock()
					t.Fatalf("seed %d step %d: chunk %d is queued for a retry while the code still owns it", seed, step, offset)
				}
			}
			f.sched.mu.Unlock()
		}
	}
}
