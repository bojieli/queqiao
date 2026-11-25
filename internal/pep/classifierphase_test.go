package pep

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	quiccongestion "github.com/apernet/quic-go/congestion"

	"github.com/bojieli/queqiao/internal/classifier"
	"github.com/bojieli/queqiao/internal/coded"
	"github.com/bojieli/queqiao/internal/pathmodel"
	"github.com/bojieli/queqiao/internal/profile"
	"github.com/bojieli/queqiao/internal/protocol"
	"github.com/bojieli/queqiao/internal/stripe"
)

// phaseHarness drives one endpoint's multipathFlow through a workload on a
// test clock. "write" is the application handing this endpoint bytes to send
// (a read from the inner connection); "recv" is bytes delivered to it from the
// peer. Each write reports the class byte and coding choice the flow gives
// that write, by the same functions the send path uses: the stripe scheduler's
// sparse marking, sendClass when the frame is built, and the coding policy the
// frame connection applies.
type phaseHarness struct {
	t   *testing.T
	f   *multipathFlow
	now time.Time
}

func newPhaseHarness(t *testing.T, mode ClassifierMode) *phaseHarness {
	t.Helper()
	inner, peer := net.Pipe()
	t.Cleanup(func() { inner.Close(); peer.Close() })
	f := newMultipathFlow(context.Background(), inner, [16]byte{1}, 1, 64*1024,
		protocol.FlagAckDown, protocol.FlagAckUp, nil, nil, nil)
	f.classifier = classifier.New(phasedClassifierConfig(profile.Default().Classifier, mode))
	h := &phaseHarness{t: t, f: f, now: f.started}
	f.clock = func() time.Time { return h.now }
	return h
}

func (h *phaseHarness) advance(d time.Duration) { h.now = h.now.Add(d) }

func (h *phaseHarness) recv(n int) {
	h.f.observe(n, false)
	h.f.bytesDown.Add(uint64(n))
}

// ack has the peer acknowledge everything sent so far.
func (h *phaseHarness) ack() { h.f.ackedCum.Store(h.f.bytesUp.Load()) }

// write sends n bytes. queueEmpty says the flow's send queue held nothing when
// the write arrived, which is what the scheduler checks before marking it.
func (h *phaseHarness) write(n int, queueEmpty bool) (protocol.Class, bool) {
	f := h.f
	f.observe(n, true)
	f.bytesUp.Add(uint64(n))
	sparse := queueEmpty && n <= f.sparseBytes() && f.admitSparse(n)
	class := f.sendClass(sparse)
	var coded bool
	f.lastClassified.Store(0)
	if f.phased() {
		coded = f.frameCoding(class)
	} else {
		coded = f.prefersCodingOverRetransmission()
	}
	f.noteDataBytes(class, coded, n)
	return class, coded
}

// refresh is the periodic re-examination with no new bytes.
func (h *phaseHarness) refresh() {
	h.f.lastClassified.Store(0)
	h.f.refreshClass()
}

func (h *phaseHarness) interactiveFraction() float64 {
	var interactive, total uint64
	for class := range h.f.classBytes {
		for substrate := range h.f.classBytes[class] {
			n := h.f.classBytes[class][substrate].Load()
			total += n
			if class != 2 {
				interactive += n
			}
		}
	}
	if total == 0 {
		return 0
	}
	return float64(interactive) / float64(total)
}

func (h *phaseHarness) current() classifier.Class { return classifier.Class(h.f.class.Load()) }

// keystrokes runs a typing session on the far side of an SSH connection: a
// keystroke arrives, its echo is written. It returns how many echoes were
// sent BULK or uncoded.
func (h *phaseHarness) keystrokes(d time.Duration) (bulk, uncoded int) {
	for end := h.now.Add(d); h.now.Before(end); {
		h.recv(36)
		class, coded := h.write(36, true)
		if class == protocol.ClassBulk {
			bulk++
		}
		if !coded {
			uncoded++
		}
		h.advance(75 * time.Millisecond)
		h.ack()
		h.advance(75 * time.Millisecond)
	}
	return bulk, uncoded
}

// burst is a 300 KB `cat`: 16 KiB reads from a backlogged socket over 200 ms,
// acknowledged a round trip later.
func (h *phaseHarness) burst() {
	for sent := 0; sent < 300_000; sent += 16 << 10 {
		h.write(16<<10, false)
		h.advance(10 * time.Millisecond)
	}
	h.advance(200 * time.Millisecond)
	h.ack()
}

// W6: keystrokes, a 300 KB burst, keystrokes. The burst makes the session
// BULK at both ends. The echoes after it are INTERACTIVE and coded again: at
// once by the sparse-write rule, and the flow's current class within the exit
// window. The sticky class is unchanged, so the flow keeps its lane.
func TestKeystrokesAfterABurstAreInteractiveAgain(t *testing.T) {
	for _, mode := range []ClassifierMode{ClassifierLegacy, ClassifierPhased} {
		t.Run(string(mode), func(t *testing.T) {
			h := newPhaseHarness(t, mode)
			if bulk, uncoded := h.keystrokes(10 * time.Second); bulk+uncoded != 0 {
				t.Fatalf("phase A: %d echoes bulk, %d uncoded before any burst", bulk, uncoded)
			}
			h.burst()
			if !h.f.everBulk.Load() {
				t.Fatal("a 300 KB burst on a 10 s session did not make it BULK; the test is not testing the latch")
			}
			bulk, uncoded := h.keystrokes(4 * time.Second)
			exitedWithin := h.current() != classifier.ClassBulk
			laterBulk, laterUncoded := h.keystrokes(10 * time.Second)
			if mode == ClassifierLegacy {
				// The defect this bundle exists for, reproduced: main's behaviour.
				if laterBulk == 0 || laterUncoded == 0 || h.current() != classifier.ClassBulk {
					t.Fatalf("legacy no longer latches (bulk %d uncoded %d class %v); update this test", laterBulk, laterUncoded, h.current())
				}
				return
			}
			if bulk+uncoded+laterBulk+laterUncoded != 0 {
				t.Fatalf("phase C echoes: %d bulk and %d uncoded in the first 4 s, %d and %d after", bulk, uncoded, laterBulk, laterUncoded)
			}
			if !exitedWithin {
				t.Fatal("the flow's current class was still BULK 4 s after the burst")
			}
			if !h.f.everBulk.Load() || !h.f.carriesBulk() {
				t.Fatal("the sticky class changed; lane choice would re-isolate the flow")
			}
		})
	}
}

// W5-loop at the gateway: five turns on one connection, each a 100 KB
// prompt (growing 8 KB a turn), 500 ms of think time, 400 deltas at 25 ms,
// then 2 s of tool execution. At least 99% of delta bytes are INTERACTIVE,
// counted by the per-flow class telemetry the harness reads, and every delta
// is coded.
func TestLLMDeltasOnAReusedConnectionAreInteractive(t *testing.T) {
	for _, mode := range []ClassifierMode{ClassifierLegacy, ClassifierPhased} {
		t.Run(string(mode), func(t *testing.T) {
			h := newPhaseHarness(t, mode)
			uncoded := 0
			for turn := 0; turn < 5; turn++ {
				for got, want := 0, 100_000+turn*8_000; got < want; got += 16 << 10 {
					h.recv(min(16<<10, want-got))
					h.advance(6 * time.Millisecond)
				}
				h.advance(500 * time.Millisecond)
				for i := 0; i < 400; i++ {
					if _, coded := h.write(180, true); !coded {
						uncoded++
					}
					h.advance(25 * time.Millisecond)
					h.ack()
				}
				h.advance(2 * time.Second)
			}
			fraction := h.interactiveFraction()
			if mode == ClassifierLegacy {
				if fraction >= 0.99 {
					t.Fatalf("legacy sent %.3f of deltas interactive; the latch no longer reproduces", fraction)
				}
				return
			}
			if fraction < 0.99 {
				t.Fatalf("%.3f of delta bytes interactive, want at least 0.99", fraction)
			}
			if uncoded != 0 {
				t.Fatalf("%d deltas uncoded", uncoded)
			}
		})
	}
}

// W9 after media: a messaging connection receives a 2 MB photo, then a small
// push every 30 s with the client's 60-byte acknowledgement. Every push is
// INTERACTIVE and coded.
func TestPushesAfterAPhotoAreInteractive(t *testing.T) {
	for _, mode := range []ClassifierMode{ClassifierLegacy, ClassifierPhased} {
		t.Run(string(mode), func(t *testing.T) {
			h := newPhaseHarness(t, mode)
			h.recv(400)
			h.write(2_000, true) // login
			h.advance(3 * time.Second)
			for sent := 0; sent < 2_000_000; sent += 16 << 10 {
				h.write(16<<10, false)
				h.advance(8 * time.Millisecond)
				if sent%(256<<10) == 0 {
					h.ack()
				}
			}
			h.advance(300 * time.Millisecond)
			h.ack()
			if !h.f.everBulk.Load() {
				t.Fatal("the photo did not make the connection BULK; the test is not testing the latch")
			}
			bad := 0
			for i := 0; i < 10; i++ {
				h.advance(30 * time.Second)
				class, coded := h.write(300, true)
				if class == protocol.ClassBulk || !coded {
					bad++
				}
				h.advance(200 * time.Millisecond)
				h.recv(60)
				h.ack()
			}
			if mode == ClassifierLegacy {
				if bad != 10 {
					t.Fatalf("legacy sent %d of 10 pushes as bulk or uncoded; the latch no longer reproduces", bad)
				}
				return
			}
			if bad != 0 {
				t.Fatalf("%d of 10 pushes after the photo were bulk or uncoded", bad)
			}
		})
	}
}

// W1: a backlogged download stays BULK and uncoded from the moment it is
// classified to the end -- through a source stall and a slow tail -- and every
// write gets exactly the class and coding the legacy bundle gives it, so the
// phased bundle cannot cost a bulk flow throughput.
func TestABackloggedDownloadStaysBulkThroughout(t *testing.T) {
	type decision struct {
		class protocol.Class
		coded bool
	}
	run := func(mode ClassifierMode) ([]decision, *phaseHarness) {
		h := newPhaseHarness(t, mode)
		var out []decision
		write := func() {
			class, coded := h.write(16<<10, false)
			out = append(out, decision{class, coded})
			// About 1 MiB in flight and ready behind the reads.
			if sent := h.f.bytesUp.Load(); sent > 1<<20 {
				h.f.ackedCum.Store(sent - 1<<20)
			}
		}
		h.recv(300)                 // the request
		for i := 0; i < 1000; i++ { // ~16 MB at 2 MB/s
			write()
			h.advance(8 * time.Millisecond)
		}
		// The source stalls for 1.5 s; the peer catches up meanwhile.
		for i := 0; i < 6; i++ {
			h.advance(250 * time.Millisecond)
			h.ack()
			h.refresh()
		}
		for i := 0; i < 1000; i++ {
			write()
			h.advance(8 * time.Millisecond)
		}
		// A slow tail: 40 KiB/s, under the recent-window rate, still backlogged.
		for i := 0; i < 100; i++ {
			write()
			h.advance(400 * time.Millisecond)
		}
		return out, h
	}
	legacy, _ := run(ClassifierLegacy)
	phased, h := run(ClassifierPhased)
	bulkFrom := -1
	for i := range phased {
		if phased[i] != legacy[i] {
			t.Fatalf("write %d: phased %+v, legacy %+v", i, phased[i], legacy[i])
		}
		if bulkFrom < 0 && phased[i].class == protocol.ClassBulk {
			bulkFrom = i
		}
		if bulkFrom >= 0 && (phased[i].class != protocol.ClassBulk || phased[i].coded) {
			t.Fatalf("write %d after BULK at %d was %+v", i, bulkFrom, phased[i])
		}
	}
	if bulkFrom < 0 || bulkFrom > 200 {
		t.Fatalf("the download became BULK at write %d", bulkFrom)
	}
	if exits, _ := h.f.classifier.PhaseTransitions(); exits != 0 {
		t.Fatalf("the download left BULK %d times", exits)
	}
}

// F3/R2: the recent window ages by the time that has passed. After an idle
// gap of two windows or more, nothing before the gap is "recent".
func TestRecentWindowAgesAcrossIdle(t *testing.T) {
	inner, peer := net.Pipe()
	defer inner.Close()
	defer peer.Close()
	f := newMultipathFlow(context.Background(), inner, [16]byte{1}, 1, 64*1024,
		protocol.FlagAckDown, protocol.FlagAckUp, nil, nil, nil)
	start := time.Now()
	f.recentBytes(start, 100_000, true)
	up, down := f.recentBytes(start.Add(10*time.Second), 1, false)
	if up != 0 || down != 1 {
		t.Fatalf("10 s after a 100 KB burst the window holds up=%d down=%d, want 0 and 1", up, down)
	}
	// One window later the burst is still in the finished bucket: the window
	// reports one to two windows of traffic, and that has not changed.
	f.recentBytes(start.Add(20*time.Second), 100_000, true)
	if up, _ := f.recentBytes(start.Add(21500*time.Millisecond), 0, true); up != 100_000 {
		t.Fatalf("1.5 s after a burst the window holds %d, want the burst", up)
	}
	if up, _ := f.recentBytes(start.Add(23*time.Second), 0, true); up != 0 {
		t.Fatalf("3 s after a burst the window holds %d, want 0", up)
	}

	// Through observe, on the default (legacy) bundle: 116 KB of exchange,
	// 10 s of silence, then a 16 KB response that takes the total over the
	// 128 KiB floor. The flow is old and over the floor, so only a stale
	// window -- one still holding the exchange from before the gap -- would
	// call it a transfer. This is the 120-128 KiB knife edge of F1/F3.
	h := newPhaseHarness(t, ClassifierLegacy)
	h.advance(2 * time.Second)
	for i := 0; i < 6; i++ {
		h.recv(16_000)
	}
	h.write(20_000, true)
	if got := h.current(); got == classifier.ClassBulk {
		t.Fatal("setup: the exchange alone made the flow BULK")
	}
	h.advance(10 * time.Second)
	h.write(16_000, false)
	if got := h.current(); got == classifier.ClassBulk {
		t.Fatal("the first write after a 10 s idle gap was classified by the burst before it")
	}
}

// The sparse allowance bounds how much a flow can send through the rule.
func TestSparseAllowanceIsBounded(t *testing.T) {
	h := newPhaseHarness(t, ClassifierPhased)
	admitted := 0
	for i := 0; i < 1000; i++ {
		if h.f.admitSparse(1400) {
			admitted += 1400
		}
	}
	if admitted > sparseAllowanceBytes {
		t.Fatalf("admitted %d sparse bytes in one window, allowance %d", admitted, sparseAllowanceBytes)
	}
	h.advance(3 * time.Second)
	if !h.f.admitSparse(1400) {
		t.Fatal("the allowance did not refill after the window passed")
	}
	legacy := newPhaseHarness(t, ClassifierLegacy)
	if legacy.f.sparseBytes() != 0 || legacy.f.admitSparse(10) {
		t.Fatal("the legacy bundle marks sparse writes")
	}
}

func TestClassifierModeValidation(t *testing.T) {
	if _, err := ParseClassifierMode("v2"); err == nil {
		t.Fatal("an unknown classifier mode parsed")
	}
	if mode, err := ParseClassifierMode(""); err != nil || mode != ClassifierLegacy {
		t.Fatalf("empty mode parsed as %q, %v", mode, err)
	}
	dc, err := profile.ByName(dcLongHaulProfile)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateClassifier(ClassifierPhased, dc); err == nil {
		t.Fatal("classifier=phased accepted with the dc-long-haul profile")
	}
	if err := ValidateClassifier(ClassifierPhased, profile.Default()); err != nil {
		t.Fatal(err)
	}
	if got := Bundle(CCAPIV1, MemoryLegacy, CodedRetryLegacy, GatewayAdmissionLegacy, SchedLegacy, ClassifierPhased); got != "classifier=phased" {
		t.Fatalf("bundle %q", got)
	}
}

// The send path end to end below the classifier: with a phased flow that is
// BULK, the scheduler marks a keystroke sparse and dispatches it unreliable;
// sendChunk stamps it INTERACTIVE and queues it on the lane's interactive
// queue; the frame connection codes it. A 16 KiB chunk of the same flow is
// BULK, on the bulk queue, and on the stream. Both use the flow's one data
// lane, chosen by the sticky class.
func TestASparseWriteOfABulkFlowIsSentInteractiveAndCoded(t *testing.T) {
	h := newPhaseHarness(t, ClassifierPhased)
	h.advance(2 * time.Second)
	h.burst()
	if h.current() != classifier.ClassBulk {
		t.Fatalf("setup: flow is %v, want bulk", h.current())
	}
	f := h.f
	model := pathmodel.NewPathModel()
	model.Report(1, pathmodel.Observation{Erasure: .2, BurstFactor: 1, ObservedSamples: 5000, Delivered: 2e6})
	path := coded.New(newPipeCarrier(), coded.Config{Path: model})
	defer path.Close()
	capture := newAckCaptureConn(0, nil)
	defer capture.Close()
	fc := newSplitFrameConn(capture, path)
	fc.setCodingPolicy(f.prefersCodingOverRetransmission)
	fc.setFrameCodingPolicy(f.frameCoding)
	if !fc.codingBulk() {
		t.Fatal("fixture: the coded path is not coding")
	}
	lane := &mpLane{id: 0, kind: TransportQUIC, fc: fc,
		writeQ: make(chan laneFrame, 4), writeInteractiveQ: make(chan laneFrame, 4), writeDone: make(chan struct{})}
	lane.ready.Store(true)
	f.lanes[0] = lane

	src := &chunkReader{writes: make(chan []byte, 4)}
	defer close(src.writes)
	sched := stripe.New(src, stripe.Config{
		ChunkSize: 32 << 10, LaneWindow: 64, MaxOutstanding: 64, MaxOutstandingBytes: 1 << 20,
		Reliable: f.laneRetransmits, ReliableSparse: f.laneRetransmitsSparse,
		SparseBytes: f.sparseBytes(), Sparse: f.admitSparse,
	})
	defer sched.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	send := func(n int) (laneFrame, bool) {
		t.Helper()
		src.writes <- make([]byte, n)
		for sched.ReadyBytes() == 0 {
			if ctx.Err() != nil {
				t.Fatal("the write never became ready")
			}
			time.Sleep(time.Millisecond)
		}
		chunk, err := sched.Next(ctx, 0, 0)
		if err != nil || chunk == nil {
			t.Fatal(err)
		}
		bulkLane := f.carriesBulk()
		if !f.laneCarriesData(lane, bulkLane) {
			t.Fatal("the lane is not the flow's data lane")
		}
		if err := f.sendChunk(ctx, lane, chunk, bulkLane); err != nil {
			t.Fatal(err)
		}
		select {
		case q := <-lane.writeInteractiveQ:
			if q.forceReliable != chunk.Reliable {
				t.Fatal("the queued frame lost its dispatch reliability")
			}
			return q, false
		case q := <-lane.writeQ:
			return q, true
		}
	}

	key, keyOnBulkQueue := send(36)
	if keyOnBulkQueue {
		t.Fatal("the keystroke went to the lane's bulk queue")
	}
	if key.frame.Header.Class != protocol.ClassInteractive {
		t.Fatalf("the keystroke carries class %v, want interactive", key.frame.Header.Class)
	}
	if key.forceReliable || !fc.bulkFrame(key.frame) {
		t.Fatal("the keystroke would not be coded")
	}
	if frameSendClass(key.frame) != quiccongestion.ClassInteractive {
		t.Fatal("sched=class would charge the keystroke to a class other than INTERACTIVE")
	}

	data, dataOnBulkQueue := send(16 << 10)
	if !dataOnBulkQueue || data.frame.Header.Class != protocol.ClassBulk {
		t.Fatalf("a 16 KiB chunk of a BULK flow: class %v, bulk queue %v", data.frame.Header.Class, dataOnBulkQueue)
	}
	if !data.forceReliable || fc.bulkFrame(data.frame) {
		t.Fatal("a 16 KiB chunk of a BULK flow would be coded")
	}
	if frameSendClass(data.frame) != quiccongestion.ClassBulk {
		t.Fatal("sched=class would not charge a BULK chunk to BULK")
	}
}

// chunkReader hands the scheduler one write per Read.
type chunkReader struct{ writes chan []byte }

func (r *chunkReader) Read(p []byte) (int, error) {
	w, ok := <-r.writes
	if !ok {
		return 0, io.EOF
	}
	return copy(p, w), nil
}

// A sparse write of a BULK flow is coded; if its datagrams stop getting
// through, the stall watchdog must still send its retry to the stream. It used
// to ask only whether the flow preferred coding, which a BULK flow does not.
func TestAStalledSparseWriteOfABulkFlowEscapesToTheStream(t *testing.T) {
	h := newPhaseHarness(t, ClassifierPhased)
	h.advance(2 * time.Second)
	h.burst()
	f := h.f
	if h.current() != classifier.ClassBulk {
		t.Fatalf("setup: flow is %v, want bulk", h.current())
	}
	f.stallGrace = 10 * time.Millisecond
	f.stallScan = time.Millisecond
	model := pathmodel.NewPathModel()
	model.Report(1, pathmodel.Observation{Erasure: .2, BurstFactor: 1, ObservedSamples: 5000, Delivered: 2e6})
	path := coded.New(newPipeCarrier(), coded.Config{Path: model})
	defer path.Close()
	capture := newAckCaptureConn(0, nil)
	defer capture.Close()
	fc := newSplitFrameConn(capture, path)
	fc.setCodingPolicy(f.prefersCodingOverRetransmission)
	fc.setFrameCodingPolicy(f.frameCoding)
	fc.setDataObserver(f.noteDataFrame)
	lane := &mpLane{id: 0, kind: TransportQUIC, fc: fc, writeQ: make(chan laneFrame, 2),
		writeInteractiveQ: make(chan laneFrame, 2), writeDone: make(chan struct{})}
	lane.ready.Store(true)
	f.lanes[0] = lane
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	now := time.Now()
	src := &chunkReader{writes: make(chan []byte, 1)}
	defer close(src.writes)
	sched := stripe.New(src, stripe.Config{
		Reliable: f.laneRetransmits, ReliableSparse: f.laneRetransmitsSparse,
		SparseBytes: f.sparseBytes(), Sparse: f.admitSparse,
		ForceReliable:   f.recoverChunkReliably,
		RetransmitAfter: func() time.Duration { return time.Millisecond }, Now: func() time.Time { return now },
	})
	defer sched.Close()
	f.scheduler.Store(sched)
	src.writes <- []byte("echo")
	first, err := sched.Next(ctx, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !first.Sparse || first.Reliable {
		t.Fatalf("fixture: the echo is sparse=%v reliable=%v, want a coded sparse write", first.Sparse, first.Reliable)
	}
	if err = f.sendChunk(ctx, lane, first, true); err != nil {
		t.Fatal(err)
	}
	sent := <-lane.writeInteractiveQ
	reliable, err := fc.writeContextObserved(ctx, sent.frame, sent.forceReliable, nil)
	if err != nil || reliable {
		t.Fatalf("the echo was not written coded: reliable=%v err=%v", reliable, err)
	}
	sent.onWritten(reliable)
	if !f.codedDataOutstanding() {
		t.Fatal("a coded echo was not recorded as outstanding coded DATA")
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() { f.stallWatchdog(stop); close(done) }()
	defer func() { close(stop); <-done }()
	select {
	case <-f.stallSignals():
	case <-ctx.Done():
		t.Fatal("no stall detected")
	}
	now = now.Add(time.Second)
	if sched.ReissueExpired() != 1 {
		t.Fatal("the lost echo was not reissued")
	}
	retry, err := sched.Next(ctx, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !retry.Reliable {
		t.Fatal("the stalled sparse write keeps retrying on datagrams; no stream escape")
	}
}

// The send backlog counts what the peer does not hold, including bytes it
// acknowledged selectively beyond a gap, and an acknowledgement that drains it
// below the threshold is reported to the next observation.
func TestSendBacklogCountsSelectiveCoverageAndDips(t *testing.T) {
	h := newPhaseHarness(t, ClassifierPhased)
	f := h.f
	f.bytesUp.Store(400 << 10)
	f.replayMu.Lock()
	f.highestSent = 400 << 10
	f.replayMu.Unlock()
	// The first 16 KiB is missing; everything above it arrived.
	if _, err := f.acknowledgeCoverage(0, false, [][2]uint64{{16 << 10, 400 << 10}}); err != nil {
		t.Fatal(err)
	}
	if backlog := saturatingSub(f.bytesUp.Load(), f.ackedCum.Load()); backlog != 16<<10 {
		t.Fatalf("backlog %d with one 16 KiB gap, want %d", backlog, 16<<10)
	}
	if !f.backlogDrained.Load() {
		t.Fatal("an acknowledgement that left 16 KiB outstanding was not recorded as a drain")
	}
}

// A stall on a BULK flow whose outstanding data all went on the stream is not
// a coded stall, though the lane could code; and a coded chunk the peer has
// acknowledged, selectively or not, is no longer outstanding.
func TestCodedDataOutstandingFollowsTheAttempts(t *testing.T) {
	h := newPhaseHarness(t, ClassifierPhased)
	f := h.f
	src := &chunkReader{writes: make(chan []byte, 2)}
	defer close(src.writes)
	coding := false
	sched := stripe.New(src, stripe.Config{
		ChunkSize: 32 << 10, LaneWindow: 64, MaxOutstanding: 64, MaxOutstandingBytes: 1 << 20,
		Reliable: func(uint64) bool { return !coding },
	})
	defer sched.Close()
	f.scheduler.Store(sched)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	next := func(n int) *stripe.Chunk {
		t.Helper()
		src.writes <- make([]byte, n)
		c, err := sched.Next(ctx, 0, 0)
		if err != nil || c == nil {
			t.Fatal(err)
		}
		return c
	}
	stream := next(16 << 10)
	sched.WroteAs(0, stream, true)
	if f.codedDataOutstanding() {
		t.Fatal("a stream-only chunk counted as outstanding coded DATA")
	}
	coding = true
	coded := next(36)
	sched.WroteAs(0, coded, false)
	if !f.codedDataOutstanding() {
		t.Fatal("an unacknowledged coded chunk was not outstanding")
	}
	sched.Complete(0, coded) // acknowledged above the stream chunk's gap
	if f.codedDataOutstanding() {
		t.Fatal("a coded chunk acknowledged selectively still counted as outstanding")
	}
}

// On a memory profile whose per-flow send bound is at or under the backlog
// threshold, the threshold follows the bound, so a blocked transfer can still
// show a backlog.
func TestBacklogThresholdFitsTheSendBound(t *testing.T) {
	cfg := phasedClassifierConfig(profile.Default().Classifier, ClassifierPhased)
	small := defaultFlowMemoryLimits()
	small.maxSendBytes = 48 << 10
	if got := classifierBacklogFor(cfg, small).BulkBacklogBytes; got != 24<<10 {
		t.Fatalf("threshold %d with a 48 KiB send bound, want 24 KiB", got)
	}
	if got := classifierBacklogFor(cfg, defaultFlowMemoryLimits()).BulkBacklogBytes; got != 64<<10 {
		t.Fatalf("threshold %d with the default bound, want 64 KiB", got)
	}
	legacy := phasedClassifierConfig(profile.Default().Classifier, ClassifierLegacy)
	if got := classifierBacklogFor(legacy, small); got != legacy {
		t.Fatal("the legacy classifier's configuration was changed")
	}
}

// observeCarried counts the bytes in the same step as the observation.
func TestObserveCarriedCountsTheBytes(t *testing.T) {
	h := newPhaseHarness(t, ClassifierPhased)
	h.f.observeCarried(100, true)
	h.f.observeCarried(40, false)
	if h.f.bytesUp.Load() != 100 || h.f.bytesDown.Load() != 40 {
		t.Fatalf("totals up=%d down=%d, want 100 and 40", h.f.bytesUp.Load(), h.f.bytesDown.Load())
	}
}
