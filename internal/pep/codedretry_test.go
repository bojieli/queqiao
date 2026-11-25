package pep

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/bojieli/queqiao/internal/coded"
	"github.com/bojieli/queqiao/internal/pathmodel"
	"github.com/bojieli/queqiao/internal/protocol"
	"github.com/bojieli/queqiao/internal/stripe"
)

// erasingCarrier is a connection's datagram service on a path that erases
// everything: datagrams are accepted, reported as the connection reports
// them, and never delivered. With hold set, a tagged datagram waits, as in a
// connection's datagram queue, until the test reports it.
type erasingCarrier struct {
	mu       sync.Mutex
	hold     bool
	observer func(uint64, bool)
	held     []uint64
	tagged   int
	untagged int
	closed   chan struct{}
	once     sync.Once
}

func newErasingCarrier(hold bool) *erasingCarrier {
	return &erasingCarrier{hold: hold, closed: make(chan struct{})}
}

func (c *erasingCarrier) Send([]byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.untagged++
	return nil
}

func (c *erasingCarrier) SendTagged(_ []byte, tag uint64) error {
	c.mu.Lock()
	c.tagged++
	if c.hold {
		c.held = append(c.held, tag)
		c.mu.Unlock()
		return nil
	}
	observer := c.observer
	c.mu.Unlock()
	// Registered as the packet is built, then lost on the path.
	observer(tag, true)
	return nil
}

func (c *erasingCarrier) ObserveTags(f func(uint64, bool)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.observer = f
}

func (c *erasingCarrier) Receive() ([]byte, error) {
	<-c.closed
	return nil, errors.New("closed")
}

func (c *erasingCarrier) Close() error {
	c.once.Do(func() { close(c.closed) })
	return nil
}

func (c *erasingCarrier) release(registered bool) {
	c.mu.Lock()
	held, observer := c.held, c.observer
	c.held = nil
	c.mu.Unlock()
	for _, tag := range held {
		observer(tag, registered)
	}
}

func (c *erasingCarrier) counts() (tagged, untagged, held int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.tagged, c.untagged, len(c.held)
}

// codedRetryFixture is one QUIC-like lane: a stream that records what is
// written to it, and a coded path over an erasing carrier.
type codedRetryFixture struct {
	flow    *multipathFlow
	carrier *erasingCarrier
	capture *ackCaptureConn
	fc      *frameConn
	lane    *mpLane
}

func newCodedRetryFixture(t *testing.T, sticky, hold bool) *codedRetryFixture {
	t.Helper()
	flow := newStallTestFlow(t, nil)
	flow.codedRetrySticky = sticky
	model := pathmodel.NewPathModel()
	model.Report(1, pathmodel.Observation{Erasure: .2, BurstFactor: 1, ObservedSamples: 5000, Delivered: 2e6})
	carrier := newErasingCarrier(hold)
	path := coded.New(carrier, coded.Config{Path: model})
	t.Cleanup(func() { _ = path.Close() })
	capture := newAckCaptureConn(0, nil)
	t.Cleanup(func() { _ = capture.Close() })
	fc := newSplitFrameConn(capture, path)
	fc.setCodingPolicy(flow.prefersCodingOverRetransmission)
	lane := &mpLane{id: 0, kind: TransportQUIC, fc: fc, writeQ: make(chan laneFrame, 4), writeDone: make(chan struct{})}
	lane.ready.Store(true)
	flow.lanes[0] = lane
	return &codedRetryFixture{flow: flow, carrier: carrier, capture: capture, fc: fc, lane: lane}
}

func (x *codedRetryFixture) scheduler(t *testing.T, now func() time.Time) *stripe.Scheduler {
	t.Helper()
	sched := stripe.New(bytes.NewReader([]byte("response")), stripe.Config{
		Reliable:        func(uint64) bool { return !x.fc.codesData() },
		ForceReliable:   x.flow.recoverChunkReliably,
		RetransmitAfter: x.flow.reissueDelay,
		CodedRetry:      x.flow.codedRetrySticky,
		Now:             now,
	})
	t.Cleanup(sched.Close)
	x.flow.scheduler.Store(sched)
	return sched
}

// write runs the lane writer for one queued frame, as writeLane does, and
// returns once the coded path has reported the frame's outcome, if it took
// the frame and the frame is observed.
func (x *codedRetryFixture) write(t *testing.T, ctx context.Context) laneFrame {
	t.Helper()
	queued := <-x.lane.writeQ
	settled := make(chan coded.Outcome, 1)
	onCoded := queued.onCoded
	if onCoded != nil {
		queued.onCoded = func(o coded.Outcome, at time.Time) { onCoded(o, at); settled <- o }
	}
	reliable, err := x.fc.writeContextObserved(ctx, queued.frame, queued.forceReliable, queued.onCoded)
	if err != nil {
		t.Fatal(err)
	}
	queued.onWritten(reliable)
	if onCoded != nil && !reliable {
		if _, _, held := x.carrier.counts(); held == 0 {
			select {
			case <-settled:
			case <-time.After(2 * time.Second):
				t.Fatal("the coded path never reported the frame")
			}
		}
	}
	return queued
}

func (x *codedRetryFixture) noStreamData(t *testing.T) {
	t.Helper()
	select {
	case frame := <-x.capture.frames:
		t.Fatalf("stream carried %d before the code's deadline: two repair owners", frame.Header.Type)
	default:
	}
}

func (x *codedRetryFixture) streamDelivers(t *testing.T) {
	t.Helper()
	frame := waitAckFrame(t, x.capture)
	if frame.Header.Type != protocol.TypeData || string(frame.Payload) != "response" {
		t.Fatalf("stream carried type %d %q, want the response", frame.Header.Type, frame.Payload)
	}
}

// The P0 lesson, made structural (design §8.1, §10.6 "Coded tail"): a coded
// tail lost on a healthy connection, with no later chunk to expose it, is
// retried on the existing stream once its registration deadline passes. The
// stall watchdog is not running: nothing here depends on it, on the frontier
// it pins, or on the lane manager it signals.
func TestCodedTailLostAloneIsRecoveredWithoutTheWatchdog(t *testing.T) {
	x := newCodedRetryFixture(t, true, false)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	sched := x.scheduler(t, nil)
	go x.flow.writeLane(x.lane)
	t.Cleanup(func() { x.flow.signalDone(); <-x.lane.writeDone })
	go x.flow.superviseChunks(ctx, sched)
	// Taken before anything can be dispatched, so registration is later.
	start := time.Now()
	go func() {
		for {
			chunk, err := sched.Next(ctx, 0, 0)
			if err != nil || chunk == nil {
				return
			}
			if x.flow.sendChunk(ctx, x.lane, chunk, true) != nil {
				return
			}
		}
	}()
	deadline := x.flow.reissueDelay()
	frame := waitFrameWithin(t, x.capture, deadline+chunkReissueInterval+time.Second)
	elapsed := time.Since(start)
	if frame.Header.Type != protocol.TypeData || string(frame.Payload) != "response" {
		t.Fatalf("stream carried type %d %q, want the response", frame.Header.Type, frame.Payload)
	}
	if elapsed < deadline {
		t.Fatalf("retried after %s, before the %s deadline", elapsed, deadline)
	}
	if tagged, _, _ := x.carrier.counts(); tagged == 0 {
		t.Fatal("the first attempt never went to the code")
	}
	if x.flow.codingRecoveryEnd.Load() != 0 {
		t.Fatal("recovery leaned on the watchdog's frontier")
	}
	select {
	case <-x.flow.stallSignals():
		t.Fatal("recovery signalled the lane manager")
	default:
	}
	if !x.fc.codesData() {
		t.Fatal("recovering one chunk stopped coding fresh exchanges")
	}
}

func waitFrameWithin(t *testing.T, conn *ackCaptureConn, within time.Duration) protocol.Frame {
	t.Helper()
	select {
	case frame := <-conn.frames:
		return frame
	case <-time.After(within):
		t.Fatalf("nothing reached the stream within %s", within)
		return protocol.Frame{}
	}
}

// The deadline starts when the datagram is registered in a packet, not when
// the frame is queued: a chunk that waits in the connection's datagram queue
// for many deadlines is still the code's, and goes to the stream only a full
// deadline after it actually left.
func TestCodedChunkDelayedInTheQueueIsNotRetriedEarly(t *testing.T) {
	x := newCodedRetryFixture(t, true, true)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	now := time.Unix(1000, 0)
	var clock sync.Mutex
	sched := x.scheduler(t, func() time.Time { clock.Lock(); defer clock.Unlock(); return now })
	advance := func(d time.Duration) { clock.Lock(); now = now.Add(d); clock.Unlock() }
	first, err := sched.Next(ctx, 0, 0)
	if err != nil || first.Reliable {
		t.Fatalf("first attempt = %+v, %v; want the code", first, err)
	}
	if err := x.flow.sendChunk(ctx, x.lane, first, true); err != nil {
		t.Fatal(err)
	}
	queued := <-x.lane.writeQ
	settled := make(chan coded.Outcome, 1)
	onCoded := queued.onCoded
	if onCoded == nil {
		t.Fatal("a coded chunk under the bundle is not observed")
	}
	reliable, err := x.fc.writeContextObserved(ctx, queued.frame, queued.forceReliable, func(o coded.Outcome, at time.Time) { onCoded(o, at); settled <- o })
	if err != nil || reliable {
		t.Fatalf("write = %v, %v; want the code", reliable, err)
	}
	queued.onWritten(reliable)
	waitHeld(t, x.carrier)
	deadline := x.flow.reissueDelay()
	advance(10 * deadline)
	if n := sched.ReissueExpired(); n != 0 {
		t.Fatal("a chunk still queued below the path was retried")
	}
	x.carrier.release(true)
	if got := <-settled; got != coded.Registered {
		t.Fatalf("outcome = %d, want registered", got)
	}
	// The report reaches the scheduler a little after the packet, and that
	// real delay is subtracted from the deadline; a margin far above it
	// keeps this independent of how quickly the outcome was delivered.
	advance(deadline - 50*time.Millisecond)
	if n := sched.ReissueExpired(); n != 0 {
		t.Fatal("retried before a full deadline from registration")
	}
	x.noStreamData(t)
	advance(50 * time.Millisecond)
	if n := sched.ReissueExpired(); n != 1 {
		t.Fatalf("ReissueExpired = %d a deadline after registration, want 1", n)
	}
	retry, err := sched.Next(ctx, 0, 0)
	if err != nil || !retry.Reliable {
		t.Fatalf("retry = %+v, %v; want the stream", retry, err)
	}
	if err := x.flow.sendChunk(ctx, x.lane, retry, true); err != nil {
		t.Fatal(err)
	}
	if x.write(t, ctx).onCoded != nil {
		t.Fatal("a stream retry was observed as coded")
	}
	x.streamDelivers(t)
}

func waitHeld(t *testing.T, c *erasingCarrier) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, _, held := c.counts(); held > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the coded path never offered the datagram")
		}
		time.Sleep(100 * time.Microsecond)
	}
}

// A tagged datagram the connection discards (its packer dropped it, or the
// connection closed with it queued) was never sent, so the code never owned
// it: the chunk goes to the stream at once, without waiting for a deadline.
func TestDiscardedCodedDatagramGoesToTheStream(t *testing.T) {
	x := newCodedRetryFixture(t, true, true)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	now := time.Unix(1000, 0)
	sched := x.scheduler(t, func() time.Time { return now })
	first, err := sched.Next(ctx, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := x.flow.sendChunk(ctx, x.lane, first, true); err != nil {
		t.Fatal(err)
	}
	queued := <-x.lane.writeQ
	settled := make(chan coded.Outcome, 1)
	onCoded := queued.onCoded
	reliable, err := x.fc.writeContextObserved(ctx, queued.frame, queued.forceReliable, func(o coded.Outcome, at time.Time) { onCoded(o, at); settled <- o })
	if err != nil || reliable {
		t.Fatalf("write = %v, %v; want the code", reliable, err)
	}
	queued.onWritten(reliable)
	waitHeld(t, x.carrier)
	x.carrier.release(false)
	if got := <-settled; got != coded.Abandoned {
		t.Fatalf("outcome = %d, want abandoned", got)
	}
	retry, err := sched.Next(ctx, 0, 0)
	if err != nil || !retry.Reliable || retry.Offset != first.Offset {
		t.Fatalf("retry = %+v, %v; want the same bytes on the stream", retry, err)
	}
	if err := x.flow.sendChunk(ctx, x.lane, retry, true); err != nil {
		t.Fatal(err)
	}
	x.write(t, ctx)
	x.streamDelivers(t)
	if got := sched.Stats().CodedAbandoned; got != 1 {
		t.Fatalf("CodedAbandoned = %d, want 1", got)
	}
}

// With the switch off nothing is observed or tagged, the deadline counts from
// dispatch and a retry stays on the code: today's behaviour, which the
// watchdog's frontier then rescues (TestCodedSendStallRecoversOnExistingStream).
func TestCodedRetryOffIsTodaysBehaviour(t *testing.T) {
	x := newCodedRetryFixture(t, false, true)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	now := time.Unix(1000, 0)
	sched := x.scheduler(t, func() time.Time { return now })
	first, err := sched.Next(ctx, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := x.flow.sendChunk(ctx, x.lane, first, true); err != nil {
		t.Fatal(err)
	}
	if queued := x.write(t, ctx); queued.onCoded != nil {
		t.Fatal("the legacy bundle observes coded frames")
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, untagged, _ := x.carrier.counts(); untagged > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the coded path sent nothing")
		}
		time.Sleep(100 * time.Microsecond)
	}
	if tagged, _, _ := x.carrier.counts(); tagged != 0 {
		t.Fatal("the legacy bundle tagged a datagram")
	}
	now = now.Add(x.flow.reissueDelay())
	if n := sched.ReissueExpired(); n != 1 {
		t.Fatalf("legacy ReissueExpired = %d at the dispatch deadline, want 1", n)
	}
	retry, err := sched.Next(ctx, 0, 0)
	if err != nil || retry.Reliable {
		t.Fatalf("legacy retry = %+v, %v; want the code again", retry, err)
	}
}

// coded-retry=sticky needs the datagram outcomes of the cc-api=v2 fork API
// (design §14): the validator refuses it on v1, at both roles' constructors.
func TestCodedRetryRequiresCCAPIV2(t *testing.T) {
	for _, test := range []struct {
		mode CodedRetryMode
		api  CCAPIVersion
		ok   bool
	}{
		{"", "", true},
		{CodedRetryLegacy, CCAPIV1, true},
		{CodedRetryLegacy, CCAPIV2, true},
		{CodedRetrySticky, CCAPIV2, true},
		{CodedRetrySticky, CCAPIV1, false},
		{CodedRetrySticky, "", false},
		{"eager", CCAPIV2, false},
	} {
		if err := ValidateCodedRetry(test.mode, test.api); (err == nil) != test.ok {
			t.Errorf("ValidateCodedRetry(%q, %q) = %v, want ok=%v", test.mode, test.api, err, test.ok)
		}
	}
	if got := Bundle(CCAPIV2, MemoryLegacy, CodedRetrySticky, GatewayAdmissionLegacy, SchedLegacy, CongestionScopeConnection, ClassifierLegacy); got != "cc-api=v2,coded-retry=sticky" {
		t.Fatalf("Bundle = %q", got)
	}
	if got := Bundle(CCAPIV1, MemoryLegacy, CodedRetryLegacy, GatewayAdmissionLegacy, SchedLegacy, CongestionScopeConnection, ClassifierLegacy); got != "legacy" {
		t.Fatalf("Bundle = %q", got)
	}
	certificate, credentials := testCertificate(t)
	if _, err := NewClient(ClientConfig{
		ListenAddr: "127.0.0.1:0", RemoteAddr: "127.0.0.1:1", Credentials: credentials,
		CodedRetry: CodedRetrySticky,
	}); err == nil {
		t.Error("client accepted coded-retry=sticky on cc-api=v1")
	}
	if _, err := NewServer(ServerConfig{
		ListenAddr: "127.0.0.1:0", Credentials: certificate, EnableQUIC: true,
		CodedRetry: CodedRetrySticky,
	}); err == nil {
		t.Error("server accepted coded-retry=sticky on cc-api=v1")
	}
	if _, err := NewServer(ServerConfig{
		ListenAddr: "127.0.0.1:0", Credentials: certificate, EnableQUIC: true,
		CCAPI: CCAPIV2, CodedRetry: CodedRetrySticky,
	}); err != nil {
		t.Errorf("server rejected coded-retry=sticky on cc-api=v2: %v", err)
	}
}

// Before OPEN_OK the first DATA frame still goes coded with one stream safety
// copy (PROTOCOL.md §8.1). The stream copy owns the bytes, so the code's
// report about its own copy, whichever it is, retries nothing.
func TestOpenSafetyCopyIsKeptUnderCodedRetry(t *testing.T) {
	for _, registered := range []bool{true, false} {
		x := newCodedRetryFixture(t, true, true)
		x.fc.openUnconfirmed = func() bool { return true }
		x.fc.openSafetyCopy.Store(true)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		now := time.Unix(1000, 0)
		sched := x.scheduler(t, func() time.Time { return now })
		first, err := sched.Next(ctx, 0, 0)
		if err != nil || first.Reliable {
			cancel()
			t.Fatalf("first attempt = %+v, %v; want the code", first, err)
		}
		if err := x.flow.sendChunk(ctx, x.lane, first, true); err != nil {
			cancel()
			t.Fatal(err)
		}
		queued := <-x.lane.writeQ
		settled := make(chan coded.Outcome, 1)
		onCoded := queued.onCoded
		reliable, err := x.fc.writeContextObserved(ctx, queued.frame, queued.forceReliable, func(o coded.Outcome, at time.Time) { onCoded(o, at); settled <- o })
		if err != nil || !reliable {
			cancel()
			t.Fatalf("write = %v, %v; want the coded frame plus its stream safety copy", reliable, err)
		}
		queued.onWritten(reliable)
		x.streamDelivers(t)
		waitHeld(t, x.carrier)
		x.carrier.release(registered)
		<-settled
		// The stream attempt keeps the deadline any stream attempt has.
		now = now.Add(x.flow.reissueDelay() - time.Millisecond)
		if n := sched.ReissueExpired(); n != 0 {
			t.Fatalf("registered=%v: a chunk the stream carried was retried by the code's deadline", registered)
		}
		if got := sched.Stats().CodedAbandoned; got != 0 {
			t.Fatalf("registered=%v: the stream's attempt was retired as abandoned", registered)
		}
		short, stop := context.WithTimeout(ctx, 20*time.Millisecond)
		if again, _ := sched.Next(short, 0, 0); again != nil && len(again.Data) > 0 {
			stop()
			cancel()
			t.Fatalf("registered=%v: a chunk the stream carried was dispatched again", registered)
		}
		stop()
		cancel()
	}
}
