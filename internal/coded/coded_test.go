package coded

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bojieli/queqiao/internal/pathmodel"
)

// lossyPipe is a pair of carriers connected through an erasure channel, so a
// test can put the measured path between two coded paths without a socket.
type lossyPipe struct {
	mu   sync.Mutex
	rng  *rand.Rand
	loss float64
	// drop erases particular datagrams rather than random ones, so a test can
	// ask what happens to a specific symbol.
	drop   func([]byte) bool
	closed bool
	done   chan struct{}
	out    chan []byte
	in     chan []byte
	sent   int
	lost   int
}

func newPipes(seed int64, loss float64) (*lossyPipe, *lossyPipe) {
	a2b, b2a := make(chan []byte, 8192), make(chan []byte, 8192)
	return &lossyPipe{
			rng: rand.New(rand.NewSource(seed)), loss: loss,
			out: a2b, in: b2a, done: make(chan struct{}),
		}, &lossyPipe{
			rng: rand.New(rand.NewSource(seed + 1)), loss: loss,
			out: b2a, in: a2b, done: make(chan struct{}),
		}
}

func TestConfiguredPendingBoundsBothPathMailboxes(t *testing.T) {
	carrier, _ := newPipes(1, 0)
	path := New(carrier, Config{Pending: 3})
	defer path.Close()

	if got := cap(path.pending); got != 3 {
		t.Fatalf("send mailbox capacity = %d, want 3", got)
	}
	if got := cap(path.received); got != 3 {
		t.Fatalf("receive mailbox capacity = %d, want 3", got)
	}
}

func (p *lossyPipe) Send(d []byte) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return errors.New("closed")
	}
	p.sent++
	drop := p.loss > 0 && p.rng.Float64() < p.loss
	if p.drop != nil && p.drop(d) {
		drop = true
	}
	if drop {
		p.lost++
	}
	p.mu.Unlock()
	if drop {
		return nil
	}
	cp := append([]byte(nil), d...)
	select {
	case p.out <- cp:
	default: // the peer is not keeping up, which is a drop like any other
	}
	return nil
}

func (p *lossyPipe) Receive() ([]byte, error) {
	select {
	case d := <-p.in:
		return d, nil
	case <-p.done:
		return nil, io.EOF
	}
}

func (p *lossyPipe) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.closed {
		p.closed = true
		close(p.done)
	}
	return nil
}

func (p *lossyPipe) stats() (sent, lost int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.sent, p.lost
}

// measuredPath is a model that already knows what the live channel does, which
// is what a real one gets from the congestion controller on the same
// connection.
func measuredPath(floor float64) *pathmodel.PathModel {
	m := pathmodel.NewPathModel()
	m.Report(1, pathmodel.Observation{Erasure: floor, BurstFactor: 1, ObservedSamples: 5000, Delivered: 2e6, RoundTrip: 0})
	return m
}

func paths(t *testing.T, seed int64, loss, floor float64) (*Path, *Path, *lossyPipe) {
	t.Helper()
	pa, pb := newPipes(seed, loss)
	cfg := Config{SymbolBytes: 1100, RoundTrip: 60 * time.Millisecond, Path: measuredPath(floor)}
	a, b := New(pa, cfg), New(pb, cfg)
	t.Cleanup(func() { a.Close(); b.Close() })
	return a, b, pa
}

// The whole job: frames crossing the channel the path actually is, with the
// code repairing what it erases.
func TestFramesCrossTheMeasuredChannel(t *testing.T) {
	a, b, wire := paths(t, 1, 0.42, 0.42)

	const frames = 400
	sent := make([][]byte, frames)
	rng := rand.New(rand.NewSource(2))
	go func() {
		for i := range sent {
			sent[i] = make([]byte, 200+rng.Intn(3000))
			rng.Read(sent[i])
			if err := a.Send(sent[i]); err != nil {
				return
			}
		}
	}()

	// One receiver, not one per frame: Receive blocks until something arrives,
	// so a goroutine per attempt leaks one for every frame the channel ate.
	arrived := make(chan []byte, frames)
	go func() {
		defer close(arrived)
		for {
			frame, err := b.Receive()
			if err != nil {
				return
			}
			arrived <- frame
		}
	}()

	got := make([][]byte, 0, frames)
	// The path is unreliable by design, so waiting for every frame would be
	// waiting for something that was never promised. Wait until they stop
	// coming instead.
	quiet := time.NewTimer(3 * time.Second)
	defer quiet.Stop()
collect:
	for len(got) < frames {
		select {
		case frame, ok := <-arrived:
			if !ok {
				break collect
			}
			got = append(got, frame)
			quiet.Reset(3 * time.Second)
		case <-quiet.C:
			break collect
		}
	}

	// Frames may be lost; what must never happen is a frame arriving corrupt,
	// because the session above trusts what it is handed.
	byLength := map[string]bool{}
	for _, frame := range sent {
		byLength[string(frame)] = true
	}
	for i, frame := range got {
		if !byLength[string(frame)] {
			t.Fatalf("frame %d arrived corrupt (%d bytes)", i, len(frame))
		}
	}
	stats := a.Stats()
	wireSent, wireLost := wire.stats()
	t.Logf("%d of %d frames arrived; wire sent %d dropped %d (%.0f%%); plan (%d,%d) rate %.3f",
		len(got), frames, wireSent, wireLost, 100*float64(wireLost)/float64(wireSent),
		stats.Plan.K, stats.Plan.N, stats.Plan.Rate)
	if len(got) < frames*95/100 {
		t.Fatalf("only %d of %d frames survived a 42%% erasure channel", len(got), frames)
	}
}

// A path clean enough not to warrant parity must not be given any, and must
// say so, because that is what tells the layer above to use its stream instead.
func TestACleanPathDoesNotCode(t *testing.T) {
	a, _, _ := paths(t, 3, 0, 0)
	if a.Coding() {
		t.Fatalf("a lossless path reports it is coding: %+v", a.Stats().Plan)
	}
}

// And a path that erases must say the opposite, or the layer above will keep
// bulk on a stream that head-of-line blocks at every gap.
func TestAnErasingPathCodes(t *testing.T) {
	a, _, _ := paths(t, 4, 0.42, 0.42)
	if !a.Coding() {
		t.Fatalf("a 42%% erasure channel reports it is not coding: %+v", a.Stats().Plan)
	}
}

// Frames are packed together when several are waiting, because a symbol
// carrying one small frame wastes most of the datagram it cost. Nothing is
// timed: the signal is that the queue drained.
func TestWaitingFramesShareASymbol(t *testing.T) {
	a, b, wire := paths(t, 5, 0, 0.42)
	const frames = 64
	payload := bytes.Repeat([]byte("x"), 500)
	for i := 0; i < frames; i++ {
		if err := a.Send(payload); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < frames; i++ {
		if _, err := b.Receive(); err != nil {
			t.Fatal(err)
		}
	}
	sent, _ := wire.stats()
	t.Logf("%d frames of %d bytes cost %d datagrams", frames, len(payload), sent)
	// One datagram per frame would mean nothing was ever packed. The code adds
	// parity, so the bound is generous; what it rules out is a symbol per frame
	// with no packing at all.
	if sent >= frames*3 {
		t.Fatalf("%d datagrams for %d frames: frames are not sharing symbols", sent, frames)
	}
}

// A frame larger than a symbol occupies symbols of its own, and arrives whole.
func TestALargeFrameIsCarriedInFragments(t *testing.T) {
	a, b, wire := paths(t, 20, 0, 0.42)
	frame := make([]byte, 32*1024)
	rand.New(rand.NewSource(21)).Read(frame)
	if err := a.Send(frame); err != nil {
		t.Fatal(err)
	}
	got, err := b.Receive()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, frame) {
		t.Fatalf("a %d-byte frame came back as %d bytes", len(frame), len(got))
	}
	sent, _ := wire.stats()
	t.Logf("a %d-byte frame cost %d datagrams", len(frame), sent)
	if sent < 20 {
		t.Fatalf("%d datagrams for a %d-byte frame: it was not fragmented", sent, len(frame))
	}
}

// A symbol lost for good costs the frames it carried and nothing behind them.
//
// This is the whole reason for using datagrams. A stream would have held every
// later frame until the lost one was retransmitted -- a round trip on a path
// whose round trip is the thing being avoided -- and so would a receiver that
// reassembled the symbols into one stream before reading frames out of it.
// Here the frames that were not in the lost symbol do not depend on it.
func TestALostSymbolDoesNotBlockWhatFollows(t *testing.T) {
	pa, pb := newPipes(22, 0)
	// Every repair is erased as well, so the lost symbol is lost for good and
	// what arrives is only what did not depend on it.
	sources := 0
	pa.drop = func(d []byte) bool {
		if d[4] == kindRepair {
			return true
		}
		sources++
		return sources == 2
	}
	cfg := Config{SymbolBytes: 1100, RoundTrip: 60 * time.Millisecond, Path: measuredPath(0.42)}
	a, b := New(pa, cfg), New(pb, cfg)
	t.Cleanup(func() { a.Close(); b.Close() })

	// Each frame is most of a symbol, so each gets one of its own.
	const frames = 5
	sent := make([][]byte, frames)
	for i := range sent {
		sent[i] = bytes.Repeat([]byte{byte('a' + i)}, 1000)
		if err := a.Send(sent[i]); err != nil {
			t.Fatal(err)
		}
	}

	arrived := make(chan []byte, frames)
	go func() {
		for {
			frame, err := b.Receive()
			if err != nil {
				return
			}
			arrived <- frame
		}
	}()

	var got [][]byte
	deadline := time.After(2 * time.Second)
	for len(got) < frames-1 {
		select {
		case frame := <-arrived:
			got = append(got, frame)
		case <-deadline:
			t.Fatalf("only %d of %d frames arrived; one erasure held back the "+
				"frames that did not depend on it", len(got), frames-1)
		}
	}
	for _, frame := range got {
		if len(frame) != 1000 {
			t.Fatalf("a frame arrived %d bytes long", len(frame))
		}
	}
}

// A short exchange is the case the code is worst at and the one that matters
// most: a lone symbol has nothing to share parity with, and nothing follows it
// to trigger a retransmission either. The tail protection is what covers it,
// and this measures whether it does.
func TestAShortExchangeSurvivesTheErasureChannel(t *testing.T) {
	a, b, wire := paths(t, 23, 0.42, 0.42)
	const exchanges = 100
	payload := bytes.Repeat([]byte("q"), 40)

	arrived := make(chan []byte, exchanges)
	go func() {
		for {
			frame, err := b.Receive()
			if err != nil {
				return
			}
			arrived <- frame
		}
	}()

	got := 0
	for i := 0; i < exchanges; i++ {
		if err := a.Send(payload); err != nil {
			t.Fatal(err)
		}
		select {
		case <-arrived:
			got++
		case <-time.After(100 * time.Millisecond):
		}
	}
	sent, lost := wire.stats()
	t.Logf("%d of %d short exchanges arrived; wire sent %d dropped %d (%.0f%%)",
		got, exchanges, sent, lost, 100*float64(lost)/float64(sent))
	if got < exchanges*95/100 {
		t.Fatalf("only %d of %d short exchanges survived: the tail of a burst is "+
			"not being protected", got, exchanges)
	}
}

// A frame no symbol can hold has to be refused rather than truncated, so the
// layer above can put it on the stream instead.
func TestAnOversizedFrameIsRefused(t *testing.T) {
	a, _, _ := paths(t, 6, 0, 0.42)
	huge := make([]byte, 8<<20)
	if err := a.Send(huge); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("sending an 8 MiB frame returned %v, want ErrFrameTooLarge", err)
	}
}

// A dead carrier must surface rather than hang.
func TestAClosedCarrierEndsThePath(t *testing.T) {
	a, _, _ := paths(t, 7, 0, 0)
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		_, _ = a.Receive()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("receive on a closed path hung")
	}
	if err := a.Send([]byte("x")); err == nil {
		t.Fatal("send on a closed path succeeded")
	}
}

// Incoming loss is not evidence about the direction this path sends into.
// Even on a symmetric link it includes congestion caused by the peer's
// offered rate, which this endpoint cannot observe. The bidirectional prewarm
// gives each direction's congestion controller traffic of its own; until then
// the safe substrate is the reliable stream.
func TestAPathDoesNotInferItsOutboundFloorFromReverseLoss(t *testing.T) {
	pa, pb := newPipes(30, 0)
	cfg := Config{SymbolBytes: 1100, RoundTrip: 60 * time.Millisecond, Path: pathmodel.NewPathModel()}
	quiet, loud := New(pa, cfg), New(pb, cfg)
	t.Cleanup(func() { quiet.Close(); loud.Close() })

	if quiet.Coding() {
		t.Fatal("a path that has measured nothing at all decided to code")
	}

	// Give the receiver enough independent reverse-direction loss for a formal
	// estimator verdict. It still says nothing causal about this sender's
	// outbound direction.
	payload := bytes.Repeat([]byte("x"), 1000)
	pb.mu.Lock()
	pb.loss = 0.42
	pb.mu.Unlock()
	for i := 0; i < 400; i++ {
		if err := loud.Send(payload); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.After(10 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatalf("the receiving end never measured the channel: %+v", quiet.Stats().Snapshot)
		default:
		}
		if quiet.Stats().Snapshot.Memoryless {
			break
		}
		if _, err := quiet.Receive(); err != nil {
			t.Fatal(err)
		}
	}

	// A path re-chooses its code on its own cadence. Reverse loss must not make
	// that outbound decision change.
	time.Sleep(codingTTL + 20*time.Millisecond)
	if quiet.Coding() {
		t.Errorf("reverse loss selected outbound coding without outbound evidence: %+v", quiet.Stats())
	}
}

// The incident in docs/CONTROL-REDESIGN.md happened here, between a path model
// that knew what the channel was doing and a code that was sized from
// something else. The controller's floor is biased low on purpose, and
// channel() used to build the code's whole view of the channel out of that one
// scalar -- Loss, Floor and Recent all set to it, and BurstFactor asserted to
// be 1. On the live path that meant parity sized for 1.76% while the far
// decoder measured 19.9%, and 11% of the payload handed back to the session.
//
// This holds the wiring rather than the arithmetic: given a model whose floor
// and measurement disagree the way the live one did, the code must be sized
// from the measurement.
func TestTheCodeIsSizedFromTheMeasurementNotTheFloor(t *testing.T) {
	const measured, conservativeFloor = 0.199, 0.0176
	m := pathmodel.NewPathModel()
	m.Report(1, pathmodel.Observation{
		Erasure: measured, BurstFactor: 1,
		ObservedSamples: 5000, Delivered: 2e6, RoundTrip: 250 * time.Millisecond,
	})

	pa, _ := newPipes(1, measured)
	p := New(pa, Config{SymbolBytes: 1100, RoundTrip: 250 * time.Millisecond, Path: m})
	t.Cleanup(func() { p.Close() })

	channel := p.channel()
	if channel.Loss != measured {
		t.Fatalf("the code is reading %.4f from a path measured at %.4f", channel.Loss, measured)
	}
	plan := p.coding().plan
	t.Logf("floor %.4f, measured %.4f: coded=%v rate=%.3f sized_for=%.4f residual=%.2e",
		conservativeFloor, measured, plan.Code, plan.Rate, plan.LossCoded, plan.Residual)
	if !plan.Code {
		t.Fatalf("a channel erasing %.1f%% was left uncoded: %s", measured*100, plan.Why)
	}
	if plan.LossCoded < measured {
		t.Fatalf("sized for %.4f on a channel measured at %.4f", plan.LossCoded, measured)
	}
	// The parity the incident actually shipped was 4.9% of the wire against
	// this erasure. Anything near that is the same failure with new code.
	if overhead := plan.Overhead(); overhead < 1.15 {
		t.Fatalf("%.1f%% erasure bought only %.2fx overhead", measured*100, overhead)
	}
}

// budgetedPipe is a carrier with a fixed byte allowance, which is what a
// congestion window is. It refuses once the allowance is gone, exactly as a
// QUIC connection stops packing datagrams once SendMode reports it may not
// send.
type budgetedPipe struct {
	mu        sync.Mutex
	remaining int
	sent      int
	bytes     int
	refused   int
	out       chan []byte
	in        chan []byte
	done      chan struct{}
	closed    bool
}

func newBudgetedPipes(budget int) (*budgetedPipe, *budgetedPipe) {
	a2b, b2a := make(chan []byte, 8192), make(chan []byte, 8192)
	return &budgetedPipe{remaining: budget, out: a2b, in: b2a, done: make(chan struct{})},
		&budgetedPipe{remaining: budget, out: b2a, in: a2b, done: make(chan struct{})}
}

func (p *budgetedPipe) Send(d []byte) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return errors.New("closed")
	}
	if len(d) > p.remaining {
		p.refused++
		p.mu.Unlock()
		// A window that is full is not a broken carrier. Report the datagram
		// as too large, which is the one refusal this layer already absorbs
		// without failing the path.
		return ErrDatagramTooLarge
	}
	p.remaining -= len(d)
	p.sent++
	p.bytes += len(d)
	p.mu.Unlock()
	select {
	case p.out <- append([]byte(nil), d...):
	default:
	}
	return nil
}

func (p *budgetedPipe) Receive() ([]byte, error) {
	select {
	case d := <-p.in:
		return d, nil
	case <-p.done:
		return nil, errors.New("closed")
	}
}

func (p *budgetedPipe) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.closed {
		p.closed = true
		close(p.done)
	}
	return nil
}

func (p *budgetedPipe) MaxDatagramSize() int { return 1200 }

func (p *budgetedPipe) totals() (datagrams, bytes int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.sent, p.bytes
}

// Parity costs a code rate, not a byte rate. This is the property the erasure
// floor was defending, and the reason sizing the code from the measured
// erasure instead of from a filtered floor is safe: a repair symbol crosses the
// same congestion window as a source symbol, so raising the estimate changes
// how a fixed window is spent and never how much goes on the wire.
//
// If it were false, sizing for 19.9% rather than 1.76% would be a 30% increase
// in offered load on a path that was already dropping packets -- which is the
// feedback loop 9aad61c records, the repair ratio climbing from 11% to 27% to
// 63% of sent bytes.
func TestParityCostsACodeRateAndNotAByteRate(t *testing.T) {
	const budget = 400 * 1024
	send := func(erasure float64) (wireBytes, payloadFrames int) {
		m := pathmodel.NewPathModel()
		m.Report(1, pathmodel.Observation{
			Erasure: erasure, BurstFactor: 1,
			ObservedSamples: 5000, Delivered: 2e6, RoundTrip: 200 * time.Millisecond,
		})
		pa, _ := newBudgetedPipes(budget)
		p := New(pa, Config{SymbolBytes: 1100, RoundTrip: 200 * time.Millisecond, Path: m})
		defer p.Close()

		frame := make([]byte, 900)
		for i := 0; i < 2000; i++ {
			if err := p.Send(frame); err != nil {
				break
			}
			payloadFrames++
		}
		// Let the send loop drain what it accepted.
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if _, b := pa.totals(); b >= budget-2*1200 {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		_, wireBytes = pa.totals()
		return wireBytes, payloadFrames
	}

	cleanWire, _ := send(0)
	lossyWire, _ := send(0.199)
	t.Logf("clean path put %d bytes on the wire; a 19.9%% erasure path put %d, against a %d budget",
		cleanWire, lossyWire, budget)

	if lossyWire > budget {
		t.Fatalf("a coded path put %d bytes through a %d byte window: parity is being added "+
			"on top of the budget rather than spent out of it", lossyWire, budget)
	}
	if cleanWire > budget {
		t.Fatalf("an uncoded path put %d bytes through a %d byte window", cleanWire, budget)
	}
	// And the coded path really did buy parity with that window rather than
	// simply sending less: it should have used essentially all of it.
	if lossyWire < budget/2 {
		t.Fatalf("the coded path used only %d of a %d byte window, so this proves nothing "+
			"about what it would do with a full one", lossyWire, budget)
	}
}

// A response is not one write. A gateway relays what each read from the
// destination returned, so a 100 KB body reaches the coded path as dozens of
// frames, and the send loop finds its queue empty between most of them. Each of
// those drains is a moment the producer paused, not the end of the exchange.
//
// Tail protection is for the end of an exchange: the last symbols of a burst
// have nothing following to cover them. Sizing it afresh at every drain, as
// though each were the last, buys the short-block residual over and over --
// measured on a gateway, 177 repairs for 304 datagrams. What the body needs is
// one burst's protection, topped up as it grows.
func TestTailProtectionCostsOneBurstNotOnePerWrite(t *testing.T) {
	parity := func(drainEvery bool) (sources, repairs uint64) {
		m := pathmodel.NewPathModel()
		m.Report(1, pathmodel.Observation{
			Erasure: 0.14, BurstFactor: 1,
			ObservedSamples: 5000, Delivered: 2e6, RoundTrip: 200 * time.Millisecond,
		})
		pa, _ := newPipes(1, 0)
		p := New(pa, Config{SymbolBytes: 1100, RoundTrip: 200 * time.Millisecond, Path: m, tailDefer: testTailDefer})
		defer p.Close()
		frame := make([]byte, 1000)
		for i := 0; i < 100; i++ {
			if err := p.Send(frame); err != nil {
				t.Fatal(err)
			}
			if drainEvery {
				waitDrained(t, p)
			}
		}
		waitDrained(t, p)
		// The tail is protected once the producer has stayed quiet.
		time.Sleep(2 * testTailDefer)
		stats := p.Stats()
		return stats.Sent - stats.Repairs, stats.Repairs
	}
	oneSources, oneRepairs := parity(false)
	manySources, manyRepairs := parity(true)
	t.Logf("one write: %d repairs for %d sources; 100 drained writes: %d repairs for %d sources",
		oneRepairs, oneSources, manyRepairs, manySources)
	if oneRepairs == 0 {
		t.Fatal("a 14% erasure path sent no parity at all; this test measures nothing")
	}
	if manyRepairs > oneRepairs*3/2+4 {
		t.Fatalf("the same body cost %d repairs written in pieces against %d written at once: "+
			"every drain is being protected as though it ended the exchange", manyRepairs, oneRepairs)
	}
}

// testTailDefer stands in for the production tail deferral, which is at most
// 5 ms. The test tells a drain from a pause by the time between writes, and on
// a loaded Windows runner a 2 ms sleep overran 5 ms often enough to protect a
// handful of drains -- 46 repairs where one burst needed 27. A deferral longer
// than any gap a runner puts between two writes measures the mechanism rather
// than the machine's timer.
const testTailDefer = 250 * time.Millisecond

// waitDrained returns once the send loop has taken everything queued and had a
// moment to protect it.
func waitDrained(t *testing.T, p *Path) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for len(p.pending) > 0 {
		if time.Now().After(deadline) {
			t.Fatal("the send loop did not drain")
		}
		time.Sleep(100 * time.Microsecond)
	}
	time.Sleep(2 * time.Millisecond)
}

// Deferring the tail must not lose it. A single short exchange -- the case tail
// protection exists for -- still gets its repairs, and gets them within the
// deferral rather than whenever the next exchange happens to come along.
func TestAShortExchangeIsStillProtectedPromptly(t *testing.T) {
	m := pathmodel.NewPathModel()
	m.Report(1, pathmodel.Observation{
		Erasure: 0.14, BurstFactor: 1,
		ObservedSamples: 5000, Delivered: 2e6, RoundTrip: 200 * time.Millisecond,
	})
	pa, _ := newPipes(1, 0)
	p := New(pa, Config{SymbolBytes: 1100, RoundTrip: 200 * time.Millisecond, Path: m})
	defer p.Close()
	if err := p.Send(make([]byte, 80)); err != nil {
		t.Fatal(err)
	}
	// The margin is for a loaded runner's timer, not for the design: what
	// this guards against is protection that waits for the next exchange.
	deadline := time.Now().Add(p.tailDefer() + 500*time.Millisecond)
	for p.Stats().Repairs == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("a single-symbol exchange on a 14%% path had no repair %v after it was sent",
				p.tailDefer()+500*time.Millisecond)
		}
		time.Sleep(200 * time.Microsecond)
	}
	if got := p.Stats().Repairs; got < 2 {
		t.Fatalf("a single-symbol exchange got %d repairs; the short-block residual asks for more", got)
	}
}

// boundedCarrier holds at most a fixed number of undelivered datagrams and drops
// the rest, which is what a QUIC connection does with received DATAGRAM frames
// nobody has read yet.
type boundedCarrier struct {
	queue   chan []byte
	dropped atomic.Int64
	once    sync.Once
	closed  chan struct{}
}

func newBoundedCarrier(depth int) *boundedCarrier {
	return &boundedCarrier{queue: make(chan []byte, depth), closed: make(chan struct{})}
}

func (c *boundedCarrier) deliver(d []byte) {
	select {
	case c.queue <- d:
	default:
		c.dropped.Add(1)
	}
}
func (c *boundedCarrier) Send([]byte) error { return nil }
func (c *boundedCarrier) Receive() ([]byte, error) {
	select {
	case d := <-c.queue:
		return d, nil
	case <-c.closed:
		return nil, io.EOF
	}
}
func (c *boundedCarrier) Close() error { c.once.Do(func() { close(c.closed) }); return nil }

// A consumer that is slow to take frames must not become erasure. The carrier
// discards what nobody reads, so a receive loop that stops reading while it
// waits on its consumer turns local backpressure into measured loss -- which
// sizes the peer's parity for a channel that delivered everything.
func TestASlowConsumerDoesNotStopTheCarrierBeingRead(t *testing.T) {
	sender, receiver := newPipes(1, 0)
	_ = receiver
	src := New(sender, Config{SymbolBytes: 1100, RoundTrip: 200 * time.Millisecond})
	defer src.Close()
	carrier := newBoundedCarrier(128)
	dst := New(carrier, Config{SymbolBytes: 1100, RoundTrip: 200 * time.Millisecond, Pending: 4})
	defer dst.Close()

	// Nobody reads dst's frames. A burst larger than both the consumer's
	// mailbox and the carrier's own queue arrives at once.
	for i := 0; i < 600; i++ {
		frame := make([]byte, 1000)
		binary.BigEndian.PutUint32(frame, uint32(i))
		if err := src.Send(frame); err != nil {
			t.Fatal(err)
		}
	}
	// Deliver in batches no larger than the carrier's queue, waiting for each
	// to be taken before the next, so what is measured is whether the path
	// keeps reading while its consumer is stalled -- not whether a goroutine
	// was scheduled in time on a loaded machine.
	got := 0
	for got < 600 {
		for batch := 0; batch < 100 && got < 600; batch++ {
			select {
			case d := <-sender.out:
				carrier.deliver(d)
				got++
			case <-time.After(2 * time.Second):
				t.Fatalf("the sender produced only %d datagrams", got)
			}
		}
		deadline := time.Now().Add(2 * time.Second)
		for len(carrier.queue) > 0 {
			if time.Now().After(deadline) {
				t.Fatalf("the path stopped reading its carrier after %d datagrams, with %d "+
					"left waiting: the receive loop is blocked on its consumer", got, len(carrier.queue))
			}
			time.Sleep(time.Millisecond)
		}
	}
	if dropped := carrier.dropped.Load(); dropped > 0 {
		t.Fatalf("the carrier discarded %d of %d datagrams while the path's consumer was slow", dropped, got)
	}
}
