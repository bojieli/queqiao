package pep

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apernet/quic-go"
	"github.com/bojieli/queqiao/internal/coded"
	"github.com/bojieli/queqiao/internal/identity"
	"github.com/bojieli/queqiao/internal/metrics"
	"github.com/bojieli/queqiao/internal/pathmodel"
	"github.com/bojieli/queqiao/internal/protocol"
	"github.com/bojieli/queqiao/internal/stripe"
)

// shadowTrace drives a carrierMachine through synthetic progress samples.
type shadowTrace struct {
	t      *testing.T
	m      carrierMachine
	now    time.Time
	epoch  uint64
	last   time.Time
	events []carrierEvent
}

func newShadowTrace(t *testing.T, ks, kp uint32) *shadowTrace {
	start := time.Unix(1_000_000, 0)
	return &shadowTrace{t: t, m: newCarrierMachine(ks, kp), now: start, last: start}
}

// step advances the clock, optionally acknowledges, and samples with ptos
// expiries since progress.
func (s *shadowTrace) step(progress bool, ptos uint32) []carrierEvent {
	s.now = s.now.Add(100 * time.Millisecond)
	if progress {
		s.epoch++
		s.last = s.now
	}
	s.events = s.m.observe(carrierSample{At: s.now, Progress: carrierProgress{Epoch: s.epoch, LastProgress: s.last, PTOExpiries: ptos}}, s.events[:0])
	return s.events
}

func (s *shadowTrace) end(err error) []carrierEvent {
	s.now = s.now.Add(100 * time.Millisecond)
	s.events = s.m.observe(carrierSample{At: s.now, Progress: carrierProgress{Epoch: s.epoch, LastProgress: s.last}, Ended: err}, s.events[:0])
	return s.events
}

func (s *shadowTrace) wantState(want metrics.CarrierShadowState) {
	s.t.Helper()
	if s.m.state != want {
		s.t.Fatalf("state = %s, want %s", s.m.state, want)
	}
}

func transitionsOf(events []carrierEvent) []string {
	var out []string
	for _, e := range events {
		if e.Kind == carrierTransition {
			out = append(out, e.From.String()+">"+e.To.String())
		}
	}
	return out
}

func episodesOf(events []carrierEvent) []carrierEvent {
	var out []carrierEvent
	for _, e := range events {
		if e.Kind == carrierEpisodeEnd {
			out = append(out, e)
		}
	}
	return out
}

// A carrier that keeps being acknowledged, including through single probe
// timeouts below k_s, never leaves ACTIVE.
func TestCarrierShadowHealthyCarrierStaysActive(t *testing.T) {
	trace := newShadowTrace(t, 2, 4)
	for i := 0; i < 50; i++ {
		ptos := uint32(0)
		if i%7 == 3 {
			ptos = 1 // a tail probe that is answered
		}
		if events := trace.step(i%7 != 3, ptos); len(transitionsOf(events)) != 0 {
			t.Fatalf("step %d: unexpected transitions %v", i, transitionsOf(events))
		}
	}
	trace.wantState(metrics.CarrierShadowActive)
	if trace.m.reached[0] == 0 || trace.m.reached[0] != trace.m.recovered[0] {
		t.Fatalf("one-expiry episodes reached %d recovered %d; every one recovered", trace.m.reached[0], trace.m.recovered[0])
	}
}

// k_s expiries make a carrier SUSPECT, k_p make it REPLACE, and progress
// after that is a false replacement: the promotion the design would have
// made in vain, which is exactly what calibrating k_p measures.
func TestCarrierShadowSuspectReplaceAndFalseReplacement(t *testing.T) {
	trace := newShadowTrace(t, 2, 4)
	trace.step(true, 0)
	if got := transitionsOf(trace.step(false, 1)); len(got) != 0 {
		t.Fatalf("one expiry moved the carrier: %v", got)
	}
	if got := transitionsOf(trace.step(false, 2)); len(got) != 1 || got[0] != "active>suspect" {
		t.Fatalf("k_s expiries: %v", got)
	}
	trace.step(false, 3)
	trace.wantState(metrics.CarrierShadowSuspect)
	events := trace.step(false, 4)
	if got := transitionsOf(events); len(got) != 1 || got[0] != "suspect>replace" {
		t.Fatalf("k_p expiries: %v", got)
	}
	if events[0].PTOs != 4 || events[0].SinceProgress != 400*time.Millisecond {
		t.Fatalf("replace event = %+v", events[0])
	}
	events = trace.step(true, 0)
	if got := transitionsOf(events); len(got) != 1 || got[0] != "replace>active" {
		t.Fatalf("progress after replace: %v", got)
	}
	var falseReplacement bool
	for _, e := range events {
		falseReplacement = falseReplacement || e.FalseReplacement
	}
	if !falseReplacement {
		t.Fatal("progress after REPLACE was not marked a false replacement")
	}
	episodes := episodesOf(events)
	if len(episodes) != 1 || episodes[0].PTOs != 4 || episodes[0].Outcome != episodeRecovered {
		t.Fatalf("episode = %+v", episodes)
	}
	for n := 1; n <= 4; n++ {
		if trace.m.reached[n-1] != 1 || trace.m.recovered[n-1] != 1 {
			t.Fatalf("histogram at %d: reached %d recovered %d", n, trace.m.reached[n-1], trace.m.recovered[n-1])
		}
	}
	if trace.m.reached[4] != 0 {
		t.Fatal("histogram counted an expiry the episode never reached")
	}
}

// Progress and the first expiries of a new episode can land between two
// samples. The old episode ends recovered, the carrier returns to ACTIVE, and
// the new episode is counted from the expiries it has, never mixed with the
// old one.
func TestCarrierShadowProgressBetweenSamplesStartsAFreshEpisode(t *testing.T) {
	trace := newShadowTrace(t, 2, 4)
	trace.step(true, 0)
	trace.step(false, 3)
	trace.wantState(metrics.CarrierShadowSuspect)
	events := trace.step(true, 2)
	if got := transitionsOf(events); len(got) != 2 || got[0] != "suspect>active" || got[1] != "active>suspect" {
		t.Fatalf("transitions = %v", got)
	}
	if episodes := episodesOf(events); len(episodes) != 1 || episodes[0].PTOs != 3 {
		t.Fatalf("ended episodes = %+v", episodes)
	}
	if trace.m.episodeMax != 2 {
		t.Fatalf("new episode max = %d, want 2", trace.m.episodeMax)
	}
	// k_p counts the new episode only: its 4th expiry, not the 3+1st.
	if got := transitionsOf(trace.step(false, 3)); len(got) != 0 {
		t.Fatalf("expiries leaked across episodes: %v", got)
	}
}

// Expiries that jump past both thresholds in one sample produce both
// transitions, in order, and count every level they passed.
func TestCarrierShadowJumpPastBothThresholds(t *testing.T) {
	trace := newShadowTrace(t, 2, 3)
	trace.step(true, 0)
	if got := transitionsOf(trace.step(false, 5)); len(got) != 2 || got[0] != "active>suspect" || got[1] != "suspect>replace" {
		t.Fatalf("transitions = %v", got)
	}
	for n := 0; n < 5; n++ {
		if trace.m.reached[n] != 1 {
			t.Fatalf("reached[%d] = %d", n+1, trace.m.reached[n])
		}
	}
}

// An idle timeout ends the carrier DEAD and its episode counts as a true
// failure at every level it reached. Nothing after that changes the record.
func TestCarrierShadowIdleTimeoutIsDead(t *testing.T) {
	trace := newShadowTrace(t, 2, 4)
	trace.step(true, 0)
	trace.step(false, 4)
	events := trace.end(&quic.IdleTimeoutError{})
	if got := transitionsOf(events); len(got) != 1 || got[0] != "replace>dead" {
		t.Fatalf("transitions = %v", got)
	}
	if events[len(events)-1].Reason != "idle_timeout" {
		t.Fatalf("reason = %q", events[len(events)-1].Reason)
	}
	if episodes := episodesOf(events); len(episodes) != 1 || episodes[0].Outcome != episodeDead {
		t.Fatalf("episodes = %+v", episodes)
	}
	if trace.m.dead[3] != 1 || trace.m.recovered[3] != 0 {
		t.Fatal("a dead episode was not counted as one")
	}
	if events := trace.step(true, 0); len(events) != 0 {
		t.Fatalf("a dead carrier changed: %+v", events)
	}
	trace.wantState(metrics.CarrierShadowDead)
}

// An application close is a decision, not path evidence: the carrier is
// CLOSED, and an episode it cuts short is censored rather than counted as a
// failure, so today's own carrier retirement cannot bias the calibration.
func TestCarrierShadowApplicationCloseIsCensored(t *testing.T) {
	trace := newShadowTrace(t, 2, 4)
	trace.step(true, 0)
	trace.step(false, 2)
	events := trace.end(&quic.ApplicationError{ErrorCode: 0})
	if got := transitionsOf(events); len(got) != 1 || got[0] != "suspect>closed" {
		t.Fatalf("transitions = %v", got)
	}
	if episodes := episodesOf(events); len(episodes) != 1 || episodes[0].Outcome != episodeClosed {
		t.Fatalf("episodes = %+v", episodes)
	}
	if trace.m.dead[0] != 0 || trace.m.recovered[0] != 0 {
		t.Fatal("a censored episode was counted")
	}
	if got := carrierEndReason(&quic.ApplicationError{Remote: true}); got != "peer_close" {
		t.Fatalf("remote application close reason = %q", got)
	}
	if got := carrierEndState(&quic.StatelessResetError{}); got != metrics.CarrierShadowDead {
		t.Fatalf("stateless reset = %s, want dead", got)
	}
}

func TestCarrierShadowConfig(t *testing.T) {
	cfg := CarrierShadowConfig{Enabled: true}
	if err := cfg.resolve(); err != nil {
		t.Fatal(err)
	}
	if cfg.SuspectPTOs != defaultCarrierShadowSuspectPTOs || cfg.ReplacePTOs != defaultCarrierShadowReplacePTOs || cfg.Interval != defaultCarrierShadowInterval {
		t.Fatalf("defaults = %+v", cfg)
	}
	cfg = CarrierShadowConfig{SuspectPTOs: 6}
	if err := cfg.resolve(); err != nil || cfg.ReplacePTOs != 6 {
		t.Fatalf("k_p default below a configured k_s: %+v, %v", cfg, err)
	}
	for _, bad := range [][2]uint32{{3, 2}, {2, 17}, {17, 0}} {
		if err := ValidateCarrierShadow(bad[0], bad[1]); err == nil {
			t.Fatalf("k_s=%d k_p=%d accepted", bad[0], bad[1])
		}
	}
	if newCarrierShadow(CarrierShadowConfig{}, carrierShadowClient, nil, nil) != nil {
		t.Fatal("a disabled observer was built")
	}
	var off *carrierShadow
	off.bindLane(nil, nil)
	off.noteStall(nil, nil, time.Time{}, 0, false, false)
	off.noteRescue(nil)
	if obs := off.observeLane(nil); obs.tracked {
		t.Fatal("a disabled observer observed")
	}
}

// fakeCarrier is a carrier whose transport keeps acknowledging: every read
// is a new progress epoch.
type fakeCarrier struct {
	epoch atomic.Uint64
	stall atomic.Bool
	ended atomic.Pointer[error]
}

func (c *fakeCarrier) carrierKey() any { return c }

func (c *fakeCarrier) carrierProgress() (carrierProgress, error) {
	p := carrierProgress{Epoch: c.epoch.Load(), SmoothedRTT: 10 * time.Millisecond}
	if !c.stall.Load() {
		p.Epoch = c.epoch.Add(1)
		p.LastProgress = time.Now()
	} else {
		p.PTOExpiries = 5
		p.ElicitingOutstanding = true
	}
	if err := c.ended.Load(); err != nil {
		return p, *err
	}
	return p, nil
}

// shadowCaptureConn is the coded-tail fixture's control stream on a fake
// carrier.
type shadowCaptureConn struct {
	*ackCaptureConn
	*fakeCarrier
}

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// The coded tail of TestCodedSendStallRecoversOnExistingStream, observed: a
// datagram tail is lost on a carrier QUIC keeps acknowledging. The watchdog
// stalls and today's recovery escapes the bytes to the stream; the observer
// labels the carrier live, records that the transport was acknowledged during
// the stall, and says the design would move the coded bytes to the stream and
// wait -- not replace the carrier. Everything the original test asserts about
// today's recovery still holds with the observer on.
func TestCarrierShadowLabelsCodedTailStallALiveCarrier(t *testing.T) {
	registry := metrics.New()
	var logs lockedBuffer
	shadowCfg := CarrierShadowConfig{Enabled: true, Interval: time.Millisecond}
	if err := shadowCfg.resolve(); err != nil {
		t.Fatal(err)
	}
	flow := newStallTestFlow(t, registry)
	flow.carrierShadow = newCarrierShadow(shadowCfg, carrierShadowClient, slog.New(slog.NewJSONHandler(&logs, nil)), registry)
	flow.stallGrace = 10 * time.Millisecond
	flow.stallScan = time.Millisecond
	model := pathmodel.NewPathModel()
	model.Report(1, pathmodel.Observation{Erasure: .2, BurstFactor: 1, ObservedSamples: 5000, Delivered: 2e6})
	path := coded.New(newPipeCarrier(), coded.Config{Path: model})
	defer path.Close()
	capture := newAckCaptureConn(0, nil)
	defer capture.Close()
	carrier := &fakeCarrier{}
	fc := newSplitFrameConn(&shadowCaptureConn{ackCaptureConn: capture, fakeCarrier: carrier}, path)
	fc.setCodingPolicy(flow.prefersCodingOverRetransmission)
	lane := &mpLane{id: 0, kind: TransportQUIC, fc: fc, writeQ: make(chan laneFrame, 2), writeDone: make(chan struct{})}
	lane.ready.Store(true)
	flow.lanes[0] = lane
	flow.carrierShadow.bindLane(flow, lane)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	now := time.Now()
	sched := stripe.New(bytes.NewReader([]byte("response")), stripe.Config{
		Reliable:        func(uint64) bool { return !fc.codesData() },
		ForceReliable:   flow.recoverChunkReliably,
		RetransmitAfter: func() time.Duration { return time.Millisecond }, Now: func() time.Time { return now },
	})
	defer sched.Close()
	flow.scheduler.Store(sched)
	first, err := sched.Next(ctx, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if first.Reliable {
		t.Fatal("fixture must begin on coded datagrams")
	}
	if err = flow.sendChunk(ctx, lane, first, true); err != nil {
		t.Fatal(err)
	}
	sent := <-lane.writeQ
	sent.onWritten(false)
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() { flow.stallWatchdog(stop); close(done) }()
	defer func() { close(stop); <-done }()
	select {
	case <-flow.stallSignals():
	case <-ctx.Done():
		t.Fatal("no stall detected")
	}
	// Today's recovery is unchanged: the stalled bytes escape to the stream.
	now = now.Add(time.Second)
	if sched.ReissueExpired() != 1 {
		t.Fatal("lost response was not reissued")
	}
	retry, err := sched.Next(ctx, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !retry.Reliable {
		t.Fatal("stalled response keeps retrying unreliable datagrams; no stream escape")
	}
	if !lane.suspected.Load() {
		t.Fatal("today's watchdog no longer demotes the lane")
	}

	got := registry.Snapshot().CarrierShadow
	if got.Stalls[metrics.CarrierShadowWaitMoveCoded] != 1 || got.StallsTransportAcked != 1 {
		t.Fatalf("shadow stalls = %+v, want one wait_move_coded with the transport acknowledged", got)
	}
	if got.Stalls[metrics.CarrierShadowReplaceCarrier] != 0 || got.Transitions[metrics.CarrierShadowSuspect] != 0 {
		t.Fatalf("a live carrier was suspected: %+v", got)
	}
	record := findLogRecord(t, logs.String(), "carrier shadow stall")
	for key, want := range map[string]any{
		"carrier_state": "active", "transport_acked": true, "design_remedy": "wait_move_coded",
		"legacy_remedy": "demote_and_rescue_dial", "escape_coded": true, "flow_id": float64(7),
	} {
		if record[key] != want {
			t.Fatalf("stall record %s = %v, want %v; record %v", key, record[key], want, record)
		}
	}
}

// The same watchdog stall on a carrier that has stopped being acknowledged
// is carrier evidence: the design would replace the carrier.
func TestCarrierShadowLabelsStallOnSilentCarrierForReplacement(t *testing.T) {
	registry := metrics.New()
	var logs lockedBuffer
	shadowCfg := CarrierShadowConfig{Enabled: true, Interval: time.Millisecond}
	if err := shadowCfg.resolve(); err != nil {
		t.Fatal(err)
	}
	observer := newCarrierShadow(shadowCfg, carrierShadowGateway, slog.New(slog.NewJSONHandler(&logs, nil)), registry)
	flow := newStallTestFlow(t, registry)
	carrier := &fakeCarrier{}
	capture := newAckCaptureConn(0, nil)
	defer capture.Close()
	lane := &mpLane{id: 3, kind: TransportQUIC, fc: newFrameConn(&shadowCaptureConn{ackCaptureConn: capture, fakeCarrier: carrier})}
	observer.bindLane(flow, lane)
	pendingSince := time.Now()
	carrier.stall.Store(true)
	time.Sleep(30 * time.Millisecond)
	observer.noteStall(flow, lane, pendingSince, 20*time.Millisecond, false, false)
	got := registry.Snapshot().CarrierShadow
	if got.Stalls[metrics.CarrierShadowReplaceCarrier] != 1 || got.StallsTransportAcked != 0 {
		t.Fatalf("shadow stalls = %+v, want one replace_carrier without transport progress", got)
	}
	record := findLogRecord(t, logs.String(), "carrier shadow stall")
	if record["carrier_state"] != "replace" || record["legacy_remedy"] != "demote" || record["endpoint"] != "gateway" {
		t.Fatalf("stall record = %v", record)
	}
	// A lane failure on the silent carrier is carrier evidence too; once the
	// carrier acknowledges again, the same failure is a stream to rebind.
	observer.noteLaneFailure(flow, lane, errors.New("reset"), observer.observeLane(lane))
	carrier.stall.Store(false)
	observer.noteLaneFailure(flow, lane, errors.New("reset"), observer.observeLane(lane))
	got = registry.Snapshot().CarrierShadow
	if got.LaneFailures[metrics.CarrierShadowReplaceCarrier] != 1 || got.LaneFailures[metrics.CarrierShadowRebindStream] != 1 {
		t.Fatalf("lane failures = %+v", got.LaneFailures)
	}
	if got.FalseReplacements != 1 {
		t.Fatalf("false replacements = %d, want 1", got.FalseReplacements)
	}
	// The sampler stops when its last carrier ends.
	ended := error(&quic.IdleTimeoutError{})
	carrier.ended.Store(&ended)
	deadline := time.Now().Add(2 * time.Second)
	for observer.activeCarriers() != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if observer.activeCarriers() != 0 {
		t.Fatal("an ended carrier is still watched")
	}
	summary := findLogRecord(t, logs.String(), "carrier shadow carrier ended")
	if summary["state"] != "dead" {
		t.Fatalf("summary = %v", summary)
	}
}

// findLogRecord returns the first JSON log record with this message.
func findLogRecord(t *testing.T, logs, msg string) map[string]any {
	t.Helper()
	for _, line := range strings.Split(logs, "\n") {
		if line == "" {
			continue
		}
		record := map[string]any{}
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("log line %q: %v", line, err)
		}
		if record["msg"] == msg {
			return record
		}
	}
	t.Fatalf("no %q record in logs:\n%s", msg, logs)
	return nil
}

// BenchmarkCarrierShadowSample is the observer's per-carrier cost: one read of
// a live QUIC connection's progress snapshot and one state-machine step. The
// sampler pays it once per carrier per interval (default 50 ms); a stall or a
// lane failure pays it once.
func BenchmarkCarrierShadowSample(b *testing.B) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	serverCredentials, clientCredentials := testCertificate(b)
	serverTLS, err := identity.ServerTLSConfig(serverCredentials, protocol.DataALPN, false)
	if err != nil {
		b.Fatal(err)
	}
	clientTLS, err := identity.ClientTLSConfig(clientCredentials, protocol.DataALPN)
	if err != nil {
		b.Fatal(err)
	}
	listener, err := quic.ListenAddr("127.0.0.1:0", serverTLS, quicConfig(flowWindows{}))
	if err != nil {
		b.Fatal(err)
	}
	defer listener.Close()
	conn, err := quic.DialAddr(ctx, listener.Addr().String(), clientTLS, quicConfig(flowWindows{}))
	if err != nil {
		b.Fatal(err)
	}
	defer conn.CloseWithError(0, "benchmark complete")
	cfg := CarrierShadowConfig{Enabled: true, Interval: time.Hour}
	if err := cfg.resolve(); err != nil {
		b.Fatal(err)
	}
	observer := newCarrierShadow(cfg, carrierShadowClient, nil, metrics.New())
	carrier := observer.track(&quicStreamConn{conn: conn})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		observer.sample(carrier)
	}
}

// Review fixes: SUSPECT has its own remedies; a terminal sample's expiries
// count; a recovered episode lasts from the previous progress to the
// recovering one; an older snapshot cannot rewind the epoch.
func TestCarrierShadowRemediesAndEpisodeAccounting(t *testing.T) {
	suspect := carrierObservation{tracked: true, state: metrics.CarrierShadowSuspect}
	if got := designStallRemedy(suspect, true); got != metrics.CarrierShadowDialStandby {
		t.Fatalf("stall on SUSPECT = %s", got)
	}
	if got := designLaneFailureRemedy(suspect); got != metrics.CarrierShadowWaitUnbound {
		t.Fatalf("lane failure on SUSPECT = %s", got)
	}
	if got := designStallRemedy(carrierObservation{tracked: true, state: metrics.CarrierShadowReplace}, false); got != metrics.CarrierShadowReplaceCarrier {
		t.Fatalf("stall on REPLACE = %s", got)
	}
	if (carrierObservation{tracked: true, state: metrics.CarrierShadowDead}).live() {
		t.Fatal("a dead carrier is live")
	}

	trace := newShadowTrace(t, 2, 4)
	trace.step(true, 0)
	trace.step(false, 2)
	// The carrier dies with a fifth expiry the sampler never saw on its own.
	trace.now = trace.now.Add(100 * time.Millisecond)
	events := trace.m.observe(carrierSample{At: trace.now, Progress: carrierProgress{Epoch: trace.epoch, LastProgress: trace.last, PTOExpiries: 5}, Ended: &quic.IdleTimeoutError{}}, nil)
	if episodes := episodesOf(events); len(episodes) != 1 || episodes[0].PTOs != 5 || trace.m.dead[4] != 1 {
		t.Fatalf("terminal expiries not counted: %+v, dead %v", episodes, trace.m.dead)
	}

	trace = newShadowTrace(t, 2, 4)
	trace.step(true, 0)
	start := trace.last
	trace.step(false, 3)
	trace.step(false, 3)
	// Progress at a time the sampler sees only later.
	trace.epoch++
	trace.last = start.Add(250 * time.Millisecond)
	trace.now = trace.now.Add(time.Second)
	events = trace.m.observe(carrierSample{At: trace.now, Progress: carrierProgress{Epoch: trace.epoch, LastProgress: trace.last}}, nil)
	if episodes := episodesOf(events); len(episodes) != 1 || episodes[0].SinceProgress != 250*time.Millisecond {
		t.Fatalf("recovered episode = %+v", episodes)
	}
	// A stale snapshot from before that progress changes nothing.
	if events := trace.m.observe(carrierSample{At: trace.now, Progress: carrierProgress{Epoch: trace.epoch - 1, PTOExpiries: 0}}, nil); len(events) != 0 || trace.m.epoch != trace.epoch {
		t.Fatalf("stale snapshot applied: %+v", events)
	}
}
