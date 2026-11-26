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
	"github.com/bojieli/queqiao/internal/identity"
	"github.com/bojieli/queqiao/internal/metrics"
	"github.com/bojieli/queqiao/internal/protocol"
)

// shadowTrace drives a carrierMachine through synthetic progress samples.
type shadowTrace struct {
	t      *testing.T
	m      carrierMachine
	now    time.Time
	epoch  uint64
	last   time.Time
	events []carrierEvent
	// The transport's completed-episode counters.
	episodes uint64
	reaching [metrics.CarrierShadowPTOBuckets]uint64
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
	s.events = s.m.observe(carrierSample{At: s.now, Progress: carrierProgress{Epoch: s.epoch, LastProgress: s.last, PTOExpiries: ptos, Episodes: s.episodes, EpisodesReaching: s.reaching}}, s.events[:0])
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
	off.noteRescue(context.Background(), nil)
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

// The same watchdog stall on a carrier that has stopped being acknowledged
// is carrier evidence: the design would replace the carrier.
func TestCarrierShadowLabelsStallOnSilentCarrierForReplacement(t *testing.T) {
	registry := metrics.New()
	var logs lockedBuffer
	shadowCfg := CarrierShadowConfig{Enabled: true, Interval: time.Millisecond}
	if err := shadowCfg.resolve(); err != nil {
		t.Fatal(err)
	}
	observer := newCarrierShadow(shadowCfg, carrierShadowClient, slog.New(slog.NewJSONHandler(&logs, nil)), registry)
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
	if record["carrier_state"] != "replace" || record["legacy_remedy"] != "none" || record["endpoint"] != "client" {
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
	carrier := observer.track(&quicStreamConn{conn: conn}, carrierProgress{})
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
	// Only the client dials or rebinds; the gateway waits for its JOIN.
	for _, state := range []metrics.CarrierShadowState{metrics.CarrierShadowSuspect, metrics.CarrierShadowReplace, metrics.CarrierShadowDead} {
		obs := carrierObservation{tracked: true, state: state}
		if got := designStallRemedyFor(carrierShadowGateway, obs, false); got != metrics.CarrierShadowAwaitClient {
			t.Fatalf("gateway stall on %s = %s", state, got)
		}
	}
	if got := designLaneFailureRemedyFor(carrierShadowGateway, carrierObservation{tracked: true, state: metrics.CarrierShadowActive}); got != metrics.CarrierShadowAwaitClient {
		t.Fatalf("gateway lane failure on a live carrier = %s", got)
	}
	if got := designStallRemedyFor(carrierShadowGateway, carrierObservation{tracked: true, state: metrics.CarrierShadowActive}, true); got != metrics.CarrierShadowWaitMoveCoded {
		t.Fatalf("gateway coded stall on a live carrier = %s", got)
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

// recoverAfter acknowledges after the transport closed episodes with these
// peaks since the last sample, and counted them as the fork's progress export
// does.
func (s *shadowTrace) recoverAfter(peaks ...uint32) []carrierEvent {
	s.t.Helper()
	for _, peak := range peaks {
		s.episodes++
		for n := uint32(0); n < min(peak, metrics.CarrierShadowPTOBuckets); n++ {
			s.reaching[n]++
		}
	}
	s.now = s.now.Add(100 * time.Millisecond)
	s.epoch++
	s.last = s.now.Add(-10 * time.Millisecond)
	p := carrierProgress{Epoch: s.epoch, LastProgress: s.last, Episodes: s.episodes, EpisodesReaching: s.reaching}
	s.events = s.m.observe(carrierSample{At: s.now, Progress: p}, s.events[:0])
	return s.events
}

// The sampler sees two expiries; the transport reaches four and is
// acknowledged before the next sample (a delayed ACK, or a short RTT). The
// episode is closed at the peak the transport counted, not at the last
// sampled count, and an episode that opened and closed between two samples
// is counted too.
func TestCarrierShadowCountsPeaksTheSamplerMissed(t *testing.T) {
	trace := newShadowTrace(t, 2, 4)
	trace.step(true, 0)
	trace.wantState(metrics.CarrierShadowActive)
	trace.step(false, 2)
	episodes := episodesOf(trace.recoverAfter(4))
	if len(episodes) != 1 || episodes[0].PTOs != 4 || episodes[0].Outcome != episodeRecovered || episodes[0].BetweenSamples {
		t.Fatalf("episodes = %+v, want one recovered at 4", episodes)
	}
	if episodes[0].SinceProgress <= 0 {
		t.Fatalf("the followed episode lost its duration: %+v", episodes[0])
	}
	for n := 0; n < 4; n++ {
		if trace.m.reached[n] != 1 || trace.m.recovered[n] != 1 {
			t.Fatalf("level %d: reached %d recovered %d, want 1 and 1", n+1, trace.m.reached[n], trace.m.recovered[n])
		}
	}
	if trace.m.reached[4] != 0 || trace.m.episodes != 1 {
		t.Fatalf("over-counted: reached %v, episodes %d", trace.m.reached, trace.m.episodes)
	}
	// Two episodes the sampler never saw, between two samples.
	episodes = episodesOf(trace.recoverAfter(3, 1))
	if len(episodes) != 2 || episodes[0].PTOs != 3 || episodes[1].PTOs != 1 || !episodes[0].BetweenSamples {
		t.Fatalf("unsampled episodes = %+v", episodes)
	}
	if trace.m.reached[0] != 3 || trace.m.recovered[2] != 2 || trace.m.episodes != 3 {
		t.Fatalf("reached %v recovered %v episodes %d", trace.m.reached, trace.m.recovered, trace.m.episodes)
	}
	// A transport without the counters is followed by samples, as before.
	trace = newShadowTrace(t, 2, 4)
	trace.step(true, 0)
	trace.step(false, 2)
	if episodes := episodesOf(trace.step(true, 0)); len(episodes) != 1 || episodes[0].PTOs != 2 {
		t.Fatalf("sampled episode = %+v", episodes)
	}
}

// slowHandler is a log sink that takes delay to write each record.
type slowHandler struct {
	slog.Handler
	delay time.Duration
}

func (h slowHandler) Handle(ctx context.Context, r slog.Record) error {
	time.Sleep(h.delay)
	return h.Handler.Handle(ctx, r)
}

// The outage record is written off the recovery path: with the observer on
// and a slow log sink, a flow without a lane still fails at its published
// grace deadline, not the grace plus the time the write took.
func TestCarrierShadowOutageRecordDoesNotExtendTheGrace(t *testing.T) {
	shadowCfg := CarrierShadowConfig{Enabled: true}
	if err := shadowCfg.resolve(); err != nil {
		t.Fatal(err)
	}
	var logs lockedBuffer
	flow := newStallTestFlow(t, metrics.New())
	flow.carrierShadow = newCarrierShadow(shadowCfg, carrierShadowGateway,
		slog.New(slowHandler{Handler: slog.NewJSONHandler(&logs, nil), delay: 400 * time.Millisecond}), nil)
	const grace = 100 * time.Millisecond
	started := time.Now()
	err := flow.waitForHealthyLane(context.Background(), grace)
	waited := time.Since(started)
	if !errors.Is(err, errLaneReplacementTimeout) {
		t.Fatalf("err = %v", err)
	}
	if deadline := started.Add(grace); waited > grace+250*time.Millisecond {
		t.Fatalf("waited %s for a %s grace (deadline %s): the shadow record extended it", waited, grace, deadline.Format(time.StampMilli))
	}
	record := waitForRecord(t, &logs, "carrier shadow outage", 2*time.Second, func(map[string]any) bool { return true })
	if ms, ok := record["deadline_unix_ms"].(float64); !ok || int64(ms) > started.Add(grace+50*time.Millisecond).UnixMilli() {
		t.Fatalf("outage record = %v", record)
	}
}

// Episodes that end between a carrier being watched and its first sample are
// counted from the counters read at registration. An episode whose recovery
// sample saw several packets acknowledged has only an upper bound on its
// duration; one acknowledged packet makes it exact.
func TestCarrierShadowSeedsCountersAndBoundsDurations(t *testing.T) {
	m := newCarrierMachine(2, 4)
	m.seed(carrierProgress{Epoch: 10, Episodes: 5})
	var reach [metrics.CarrierShadowPTOBuckets]uint64
	reach[0], reach[1] = 1, 1
	start := time.Unix(1_000_000, 0)
	events := m.observe(carrierSample{At: start, Progress: carrierProgress{Epoch: 12, LastProgress: start, Episodes: 6, EpisodesReaching: reach}}, nil)
	if episodes := episodesOf(events); len(episodes) != 1 || episodes[0].PTOs != 2 || !episodes[0].BetweenSamples {
		t.Fatalf("an episode before the first sample = %+v", episodes)
	}
	if m.reached[1] != 1 || m.recovered[1] != 1 {
		t.Fatalf("reached %v recovered %v", m.reached, m.recovered)
	}

	for _, acked := range []uint64{1, 3} {
		trace := newShadowTrace(t, 2, 4)
		trace.step(true, 0)
		trace.step(false, 2)
		trace.epoch += acked - 1 // recoverAfter acknowledges one more
		episodes := episodesOf(trace.recoverAfter(2))
		if len(episodes) != 1 || episodes[0].DurationUpperBound != (acked > 1) {
			t.Fatalf("%d acknowledged: episodes = %+v", acked, episodes)
		}
	}
}

// The transport's last bucket counts its peak or more: a single episode
// takes its exact peak from the transport, several are marked lower bounds.
func TestCarrierShadowPeakPastTheLastBucket(t *testing.T) {
	trace := newShadowTrace(t, 2, 4)
	trace.step(true, 0)
	trace.step(false, 3)
	trace.episodes++
	for n := range trace.reaching {
		trace.reaching[n]++
	}
	trace.now = trace.now.Add(100 * time.Millisecond)
	trace.epoch++
	events := trace.m.observe(carrierSample{At: trace.now, Progress: carrierProgress{Epoch: trace.epoch, LastProgress: trace.now,
		Episodes: trace.episodes, EpisodesReaching: trace.reaching, LastEpisodePeak: 19}}, nil)
	if episodes := episodesOf(events); len(episodes) != 1 || episodes[0].PTOs != 19 || episodes[0].PeakLowerBound {
		t.Fatalf("one episode past the last bucket = %+v", episodes)
	}
	episodes := episodesOf(trace.recoverAfter(20, 18))
	if len(episodes) != 2 || episodes[0].PTOs != metrics.CarrierShadowPTOBuckets || !episodes[0].PeakLowerBound || !episodes[1].PeakLowerBound {
		t.Fatalf("two episodes past the last bucket = %+v", episodes)
	}
}

// An outage and its end carry the same identifier, so a reader can pair
// them even when they fall in the same millisecond and are logged in either
// order.
func TestCarrierShadowOutageEndNamesItsOutage(t *testing.T) {
	shadowCfg := CarrierShadowConfig{Enabled: true}
	if err := shadowCfg.resolve(); err != nil {
		t.Fatal(err)
	}
	var logs lockedBuffer
	flow := newStallTestFlow(t, metrics.New())
	flow.carrierShadow = newCarrierShadow(shadowCfg, carrierShadowClient, slog.New(slog.NewJSONHandler(&logs, nil)), nil)
	if flow.replacementBudget(time.Now(), time.Minute) != time.Minute {
		t.Fatal("no outage opened")
	}
	flow.endReplacementOutage()
	anyRecord := func(map[string]any) bool { return true }
	started := waitForRecord(t, &logs, "carrier shadow outage", 2*time.Second, anyRecord)
	ended := waitForRecord(t, &logs, "carrier shadow outage ended", 2*time.Second, anyRecord)
	if started == nil || ended == nil {
		t.Fatalf("records: %v %v", started, ended)
	}
	if id, ok := started["outage_id"].(float64); !ok || id == 0 || ended["outage_id"] != id {
		t.Fatalf("outage %v, end %v: not paired", started["outage_id"], ended["outage_id"])
	}
}
