package pep

import (
	"testing"
	"time"

	"github.com/bojieli/queqiao/internal/pathsim"
)

// An idle flow's carrier under recovery=carrier, when the transport's own
// timers have been stretched by a stale round-trip estimate.
//
// The live idle-wake session that found this lost every connection idle for
// sixteen minutes under recovery=carrier, and none under legacy recovery. A
// stall of some ten seconds left round-trip samples of many seconds in the
// idle carrier's estimate: the client's probe timeout was 33 s. The
// transport spaces its keep-alives max(KeepAlivePeriod, 1.5 PTO) apart and
// times out after max(MaxIdleTimeout, 3 PTO) of silence, and an idle carrier
// whose peer's keep-alives arrive first sends nothing ack-eliciting, so it
// takes no sample that would bring the estimate back. The client's
// keep-alives were 50 s apart and its idle timeout 100 s. When the path then
// went away for half a minute, the gateway closed the carrier at its own
// timeout and kept the flows for its 45 s grace; the client noticed 152 s
// after the gateway did, and its JOINs met "session ended".
//
// These tests give both endpoints those timers directly (quicTimersForTest)
// rather than reproduce a stall: a stale estimate cannot be produced on
// demand, since a packet whose frames a probe timeout retransmitted is no
// longer a round-trip sample. They check the two things an idle carrier owes
// its flows whatever its transport's timers are: it crosses the path often
// enough to keep a middlebox's mapping, and it is found dead within the
// detection bound the binding deadlines assume.

// staleTimers are the live session's: a keep-alive every 1.5 PTO and an idle
// timeout of 3 PTO, for the 33 s probe timeout its client had.
var staleTimers = quicTimers{keepAlive: 50 * time.Second, idle: 100 * time.Second}

// newIdleFlow starts one flow on a carrier with the given transport timers
// (nil for the configured ones), carries some data on it and leaves it idle.
func newIdleFlow(t *testing.T, path pathsim.Config, timers *quicTimers) (*carrierPair, *echoStream) {
	t.Helper()
	if timers != nil {
		quicTimersForTest.Store(timers)
		t.Cleanup(func() { quicTimersForTest.Store(nil) })
	}
	o := carrierPairOptions{path: path}
	if timers != nil {
		// The estimate itself is not stale here, so the carrier's probe
		// timeouts still come a fraction of a second apart. With the live
		// session's estimate the second was more than a minute after the
		// first: k_s and k_p at their maximum stand for that.
		o.recovery = CarrierRecoveryConfig{SuspectPTOs: maxCarrierShadowPTOs, ReplacePTOs: maxCarrierShadowPTOs}
	}
	p := newCarrierPair(t, o)
	e := startEchoStream(t, p, 512, 20*time.Millisecond)
	e.waitEchoed(t, 8192, 20*time.Second)
	e.pause.Store(true)
	// What was written before the pause took effect comes back.
	time.Sleep(200 * time.Millisecond)
	e.waitEchoed(t, e.written.Load(), 10*time.Second)
	return p, e
}

// moved is how many times the manager moved a flow or replaced, or tried to
// replace, a carrier.
func (m *carrierManager) moved() int64 {
	return m.suspects.Load() + m.promotions.Load() + m.standbyDials.Load() + m.replacementDials.Load() +
		m.sameCarrierJoins.Load() + m.newCarrierJoins.Load() + m.silentDeaths.Load()
}

// keepAlivesWithin is how many keep-alive PINGs one endpoint's adaptive
// schedule (design §10.2: from the floor, doubling to the keep-alive period)
// sends in window, starting from the floor.
func keepAlivesWithin(window, floor, period time.Duration) int {
	n := 0
	for at, step := floor, floor; at <= window; n++ {
		step = min(2*step, period)
		at += step
	}
	return n
}

// A healthy idle carrier is kept by the transport's own keep-alive, as under
// legacy recovery: the manager adds no traffic to it.
func TestCarrierManagerAddsNothingToAHealthyIdleCarrier(t *testing.T) {
	p, e := newIdleFlow(t, pathsim.Config{OneWayDelay: 20 * time.Millisecond, Seed: 7}, nil)
	m := p.client.carriers
	up, _ := p.relay.Stats()
	probes, sent := m.pathProbes.Load(), up.PacketsIn
	const idle = 2 * carrierQuietProbeAfter
	time.Sleep(idle)
	if got := m.pathProbes.Load() - probes; got != 0 {
		t.Fatalf("the manager probed a healthy idle carrier %d times in %v", got, idle)
	}
	// What the client sends is then the transport's keep-alive schedule and
	// nothing else. Each endpoint's schedule starts at its floor when the
	// flow's last payload crosses and doubles to the five-second period:
	// 0.5, 1.5, 3.5, 7.5 and 12.5 s in, five PINGs in this window. The two
	// schedules interleave (each is timed from the last packet that endpoint
	// heard), so the client sends at most its own five PINGs and five
	// acknowledgements of the gateway's. The bound is twice that: the
	// schedule starts again from its floor if a late frame of the flow (an
	// acknowledgement the runner was slow to send) crosses inside the
	// window, and it can do so once. Fifteen idle seconds measured 8 packets
	// on a quiet host and 11 to 13 on a loaded CI runner. A manager probing
	// this carrier would be caught by the count above first; this bound is
	// for anything else that starts sending on an idle carrier, at a packet
	// a second or more.
	up, _ = p.relay.Stats()
	schedule := keepAlivesWithin(idle, keepAliveFloorMin, 5*time.Second)
	if got, most := up.PacketsIn-sent, uint64(2*2*schedule); got > most {
		t.Fatalf("an idle carrier sent %d packets in %v, want at most %d (%d keep-alives a side, acknowledged, and one restart of the schedule)", got, idle, most, schedule)
	}
	if got := m.moved(); got != 0 {
		t.Fatalf("an idle flow on a live path was moved or its carrier replaced (%d):\n%s", got, p.clientLogs.String())
	}
	before := e.echoed.Load()
	e.pause.Store(false)
	e.waitEchoed(t, before+2048, 5*time.Second)
}

// The path's relay forgets a mapping nothing has crossed for ten seconds: a
// forwarding relay's idle timeout, scaled down, or a NAT's. Neither
// endpoint's transport sends a keep-alive that often any more, so without the
// manager's own bound the mapping expires under the idle flow. With it the
// carrier hears nothing for at most carrierQuietProbeAfter before it probes:
// the mapping is never lost, the carrier is the one the flow started on, and
// the wake is delivered at once.
func TestCarrierIdleFlowOutlivesARelayIdleTimeout(t *testing.T) {
	const relayIdle = 10 * time.Second
	if relayIdle <= carrierQuietProbeAfter || relayIdle >= staleTimers.keepAlive {
		t.Fatalf("the relay's idle timeout (%v) must lie between the manager's bound (%v) and the transport's keep-alive (%v)",
			relayIdle, carrierQuietProbeAfter, staleTimers.keepAlive)
	}
	p, e := newIdleFlow(t, pathsim.Config{OneWayDelay: 20 * time.Millisecond, Seed: 11, SourceIdleTimeout: relayIdle}, &staleTimers)
	m := p.client.carriers
	// The gateway's keep-alive schedule doubles from its floor to the
	// keep-alive period, so its first gap longer than the relay's timeout
	// ends some thirty seconds in.
	const idle = 7 * relayIdle / 2
	time.Sleep(idle)
	if got := p.relay.ExpiredSources(); got != 0 {
		t.Fatalf("the relay expired the idle carrier's mapping %d times: nothing crossed it for %v", got, relayIdle)
	}
	before := e.echoed.Load()
	e.pause.Store(false)
	took := e.waitEchoed(t, before+2048, 5*time.Second)
	if got := m.moved(); got != 0 {
		t.Fatalf("an idle flow on a live path was moved or its carrier replaced (%d):\n%s", got, p.clientLogs.String())
	}
	if got := p.relay.Sources(); got != 1 {
		t.Fatalf("%d sources, want the one carrier", got)
	}
	// One probe each time the carrier had heard nothing for
	// carrierQuietProbeAfter, and no more: the reply ends the quiet.
	if got, most := m.pathProbes.Load(), int64(idle/carrierQuietProbeAfter)+1; got == 0 || got > most {
		t.Fatalf("%d probes in %v idle, want between 1 and %d", got, idle, most)
	}
	assertNoLegacyRecovery(t, p)
	t.Logf("woken in %v after %v idle behind a relay that expires mappings after %v; %d probes", took, idle, relayIdle, m.pathProbes.Load())
}

// The path goes away under the idle flow. The transport would send its next
// keep-alive 50 s later and time out 100 s after that. The manager finds the
// carrier dead within carrierQuietProbeAfter + carrierSilenceDead of the
// last packet it heard and replaces it, and the flow -- still idle -- is
// rebound inside its binding deadline, so the wake that follows is
// delivered.
func TestCarrierIdleFlowIsReboundInTimeWhenItsCarrierDies(t *testing.T) {
	p, e := newIdleFlow(t, pathsim.Config{OneWayDelay: 20 * time.Millisecond, Seed: 13}, &staleTimers)
	m := p.client.carriers
	p.relay.BlackholeSources()
	blackholed := time.Now()
	const bound = carrierQuietProbeAfter + carrierSilenceDead
	for m.silentDeaths.Load() == 0 {
		if err := e.failure(); err != nil {
			t.Fatalf("the idle flow failed %v into the outage: %v", time.Since(blackholed), err)
		}
		if waited := time.Since(blackholed); waited > bound+2*time.Second {
			t.Fatalf("the dead carrier was not noticed within %v of the outage (bound %v):\n%s", waited, bound, p.clientLogs.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
	noticed := time.Since(blackholed)
	// Not before the transport itself would have said so with sane timers:
	// fifteen seconds after its first unanswered packet.
	if noticed < carrierSilenceDead {
		t.Fatalf("the carrier was ended %v into the outage, before the %v detection bound", noticed, carrierSilenceDead)
	}
	// The wake: the flow carries data again, on a new carrier, before the
	// deadline its binding had from the moment the outage began.
	_, deadline := m.cfg.bindingDeadlines(blackholed)
	before := e.echoed.Load()
	e.pause.Store(false)
	e.waitEchoed(t, before+2048, time.Until(deadline))
	if got := m.silentDeaths.Load(); got != 1 {
		t.Fatalf("%d carriers ended at the silence bound, want 1", got)
	}
	if got := m.newCarrierJoins.Load(); got != 1 {
		t.Fatalf("%d JOINs onto a new carrier, want 1", got)
	}
	if got := m.deadlineFailures.Load(); got != 0 {
		t.Fatalf("%d flows reached their binding deadline unbound", got)
	}
	if got := p.relay.Sources(); got != 2 {
		t.Fatalf("%d sources, want the dead carrier and its replacement", got)
	}
	assertNoLegacyRecovery(t, p)
	t.Logf("dead carrier noticed %v into the outage (bound %v); flow carrying data again %v into it", noticed, bound, time.Since(blackholed))
}
