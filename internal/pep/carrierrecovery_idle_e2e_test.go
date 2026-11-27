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

// A healthy idle carrier is kept by the transport's own keep-alive, as under
// legacy recovery: the manager adds no traffic to it. The test checks the two
// things the manager controls, both exactly: it initiated no probe, and the
// keep-alive floor it gave the transport is the design's (§10.2), never a
// shorter one. It does not count packets on the path. That count also holds
// the acknowledgements of the gateway's keep-alives, the flow's last
// acknowledgements and any probe timeout a loaded runner provokes, none of
// which the manager sends, and a loaded runner measured up to 13 where a
// quiet one measured 8.
func TestCarrierManagerAddsNothingToAHealthyIdleCarrier(t *testing.T) {
	p, e := newIdleFlow(t, pathsim.Config{OneWayDelay: 20 * time.Millisecond, Seed: 7}, nil)
	m := p.client.carriers
	probes := m.pathProbes.Load()
	const idle = 2 * carrierQuietProbeAfter
	time.Sleep(idle)
	if got := m.pathProbes.Load() - probes; got != 0 {
		t.Fatalf("the manager probed a healthy idle carrier %d times in %v", got, idle)
	}
	p.client.quicMu.Lock()
	g := p.client.quicGeneration
	p.client.quicMu.Unlock()
	if g == nil || g.managed == nil {
		t.Fatal("the client has no managed carrier")
	}
	st := g.conn.ConnectionStats()
	m.mu.Lock()
	floor := g.managed.keepAlive
	m.mu.Unlock()
	// The manager re-applies the floor only when it has moved by a quarter,
	// so what is set may be that far from the floor of this instant's SRTT.
	if want := keepAliveFloor(st.SmoothedRTT); floor < keepAliveFloorMin || floor < want*3/4 {
		t.Fatalf("the manager set a keep-alive floor of %v on an idle carrier with flows; the design's is max(4 SRTT, %v) = %v",
			floor, keepAliveFloorMin, want)
	}
	if got := m.moved(); got != 0 {
		t.Fatalf("an idle flow on a live path was moved or its carrier replaced (%d):\n%s", got, p.clientLogs.String())
	}
	before := e.echoed.Load()
	e.pause.Store(false)
	e.waitEchoed(t, before+2048, 5*time.Second)
}

// The path's relay forgets a mapping nothing has crossed for twelve seconds: a
// forwarding relay's idle timeout, scaled down, or a NAT's. Neither
// endpoint's transport sends a keep-alive that often any more, so without the
// manager's own bound the mapping expires under the idle flow. With it the
// carrier hears nothing for at most carrierQuietProbeAfter before it probes:
// the mapping is never lost, the carrier is the one the flow started on, and
// the wake is delivered at once.
func TestCarrierIdleFlowOutlivesARelayIdleTimeout(t *testing.T) {
	// 4.5 s past the manager's bound, for a loaded runner; the gap it must
	// still catch without the bound is 16 s (below).
	const relayIdle = 12 * time.Second
	if relayIdle <= carrierQuietProbeAfter || relayIdle >= staleTimers.keepAlive {
		t.Fatalf("the relay's idle timeout (%v) must lie between the manager's bound (%v) and the transport's keep-alive (%v)",
			relayIdle, carrierQuietProbeAfter, staleTimers.keepAlive)
	}
	p, e := newIdleFlow(t, pathsim.Config{OneWayDelay: 20 * time.Millisecond, Seed: 11, SourceIdleTimeout: relayIdle}, &staleTimers)
	m := p.client.carriers
	// The transport's keep-alive schedule doubles from its floor towards
	// the keep-alive period: 0.5, 1.5, 3.5, 7.5, 15.5 and 31.5 s in. Its
	// first gap longer than the relay's timeout is the 16 s one, which ends
	// 31.5 s in.
	const idle = 3 * relayIdle
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
	// carrierQuietProbeAfter; the reply ends the quiet. Twice that many are
	// allowed: a reply a loaded runner delivers more than a second late is
	// followed by a second probe, at the path-probe cadence.
	if got, most := m.pathProbes.Load(), 2*(int64(idle/carrierQuietProbeAfter)+1); got == 0 || got > most {
		t.Fatalf("%d probes in %v idle, want between 1 and %d", got, idle, most)
	}
	assertNoLegacyRecovery(t, p)
	t.Logf("woken in %v after %v idle behind a relay that expires mappings after %v; %d probes", took, idle, relayIdle, m.pathProbes.Load())
}

// schedulerMargin is what these tests allow a loaded runner on top of a
// design bound: the manager's ticks and the transport's run loop are
// goroutines, and a starved host runs them late.
const schedulerMargin = 3 * time.Second

// The path goes away under the idle flow. The transport would send its next
// keep-alive 50 s later and time out 100 s after that. The manager finds the
// carrier dead carrierSilenceDead after its first unanswered packet and at
// most carrierQuietProbeAfter + carrierSilenceDead after the last packet it
// heard, and replaces it; the flow -- still idle -- is rebound inside its
// binding deadline, so the wake that follows is delivered.
//
// Both bounds are judged on the manager's own record of the silence (what it
// logs when it ends the carrier), not on the test's clock: the test cannot
// see when the last packet crossed.
func TestCarrierIdleFlowIsReboundInTimeWhenItsCarrierDies(t *testing.T) {
	p, e := newIdleFlow(t, pathsim.Config{OneWayDelay: 20 * time.Millisecond, Seed: 13}, &staleTimers)
	m := p.client.carriers
	p.relay.BlackholeSources()
	blackholed := time.Now()
	const bound = carrierQuietProbeAfter + carrierSilenceDead
	const silentMsg = "carrier silent past the detection bound; treating it as dead"
	for m.silentDeaths.Load() == 0 {
		if err := e.failure(); err != nil {
			t.Fatalf("the idle flow failed %v into the outage: %v", time.Since(blackholed), err)
		}
		// Far past the bound: the judgement is on the record below.
		if waited := time.Since(blackholed); waited > 2*bound {
			t.Fatalf("the dead carrier was not noticed within %v of the outage (bound %v):\n%s", waited, bound, p.clientLogs.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
	// The client observed the loss now: the flow's binding deadline counts
	// from here (§10.3 step 4).
	_, deadline := m.cfg.bindingDeadlines(time.Now())
	record := waitForRecord(t, p.clientLogs, silentMsg, 5*time.Second, func(map[string]any) bool { return true })
	if record == nil {
		t.Fatalf("the manager ended a carrier at the silence bound without recording it:\n%s", p.clientLogs.String())
	}
	unanswered := time.Duration(record["unanswered_for"].(float64))
	unheard := time.Duration(record["nothing_heard_for"].(float64))
	if unanswered < carrierSilenceDead {
		t.Fatalf("the carrier was ended %v after its first unanswered packet, before the %v detection bound", unanswered, carrierSilenceDead)
	}
	if unanswered > carrierSilenceDead+schedulerMargin {
		t.Fatalf("the carrier was ended %v after its first unanswered packet, want within %v of the %v detection bound",
			unanswered, schedulerMargin, carrierSilenceDead)
	}
	if unheard > bound+schedulerMargin {
		t.Fatalf("the carrier was ended %v after the last packet it heard, want within %v of %v", unheard, schedulerMargin, bound)
	}
	// The wake: the flow carries data again, on a new carrier, before its
	// binding deadline.
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
	// The dead carrier and what replaced it. While there is no carrier the
	// manager starts a fresh replacement handshake every
	// replacementProbeEvery and installs the first to be verified, so a slow
	// runner sees more than one new source; the flow joins exactly one
	// (above).
	if got := p.relay.Sources(); got < 2 {
		t.Fatalf("%d sources, want the dead carrier and at least one replacement", got)
	}
	assertNoLegacyRecovery(t, p)
	t.Logf("carrier ended %v after its first unanswered packet and %v after the last it heard (bounds %v and %v); %d replacement dials; flow carrying data again %v into the outage",
		unanswered, unheard, carrierSilenceDead, bound, m.replacementDials.Load(), time.Since(blackholed))
}
