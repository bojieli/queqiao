package pep

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"
)

// bindingRace is the binding loop's choice between a QUIC JOIN and AUTO's
// TCP handoff for one unbound flow, on an injected clock with fake
// transports: the loop's own rules (when the handoff is due, its backoff,
// the latest handoff start) around the real tcpHandoffGate.
type bindingRace struct {
	// tcpUp and quicUp say whether a TCP JOIN started at t succeeds and
	// whether a QUIC carrier is eligible at t, measured from the moment the
	// flow lost its binding.
	tcpUp, quicUp func(t time.Duration) bool
	// quicComing says a QUIC replacement is being dialled at t.
	quicComing func(t time.Duration) bool
	rtt        time.Duration
	// gated is false for the loop as it was before the gate.
	gated bool
}

const (
	raceGrace      = 3 * time.Second  // the fallback grace
	raceLatest     = 25 * time.Second // the latest handoff start
	raceDeadline   = 30 * time.Second // the binding deadline
	raceTCPTimeout = 2 * time.Second  // a TCP JOIN into a dead path
	raceTick       = 50 * time.Millisecond
)

// run returns which transport the flow ended on, and when.
func (r bindingRace) run() (string, time.Duration) {
	start := time.Unix(1_000_000, 0)
	latest := start.Add(raceLatest)
	var gate tcpHandoffGate
	var tcpNextAt time.Time
	var tcpBackoff time.Duration
	for t := time.Duration(0); t < raceDeadline; t += raceTick {
		now := start.Add(t)
		target := r.quicUp(t)
		due := !now.Before(latest) || (!target && t >= raceGrace)
		held := false
		if due && !now.Before(tcpNextAt) && r.gated {
			held, _ = gate.hold(now, latest, !now.Before(latest), !target && r.quicComing(t), quicHandshakeWaitFor(r.rtt))
		}
		if due && !held && !now.Before(tcpNextAt) {
			if r.tcpUp(t) {
				return "tcp", t
			}
			// The attempt runs out of time, and the loop sees it then.
			t += raceTCPTimeout
			now = start.Add(t)
			if r.gated && isTimeoutError(&net.DNSError{IsTimeout: true}) {
				gate.timedOut = true
			}
			tcpBackoff = max(min(2*tcpBackoff, carrierRoundCooldownMax), time.Second)
			tcpNextAt = now.Add(tcpBackoff)
			continue
		}
		if target {
			return "quic", t
		}
	}
	return "deadline", raceDeadline
}

// The race the live blackhole campaign lost in every block: both transports
// down, then both back, with the QUIC replacement usable a second after TCP
// is. The flow must end on QUIC. Before the gate, the TCP retry that landed
// in that second took it.
func TestAReturningPathDoesNotLoseItsFlowsToTCP(t *testing.T) {
	const back = 10 * time.Second
	outage := bindingRace{
		tcpUp:      func(t time.Duration) bool { return t >= back },
		quicUp:     func(t time.Duration) bool { return t >= back+time.Second },
		quicComing: func(time.Duration) bool { return true },
		rtt:        360 * time.Millisecond,
	}
	if via, at := outage.run(); via != "tcp" || at < back || at >= back+time.Second {
		t.Fatalf("setup: without the gate the flow ends on %s at %v; the scenario must lose the race to TCP in the second after %v", via, at, back)
	}
	outage.gated = true
	via, at := outage.run()
	if via != "quic" {
		t.Fatalf("with both transports back and QUIC a second behind, the flow ended on %s at %v", via, at)
	}
	if at > back+quicHandshakeWaitFor(outage.rtt)+raceTick {
		t.Fatalf("the flow JOINed QUIC at %v, later than one handshake wait after the path returned at %v", at, back)
	}
}

// A network that blocks UDP: TCP works from the start and QUIC never does.
// The first handoff succeeds, so nothing ever timed out and nothing waits:
// the flow moves to TCP when the fallback grace has passed, as before.
func TestABlockedUDPPathStillHandsOffToTCPAtOnce(t *testing.T) {
	for _, gated := range []bool{false, true} {
		blocked := bindingRace{
			tcpUp:      func(time.Duration) bool { return true },
			quicUp:     func(time.Duration) bool { return false },
			quicComing: func(time.Duration) bool { return true },
			rtt:        360 * time.Millisecond, gated: gated,
		}
		if via, at := blocked.run(); via != "tcp" || at != raceGrace {
			t.Fatalf("gated=%v: on a UDP-blocked path the flow ended on %s at %v, want tcp at the %v grace", gated, via, at, raceGrace)
		}
	}
}

// The path returns for TCP only: the gate gives QUIC one handshake and then
// lets the handoff go. And it never delays a handoff past the latest start.
func TestTheHandoffGateGivesQUICOneHandshakeAndNoMore(t *testing.T) {
	const back = 10 * time.Second
	rtt := 360 * time.Millisecond
	tcpOnly := bindingRace{
		tcpUp:      func(t time.Duration) bool { return t >= back },
		quicUp:     func(time.Duration) bool { return false },
		quicComing: func(time.Duration) bool { return true },
		rtt:        rtt, gated: true,
	}
	via, at := tcpOnly.run()
	if via != "tcp" || at > back+2*quicHandshakeWaitFor(rtt)+time.Second {
		t.Fatalf("with only TCP back at %v the flow ended on %s at %v", back, via, at)
	}

	start := time.Unix(1_000_000, 0)
	latest := start.Add(raceLatest)
	wait := quicHandshakeWaitFor(rtt)
	g := tcpHandoffGate{timedOut: true}
	if held, until := g.hold(start, latest, false, true, wait); !held || !until.Equal(start.Add(wait)) {
		t.Fatalf("a retry after a timeout, with QUIC coming, was held=%v until %v", held, until)
	}
	if held, _ := g.hold(start.Add(wait), latest, false, true, wait); held {
		t.Fatal("the retry was still held after one handshake wait")
	}
	if held, until := g.hold(latest.Add(-wait/2), latest, false, true, wait); !held || until.After(latest) {
		t.Fatalf("near the latest handoff start the hold runs until %v, past %v (held=%v)", until, latest, held)
	}
	for name, c := range map[string][2]bool{"forced": {true, true}, "no QUIC coming": {false, false}} {
		g := tcpHandoffGate{timedOut: true}
		if held, _ := g.hold(start, latest, c[0], c[1], wait); held {
			t.Fatalf("%s: the handoff was held", name)
		}
	}
	if held, _ := (&tcpHandoffGate{}).hold(start, latest, false, true, wait); held {
		t.Fatal("a handoff was held although none had timed out")
	}
}

func TestTimeoutsAreToldFromRefusals(t *testing.T) {
	for _, err := range []error{context.DeadlineExceeded, fmt.Errorf("tcp lane: %w", context.DeadlineExceeded),
		fmt.Errorf("tcp lane: %w", &net.OpError{Op: "dial", Err: &net.DNSError{IsTimeout: true}})} {
		if !isTimeoutError(err) {
			t.Errorf("%v was not taken for a timeout", err)
		}
	}
	for _, err := range []error{errLaneJoinRejected, errors.New("connection refused"), context.Canceled} {
		if isTimeoutError(err) {
			t.Errorf("%v was taken for a timeout", err)
		}
	}
	if got := quicHandshakeWaitFor(0); got != quicHandshakeWaitMin {
		t.Errorf("handshake wait with no round trip known is %v, want %v", got, quicHandshakeWaitMin)
	}
	if got := quicHandshakeWaitFor(5 * time.Second); got != quicHandshakeWaitMax {
		t.Errorf("handshake wait on a 5 s path is %v, want the %v ceiling", got, quicHandshakeWaitMax)
	}
}

// A TCP lane that is ready waits for QUIC before its JOIN only when the path
// was away for TCP too, and never once the latest handoff start has come.
func TestATCPHandoffYieldsOnlyAfterAnOutage(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	latest := now.Add(10 * time.Second)
	wait := quicHandshakeWaitFor(360 * time.Millisecond)
	for _, c := range []struct {
		name     string
		timedOut bool
		dial     time.Duration
		now      time.Time
		want     bool
	}{
		{"UDP blocked: a prompt dial, nothing timed out", false, 800 * time.Millisecond, now, false},
		{"a dial that spanned the outage", false, 9 * time.Second, now, true},
		{"a prompt dial after an attempt that timed out", true, 800 * time.Millisecond, now, true},
		{"at the latest handoff start", true, 9 * time.Second, latest, false},
	} {
		if got := tcpHandoffYields(c.timedOut, c.dial, wait, c.now, latest); got != c.want {
			t.Errorf("%s: yields=%v, want %v", c.name, got, c.want)
		}
	}
}
