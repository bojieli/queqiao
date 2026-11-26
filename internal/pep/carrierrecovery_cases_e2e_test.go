package pep

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bojieli/queqiao/internal/pathsim"
)

// flowTracker collects the flows a client starts, in order.
type flowTracker struct {
	mu    sync.Mutex
	flows []*multipathFlow
}

func (f *flowTracker) add(flow *multipathFlow) {
	f.mu.Lock()
	f.flows = append(f.flows, flow)
	f.mu.Unlock()
}

func (f *flowTracker) at(t *testing.T, i int) *multipathFlow {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		f.mu.Lock()
		if i < len(f.flows) {
			flow := f.flows[i]
			f.mu.Unlock()
			return flow
		}
		f.mu.Unlock()
		if time.Now().After(deadline) {
			t.Fatalf("flow %d never started", i)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// failStream fails the flow's current lane as a definitive stream failure
// would (a reset, a read or write error): the transport's own failLane.
func failStream(t *testing.T, flow *multipathFlow) {
	t.Helper()
	lanes := flow.healthyLanes()
	if len(lanes) == 0 {
		t.Fatal("flow has no lane to fail")
	}
	flow.failLane(lanes[0], errors.New("injected stream reset"))
}

// waitCarrierState waits for the client's carrier manager to log a
// transition to state.
func waitCarrierState(t *testing.T, p *carrierPair, state string, budget time.Duration) {
	t.Helper()
	if waitForRecord(t, p.clientLogs, "carrier state", budget, func(r map[string]any) bool { return r["to"] == state }) == nil {
		t.Fatalf("no carrier became %s within %v:\n%s", state, budget, p.clientLogs.String())
	}
}

// blockingEcho echoes, except that while held is set it stops reading: the
// destination application withholds credit.
func blockingEcho(held *atomic.Bool, release <-chan struct{}) func(net.Listener) {
	return func(l net.Listener) {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				buf := make([]byte, 32<<10)
				for {
					if held.Load() {
						<-release
					}
					n, err := conn.Read(buf)
					if n > 0 {
						if _, werr := conn.Write(buf[:n]); werr != nil {
							return
						}
					}
					if err != nil {
						return
					}
				}
			}()
		}
	}
}

// The switch is what removes per-flow rescue (§14 stage 8): the same credit
// stall that makes today's watchdog demote the lane and ask for a rescue
// demotes nothing and dials nothing under recovery=carrier.
func TestCarrierRecoveryDisablesRescueDialsAndDemotion(t *testing.T) {
	for _, mode := range []RecoveryMode{RecoveryFlow, RecoveryCarrier} {
		t.Run(string(mode), func(t *testing.T) {
			var held atomic.Bool
			release := make(chan struct{})
			p := newCarrierPair(t, carrierPairOptions{
				path:  pathsim.Config{OneWayDelay: 15 * time.Millisecond, Seed: 6},
				serve: blockingEcho(&held, release), clientRecovery: mode, gatewayRecovery: mode,
			})
			e := startEchoStream(t, p, 4096, 2*time.Millisecond)
			e.waitEchoed(t, 32<<10, 20*time.Second)
			held.Store(true)
			time.Sleep(3 * time.Second)
			close(release)
			e.waitEchoed(t, e.echoed.Load()+64<<10, 30*time.Second)
			demotions := countRecords(t, p.clientLogs, "flow stall suspected; lane demoted and rescue requested")
			if mode == RecoveryFlow {
				if demotions == 0 {
					t.Fatalf("today's recovery never demoted the stalled lane; the stall did not happen")
				}
				return
			}
			assertNoLegacyRecovery(t, p)
			if countRecords(t, p.clientLogs, "flow credit missing on a live stream; waiting") == 0 {
				t.Fatalf("the stall did not happen")
			}
		})
	}
}

// A flow whose stream fails while its carrier is SUSPECT waits, unbound,
// for whichever comes first (§10.3): the carrier's recovery, or a
// promotion. It is rebound either way (test 9).
func TestCarrierRecoveryRebindsAStreamLostWhileSuspect(t *testing.T) {
	for _, order := range []string{"recovers", "promoted"} {
		t.Run(order, func(t *testing.T) {
			tracker := &flowTracker{}
			o := carrierPairOptions{path: pathsim.Config{OneWayDelay: 20 * time.Millisecond, Seed: 7}, onFlow: tracker.add}
			if order == "recovers" {
				// Far enough that the restore comes first.
				o.recovery.ReplacePTOs = 12
			}
			p := newCarrierPair(t, o)
			victim := startEchoStream(t, p, 1024, 5*time.Millisecond)
			victim.waitEchoed(t, 16<<10, 20*time.Second)
			flow := tracker.at(t, 0)
			p.relay.BlackholeSources()
			waitCarrierState(t, p, "suspect", 10*time.Second)
			failStream(t, flow)
			if order == "recovers" {
				time.Sleep(300 * time.Millisecond)
				if got := p.client.carriers.sameCarrierJoins.Load() + p.client.carriers.newCarrierJoins.Load(); got != 0 {
					t.Fatalf("a flow was rebound onto a SUSPECT carrier")
				}
				p.relay.RestoreSources()
			}
			before := victim.echoed.Load()
			victim.waitEchoed(t, before+32<<10, 30*time.Second)
			m := p.client.carriers
			switch order {
			case "recovers":
				if m.promotions.Load() != 0 || m.sameCarrierJoins.Load() != 1 {
					t.Fatalf("promotions %d, same-carrier JOINs %d; want 0 and 1:\n%s", m.promotions.Load(), m.sameCarrierJoins.Load(), p.clientLogs.String())
				}
			case "promoted":
				if m.promotions.Load() != 1 || m.newCarrierJoins.Load() != 1 {
					t.Fatalf("promotions %d, new-carrier JOINs %d; want 1 and 1:\n%s", m.promotions.Load(), m.newCarrierJoins.Load(), p.clientLogs.String())
				}
			}
			assertNoLegacyRecovery(t, p)
		})
	}
}

// Either dead direction stops the client's progress epoch (§10.6 #92): a
// carrier blackholed one way only is replaced as one blackholed both ways
// (test 10).
func TestCarrierRecoveryReplacesAOneWayDeadCarrier(t *testing.T) {
	for _, dir := range []string{"up", "down"} {
		t.Run(dir, func(t *testing.T) {
			p := newCarrierPair(t, carrierPairOptions{path: pathsim.Config{OneWayDelay: 20 * time.Millisecond, Seed: 8}})
			e := startEchoStream(t, p, 2048, 5*time.Millisecond)
			e.waitEchoed(t, 32<<10, 20*time.Second)
			p.relay.BlackholeSourcesDirection(dir == "up", dir == "down")
			e.waitEchoed(t, e.echoed.Load()+64<<10, 30*time.Second)
			if got := p.client.carriers.promotions.Load(); got != 1 {
				t.Fatalf("%d promotions, want 1", got)
			}
			assertNoLegacyRecovery(t, p)
		})
	}
}

// Progress before promotion returns a SUSPECT carrier to ACTIVE (§10.2):
// nothing was moved, nothing is promoted, and the flow carries on, on the
// carrier it had. The verified standby is kept warm rather than closed at
// once.
func TestCarrierRecoveryReturnsASuspectCarrierToActive(t *testing.T) {
	p := newCarrierPair(t, carrierPairOptions{path: pathsim.Config{OneWayDelay: 20 * time.Millisecond, Seed: 9}, recovery: CarrierRecoveryConfig{ReplacePTOs: 12}})
	e := startEchoStream(t, p, 2048, 5*time.Millisecond)
	e.waitEchoed(t, 32<<10, 20*time.Second)
	p.relay.BlackholeSources()
	waitCarrierState(t, p, "suspect", 10*time.Second)
	time.Sleep(200 * time.Millisecond)
	p.relay.RestoreSources()
	waitCarrierState(t, p, "active", 10*time.Second)
	e.waitEchoed(t, e.echoed.Load()+64<<10, 20*time.Second)
	m := p.client.carriers
	if m.promotions.Load() != 0 || m.sameCarrierJoins.Load()+m.newCarrierJoins.Load() != 0 {
		t.Fatalf("promotions %d, JOINs %d after a recovery before promotion", m.promotions.Load(), m.sameCarrierJoins.Load()+m.newCarrierJoins.Load())
	}
	assertNoLegacyRecovery(t, p)
}

// Mixed peers (test 17). A new client against a gateway with every switch
// off and released admission, and an old client against a new gateway:
// either way a dead carrier is recovered and the bytes arrive. Against the
// released gateway the rebinding JOIN adds a lane beside the dead one and
// the client closes the old stream.
func TestCarrierRecoveryWithMixedPeers(t *testing.T) {
	cases := []struct {
		name string
		o    carrierPairOptions
	}{
		{"new client, released gateway", carrierPairOptions{releasedGateway: true}},
		{"old client, new gateway", carrierPairOptions{legacyClient: true}},
		{"new client on wire 1", carrierPairOptions{clientWires: []byte{1}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tc.o.path = pathsim.Config{OneWayDelay: 20 * time.Millisecond, Seed: 10}
			p := newCarrierPair(t, tc.o)
			e := startEchoStream(t, p, 2048, 5*time.Millisecond)
			e.waitEchoed(t, 32<<10, 20*time.Second)
			p.relay.BlackholeSources()
			e.waitEchoed(t, e.echoed.Load()+64<<10, 40*time.Second)
			if tc.o.legacyClient {
				if n := countRecords(t, p.gatewayLogs, "flow stall suspected; lane demoted and rescue requested"); n != 0 {
					t.Fatalf("the new gateway demoted a lane %d times", n)
				}
				return
			}
			if got := p.client.carriers.promotions.Load(); got != 1 {
				t.Fatalf("%d promotions, want 1", got)
			}
			assertNoLegacyRecovery(t, p)
		})
	}
}

// A NAT rebinding moves the client's connection to a new address at the
// gateway, which the gateway's QUIC follows as a migration: the carrier stays
// ACTIVE, nothing is replaced or rebound, and the gateway's path controller
// is kept, because the port is not part of the path key (test 17).
func TestCarrierRecoveryKeepsItsCarrierAcrossANATRebinding(t *testing.T) {
	p := newCarrierPair(t, carrierPairOptions{path: pathsim.Config{OneWayDelay: 15 * time.Millisecond, Seed: 11}})
	e := startEchoStream(t, p, 2048, 5*time.Millisecond)
	e.waitEchoed(t, 32<<10, 20*time.Second)
	controllers := func() int {
		pathAuthorities.mu.Lock()
		defer pathAuthorities.mu.Unlock()
		return len(pathAuthorities.controllers)
	}
	before := controllers()
	if err := p.relay.RebindSources(); err != nil {
		t.Fatal(err)
	}
	e.waitEchoed(t, e.echoed.Load()+128<<10, 20*time.Second)
	m := p.client.carriers
	if got := m.promotions.Load() + m.standbyDials.Load() + m.sameCarrierJoins.Load() + m.newCarrierJoins.Load(); got != 0 {
		t.Fatalf("a NAT rebinding replaced or rebound something (%d):\n%s", got, p.clientLogs.String())
	}
	if after := controllers(); after != before {
		t.Fatalf("%d path controllers after the rebinding, %d before: the authority was not kept", after, before)
	}
	assertNoLegacyRecovery(t, p)
}

// An uplink change is a new path (§10.2): the old carriers are DEAD, there is
// no standby to promote, and every flow is rebound, break-before-make, onto
// one fresh carrier, each by one JOIN.
func TestCarrierRecoveryRebindsEveryFlowAfterAnUplinkChange(t *testing.T) {
	p := newCarrierPair(t, carrierPairOptions{path: pathsim.Config{OneWayDelay: 10 * time.Millisecond, Seed: 12}})
	const flows = 4
	var streams []*echoStream
	for range flows {
		e := startEchoStream(t, p, 1024, 5*time.Millisecond)
		e.waitEchoed(t, 8<<10, 20*time.Second)
		streams = append(streams, e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.client.onUplinkTick(ctx, false, true, "uplink-a", "uplink-b")
	for _, e := range streams {
		e.waitEchoed(t, e.echoed.Load()+16<<10, 30*time.Second)
	}
	m := p.client.carriers
	if got := m.newCarrierJoins.Load(); got != flows {
		t.Fatalf("%d JOINs onto the new carrier, want one per flow (%d):\n%s", got, flows, p.clientLogs.String())
	}
	if got := m.promotions.Load(); got != 0 {
		t.Fatalf("%d promotions after an uplink change; there is no standby to promote", got)
	}
	assertNoLegacyRecovery(t, p)
}

// A long idle and a wake (W9b; deterministic, injected clock). The flow sits
// idle on a healthy carrier for longer than QUIC's idle timeout, which the
// adaptive keep-alive schedule bridges, while the carrier manager's clock
// jumps an hour: a bound flow has no binding deadline running, so the jump
// expires nothing. The wake is delivered on the same carrier with nothing
// dialled, replaced or rebound, and the idle cost is a handful of packets.
func TestCarrierRecoveryLongIdleThenWake(t *testing.T) {
	if testing.Short() {
		t.Skip("idles past QUIC's idle timeout")
	}
	var skew atomic.Int64
	clock := func() time.Time { return time.Now().Add(time.Duration(skew.Load())) }
	p := newCarrierPair(t, carrierPairOptions{
		path:     pathsim.Config{OneWayDelay: 15 * time.Millisecond, Seed: 13},
		recovery: CarrierRecoveryConfig{clock: clock},
	})
	e := startEchoStream(t, p, 1024, 5*time.Millisecond)
	e.waitEchoed(t, 16<<10, 20*time.Second)
	e.pause.Store(true)
	time.Sleep(500 * time.Millisecond)
	upBefore, _ := p.relay.Stats()
	idle := laneDeadPathDetection + 2*time.Second
	time.Sleep(idle / 2)
	skew.Store(int64(time.Hour))
	time.Sleep(idle / 2)
	upAfter, _ := p.relay.Stats()
	e.pause.Store(false)
	e.waitEchoed(t, e.echoed.Load()+16<<10, 10*time.Second)
	m := p.client.carriers
	if got := m.promotions.Load() + m.standbyDials.Load() + m.replacementDials.Load() + m.sameCarrierJoins.Load() + m.newCarrierJoins.Load() + m.deadlineFailures.Load(); got != 0 {
		t.Fatalf("an idle flow's wake moved or failed something (%d):\n%s", got, p.clientLogs.String())
	}
	if got := p.relay.Sources(); got != 1 {
		t.Fatalf("%d sources after the wake", got)
	}
	// The schedule from a 500 ms floor doubling to the 5 s period: about
	// 0.5, 1, 2, 4, 5, 5 s after the last payload, so a handful of PINGs
	// (and acknowledgements of the gateway's) over the idle.
	packets := upAfter.PacketsIn - upBefore.PacketsIn
	if packets < 3 || packets > 24 {
		t.Fatalf("%d client packets over a %v idle; the keep-alive schedule predicts a handful", packets, idle)
	}
	t.Logf("%d client packets over a %v idle", packets, idle)
	assertNoLegacyRecovery(t, p)
}

// QUIC-only, a flow unbound with no carrier to JOIN fails at its binding
// deadline, G − S after the client observed the loss, not at the end of
// today's 45 s grace (§10.3 step 4, test 11).
func TestCarrierRecoveryFailsAnUnboundFlowAtItsBindingDeadline(t *testing.T) {
	tracker := &flowTracker{}
	p := newCarrierPair(t, carrierPairOptions{
		path:     pathsim.Config{OneWayDelay: 10 * time.Millisecond, Seed: 14},
		recovery: CarrierRecoveryConfig{grace: 6 * time.Second, DetectionSkew: time.Second, HandoffBudget: time.Second},
		onFlow:   tracker.add,
	})
	e := startEchoStream(t, p, 1024, 5*time.Millisecond)
	e.waitEchoed(t, 8<<10, 20*time.Second)
	flow := tracker.at(t, 0)
	// Nothing reaches the gateway any more, from any source.
	p.relay.SetLossRate(1, 1)
	p.relay.BlackholeSources()
	failStream(t, flow)
	lost := time.Now()
	select {
	case <-flow.doneChan():
	case <-time.After(20 * time.Second):
		t.Fatal("the unbound flow outlived its binding deadline")
	}
	took := time.Since(lost)
	if took < 4*time.Second || took > 8*time.Second {
		t.Fatalf("flow ended %v after its loss; its binding deadline was 5s", took)
	}
	if got := p.client.carriers.deadlineFailures.Load(); got != 1 {
		t.Fatalf("%d binding-deadline failures, want 1", got)
	}
	assertNoLegacyRecovery(t, p)
}

// AUTO hands a flow with no QUIC carrier to rebind it to TCP, before its
// latest handoff start (§10.3 step 4): the bytes keep arriving, on TCP.
func TestCarrierRecoveryHandsOffToTCPBeforeTheLatestStart(t *testing.T) {
	tracker := &flowTracker{}
	p := newCarrierPair(t, carrierPairOptions{
		path: pathsim.Config{OneWayDelay: 10 * time.Millisecond, Seed: 15}, transport: TransportAuto, onFlow: tracker.add,
	})
	e := startEchoStream(t, p, 1024, 5*time.Millisecond)
	e.waitEchoed(t, 8<<10, 20*time.Second)
	flow := tracker.at(t, 0)
	if lanes := flow.healthyLanes(); len(lanes) == 0 || lanes[0].kind != TransportQUIC {
		t.Skip("the flow did not start on QUIC")
	}
	p.relay.SetLossRate(1, 1)
	p.relay.BlackholeSources()
	failStream(t, flow)
	lost := time.Now()
	e.waitEchoed(t, e.echoed.Load()+16<<10, 30*time.Second)
	latest, _ := p.client.carriers.cfg.bindingDeadlines(lost)
	if time.Now().After(latest.Add(2 * time.Second)) {
		t.Fatalf("handed off after the latest handoff start")
	}
	if got := p.client.carriers.tcpHandoffs.Load(); got != 1 {
		t.Fatalf("%d TCP handoffs, want 1:\n%s", got, p.clientLogs.String())
	}
	if !hasTCPLane(flow) {
		t.Fatal("the flow is not on TCP")
	}
}

// Promotion at scale, scaled down (test 11): many flows on one carrier when
// it dies are each rebound by one JOIN onto the promoted carrier, and every
// one of them keeps its bytes.
func TestCarrierRecoveryRebindsManyFlowsAfterOnePromotion(t *testing.T) {
	p := newCarrierPair(t, carrierPairOptions{path: pathsim.Config{OneWayDelay: 15 * time.Millisecond, Seed: 16}})
	const flows = 48
	var streams []*echoStream
	for range flows {
		streams = append(streams, startEchoStream(t, p, 256, 20*time.Millisecond))
	}
	for _, e := range streams {
		e.waitEchoed(t, 2<<10, 30*time.Second)
	}
	p.relay.BlackholeSources()
	blackholed := time.Now()
	for _, e := range streams {
		e.waitEchoed(t, e.echoed.Load()+4<<10, 45*time.Second)
	}
	t.Logf("%d flows resumed %v after the blackhole", flows, time.Since(blackholed))
	m := p.client.carriers
	if m.promotions.Load() != 1 || m.newCarrierJoins.Load() != flows {
		t.Fatalf("promotions %d, JOINs onto the new carrier %d; want 1 and %d", m.promotions.Load(), m.newCarrierJoins.Load(), flows)
	}
	if m.deadlineFailures.Load() != 0 {
		t.Fatal("a flow failed waiting in the JOIN queue")
	}
	assertNoLegacyRecovery(t, p)
}

// While flows wait to be rebound onto a carrier, a new OPEN does not take a
// stream slot one of them needs (§10.3 step 2): it waits for the queue, and
// opens as soon as the queue is empty.
func TestCarrierRecoveryHoldsNewOpensWhileFlowsRebind(t *testing.T) {
	p := newCarrierPair(t, carrierPairOptions{path: pathsim.Config{OneWayDelay: 5 * time.Millisecond, Seed: 17}})
	first := startEchoStream(t, p, 512, 5*time.Millisecond)
	first.waitEchoed(t, 4<<10, 20*time.Second)
	carrier := p.client.carriers.eligible()
	if carrier == nil || carrier.managed == nil {
		t.Fatal("no managed carrier")
	}
	carrier.managed.rebinding.Add(1)
	opened := make(chan error, 1)
	go func() {
		conn, err := trySocksDial(p.socks, p.destination, 20*time.Second)
		if err == nil {
			_, err = conn.Write([]byte("x"))
			if err == nil {
				var b [1]byte
				_, err = io.ReadFull(conn, b[:])
			}
			conn.Close()
		}
		opened <- err
	}()
	select {
	case err := <-opened:
		t.Fatalf("a new flow opened while a rebinding was waiting (err %v)", err)
	case <-time.After(700 * time.Millisecond):
	}
	carrier.managed.rebinding.Add(-1)
	select {
	case err := <-opened:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the new flow did not open once the rebinding queue emptied")
	}
}
