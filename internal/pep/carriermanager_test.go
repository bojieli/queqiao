package pep

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/apernet/quic-go"
)

// §10.2 carrier states, deterministically (test 10).
func TestCarrierStatesFollowTransportEvidence(t *testing.T) {
	f := newCarrierFSM(2, 4)
	step := func(epoch uint64, ptos uint32) (carrierState, bool) {
		_, to, promote := f.step(carrierEvidence{Epoch: epoch, PTOs: ptos})
		return to, promote
	}
	if s, p := step(10, 0); s != carrierActive || p {
		t.Fatalf("fresh carrier %v promote=%v", s, p)
	}
	if s, _ := step(10, 1); s != carrierActive {
		t.Fatalf("one expiry made it %v; k_s is 2", s)
	}
	if s, p := step(10, 2); s != carrierSuspect || p {
		t.Fatalf("k_s expiries: %v promote=%v, want suspect without promotion", s, p)
	}
	if s, p := step(10, 3); s != carrierSuspect || p {
		t.Fatalf("below k_p: %v promote=%v", s, p)
	}
	if s, p := step(10, 4); s != carrierSuspect || !p {
		t.Fatalf("k_p expiries, epoch unchanged: %v promote=%v, want promotion due", s, p)
	}
	// Progress before promotion returns it to ACTIVE.
	if s, p := step(11, 0); s != carrierActive || p {
		t.Fatalf("progress before promotion: %v promote=%v", s, p)
	}
	// A new episode needs k_s again, from the new epoch. Progress and, in
	// the same reading, k_p expiries since it make the carrier SUSPECT
	// again but do not promote: promotion needs a later reading with the
	// epoch unchanged.
	step(11, 2)
	if s, p := step(12, 4); s != carrierSuspect || p {
		t.Fatalf("progress with expiries: %v promote=%v", s, p)
	}
	if s, p := step(12, 4); s != carrierSuspect || !p {
		t.Fatalf("next reading, epoch unchanged: %v promote=%v", s, p)
	}
}

// A promotion needs the epoch unchanged since SUSPECT: an expiry count that
// reaches k_p only after an intervening acknowledgement is a new episode.
func TestCarrierPromotionNeedsAnUnchangedEpoch(t *testing.T) {
	f := newCarrierFSM(2, 4)
	f.step(carrierEvidence{Epoch: 5, PTOs: 0})
	f.step(carrierEvidence{Epoch: 5, PTOs: 3})
	if f.state != carrierSuspect || f.suspectEpoch != 5 {
		t.Fatalf("state %v suspect epoch %d", f.state, f.suspectEpoch)
	}
	// k_p reached in the reading that made it SUSPECT is not enough.
	f2 := newCarrierFSM(2, 4)
	f2.step(carrierEvidence{Epoch: 5})
	if _, _, promote := f2.step(carrierEvidence{Epoch: 5, PTOs: 6}); promote {
		t.Fatal("promotion due in the reading that made the carrier SUSPECT")
	}
	// Progress and, in the same reading, four new expiries.
	_, to, promote := f.step(carrierEvidence{Epoch: 6, PTOs: 4})
	if to != carrierSuspect || promote {
		// It is SUSPECT again from epoch 6; promotion is due only on a
		// later reading with that epoch unchanged.
		t.Fatalf("after progress: %v promote=%v", to, promote)
	}
	if _, _, promote = f.step(carrierEvidence{Epoch: 6, PTOs: 4}); !promote {
		t.Fatal("unchanged epoch with k_p expiries did not make promotion due")
	}
}

// After promotion nothing returns (§10.2): a late acknowledgement on a
// DRAINING carrier changes nothing, and its end is still recorded.
func TestALateAcknowledgementOnADrainingCarrierChangesNothing(t *testing.T) {
	f := newCarrierFSM(2, 4)
	f.step(carrierEvidence{Epoch: 1})
	f.step(carrierEvidence{Epoch: 1, PTOs: 4})
	f.drain()
	if _, to, promote := f.step(carrierEvidence{Epoch: 9}); to != carrierDraining || promote {
		t.Fatalf("late ACK on a draining carrier: %v promote=%v", to, promote)
	}
	if _, to, promote := f.step(carrierEvidence{Epoch: 9, PTOs: 8}); to != carrierDraining || promote {
		t.Fatalf("expiries on a draining carrier: %v promote=%v", to, promote)
	}
	if _, to, _ := f.step(carrierEvidence{Ended: &quic.IdleTimeoutError{}}); to != carrierDead {
		t.Fatalf("idle timeout of a draining carrier: %v", to)
	}
	if _, to, _ := f.step(carrierEvidence{Epoch: 10}); to != carrierDead {
		t.Fatalf("a dead carrier came back: %v", to)
	}
}

func TestCarrierEndIsDeadOrClosed(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want carrierState
	}{
		{&quic.IdleTimeoutError{}, carrierDead},
		{&quic.StatelessResetError{}, carrierDead},
		{&quic.ApplicationError{ErrorCode: 0}, carrierClosed},
		{context.Canceled, carrierClosed},
	} {
		f := newCarrierFSM(2, 4)
		if _, to, _ := f.step(carrierEvidence{Ended: tc.err}); to != tc.want {
			t.Errorf("%v: %v, want %v", tc.err, to, tc.want)
		}
	}
}

// Every dial round is charged to one history (§10.2): the cooldown doubles,
// replacement rounds stop at today's cap, and only a sustained healthy dwell
// resets it. A carrier that flaps between SUSPECT and ACTIVE therefore
// cannot produce a dial storm (test 11).
func TestAttemptHistoryRateLimitsAFlappingCarrier(t *testing.T) {
	var h attemptHistory
	now := time.Unix(1000, 0)
	var gaps []time.Duration
	last := now
	dials := 0
	for i := 0; i < 2000; i++ {
		// A flap every 100 ms asks for a standby each time, with healthy
		// moments between that never last.
		now = now.Add(100 * time.Millisecond)
		h.observe(now, i%2 == 0)
		if ok, _ := h.admit(now, true); ok {
			if dials > 0 {
				gaps = append(gaps, now.Sub(last))
			}
			last = now
			dials++
		}
	}
	if dials != maxLaneRecoveryAttempts {
		t.Fatalf("%d replacement rounds over 200 s of flapping, want the cap %d", dials, maxLaneRecoveryAttempts)
	}
	for i := 1; i < len(gaps); i++ {
		if gaps[i] < gaps[i-1] {
			t.Fatalf("cooldown shrank: %v", gaps)
		}
	}
	if gaps[0] < time.Second || gaps[len(gaps)-1] > carrierRoundCooldownMax+100*time.Millisecond {
		t.Fatalf("cooldowns %v outside [1s, %v]", gaps, carrierRoundCooldownMax)
	}
	// A non-replacement round (no flow waiting) is paced but not capped.
	now = now.Add(time.Minute)
	if ok, _ := h.admit(now, false); !ok {
		t.Fatal("a dial for a new flow was refused by the replacement cap")
	}
	// A sustained healthy dwell resets the history.
	h.observe(now, true)
	h.observe(now.Add(laneRecoveryResetAfter), true)
	if got := h.rounds(); got != 0 {
		t.Fatalf("%d rounds remembered after a healthy dwell", got)
	}
	if ok, _ := h.admit(now.Add(laneRecoveryResetAfter), true); !ok {
		t.Fatal("no round admitted after the reset")
	}
}

// The skew rule of §10.3 step 4. The gateway's grace G starts when it loses
// the flow's last lane; the client observes its own loss at most S later.
// A JOIN started by the latest handoff start whose handoff takes at most J
// arrives inside the gateway's grace, however the two observation times are
// placed within S (test 11).
func TestBindingDeadlinesFollowTheSkewRule(t *testing.T) {
	cfg := CarrierRecoveryConfig{}
	if err := cfg.resolve(); err != nil {
		t.Fatal(err)
	}
	if cfg.grace != laneReplacementWait || cfg.DetectionSkew != 15*time.Second || cfg.HandoffBudget != 5*time.Second {
		t.Fatalf("defaults G %v S %v J %v", cfg.grace, cfg.DetectionSkew, cfg.HandoffBudget)
	}
	observed := time.Unix(5000, 0)
	latest, deadline := cfg.bindingDeadlines(observed)
	if got := deadline.Sub(observed); got != 30*time.Second {
		t.Fatalf("binding deadline %v after observation, want G-S = 30s", got)
	}
	if got := deadline.Sub(latest); got != cfg.HandoffBudget {
		t.Fatalf("latest start %v before the deadline, want J", got)
	}
	for skew := time.Duration(0); skew <= cfg.DetectionSkew; skew += time.Second {
		gatewayLost := observed.Add(-skew)
		gatewayExpiry := gatewayLost.Add(cfg.grace)
		for transit := time.Duration(0); transit <= cfg.HandoffBudget; transit += 500 * time.Millisecond {
			arrival := latest.Add(transit)
			if arrival.After(gatewayExpiry) {
				t.Fatalf("skew %v, transit %v: a JOIN started at the latest start arrives %v after the gateway's grace", skew, transit, arrival.Sub(gatewayExpiry))
			}
		}
	}
	// Past S the rule is best effort, as the design states: a client that
	// observes the loss later than S can start in time and still be late.
	gatewayExpiry := observed.Add(-cfg.DetectionSkew - time.Second).Add(cfg.grace)
	if !latest.Add(cfg.HandoffBudget).After(gatewayExpiry) {
		t.Fatal("the boundary case beyond S is not where the design says it is")
	}
}

func TestCarrierRecoveryConfigRejectsDeadlinesWithNoRoom(t *testing.T) {
	for _, tc := range []struct {
		cfg CarrierRecoveryConfig
		ok  bool
	}{
		{CarrierRecoveryConfig{}, true},
		{CarrierRecoveryConfig{SuspectPTOs: 3, ReplacePTOs: 6, DetectionSkew: 10 * time.Second, HandoffBudget: 3 * time.Second}, true},
		{CarrierRecoveryConfig{SuspectPTOs: 4, ReplacePTOs: 3}, false},
		{CarrierRecoveryConfig{ReplacePTOs: 99}, false},
		{CarrierRecoveryConfig{DetectionSkew: 40 * time.Second, HandoffBudget: 5 * time.Second}, false},
		{CarrierRecoveryConfig{HandoffBudget: -time.Second}, false},
	} {
		err := ValidateCarrierRecovery(tc.cfg)
		if (err == nil) != tc.ok {
			t.Errorf("%+v: %v, want ok=%v", tc.cfg, err, tc.ok)
		}
	}
}

// The JOIN queue of §10.3 step 3: pending FIN/ACK first, then INTERACTIVE,
// then BULK, earliest deadline first within each; at least 16 in flight, and
// max(16, N×r/T) for a long queue.
func TestJoinQueueServesEarliestDeadlineFirstByClass(t *testing.T) {
	q := &joinQueue{rtt: func() time.Duration { return 100 * time.Millisecond }}
	ctx := context.Background()
	base := time.Unix(100, 0)
	// Fill the floor so the rest queue.
	var releases []func()
	for range minJoinConcurrency {
		r, err := q.acquire(ctx, 2, base)
		if err != nil {
			t.Fatal(err)
		}
		releases = append(releases, r)
	}
	type entry struct {
		priority int
		deadline time.Duration
	}
	queued := []entry{{2, 1}, {1, 9}, {0, 20}, {1, 3}, {2, 0}}
	type grant struct {
		e       entry
		release func()
	}
	order := make(chan grant, len(queued))
	for _, e := range queued {
		e := e
		go func() {
			r, err := q.acquire(ctx, e.priority, base.Add(e.deadline*time.Second))
			if err != nil {
				return
			}
			order <- grant{e, r}
		}()
	}
	for {
		q.mu.Lock()
		n := len(q.waiters)
		q.mu.Unlock()
		if n == len(queued) {
			break
		}
		time.Sleep(time.Millisecond)
	}
	// Release one slot at a time: each grant is the head of the order.
	want := []entry{{0, 20}, {1, 3}, {1, 9}, {2, 0}, {2, 1}}
	for i := range want {
		releases[i]()
		got := <-order
		if got.e != want[i] {
			t.Fatalf("grant %d: %+v, want %+v", i, got.e, want[i])
		}
		defer got.release()
	}
	for _, r := range releases[len(want):] {
		r()
	}

	// Concurrency scales with the queue: 1024 flows at r = 100 ms against
	// T = 3 s is 35 at once.
	q2 := &joinQueue{rtt: func() time.Duration { return 100 * time.Millisecond }}
	q2.mu.Lock()
	q2.inflight = 1024
	limit := q2.limitLocked()
	q2.mu.Unlock()
	if want := int((1024*100*time.Millisecond + joinTarget - 1) / joinTarget); limit != want || limit <= minJoinConcurrency {
		t.Fatalf("limit %d for 1024 flows, want %d", limit, want)
	}

	// A waiter that gives up leaves the queue without a slot.
	q3 := &joinQueue{}
	for range minJoinConcurrency {
		if _, err := q3.acquire(ctx, 0, base); err != nil {
			t.Fatal(err)
		}
	}
	cctx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	if _, err := q3.acquire(cctx, 0, base); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("cancelled waiter: %v", err)
	}
	q3.mu.Lock()
	defer q3.mu.Unlock()
	if len(q3.waiters) != 0 || q3.inflight != minJoinConcurrency {
		t.Fatalf("cancelled waiter left %d waiters, %d in flight", len(q3.waiters), q3.inflight)
	}
}

// The keep-alive floor of §10.2: max(4×SRTT, 500 ms).
func TestKeepAliveFloorIsFourRoundTripsOrHalfASecond(t *testing.T) {
	for srtt, want := range map[time.Duration]time.Duration{
		0:                      500 * time.Millisecond,
		20 * time.Millisecond:  500 * time.Millisecond,
		200 * time.Millisecond: 800 * time.Millisecond,
		2 * time.Second:        8 * time.Second,
	} {
		if got := keepAliveFloor(srtt); got != want {
			t.Errorf("SRTT %v: floor %v, want %v", srtt, got, want)
		}
	}
}

// Replay on a rebound lane waits for the first ACK after the rebinding, or
// one SRTT (§10.3 step 5).
func TestReplayGateOpensOnTheFirstACKOrAfterOneSRTT(t *testing.T) {
	f := &multipathFlow{done: make(chan struct{})}
	lane := &mpLane{replayGate: &replayGate{acks: 0, until: time.Now().Add(time.Hour)}}
	go func() {
		time.Sleep(30 * time.Millisecond)
		f.acksIn.Add(1)
	}()
	start := time.Now()
	f.waitReplayGate(context.Background(), lane)
	if took := time.Since(start); took < 25*time.Millisecond || took > time.Second {
		t.Fatalf("gate opened after %v; the ACK came at 30ms", took)
	}
	lane.replayGate = &replayGate{acks: f.acksIn.Load(), until: time.Now().Add(40 * time.Millisecond)}
	start = time.Now()
	f.waitReplayGate(context.Background(), lane)
	if took := time.Since(start); took < 35*time.Millisecond || took > time.Second {
		t.Fatalf("gate without an ACK opened after %v, want one SRTT (40ms)", took)
	}
	// No gate, no wait.
	start = time.Now()
	f.waitReplayGate(context.Background(), &mpLane{})
	if time.Since(start) > 10*time.Millisecond {
		t.Fatal("an ungated lane waited")
	}
}

// Bundles (test 18): recovery=carrier needs the path controller and the
// sticky coded retry on both ends, and on a client the single topology
// without a fan-out; anything else is rejected at start-up.
func TestRecoveryBundleIsValidated(t *testing.T) {
	cases := []struct {
		recovery RecoveryMode
		scope    CongestionScope
		coded    CodedRetryMode
		topology Topology
		fanOut   int
		gateway  bool
		ok       bool
	}{
		{"", "", "", "", 0, false, true},
		{RecoveryFlow, CongestionScopeConnection, CodedRetryLegacy, TopologyIsolated, 0, false, true},
		{RecoveryCarrier, CongestionScopePath, CodedRetrySticky, TopologySingle, 0, false, true},
		{RecoveryCarrier, CongestionScopePath, CodedRetrySticky, "", 0, true, true},
		{RecoveryCarrier, CongestionScopePath, CodedRetrySticky, TopologyIsolated, 0, false, false},
		{RecoveryCarrier, CongestionScopePath, CodedRetrySticky, TopologySingle, 2, false, false},
		{RecoveryCarrier, CongestionScopePath, CodedRetryLegacy, TopologySingle, 0, false, false},
		{RecoveryCarrier, CongestionScopeConnection, CodedRetrySticky, TopologySingle, 0, false, false},
		{RecoveryCarrier, CongestionScopeConnection, CodedRetrySticky, "", 0, true, false},
		{RecoveryCarrier, CongestionScopePath, CodedRetryLegacy, "", 0, true, false},
		{"bogus", CongestionScopePath, CodedRetrySticky, TopologySingle, 0, false, false},
	}
	for _, tc := range cases {
		err := ValidateRecovery(tc.recovery, tc.scope, tc.coded, tc.topology, tc.fanOut, tc.gateway)
		if (err == nil) != tc.ok {
			t.Errorf("%+v: %v, want ok=%v", tc, err, tc.ok)
		}
	}
	got := Bundle(CCAPIV2, MemoryOwned, CodedRetrySticky, GatewayAdmissionLegacy, SchedClass, ClassifierPhased, CongestionScopePath, TopologySingle, RecoveryCarrier)
	if want := "cc-api=v2,sched=class,congestion=path,topology=single,recovery=carrier,coded-retry=sticky,mem=owned,classifier=phased"; got != want {
		t.Fatalf("bundle %q, want %q", got, want)
	}
}

// Both constructors apply the validator; the full stage 1-7 bundle with
// classifier=phased composes with recovery=carrier.
func TestClientAndServerValidateTheRecoveryBundle(t *testing.T) {
	certificate, roots := testCertificate(t)
	full := func(cfg *ClientConfig) {
		cfg.CCAPI, cfg.Sched, cfg.CongestionScope, cfg.Topology = CCAPIV2, SchedClass, CongestionScopePath, TopologySingle
		cfg.CodedRetry, cfg.Memory, cfg.Classifier, cfg.EnableQUICPool = CodedRetrySticky, MemoryOwned, ClassifierPhased, true
	}
	for _, tc := range []struct {
		name   string
		mutate func(*ClientConfig)
		ok     bool
	}{
		{"full bundle", func(c *ClientConfig) { full(c); c.Recovery = RecoveryCarrier }, true},
		{"without topology=single", func(c *ClientConfig) { full(c); c.Topology = TopologyIsolated; c.Recovery = RecoveryCarrier }, false},
		{"without coded-retry", func(c *ClientConfig) { full(c); c.CodedRetry = CodedRetryLegacy; c.Recovery = RecoveryCarrier }, false},
		{"bad thresholds", func(c *ClientConfig) {
			full(c)
			c.Recovery = RecoveryCarrier
			c.CarrierRecovery = CarrierRecoveryConfig{SuspectPTOs: 5, ReplacePTOs: 2}
		}, false},
	} {
		cfg := ClientConfig{ListenAddr: "127.0.0.1:0", RemoteAddr: "127.0.0.1:1", Credentials: roots}
		tc.mutate(&cfg)
		client, err := NewClient(cfg)
		if (err == nil) != tc.ok {
			t.Errorf("client %s: %v, want ok=%v", tc.name, err, tc.ok)
		}
		if err == nil && (client.carriers == nil) != (cfg.Recovery != RecoveryCarrier) {
			t.Errorf("client %s: carrier manager present=%v", tc.name, client.carriers != nil)
		}
	}
	for _, tc := range []struct {
		scope CongestionScope
		coded CodedRetryMode
		ok    bool
	}{
		{CongestionScopePath, CodedRetrySticky, true},
		{CongestionScopeConnection, CodedRetrySticky, false},
		{CongestionScopePath, CodedRetryLegacy, false},
	} {
		_, err := NewServer(ServerConfig{
			ListenAddr: "127.0.0.1:0", Credentials: certificate, EnableQUIC: true,
			CCAPI: CCAPIV2, Sched: SchedClass, CongestionScope: tc.scope, CodedRetry: tc.coded, Recovery: RecoveryCarrier,
		})
		if (err == nil) != tc.ok {
			t.Errorf("server %s/%s: %v, want ok=%v", tc.scope, tc.coded, err, tc.ok)
		}
	}
}
