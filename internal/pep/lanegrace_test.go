package pep

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/bojieli/queqiao/internal/identity"
	"github.com/bojieli/queqiao/internal/metrics"
	"github.com/bojieli/queqiao/internal/protocol"
)

// The replacement grace covers the time a replacement lane needs to arrive.
// The flow used to end it only on an explicit refusal, which arrives when a
// rescue handshake completes -- and on a path lossy enough to have killed
// every lane, the rescue handshake is usually what fails instead. So the
// answer often never came and the application waited in silence: 25s on
// average and 573s at worst in the reported incident.
func TestWaitEndsWhenNoReplacementWillBeAttempted(t *testing.T) {
	flow := newGraceTestFlow(t)
	waits := make(chan error, 1)
	go func() { waits <- flow.waitForHealthyLane(context.Background(), laneReplacementWait) }()

	select {
	case err := <-waits:
		t.Fatalf("the wait ended with no evidence at all: %v", err)
	case <-time.After(150 * time.Millisecond):
	}

	flow.noteReplacementAbandoned()
	select {
	case err := <-waits:
		if !errors.Is(err, errReplacementAbandoned) {
			t.Fatalf("wait error = %v, want %v", err, errReplacementAbandoned)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("the flow waited out its grace after replacement was abandoned")
	}
}

// A flow that already knows nothing is coming must not enter the wait at all.
func TestWaitIsNotEnteredOnceReplacementIsAbandoned(t *testing.T) {
	flow := newGraceTestFlow(t)
	flow.noteReplacementAbandoned()
	start := time.Now()
	if err := flow.waitForHealthyLane(context.Background(), laneReplacementWait); !errors.Is(err, errReplacementAbandoned) {
		t.Fatalf("wait error = %v, want %v", err, errReplacementAbandoned)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("the wait took %s to report what it already knew", elapsed)
	}
}

// A healthy lane is still the answer, whatever the manager has stopped doing.
func TestAbandonedReplacementDoesNotFailAFlowWithALane(t *testing.T) {
	flow := newGraceTestFlow(t)
	local, remote := net.Pipe()
	t.Cleanup(func() { _ = local.Close(); _ = remote.Close() })
	flow.lanes[1] = &mpLane{id: 1, kind: TransportQUIC, fc: newFrameConn(local)}
	flow.noteReplacementAbandoned()
	if err := flow.waitForHealthyLane(context.Background(), laneReplacementWait); err != nil {
		t.Fatalf("a flow with a healthy lane failed: %v", err)
	}
}

// The wait is a client-side conclusion drawn from the client's own behaviour.
// A server flow never draws it: the rescue is still somebody's to send, and
// its grace is what gives the peer time to send it.
func TestTheLaneManagerIsWhatAbandonsReplacement(t *testing.T) {
	flow := newGraceTestFlow(t)
	if flow.replacementAbandoned.Load() {
		t.Fatal("a fresh flow starts with its replacement grace already spent")
	}
	client := &Client{cfg: ClientConfig{Logger: slog.New(slog.NewTextHandler(discardWriter{}, nil))}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	client.manageQUICLanes(ctx, flow, flow.sessionID, flow.flowID)
	if !flow.replacementAbandoned.Load() {
		t.Fatal("the lane manager returned without ending the flow's replacement grace")
	}
}

type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }

func newGraceTestFlow(t *testing.T) *multipathFlow {
	t.Helper()
	inner, peer := net.Pipe()
	t.Cleanup(func() { _ = inner.Close(); _ = peer.Close() })
	return newMultipathFlow(context.Background(), inner, [16]byte{2}, 3, defaultChunkSize, protocol.FlagAckUp, protocol.FlagAckDown, nil, nil)
}

// A flow that fails with "lane replacement timeout" used to say only that it
// gave up. The live gateway produced 521 of those in two hours, all of them
// ordinary small exchanges that had already moved a few kilobytes, and the
// record could not say whether a replacement lane had ever been offered or how
// many graces the flow burned before failing. That is the difference between a
// pool that will not rebuild and a path that will not carry a handshake, and
// neither the flow's own log nor the gateway's could distinguish them. See
// issue #53.
func TestAFlowRecordsWhatItsLaneReplacementsDid(t *testing.T) {
	flow := newGraceTestFlow(t)

	if waits, timeouts, joined, waited := flow.replacementDiagnostics(); waits != 0 || timeouts != 0 || joined != 0 || waited != 0 {
		t.Fatalf("a flow that has not waited reports waits=%d timeouts=%d joined=%d waited=%s",
			waits, timeouts, joined, waited)
	}

	// A grace that runs out is the case the incident was made of.
	const grace = 200 * time.Millisecond
	if err := flow.waitForHealthyLane(context.Background(), grace); err == nil {
		t.Fatal("the wait succeeded with no healthy lane")
	}
	waits, timeouts, _, waited := flow.replacementDiagnostics()
	if waits != 1 || timeouts != 1 {
		t.Fatalf("one exhausted grace recorded waits=%d timeouts=%d, want 1 and 1", waits, timeouts)
	}
	if waited < grace {
		t.Fatalf("the flow waited %s but recorded %s", grace, waited)
	}

	// A second waiter for the same missing lane has to be visible as a second
	// waiter -- the counters are how a gateway record separates one call site
	// giving up from four of them -- but it must not buy the flow a second
	// grace. The observed durations clustered at roughly twice
	// laneReplacementWait, which is what that used to cost.
	if err := flow.waitForHealthyLane(context.Background(), grace); err == nil {
		t.Fatal("the second wait succeeded with no healthy lane")
	}
	waits, timeouts, _, twice := flow.replacementDiagnostics()
	if waits != 2 || timeouts != 2 {
		t.Fatalf("two waiters recorded waits=%d timeouts=%d, want 2 and 2", waits, timeouts)
	}
	if twice >= 2*grace {
		t.Fatalf("two waiters on one outage spent %s, at least two graces of %s", twice, grace)
	}

	// And an exit that is not a timeout must not be counted as one, or the
	// field cannot separate "nothing came" from "we stopped waiting".
	flow.noteReplacementAbandoned()
	if err := flow.waitForHealthyLane(context.Background(), grace); !errors.Is(err, errReplacementAbandoned) {
		t.Fatalf("wait error = %v, want %v", err, errReplacementAbandoned)
	}
	if _, timeouts, _, _ := flow.replacementDiagnostics(); timeouts != 2 {
		t.Fatalf("abandoning replacement counted as a timeout: timeouts=%d, want 2", timeouts)
	}
}

// The grace is what a flow is prepared to wait for a lane that is not there.
// It was being spent per waiter rather than per outage: run, the frame and
// control writers and the acknowledgement loop all wait for the same missing
// lane, so a flow that was never going to be rescued could pay the whole
// grace once for each of them. Issue #53 first attributed its 76-106 s
// failure cluster to this; the re-measurement withdrew that (failing flows
// time out once, and the cluster is idle time plus one grace), but bounding
// an outage to one grace still bounds the small tail that did wait longer.
func TestOneOutageSpendsOneGraceHoweverManyWaitersItHas(t *testing.T) {
	flow := newGraceTestFlow(t)
	const grace = 300 * time.Millisecond
	const concurrent = 3

	// Waiters that are already blocked when the grace runs out share it: a
	// lane dying with writes in flight puts several of them here at once.
	started := time.Now()
	errs := make(chan error, concurrent)
	for i := 0; i < concurrent; i++ {
		go func() { errs <- flow.waitForHealthyLane(context.Background(), grace) }()
	}
	for i := 0; i < concurrent; i++ {
		select {
		case err := <-errs:
			if !errors.Is(err, errLaneReplacementTimeout) {
				t.Fatalf("wait error = %v, want %v", err, errLaneReplacementTimeout)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("a waiter never returned")
		}
	}

	// And a waiter that arrives afterwards is told at once. This is the one
	// that cost the flows in the incident: run waits for the lane only after
	// the failure reaches it, which on a flow whose writer was already blocked
	// is one whole grace after the outage began. It used to start a second.
	arrived := time.Now()
	if err := flow.waitForHealthyLane(context.Background(), grace); !errors.Is(err, errLaneReplacementTimeout) {
		t.Fatalf("wait error = %v, want %v", err, errLaneReplacementTimeout)
	}
	if late := time.Since(arrived); late > grace/2 {
		t.Fatalf("a waiter arriving after the grace was spent waited %s of a fresh %s", late, grace)
	}
	if elapsed := time.Since(started); elapsed >= 2*grace {
		t.Fatalf("one outage took %s, at least two graces of %s", elapsed, grace)
	}

	// Every waiter still has to be counted, or the record cannot say how many
	// call sites were stuck on the same absent lane.
	const waiters = concurrent + 1
	if waits, timeouts, _, _ := flow.replacementDiagnostics(); waits != waiters || timeouts != waiters {
		t.Fatalf("recorded waits=%d timeouts=%d, want %d and %d", waits, timeouts, waiters, waiters)
	}
}

// Spending the grace once per outage must not mean spending it once per flow.
// A long-lived flow that is genuinely being rescued loses a lane, gets one
// back, and may lose another an hour later; that second outage is a new one
// and is owed the whole grace.
func TestANewOutageIsOwedAWholeGrace(t *testing.T) {
	flow := newGraceTestFlow(t)
	const grace = 200 * time.Millisecond

	if err := flow.waitForHealthyLane(context.Background(), grace); !errors.Is(err, errLaneReplacementTimeout) {
		t.Fatalf("wait error = %v, want %v", err, errLaneReplacementTimeout)
	}

	// A lane arriving is what ends an outage, and it ends it in startLane --
	// the one point a lane starts carrying traffic -- rather than wherever a
	// waiter next happens to look.
	replacement := &mpLane{id: 7, kind: TransportQUIC, fc: newFrameConn(gracePipe(t))}
	if err := flow.addLane(replacement); err != nil {
		t.Fatalf("adding a replacement lane: %v", err)
	}
	if flow.replacementDeadline.Load() != 0 {
		t.Fatal("a replacement lane arrived and the spent outage was still holding its deadline")
	}

	flow.failLane(replacement, errors.New("lane died again"))
	started := time.Now()
	if err := flow.waitForHealthyLane(context.Background(), grace); !errors.Is(err, errLaneReplacementTimeout) {
		t.Fatalf("wait error = %v, want %v", err, errLaneReplacementTimeout)
	}
	if elapsed := time.Since(started); elapsed < grace {
		t.Fatalf("the second outage waited %s of its %s grace", elapsed, grace)
	}
}

func gracePipe(t *testing.T) net.Conn {
	t.Helper()
	local, remote := net.Pipe()
	t.Cleanup(func() { _ = local.Close(); _ = remote.Close() })
	return local
}

// A deadline can outlive the outage it belonged to. A waiter that finds no
// lane, and whose lane arrives before it records its deadline, writes that
// deadline after the arriving lane has already cleared the outage, and nothing
// is then left to remove it. Read as current, it would expire the flow's next
// outage before that outage began -- failing in an instant a flow that had a
// full grace coming to it.
func TestAStaleDeadlineDoesNotDenyTheNextOutageItsGrace(t *testing.T) {
	flow := newGraceTestFlow(t)
	const grace = 200 * time.Millisecond
	flow.replacementDeadline.Store(time.Now().Add(-time.Hour).UnixNano())

	started := time.Now()
	if err := flow.waitForHealthyLane(context.Background(), grace); !errors.Is(err, errLaneReplacementTimeout) {
		t.Fatalf("wait error = %v, want %v", err, errLaneReplacementTimeout)
	}
	if elapsed := time.Since(started); elapsed < grace {
		t.Fatalf("an outage inheriting a stale deadline waited %s of its %s grace", elapsed, grace)
	}
}

func replacementFields(f *multipathFlow) map[string]any {
	raw := f.replacementLogFields()
	out := map[string]any{}
	for i := 0; i+1 < len(raw); i += 2 {
		out[raw[i].(string)] = raw[i+1]
	}
	return out
}

// The gateway's failing flows say a replacement never arrived: 84% of its
// lane-replacement-timeout failures ended with no replacement lane ever
// admitted. The record could not say why, and the two reasons need different
// fixes -- a client pool that will not rebuild opens nothing, and a path that
// will not carry a handshake opens attempts that never complete. A dial that
// fails never reaches addLane, so `lanes_joined` reads the same either way.
// See issue #53.
func TestAFlowRecordsWhetherAReplacementWasEvenAttempted(t *testing.T) {
	// No replacement attempt was recorded.
	quiet := newGraceTestFlow(t)
	got := replacementFields(quiet)
	if got["lane_replacement_last_failure"] != nil || got["lane_replacement_attempts"] != uint64(0) || got["lane_replacement_failures"] != uint64(0) {
		t.Fatalf("a flow that attempted nothing reports attempts=%v failures=%v",
			got["lane_replacement_attempts"], got["lane_replacement_failures"])
	}

	// Each completion snapshot records the last returned error category. The
	// log must never contain the address or free text carried by the error.
	refused := newGraceTestFlow(t)
	for i, tc := range []struct {
		err  error
		want string
	}{
		{context.DeadlineExceeded, "timeout"},
		{errLaneJoinCapacity, "peer_capacity"},
		{errLaneJoinRejected, "peer_rejected"},
		{context.Canceled, "canceled"},
		{errors.New("private endpoint"), "other"},
	} {
		refused.noteReplacementAttempt()
		refused.noteReplacementFailure(fmt.Errorf("dial: %w", tc.err))
		got := replacementFields(refused)
		if got["lane_replacement_attempts"] != uint64(i+1) || got["lane_replacement_failures"] != uint64(i+1) ||
			got["lane_replacement_last_failure"] != tc.want {
			t.Fatalf("after replacement %d: got %v, want last failure %q", i+1, got, tc.want)
		}
	}

	// Both flows are indistinguishable by the fields that existed before: they
	// waited for nothing and nothing joined. That is the gap being closed.
	if replacementFields(quiet)["lanes_joined"] != replacementFields(refused)["lanes_joined"] {
		t.Fatal("the test no longer exercises the case the attempt counts were added for")
	}

	// A dial still outstanding is not yet a failure, or "nothing came back
	// yet" would read as "the path refused it".
	pending := newGraceTestFlow(t)
	pending.noteReplacementAttempt()
	got = replacementFields(pending)
	if got["lane_replacement_attempts"] != uint64(1) || got["lane_replacement_failures"] != uint64(0) {
		t.Fatalf("an outstanding dial reported attempts=%v failures=%v, want 1 and 0",
			got["lane_replacement_attempts"], got["lane_replacement_failures"])
	}
}

func TestZeroLaneRecoveryLogsReturnedRefusal(t *testing.T) {
	for _, transport := range []TransportKind{TransportQUIC, TransportTCP} {
		t.Run(string(transport), func(t *testing.T) {
			rig := newJoinTestRig(t, transport, transport, 1)
			// The client is not serving yet, so nothing else reads its
			// logger while it is replaced.
			records := &lockedRecords{}
			rig.client.cfg.Logger = slog.New(slog.NewJSONHandler(records, &slog.HandlerOptions{Level: slog.LevelInfo}))
			flow := newGraceTestFlow(t)
			t.Cleanup(flow.closeAll)
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()
			// This established client lost all lanes. The gateway no longer
			// has its session and answers the replacement JOIN with a refusal.
			rig.client.manageLanes(ctx, flow, [16]byte{99}, flow.flowID, transport)
			got := replacementFields(flow)
			if got["lane_replacement_last_failure"] != "peer_rejected" || got["lane_replacement_failures"] != uint64(1) {
				t.Fatalf("replacement completion fields = %v", got)
			}
			// The counted refusal also has its own line, keyed by flow and
			// attempt, even though the refusal ends the manager at once.
			msg := "lane recovery unavailable"
			if transport == TransportTCP {
				msg = "TCP bundle lane unavailable"
			}
			lines := records.records(t, msg)
			if len(lines) != 1 {
				t.Fatalf("refused replacement wrote %d %q lines, want 1", len(lines), msg)
			}
			if r := lines[0]; r["level"] != "INFO" || r["flow_id"] != float64(flow.flowID) ||
				r["attempt"] != float64(1) || r["failure_kind"] != "peer_rejected" {
				t.Fatalf("refusal line = %v", r)
			}
		})
	}
}

// An already armed wait must observe rescue evidence, not just a waiter that
// starts after the extension has been stored.
func TestWaitingFlowHonorsAnExtendedReplacementGrace(t *testing.T) {
	flow := newGraceTestFlow(t)
	const grace = 400 * time.Millisecond
	originalDeadline := time.Now().Add(grace)
	flow.replacementDeadline.Store(originalDeadline.UnixNano())
	waits := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go func() { waits <- flow.waitForHealthyLane(ctx, grace) }()

	// Let the waiter arm its timer before validated rescue evidence extends
	// the shared deadline. Merely changing the stored deadline is insufficient.
	select {
	case err := <-waits:
		t.Fatalf("wait ended before rescue evidence: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if !flow.extendReplacementOutage(time.Now(), time.Second) {
		t.Fatal("the active outage refused its extension")
	}
	select {
	case err := <-waits:
		t.Fatalf("wait ignored the extended deadline: %v", err)
	case <-time.After(time.Until(originalDeadline.Add(75 * time.Millisecond))):
	}

	local, remote := net.Pipe()
	t.Cleanup(func() { _ = local.Close(); _ = remote.Close() })
	flow.lanesMu.Lock()
	flow.lanes[1] = &mpLane{id: 1, kind: TransportQUIC, fc: newFrameConn(local)}
	flow.lanesMu.Unlock()
	select {
	case err := <-waits:
		if err != nil {
			t.Fatalf("a rescued flow failed: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("the waiter did not observe the rescued lane")
	}
	if got := flow.replacementTimeouts.Load(); got != 0 {
		t.Fatalf("successful recovery recorded %d timeouts", got)
	}
}

// A rescue JOIN arriving mid-grace is the peer's recovery proving itself
// alive. The grace restarts from that evidence rather than expiring
// underneath the handshake it was waiting for; a flow not in an outage has
// nothing to extend, and a deadline a whole grace in the past is an ended
// outage's residue, not evidence to build on.
func TestRescueEvidenceRestartsTheReplacementGrace(t *testing.T) {
	flow := newGraceTestFlow(t)
	now := time.Now()
	if flow.extendReplacementOutage(now, laneReplacementWait) {
		t.Fatal("a flow not in an outage had its grace extended")
	}
	if remaining := flow.replacementBudget(now, laneReplacementWait); remaining != laneReplacementWait {
		t.Fatalf("opening an outage left %s, want the whole grace", remaining)
	}
	before := flow.replacementDeadline.Load()
	later := now.Add(time.Second)
	if !flow.extendReplacementOutage(later, laneReplacementWait) {
		t.Fatal("a live outage refused its extension")
	}
	if got, want := flow.replacementDeadline.Load(), later.Add(laneReplacementWait).UnixNano(); got != want {
		t.Fatalf("extended deadline = %d, want %d", got, want)
	}
	if flow.replacementDeadline.Load() <= before {
		t.Fatal("the extension did not move the deadline forward")
	}
	flow.replacementDeadline.Store(now.Add(-2 * laneReplacementWait).UnixNano())
	if flow.extendReplacementOutage(now, laneReplacementWait) {
		t.Fatal("a stale deadline was revived by rescue evidence")
	}
}

// The gateway observes the rescue at JOIN validation time: a JOIN naming a
// live session currently waiting out an outage restarts that outage's grace
// and is counted.
func TestLaneJoinDuringGraceExtendsTheBudget(t *testing.T) {
	owner := identity.Principal{ProviderID: "provider", AccountID: "account", DeviceID: "owner"}
	registry := metrics.New()
	server := &Server{
		cfg:      ServerConfig{Logger: slog.New(slog.NewTextHandler(discardWriter{}, nil))},
		sessions: map[[16]byte]*serverFlow{},
		metrics:  registry,
	}
	flow := newGraceTestFlow(t)
	server.sessions[flow.sessionID] = newServerFlow(flow, owner, TransportTCP, 1)
	now := time.Now()
	if remaining := flow.replacementBudget(now, laneReplacementWait); remaining != laneReplacementWait {
		t.Fatalf("opening an outage left %s, want the whole grace", remaining)
	}

	local, remote := net.Pipe()
	t.Cleanup(func() { _ = local.Close(); _ = remote.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	request := protocol.Frame{Header: protocol.Header{
		Version: protocol.Version, Type: protocol.TypeJoin, SessionID: flow.sessionID,
		FlowID: flow.flowID, Class: protocol.ClassBulk,
	}}
	go server.handleLaneJoinOpen(ctx, local, newFrameConn(local), owner, flow.sessionID, 1, request)
	response, err := newFrameConn(remote).Read()
	if err != nil {
		t.Fatal(err)
	}
	if response.Header.Type != protocol.TypeOpenOK {
		t.Fatalf("response = %d, want OPEN_OK", response.Header.Type)
	}
	if got := registry.Snapshot().LaneGraceExtensions; got != 1 {
		t.Fatalf("grace extensions = %d, want 1", got)
	}
	// The deadline itself goes away once the admitted lane is activated,
	// which is the extension working as intended: what it bought was the time
	// between this JOIN becoming observable and its admission completing.
	// Activation trails the acknowledgement by a scheduling beat, so wait
	// for it.
	for deadline := time.Now().Add(2 * time.Second); ; {
		if flow.replacementDeadline.Load() == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the admitted lane never ended the outage")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// lockedRecords is a JSON log sink a test can read while the code under test
// may still be writing from another goroutine.
type lockedRecords struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (r *lockedRecords) Write(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.buf.Write(p)
}

func (r *lockedRecords) records(t *testing.T, msg string) []map[string]any {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []map[string]any
	for _, line := range bytes.Split(r.buf.Bytes(), []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var record map[string]any
		if err := json.Unmarshal(line, &record); err != nil {
			t.Fatalf("log line is not JSON: %q: %v", line, err)
		}
		if record["msg"] == msg {
			out = append(out, record)
		}
	}
	return out
}

// A zero-lane recovery round that ran out of time is the "path will not carry
// a handshake" case issue #53 needs to count per attempt. It used to be
// skipped entirely, leaving only the last-failure category on the flow
// record. It is now written, keyed by flow and attempt, below warn; a round
// cancelled because the flow ended stays out of the log.
func TestLaneRecoveryFailureLogLevels(t *testing.T) {
	records := &lockedRecords{}
	client := &Client{cfg: ClientConfig{Logger: slog.New(slog.NewJSONHandler(records, &slog.HandlerOptions{Level: slog.LevelDebug}))}}
	live := context.Background()
	ended, cancel := context.WithCancel(context.Background())
	cancel()

	client.logLaneRecoveryFailure(live, 7, 1, fmt.Errorf("dial: %w", context.DeadlineExceeded))
	client.logLaneRecoveryFailure(live, 7, 2, errors.New("handshake failed"))
	client.logLaneRecoveryFailure(ended, 7, 3, context.Canceled)
	client.logLaneRecoveryFailure(live, 7, 4, context.Canceled)
	client.logLaneRecoveryFailure(live, 7, 5, fmt.Errorf("%w: session", errLaneJoinRejected))
	client.logLaneRecoveryFailure(live, 7, 6, fmt.Errorf("%w: full", errLaneJoinCapacity))
	// A JOIN that hits the handshake's socket deadline is not a context
	// deadline, but it is reported as a timeout and logged like one.
	client.logLaneRecoveryFailure(live, 7, 7, &net.OpError{Op: "read", Net: "tcp", Err: os.ErrDeadlineExceeded})

	got := records.records(t, "lane recovery unavailable")
	want := []struct {
		level, kind string
		attempt     float64
	}{
		{"INFO", "timeout", 1},
		{"WARN", "other", 2},
		{"DEBUG", "canceled", 4},
		{"INFO", "peer_rejected", 5},
		{"INFO", "peer_capacity", 6},
		{"INFO", "timeout", 7},
	}
	if len(got) != len(want) {
		t.Fatalf("logged %d recovery failures, want %d: %v", len(got), len(want), got)
	}
	for i, w := range want {
		r := got[i]
		if r["level"] != w.level || r["failure_kind"] != w.kind || r["attempt"] != w.attempt || r["flow_id"] != float64(7) {
			t.Fatalf("record %d = %v, want level %s kind %s attempt %v flow_id 7", i, r, w.level, w.kind, w.attempt)
		}
	}
}

// The TCP failure line has to be findable by flow the same way the QUIC one
// is, or the attempts behind a TCP-carried failure cannot be lined up with
// its completion record.
func TestTCPReplacementFailureLogNamesFlowAndAttempt(t *testing.T) {
	_, roots := testCertificate(t)
	closed, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	remote := closed.Addr().String()
	_ = closed.Close()

	records := &lockedRecords{}
	client, err := NewClient(ClientConfig{
		ListenAddr: "127.0.0.1:0", RemoteAddr: remote,
		Credentials: roots, Transport: TransportTCP,
		DialTimeout: time.Second, HandshakeTimeout: time.Second,
		Logger: slog.New(slog.NewJSONHandler(records, &slog.HandlerOptions{Level: slog.LevelInfo})),
	})
	if err != nil {
		t.Fatal(err)
	}
	flow := newGraceTestFlow(t)
	t.Cleanup(flow.closeAll)
	ctx, cancel := context.WithTimeout(context.Background(), 2500*time.Millisecond)
	defer cancel()
	client.manageLanes(ctx, flow, [16]byte{98}, flow.flowID, TransportTCP)

	got := records.records(t, "TCP bundle lane unavailable")
	if len(got) == 0 {
		t.Fatal("a refused TCP replacement wrote no failure line")
	}
	attempts := flow.replacementAttempts.Load()
	for i, r := range got {
		if r["flow_id"] != float64(flow.flowID) || r["replacement"] != true || r["attempt"] != float64(i+1) {
			t.Fatalf("record %d = %v, want flow_id %d, replacement, attempt %d", i, r, flow.flowID, i+1)
		}
		if r["failure_kind"] == nil {
			t.Fatalf("record %d has no failure_kind: %v", i, r)
		}
	}
	// Every logged attempt was counted; one launched just before the manager
	// stopped may have been counted without returning.
	if uint64(len(got)) > attempts || attempts > uint64(len(got))+1 {
		t.Fatalf("logged %d failed attempts against lane_replacement_attempts %d", len(got), attempts)
	}
}

// Issue #187. The TCP lane manager could take a tick that arrived with the
// flow's end and dial one more replacement after the completion record was
// written, so a gateway saw a dial the record never counted. An attempt that
// starts is counted before the flow's end can be observed, and a flow that
// has ended starts none.
func TestAReplacementThatStartsIsInTheCompletionRecord(t *testing.T) {
	flow := newGraceTestFlow(t)
	if attempt, ok := flow.beginReplacementAttempt(); !ok || attempt != 1 {
		t.Fatalf("a live flow's first replacement = %d, %v; want 1, true", attempt, ok)
	}
	flow.signalDone()
	if attempt, ok := flow.beginReplacementAttempt(); ok {
		t.Fatalf("an ended flow started replacement %d", attempt)
	}
	if got := replacementFields(flow)["lane_replacement_attempts"]; got != uint64(1) {
		t.Fatalf("an ended flow counted %v attempts, want the 1 it started", got)
	}

	// The race itself: the record is read once done has closed, as the
	// client writes it, while a manager keeps starting attempts.
	for range 200 {
		flow := newGraceTestFlow(t)
		started := make(chan uint64, 1024)
		go func() {
			defer close(started)
			for {
				attempt, ok := flow.beginReplacementAttempt()
				if !ok {
					return
				}
				started <- attempt
			}
		}()
		for len(started) == 0 {
			runtime.Gosched()
		}
		flow.signalDone()
		recorded := replacementFields(flow)["lane_replacement_attempts"].(uint64)
		var admitted uint64
		for attempt := range started {
			if attempt > recorded {
				t.Fatalf("replacement %d started after a completion record that counts %d", attempt, recorded)
			}
			admitted++
		}
		if admitted != recorded {
			t.Fatalf("the completion record counts %d replacements; %d started", recorded, admitted)
		}
	}
}
