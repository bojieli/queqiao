package pep

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bojieli/queqiao/internal/metrics"
)

// Issue #53: a TCP flow whose only lane ends -- typically with an EOF from the
// peer -- has to get a zero-lane replacement attempt before its grace runs out.
// Stall rescues beside a still-open lane used to draw on the same budget, so a
// flow that had been rescued maxLaneRecoveryAttempts times in the last few
// minutes lost its last lane with nothing left to spend: the manager skipped
// every tick without counting an attempt and without abandoning, and the flow
// sat out the whole grace to fail with "lane replacement timeout" and
// lane_replacement_attempts == 0.
func TestTCPStallRescuesDoNotSpendZeroLaneRecovery(t *testing.T) {
	flow := newGraceTestFlow(t)
	flow.stallGrace = time.Millisecond
	// An unbuffered signal proves the manager took each request.
	flow.stallSignal = make(chan struct{})
	t.Cleanup(flow.closeAll)
	current := isolationLaneKind(t, 0, TransportTCP)
	if err := flow.addLane(current); err != nil {
		t.Fatal(err)
	}
	var stallJoins, zeroLaneJoins atomic.Int32
	client := &Client{
		cfg:     ClientConfig{Transport: TransportTCP, TCPFallbackLanes: 1, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))},
		metrics: metrics.New(),
		openJoinLaneForTest: func(ctx context.Context, _ TransportKind, id uint64) (*mpLane, error) {
			if _, stall := ctx.Value(laneReplacementContextKey{}).(laneReplacementHint); stall {
				stallJoins.Add(1)
			} else {
				zeroLaneJoins.Add(1)
			}
			return isolationLaneKind(t, id, TransportTCP), nil
		},
	}
	attachTestCarriers(client)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	done := make(chan struct{})
	go func() {
		defer close(done)
		client.manageTCPBundle(ctx, flow, flow.sessionID, flow.flowID)
	}()
	t.Cleanup(func() { cancel(); <-done })

	waitReplaced := func(previous *mpLane) *mpLane {
		t.Helper()
		for {
			if lanes := flow.healthyLanes(); len(lanes) == 1 && lanes[0] != previous {
				return lanes[0]
			}
			select {
			case <-ctx.Done():
				t.Fatalf("lane %d was not replaced", previous.id)
			case <-done:
				t.Fatal("TCP manager returned")
			case <-time.After(5 * time.Millisecond):
			}
		}
	}
	for range maxLaneRecoveryAttempts {
		current.suspected.Store(true)
		flow.setRescueReplacement(current.id)
		before := stallJoins.Load()
		// The manager paces rescues by the stall threshold, so a signal can
		// be taken and declined; keep asking until this round's JOIN starts.
		for stallJoins.Load() == before {
			select {
			case flow.stallSignal <- struct{}{}:
			case <-ctx.Done():
				t.Fatal("TCP manager did not take the stall signal")
			case <-time.After(5 * time.Millisecond):
			}
		}
		current = waitReplaced(current)
	}
	if got := stallJoins.Load(); got != maxLaneRecoveryAttempts {
		t.Fatalf("stall rescues = %d, want %d", got, maxLaneRecoveryAttempts)
	}

	// The peer ends the only lane.
	if !flow.closeFailedLane(current) {
		t.Fatal("lane was already closed")
	}
	waitReplaced(current)
	if zeroLaneJoins.Load() == 0 || flow.replacementAttempts.Load() == 0 {
		t.Fatalf("zero-lane joins=%d attempts=%d, want a counted replacement", zeroLaneJoins.Load(), flow.replacementAttempts.Load())
	}
}

// A zero-lane budget that is spent must end the wait, not skip every tick in
// silence: nothing else opens a lane for the flow, so its grace would only
// hold the application open before failing with the same answer.
func TestTCPZeroLaneRecoveryExhaustionAbandonsTheFlow(t *testing.T) {
	flow := newGraceTestFlow(t)
	t.Cleanup(flow.closeAll)
	var joins atomic.Int32
	client := &Client{
		cfg:     ClientConfig{Transport: TransportTCP, TCPFallbackLanes: 1, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))},
		metrics: metrics.New(),
		openJoinLaneForTest: func(_ context.Context, _ TransportKind, id uint64) (*mpLane, error) {
			joins.Add(1)
			// An accept-then-close peer: the JOIN completes, the lane dies.
			lane := isolationLaneKind(t, id, TransportTCP)
			lane.closed.Store(true)
			return lane, nil
		},
	}
	attachTestCarriers(client)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		client.manageTCPBundle(ctx, flow, flow.sessionID, flow.flowID)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatalf("manager kept running after %d zero-lane attempts", joins.Load())
	}
	if got := flow.replacementAttempts.Load(); got != maxLaneRecoveryAttempts {
		t.Fatalf("replacement attempts = %d, want %d", got, maxLaneRecoveryAttempts)
	}
	if err := flow.waitForHealthyLane(context.Background(), time.Minute); !errors.Is(err, errReplacementAbandoned) {
		t.Fatalf("wait after exhaustion = %v, want %v", err, errReplacementAbandoned)
	}
}
