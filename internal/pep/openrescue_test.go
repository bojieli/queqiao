package pep

import (
	"context"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bojieli/queqiao/internal/metrics"
	"github.com/bojieli/queqiao/internal/protocol"
)

// A live original lane owns the optimistic OPEN until OPEN_OK or its existing
// deadline. A JOIN sent sooner can overtake OPEN and turn an ordinary delayed
// handshake into a terminal "unknown session" refusal.
func TestStallRescueWaitsForOptimisticOpenConfirmation(t *testing.T) {
	for _, loseOriginal := range []bool{false, true} {
		name := "confirmed OPEN resumes stall rescue"
		if loseOriginal {
			name = "lost original lane still gets replacement"
		}
		t.Run(name, func(t *testing.T) {
			flow := newGraceTestFlow(t)
			t.Cleanup(flow.closeAll)
			flow.requireOpenConfirmation()
			// Two unbuffered sends prove the manager has processed at least
			// the first signal, without using a sleep as a scheduling guess.
			flow.stallSignal = make(chan struct{})
			original := isolationLane(t, 0)
			if err := flow.addLane(original); err != nil {
				t.Fatal(err)
			}
			var joins atomic.Int32
			client := &Client{
				cfg:     ClientConfig{Transport: TransportAuto, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))},
				metrics: metrics.New(),
				openJoinLaneForTest: func(context.Context, TransportKind, uint64) (*mpLane, error) {
					joins.Add(1)
					return nil, errLaneJoinRejected
				},
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			done := make(chan struct{})
			go func() {
				defer close(done)
				client.manageQUICLanes(ctx, flow, flow.sessionID, flow.flowID)
			}()
			t.Cleanup(func() { cancel(); <-done })
			for range 2 {
				select {
				case flow.stallSignal <- struct{}{}:
				case <-done:
					t.Fatal("stall rescue ended a still-pending optimistic OPEN")
				case <-ctx.Done():
					t.Fatal("lane manager did not process the stall signal")
				}
			}
			if got := joins.Load(); got != 0 {
				t.Fatalf("JOIN attempts before OPEN confirmation = %d, want 0", got)
			}
			if flow.doneChanClosed() || original.closed.Load() || flow.resumeRefused.Load() {
				t.Fatal("the pending OPEN lost its original lane or became unresumable")
			}

			if loseOriginal {
				// The periodic zero-lane path must still try recovery when
				// the OPEN may have arrived but its original carrier died.
				if !flow.closeFailedLane(original) {
					t.Fatal("original lane was already closed")
				}
			} else {
				if err := flow.acceptOpenConfirmation(protocol.Frame{Header: protocol.Header{
					Version: protocol.Version, Type: protocol.TypeOpenOK,
					SessionID: flow.sessionID, FlowID: flow.flowID,
				}}); err != nil {
					t.Fatal(err)
				}
				select {
				case flow.stallSignal <- struct{}{}:
				case <-done:
					// The preceding signal may already have observed OPEN_OK.
				case <-ctx.Done():
					t.Fatal("confirmed flow did not accept another stall signal")
				}
			}
			select {
			case <-done:
			case <-ctx.Done():
				t.Fatal("terminal refusal did not end recovery")
			}
			if got := joins.Load(); got != 1 || !flow.resumeRefused.Load() {
				t.Fatalf("terminal refusal: JOINs=%d, refused=%t", got, flow.resumeRefused.Load())
			}
			if !loseOriginal && !flow.doneChanClosed() {
				t.Fatal("terminal refusal after OPEN confirmation left the flow open")
			}
		})
	}
}
