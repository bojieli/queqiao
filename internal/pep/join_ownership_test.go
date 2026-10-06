package pep

import (
	"context"
	"github.com/bojieli/queqiao/internal/metrics"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"
)

// A JOIN can finish after its manager has returned. The late transport must
// be closed, not stranded in a result queue with no remaining reader.
func TestLaneManagerClosesJoinCompletedAfterExit(t *testing.T) {
	for _, kind := range []TransportKind{TransportQUIC, TransportTCP} {
		t.Run(string(kind), func(t *testing.T) {
			for i := 0; i < 12; i++ {
				flow := newIsolationTestFlow(t, kind == TransportQUIC)
				flow.started = time.Now().Add(-time.Minute)
				flow.bytesDown.Store(1024 * 1024)
				if kind == TransportQUIC {
					flow.lanes[0] = isolationLaneKind(t, 0, kind)
				}
				c := &Client{cfg: ClientConfig{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}, metrics: metrics.New()}
				c.quicPoolActive.Store(2)
				local, peer := net.Pipe()
				started := make(chan struct{})
				release := make(chan struct{})
				returned := make(chan struct{})
				c.openJoinLaneForTest = func(ctx context.Context, k TransportKind, id uint64) (*mpLane, error) {
					close(started)
					<-release
					return &mpLane{id: id, kind: k, fc: newFrameConn(local)}, nil
				}
				ctx, cancel := context.WithCancel(context.Background())
				go func() { c.manageLanes(ctx, flow, flow.sessionID, flow.flowID, kind); close(returned) }()
				select {
				case <-started:
				case <-time.After(3 * time.Second):
					cancel()
					t.Fatal("manager did not start JOIN")
				}
				cancel()
				select {
				case <-returned:
				case <-time.After(time.Second):
					t.Fatal("manager did not exit")
				}
				close(release)
				_ = peer.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
				_, err := peer.Read(make([]byte, 1))
				_ = local.Close()
				_ = peer.Close()
				flow.closeAll()
				if err != io.EOF {
					t.Fatalf("late %s JOIN transport leaked after manager exit: %v", kind, err)
				}
			}
		})
	}
}
