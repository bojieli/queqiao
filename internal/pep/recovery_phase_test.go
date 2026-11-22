package pep

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bojieli/queqiao/internal/metrics"
	"github.com/bojieli/queqiao/internal/protocol"
)

// A TCP JOIN retires QUIC on the server before returning to the client. Even
// a delayed QUIC success must exclude that irreversible side effect entirely.
func TestRecoveryQUICWinnerExcludesTCPHandoff(t *testing.T) {
	client := &Client{cfg: ClientConfig{Transport: TransportAuto, FallbackGrace: time.Second}, metrics: metrics.New()}
	flow := newGraceTestFlow(t)
	server := &serverFlow{flow: newGraceTestFlow(t), maxLanes: 2, tcpMaxLanes: 1}
	quic, _ := rescueTestLane(t, 1)
	tcp, _ := rescueTestLane(t, 2)
	tcp.kind = TransportTCP
	var committed atomic.Bool
	lane, err := client.raceRescueWithFallback(context.Background(), flow, []rescueAttempt{
		func(ctx context.Context) (*mpLane, error) {
			if err := server.addLane(quic); err != nil {
				return nil, err
			}
			time.Sleep(20 * time.Millisecond) // delayed JOIN acknowledgement
			return quic, nil
		},
		func(ctx context.Context) (*mpLane, error) { <-ctx.Done(); return nil, ctx.Err() },
	}, func(ctx context.Context) (*mpLane, error) {
		committed.Store(true)
		return tcp, server.addLane(tcp)
	})
	if err != nil {
		t.Fatal(err)
	}
	if lane != quic || committed.Load() || quic.closed.Load() || server.tcpMode {
		t.Fatal("TCP handoff invalidated the QUIC race winner")
	}
}

func TestRecoveryTCPFollowsFailedQUICPhase(t *testing.T) {
	for _, mode := range []string{"failed", "deadline", "peer-tcp", "capacity", "permanent", "cancelled", "quic-only"} {
		t.Run(mode, func(t *testing.T) {
			client := &Client{cfg: ClientConfig{Transport: TransportAuto, FallbackGrace: 20 * time.Millisecond}, metrics: metrics.New()}
			flow := newGraceTestFlow(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if mode == "quic-only" {
				client.cfg.Transport = TransportQUIC
			}
			if mode == "cancelled" {
				cancel()
			}
			tcp, _ := rescueTestLane(t, 2)
			tcp.kind = TransportTCP
			var tcpCalls atomic.Int32
			attempt := func(ctx context.Context) (*mpLane, error) {
				switch mode {
				case "deadline", "cancelled":
					<-ctx.Done()
					return nil, ctx.Err()
				case "peer-tcp":
					return nil, errLaneJoinTCPMode
				case "capacity":
					return nil, errLaneJoinCapacity
				case "permanent":
					return nil, errLaneJoinRejected
				default:
					return nil, errors.New("QUIC unavailable")
				}
			}
			lane, err := client.raceRescueWithFallback(ctx, flow, []rescueAttempt{attempt, attempt}, func(ctx context.Context) (*mpLane, error) { tcpCalls.Add(1); return tcp, nil })
			wantTCP := mode == "failed" || mode == "deadline" || mode == "peer-tcp" || mode == "capacity"
			if wantTCP {
				if err != nil || lane != tcp || tcpCalls.Load() != 1 {
					t.Fatalf("lane=%v error=%v TCP calls=%d", lane, err, tcpCalls.Load())
				}
			} else if err == nil || tcpCalls.Load() != 0 {
				t.Fatalf("unexpected fallback: error=%v TCP calls=%d", err, tcpCalls.Load())
			}
		})
	}
}

// A peer can stop during either half of JOIN. Cancellation must release the
// stream promptly instead of waiting for the independent handshake timeout.
func TestLaneJoinCancellationInterruptsHandshake(t *testing.T) {
	for _, readJoin := range []bool{false, true} {
		t.Run(map[bool]string{false: "blocked-write", true: "blocked-read"}[readJoin], func(t *testing.T) {
			local, remote := net.Pipe()
			defer local.Close()
			defer remote.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if readJoin {
				go func() { _, _ = newFrameConn(remote).Read(); cancel() }()
			} else {
				cancel()
			}
			client := &Client{cfg: ClientConfig{HandshakeTimeout: 10 * time.Second}}
			done := make(chan error, 1)
			go func() {
				_, err := client.completeLaneJoin(ctx, &authenticatedLane{fc: newFrameConn(local), outer: local, kind: TransportQUIC}, 7, 0)
				done <- err
			}()
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("cancelled handshake succeeded")
				}
			case <-time.After(time.Second):
				t.Fatal("JOIN ignored cancellation")
			}
		})
	}
}

// Once JOIN succeeds its stream belongs to the flow, not the dial context.
func TestLaneJoinSuccessSurvivesDialCancellation(t *testing.T) {
	local, remote := net.Pipe()
	defer local.Close()
	defer remote.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan struct{})
	go func() {
		fc := newFrameConn(remote)
		request, err := fc.Read()
		if err != nil {
			return
		}
		_ = fc.Write(protocol.Frame{Header: protocol.Header{Version: protocol.Version, Type: protocol.TypeOpenOK, SessionID: request.Header.SessionID, FlowID: request.Header.FlowID}})
		<-ready
		_, _ = remote.Write([]byte{42})
	}()
	client := &Client{cfg: ClientConfig{HandshakeTimeout: time.Second}}
	_, err := client.completeLaneJoin(ctx, &authenticatedLane{fc: newFrameConn(local), outer: local, kind: TransportQUIC}, 7, 0)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	close(ready)
	_ = local.SetReadDeadline(time.Now().Add(time.Second))
	b := make([]byte, 1)
	if _, err := local.Read(b); err != nil {
		t.Fatalf("dial cancellation closed installed lane: %v", err)
	}
}
