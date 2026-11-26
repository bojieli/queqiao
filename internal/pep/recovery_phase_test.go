package pep

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/bojieli/queqiao/internal/protocol"
)

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
			attachTestCarriers(client)
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
	attachTestCarriers(client)
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
