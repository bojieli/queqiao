package pep

import (
	"context"
	"github.com/bojieli/queqiao/internal/protocol"
	"net"
	"testing"
	"time"
)

func TestReceiveWorkerExitsWhenSendCompletesAfterRemoteFIN(t *testing.T) {
	inner, peer := net.Pipe()
	defer inner.Close()
	defer peer.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f := newMultipathFlow(ctx, inner, [16]byte{1}, 7, 1024, protocol.FlagAckUp, protocol.FlagAckDown, nil, nil)
	capture := newAckCaptureConn(0, nil)
	defer capture.Close()
	f.lanes[0] = &mpLane{id: 0, fc: newFrameConn(capture)}
	result := make(chan error, 1)
	go func() { result <- f.receiveInner(ctx) }()
	f.events <- inboundEvent{frame: protocol.Frame{Header: protocol.Header{Version: protocol.Version, Type: protocol.TypeClose, SessionID: f.sessionID, FlowID: f.flowID, Flags: protocol.FlagFin}}}
	waitAckFrame(t, capture)
	// A second historical FIN provides an event-loop barrier: the first FIN's
	// completed state has been committed before this acknowledgement is emitted.
	f.events <- inboundEvent{frame: protocol.Frame{Header: protocol.Header{Version: protocol.Version, Type: protocol.TypeClose, SessionID: f.sessionID, FlowID: f.flowID, Flags: protocol.FlagFin}}}
	waitAckFrame(t, capture)
	close(f.sendDone)
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		cancel()
		<-result
		t.Fatal("completed receive worker waits for watchdog")
	}
}
