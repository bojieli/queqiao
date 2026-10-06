package pep

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/bojieli/queqiao/internal/memlimit"
	"github.com/bojieli/queqiao/internal/protocol"
)

type deliveryBarrierConn struct {
	net.Conn
	entered chan struct{}
}

func (c *deliveryBarrierConn) Write(p []byte) (int, error) {
	select {
	case c.entered <- struct{}{}:
	default:
	}
	return c.Conn.Write(p)
}

func TestReceiveBudgetIncludesBlockedApplicationWrite(t *testing.T) {
	inner, app := net.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	barrier := &deliveryBarrierConn{Conn: inner, entered: make(chan struct{}, 1)}
	f := newMultipathFlow(ctx, barrier, [16]byte{1}, 7, 1024, protocol.FlagAckUp, protocol.FlagAckDown, nil, nil)
	f.receiveMemory = memlimit.New(4)
	result := make(chan error, 1)
	go func() { result <- f.receiveInner(ctx) }()
	defer func() {
		cancel()
		f.closeAll()
		inner.Close()
		app.Close()
		select {
		case <-result:
		case <-time.After(time.Second):
			t.Error("receiver did not stop")
		}
		if got := f.receiveMemory.Snapshot().Used; got != 0 {
			t.Errorf("retained after close: %d", got)
		}
	}()
	frame := protocol.Frame{Header: protocol.Header{Version: protocol.Version, Type: protocol.TypeData, SessionID: f.sessionID, FlowID: f.flowID}, Payload: []byte("data")}
	if !f.deliverInbound(&mpLane{}, frame) {
		t.Fatal("data rejected")
	}
	select {
	case <-barrier.entered:
	case <-time.After(time.Second):
		t.Fatal("delivery did not start")
	}
	if got := f.receiveMemory.Snapshot().Used; got != 4 {
		t.Fatalf("blocked data not charged: %d", got)
	}
	if f.receiveMemory.TryAcquire(1) {
		f.receiveMemory.Release(1)
		t.Fatal("blocked data allowed overcommit")
	}
	// Reverse-direction ACKs must still progress while the application is blocked.
	f.noteSent(0, 4)
	frame.Header.Type, frame.Header.Flags, frame.Header.Sequence = protocol.TypeAck, protocol.FlagAckUp, 4
	frame.Payload = nil
	if !f.deliverInbound(&mpLane{}, frame) {
		t.Fatal("ACK blocked by full receive budget")
	}
	f.replayMu.Lock()
	acked := f.acked
	f.replayMu.Unlock()
	if acked != 4 {
		t.Fatalf("ACK progress = %d", acked)
	}
}
