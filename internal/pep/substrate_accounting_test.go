package pep

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/bojieli/queqiao/internal/coded"
	"github.com/bojieli/queqiao/internal/protocol"
)

func TestCodedFallbackCountsSuccessfulSubstrate(t *testing.T) {
	for _, contextual := range []bool{false, true} {
		inner, peer := net.Pipe()
		path := coded.New(newPipeCarrier(), coded.Config{})
		_ = path.Close()
		fc := newSplitFrameConn(inner, path)
		go func() { _, _ = io.Copy(io.Discard, peer) }()
		frame := protocol.Frame{Header: protocol.Header{Version: protocol.Version, Type: protocol.TypePacket}}
		var err error
		if contextual {
			err = fc.WriteContext(context.Background(), frame)
		} else {
			err = fc.Write(frame)
		}
		if err != nil {
			t.Fatal(err)
		}
		if fc.codedPackets.Load() != 0 || fc.streamPackets.Load() != 1 {
			t.Fatal("fallback counted the failed coded attempt")
		}
		_ = inner.Close()
		_ = peer.Close()
		if err = fc.WriteContext(context.Background(), frame); err == nil {
			t.Fatal("closed write succeeded")
		}
		if fc.codedPackets.Load() != 0 || fc.streamPackets.Load() != 1 {
			t.Fatal("failed write changed counters")
		}
	}
}

func TestUDPWriterWaitHonorsCancellationAndPacketAge(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f := newUDPForwarder(ctx, nil, nil, nil)
	f.writeGate <- struct{}{}
	result := make(chan error, 1)
	go func() { result <- f.writePacket(udpForwardPacket{queued: time.Now()}, nil) }()
	cancel()
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("cancelled write succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("writer ignored cancellation")
	}
	<-f.writeGate
	f.ctx = context.Background()
	if err := f.writePacket(udpForwardPacket{queued: time.Now().Add(-2 * udpForwardPacketAge)}, nil); err == nil {
		t.Fatal("expired packet sent")
	}
}
