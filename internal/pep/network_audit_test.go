package pep

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/bojieli/queqiao/internal/coded"
	"github.com/bojieli/queqiao/internal/protocol"
)

func TestCodedFallbackHonorsWriteContext(t *testing.T) {
	inner, peer := net.Pipe()
	defer inner.Close()
	defer peer.Close()
	path := coded.New(newPipeCarrier(), coded.Config{})
	_ = path.Close()
	fc := newSplitFrameConn(inner, path)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := fc.WriteContext(ctx, protocol.Frame{Header: protocol.Header{Type: protocol.TypePacket}})
	if err == nil || time.Since(start) > time.Second {
		t.Fatalf("fallback write: %v after %v", err, time.Since(start))
	}
}

func TestWriteContextCancelsWhileWaitingForWriter(t *testing.T) {
	inner, peer := net.Pipe()
	defer inner.Close()
	defer peer.Close()
	fc := newFrameConn(inner)
	fc.writeMu.Lock()
	defer fc.writeMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := fc.WriteContext(ctx, protocol.Frame{}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waiting writer: %v", err)
	}
}
