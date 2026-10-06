package pep

import (
	"context"
	"errors"
	"net"
	"syscall"
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
	fc.writeMu <- struct{}{}
	defer func() { <-fc.writeMu }()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := fc.WriteContext(ctx, protocol.Frame{}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waiting writer: %v", err)
	}
}

func TestBulkPoolCountsPendingDialsAndCancelsOnReset(t *testing.T) {
	rig := newJoinTestRig(t, TransportQUIC, TransportQUIC, 1)
	client := rig.client
	client.memoryLimits.maxBulkConnections = 2
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	client.cfg.SocketControl = func(_, _ string, _ syscall.RawConn) error {
		entered <- struct{}{}
		<-release
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	result := make(chan error, 2)
	for range 2 {
		go func() { _, err := client.reserveBulkConn(ctx); result <- err }()
	}
	for range 2 {
		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal("pending dial did not start")
		}
	}
	if _, err := client.reserveBulkConn(ctx); !errors.Is(err, errBulkConnectionLimit) {
		t.Fatalf("third dial: %v", err)
	}
	client.closeBulkQUICPool("test reset")
	close(release)
	for range 2 {
		select {
		case err := <-result:
			if err == nil {
				t.Fatal("pre-reset dial was admitted")
			}
		case <-ctx.Done():
			t.Fatal("reset did not cancel dial")
		}
	}
	if client.bulkConnCount() != 0 {
		t.Fatal("old dial repopulated reset pool")
	}
	client.cfg.SocketControl = nil
	entry, err := client.reserveBulkConn(ctx)
	if err != nil {
		t.Fatalf("capacity was not released: %v", err)
	}
	client.releaseBulkConn(entry, true)
}
