package pep

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/bojieli/queqiao/internal/metrics"
)

// A gateway that does not answer must not hold the listener shut. The prewarm
// is bounded by its own timeout, and while it ran nothing was accepted: a
// browser's connections queued in the kernel for the whole of it.
func TestTheListenerDoesNotWaitOutASlowPrewarm(t *testing.T) {
	if testing.Short() {
		t.Skip("waits on a QUIC dial to a gateway that never answers")
	}
	silent, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer silent.Close()
	_, roots := testCertificate(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewClient(ClientConfig{
		ListenAddr: listener.Addr().String(), RemoteAddr: silent.LocalAddr().String(),
		Credentials: roots, Transport: TransportQUIC, EnableQUICPool: true,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Metrics: metrics.New(),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = client.ServeListener(ctx, listener) }()

	start := time.Now()
	conn, err := net.DialTimeout("tcp", listener.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
	if _, err := conn.Write([]byte{5, 1, 0}); err != nil {
		t.Fatal(err)
	}
	var method [2]byte
	if _, err := io.ReadFull(conn, method[:]); err != nil {
		t.Fatalf("no SOCKS greeting reply: %v", err)
	}
	if waited := time.Since(start); waited > 5*time.Second {
		t.Fatalf("the listener answered its first client after %v; it waited out the prewarm", waited)
	}
}
