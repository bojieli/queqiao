package pep

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/bojieli/queqiao/internal/metrics"
)

// An SSH-shaped session end to end, through a real client and gateway: one
// SOCKS connection to an echo destination, keystrokes for over a second, a
// 1 MB burst, then keystrokes again. The gateway's per-class byte telemetry
// says how it sent the echoes after the burst: BULK under the legacy bundle,
// which is the latch, and INTERACTIVE under the phased one. Every byte is
// echoed intact in both.
func TestEchoesAfterABurstEndToEnd(t *testing.T) {
	for _, mode := range []ClassifierMode{ClassifierLegacy, ClassifierPhased} {
		t.Run(string(mode), func(t *testing.T) {
			destinationListener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer destinationListener.Close()
			go echoDestination(destinationListener)

			certificate, roots := testCertificate(t)
			logger := slog.New(slog.NewTextHandler(io.Discard, nil))
			packetConn, err := net.ListenPacket("udp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			serverMetrics := metrics.New()
			server, err := NewServer(ServerConfig{
				ListenAddr: "127.0.0.1:0", Credentials: certificate, Classifier: mode,
				DestinationPolicy: DestinationPolicy{AllowPrivate: true}, EnableQUIC: true, Logger: logger,
				Metrics: serverMetrics,
			})
			if err != nil {
				t.Fatal(err)
			}
			clientListener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			client, err := NewClient(ClientConfig{
				ListenAddr: clientListener.Addr().String(), RemoteAddr: packetConn.LocalAddr().String(), Credentials: roots,
				Transport: TransportQUIC, Logger: logger, Metrics: metrics.New(), Classifier: mode,
			})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go func() { _ = server.ServePacketConn(ctx, packetConn) }()
			go func() { _ = client.ServeListener(ctx, clientListener) }()

			conn := dialTestSOCKS(t, clientListener.Addr().String(), destinationListener.Addr().String())
			defer conn.Close()
			_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
			key := []byte("0123456789abcdef0123456789abcdef1234")
			echo := make([]byte, len(key))
			typeFor := func(d time.Duration) int {
				n := 0
				for end := time.Now().Add(d); time.Now().Before(end); n++ {
					if _, err := conn.Write(key); err != nil {
						t.Fatal(err)
					}
					if _, err := io.ReadFull(conn, echo); err != nil {
						t.Fatal(err)
					}
					if !bytes.Equal(echo, key) {
						t.Fatalf("echo %q, want %q", echo, key)
					}
					time.Sleep(30 * time.Millisecond)
				}
				return n
			}
			typeFor(1500 * time.Millisecond)
			burst := bytes.Repeat([]byte("burst-of-output."), 64<<10) // 1 MiB
			go func() { _, _ = conn.Write(burst) }()
			got := make([]byte, len(burst))
			if _, err := io.ReadFull(conn, got); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, burst) {
				t.Fatal("the burst was not echoed intact")
			}
			// Let the burst's window age out, as a session's think time does.
			time.Sleep(300 * time.Millisecond)
			before := serverMetrics.Snapshot().DataClassBytes
			keys := typeFor(1500 * time.Millisecond)
			after := serverMetrics.Snapshot().DataClassBytes
			delta := func(class int) uint64 {
				return after[class][0] + after[class][1] - before[class][0] - before[class][1]
			}
			echoed := uint64(keys * len(key))
			if total := delta(0) + delta(1) + delta(2); total != echoed {
				t.Fatalf("the gateway counted %d DATA bytes for %d echoed", total, echoed)
			}
			if mode == ClassifierLegacy {
				if delta(2) == 0 {
					t.Fatal("legacy echoes after the burst were not BULK; the latch no longer reproduces end to end")
				}
				return
			}
			if delta(2) != 0 {
				t.Fatalf("%d of %d echo bytes after the burst were sent BULK", delta(2), echoed)
			}
		})
	}
}
