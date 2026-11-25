package congestion

import (
	"context"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apernet/quic-go"
)

// rebindingRelay forwards a client's datagrams to a server from an outbound
// socket that can be replaced, which is what a NAT rebinding looks like from
// the server: the same connection, arriving from a new source port.
type rebindingRelay struct {
	front  *net.UDPConn
	target *net.UDPAddr

	mu     sync.Mutex
	client *net.UDPAddr
	out    *net.UDPConn
	closed atomic.Bool
}

func newRebindingRelay(t *testing.T, target *net.UDPAddr) *rebindingRelay {
	t.Helper()
	front, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	r := &rebindingRelay{front: front, target: target}
	r.rebind(t)
	t.Cleanup(func() {
		r.closed.Store(true)
		_ = front.Close()
		r.mu.Lock()
		_ = r.out.Close()
		r.mu.Unlock()
	})
	go r.forward()
	return r
}

func (r *rebindingRelay) rebind(t *testing.T) {
	t.Helper()
	out, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	r.mu.Lock()
	old := r.out
	r.out = out
	r.mu.Unlock()
	go r.backward(out)
	if old != nil {
		// Keep the old binding open briefly for packets already in flight.
		time.AfterFunc(time.Second, func() { _ = old.Close() })
	}
}

func (r *rebindingRelay) forward() {
	buf := make([]byte, 65536)
	for {
		n, from, err := r.front.ReadFromUDP(buf)
		if err != nil {
			return
		}
		r.mu.Lock()
		r.client = from
		out := r.out
		r.mu.Unlock()
		_, _ = out.WriteToUDP(buf[:n], r.target)
	}
}

func (r *rebindingRelay) backward(out *net.UDPConn) {
	buf := make([]byte, 65536)
	for {
		n, _, err := out.ReadFromUDP(buf)
		if err != nil {
			return
		}
		r.mu.Lock()
		client := r.client
		r.mu.Unlock()
		if client != nil {
			_, _ = r.front.WriteToUDP(buf[:n], client)
		}
	}
}

// A NAT rebinding of the client, seen from the gateway: through the
// per-packet callbacks quic-go replaces the gateway's controller with Reno, so
// the ErasureSender it installed stops measuring the path. Through cc-api v2
// the controller is kept and goes on sampling the same download.
func TestNATRebindingKeepsTheControllerUnderCCAPIV2(t *testing.T) {
	if testing.Short() {
		t.Skip("runs a real QUIC transfer")
	}
	for _, v2 := range []bool{false, true} {
		name := "v1"
		if v2 {
			name = "v2"
		}
		t.Run(name, func(t *testing.T) {
			before, after := rebindDuringDownload(t, v2)
			t.Logf("cc-api %s: gateway controller samples %d before the rebinding, %d after", name, before, after)
			if v2 && after <= before {
				t.Fatalf("the gateway's controller stopped sampling after the rebinding (%d -> %d)", before, after)
			}
			if !v2 && after != before {
				t.Fatalf("v1 kept the controller across the rebinding (%d -> %d); it must stay as main", before, after)
			}
		})
	}
}

func rebindDuringDownload(t *testing.T, v2 bool) (before, after uint64) {
	t.Helper()
	serverTLS, clientTLS := reverseAckTLS(t)
	quicCfg := &quic.Config{MaxIdleTimeout: 10 * time.Second}
	listener, err := quic.ListenAddr("127.0.0.1:0", serverTLS, quicCfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	relay := newRebindingRelay(t, listener.Addr().(*net.UDPAddr))
	accepted := make(chan *quic.Conn, 1)
	go func() {
		if conn, err := listener.Accept(context.Background()); err == nil {
			accepted <- conn
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	client, err := quic.DialAddr(ctx, relay.front.LocalAddr().String(), clientTLS, quicCfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.CloseWithError(0, "") })
	var server *quic.Conn
	select {
	case server = <-accepted:
	case <-ctx.Done():
		t.Fatal("server did not accept")
	}
	t.Cleanup(func() { _ = server.CloseWithError(0, "") })

	sender := NewErasureSender(server.InitialPacketSize())
	if v2 {
		server.SetCongestionControl(NewErasurePathSender(sender, nil))
	} else {
		server.SetCongestionControl(sender)
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		stream, err := server.OpenStreamSync(ctx)
		if err != nil {
			return
		}
		buf := make([]byte, 16<<10)
		for {
			select {
			case <-stop:
				_ = stream.Close()
				return
			default:
			}
			if _, err := stream.Write(buf); err != nil {
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		stream, err := client.AcceptStream(ctx)
		if err != nil {
			return
		}
		_, _ = io.Copy(io.Discard, stream)
	}()

	time.Sleep(time.Second)
	remote := server.RemoteAddr().String()
	relay.rebind(t)
	deadline := time.Now().Add(5 * time.Second)
	for server.RemoteAddr().String() == remote {
		if time.Now().After(deadline) {
			t.Fatal("the gateway never migrated to the client's new address")
		}
		time.Sleep(5 * time.Millisecond)
	}
	// Give the switch a moment to settle, then measure.
	time.Sleep(50 * time.Millisecond)
	before = sender.Telemetry().Samples
	time.Sleep(time.Second)
	after = sender.Telemetry().Samples
	close(stop)
	_ = server.CloseWithError(0, "")
	_ = client.CloseWithError(0, "")
	wg.Wait()
	return before, after
}
