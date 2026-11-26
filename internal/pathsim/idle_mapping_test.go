package pathsim

import (
	"net"
	"testing"
	"time"
)

// roundTrip sends one datagram through the relay to an echo server and
// reports how long the echo took, and whether one came within budget.
func roundTrip(t *testing.T, client net.PacketConn, target net.Addr, budget time.Duration) (time.Duration, bool) {
	t.Helper()
	started := time.Now()
	if _, err := client.WriteTo([]byte("probe"), target); err != nil {
		t.Fatal(err)
	}
	_ = client.SetReadDeadline(time.Now().Add(budget))
	buf := make([]byte, 64)
	if _, _, err := client.ReadFrom(buf); err != nil {
		return 0, false
	}
	return time.Since(started), true
}

// A source's mapping lives while packets cross it in either direction and is
// expired once nothing has for the idle timeout: the source's next packet
// then reaches the server from a new address.
func TestSourceMappingExpiresWhenIdle(t *testing.T) {
	seen := make(chan string, 16)
	server, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	go func() {
		buf := make([]byte, 2048)
		for {
			n, addr, err := server.ReadFrom(buf)
			if err != nil {
				return
			}
			seen <- addr.String()
			_, _ = server.WriteTo(buf[:n], addr)
		}
	}()
	const idle = 300 * time.Millisecond
	relay, err := New("127.0.0.1:0", server.LocalAddr().String(), Config{SourceIdleTimeout: idle})
	if err != nil {
		t.Fatal(err)
	}
	defer relay.Close()
	client, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	target, _ := net.ResolveUDPAddr("udp", relay.LocalAddr())

	// Crossed more often than the timeout, the mapping stays.
	var first string
	for i := 0; i < 6; i++ {
		if _, ok := roundTrip(t, client, target, 2*time.Second); !ok {
			t.Fatal("no echo across a live mapping")
		}
		from := <-seen
		if first == "" {
			first = from
		}
		if from != first {
			t.Fatalf("the server saw %s then %s across a mapping that was kept busy", first, from)
		}
		time.Sleep(idle / 3)
	}
	if got := relay.ExpiredSources(); got != 0 {
		t.Fatalf("%d mappings expired while kept busy", got)
	}
	// Left alone past the timeout, it is gone, and the next packet opens a
	// new one.
	time.Sleep(2 * idle)
	if _, ok := roundTrip(t, client, target, 2*time.Second); !ok {
		t.Fatal("no echo across the new mapping")
	}
	if from := <-seen; from == first {
		t.Fatalf("the server still saw %s after the mapping's idle timeout", from)
	}
	if got := relay.ExpiredSources(); got != 1 {
		t.Fatalf("%d mappings expired, want 1", got)
	}
}
