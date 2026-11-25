package pep

import (
	"context"
	"net"
	"testing"
)

// Issue #187. A QUIC dial with no local binding listened on a dual-stack
// socket. On macOS the kernel can give such a socket an ephemeral port that an
// IPv4 socket elsewhere on the host already holds, and the older IPv4 socket
// then receives every datagram sent to that port: the gateway answered the
// handshake, its answers went to the other socket, and the dial retransmitted
// its first flight into silence for its whole timeout. Under a parallel test
// run that hold many IPv4 sockets this failed a few dials in a hundred.
//
// This holds many IPv4 sockets, then opens dial sockets the way a dial does
// and checks that none is given a port one of them holds. Before the fix,
// several thousand dual-stack binds collided about once in every 30 on macOS.
// Linux never gives out such a port, so there it passes either way.
func TestAQUICDialSocketNeverSharesAPortWithAnIPv4Socket(t *testing.T) {
	const holders, dials = 600, 3000
	held := make(map[int]bool, holders)
	for range holders {
		conn, err := net.ListenPacket("udp4", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		held[conn.LocalAddr().(*net.UDPAddr).Port] = true
	}
	gateway := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 443}
	network, address := quicDialSocket(":0", gateway)
	for range dials {
		conn, err := (&net.ListenConfig{}).ListenPacket(context.Background(), network, address)
		if err != nil {
			t.Fatal(err)
		}
		port := conn.LocalAddr().(*net.UDPAddr).Port
		_ = conn.Close()
		if held[port] {
			t.Fatalf("a %s dial socket on %s was given port %d, which an IPv4 socket already holds; "+
				"that socket would receive the gateway's answers", network, address, port)
		}
	}
}

// The socket follows the gateway's family when nothing binds it, and a local
// binding, which names an address, is kept as given.
func TestQUICDialSocketFamily(t *testing.T) {
	for _, tc := range []struct {
		listen, remote   string
		network, address string
	}{
		{":0", "192.0.2.1", "udp4", "0.0.0.0:0"},
		{":0", "::ffff:192.0.2.1", "udp4", "0.0.0.0:0"},
		{":0", "2001:db8::1", "udp6", "[::]:0"},
		{"192.0.2.7:0", "192.0.2.1", "udp", "192.0.2.7:0"},
		{"[2001:db8::7]:0", "2001:db8::1", "udp", "[2001:db8::7]:0"},
	} {
		network, address := quicDialSocket(tc.listen, &net.UDPAddr{IP: net.ParseIP(tc.remote), Port: 443})
		if network != tc.network || address != tc.address {
			t.Errorf("listen %q to %s: got %s %s, want %s %s", tc.listen, tc.remote, network, address, tc.network, tc.address)
		}
	}
}
