package pep

import (
	"bytes"
	"context"
	"net"
	"testing"
	"time"

	"github.com/bojieli/queqiao/internal/socks5"
)

// Issue #197. The gateway's direct UDP relay listened on a dual-stack
// wildcard socket. On macOS the kernel can give such a socket an ephemeral
// port that an IPv4 socket elsewhere on the host already holds, and the older
// IPv4 socket then receives every datagram sent to that port: the relay never
// sees the destination's answers. A standalone check found about 13 of 500
// dual-stack binds on a held port.
//
// Like TestAQUICDialSocketNeverSharesAPortWithAnIPv4Socket (#192), this holds
// many IPv4 sockets, then opens relays through OpenUDPRelay and checks that
// none is given a port one of them holds. Linux never gives out such a port,
// so there it passes either way.
func TestAGatewayUDPRelayNeverSharesAPortWithAnIPv4Socket(t *testing.T) {
	const holders, relays, spare = 600, 3000, 32
	var conns []net.PacketConn
	for range holders {
		conn, err := net.ListenPacket("udp4", "127.0.0.1:0")
		if err != nil {
			for range min(spare, len(conns)) {
				_ = conns[len(conns)-1].Close()
				conns = conns[:len(conns)-1]
			}
			break
		}
		conns = append(conns, conn)
	}
	held := make(map[int]bool, len(conns))
	for _, conn := range conns {
		t.Cleanup(func() { _ = conn.Close() })
		held[conn.LocalAddr().(*net.UDPAddr).Port] = true
	}
	if len(conns) < 100 {
		t.Skipf("descriptors for only %d IPv4 holders", len(conns))
	}
	policy := DestinationPolicy{AllowPrivate: true}
	for range relays {
		relay, err := policy.OpenUDPRelay(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		local := relay.LocalAddr().(*net.UDPAddr)
		_ = relay.Close()
		if held[local.Port] {
			t.Fatalf("a gateway UDP relay on %s was given port %d, which an IPv4 socket already holds; "+
				"that socket would receive the destination's answers", local, local.Port)
		}
	}
}

func udpEcho(t *testing.T, network, address string) *net.UDPAddr {
	t.Helper()
	conn, err := net.ListenPacket(network, address)
	if err != nil {
		t.Skipf("%s echo on %s: %v", network, address, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	go func() {
		buf := make([]byte, 2048)
		for {
			n, source, err := conn.ReadFrom(buf)
			if err != nil {
				return
			}
			_, _ = conn.WriteTo(buf[:n], source)
		}
	}()
	return conn.LocalAddr().(*net.UDPAddr)
}

// One association reaches IPv4 and IPv6 destinations alike: each datagram
// goes out on the socket of its destination's family, and the answers of both
// come back through the one relay. The IPv6 socket opens only when an IPv6
// destination is first used.
func TestGatewayUDPRelayCarriesBothFamilies(t *testing.T) {
	relay, err := DestinationPolicy{AllowPrivate: true}.OpenUDPRelay(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer relay.Close()
	if local := relay.LocalAddr().(*net.UDPAddr); local.IP.To4() == nil {
		t.Fatalf("relay local address %s is not IPv4", local)
	}
	roundTrip := func(destination *net.UDPAddr, payload []byte) {
		t.Helper()
		_ = relay.SetWriteDeadline(time.Now().Add(2 * time.Second))
		if _, err := relay.WriteToUDP(payload, destination); err != nil {
			t.Fatalf("write to %s: %v", destination, err)
		}
		_ = relay.SetReadDeadline(time.Now().Add(2 * time.Second))
		buf := make([]byte, 2048)
		n, source, err := relay.ReadFromUDP(buf)
		if err != nil {
			t.Fatalf("answer from %s: %v", destination, err)
		}
		if !bytes.Equal(buf[:n], payload) || !source.IP.Equal(destination.IP) || source.Port != destination.Port {
			t.Fatalf("answer %q from %s; want %q from %s", buf[:n], source, payload, destination)
		}
	}
	roundTrip(udpEcho(t, "udp4", "127.0.0.1:0"), []byte("over IPv4"))
	if family, ok := relay.(*familyUDPRelay); ok {
		family.mu.Lock()
		opened := family.conns[familyIPv6] != nil
		family.mu.Unlock()
		if opened {
			t.Fatal("a relay that has only seen IPv4 destinations opened an IPv6 socket")
		}
	}
	roundTrip(udpEcho(t, "udp6", "[::1]:0"), []byte("over IPv6"))
	roundTrip(udpEcho(t, "udp4", "127.0.0.1:0"), []byte("IPv4 again"))
}

// The gateway's relay reader interrupts a read by moving the deadline to now
// (startUDPRelayReader), and a closed relay ends every read.
func TestGatewayUDPRelayReadDeadlineAndClose(t *testing.T) {
	relay, err := DestinationPolicy{AllowPrivate: true}.OpenUDPRelay(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	_ = relay.SetReadDeadline(time.Time{})
	go func() {
		_, _, err := relay.ReadFromUDP(make([]byte, 64))
		result <- err
	}()
	time.Sleep(20 * time.Millisecond)
	_ = relay.SetReadDeadline(time.Now())
	select {
	case err := <-result:
		if ne, ok := err.(net.Error); !ok || !ne.Timeout() {
			t.Fatalf("interrupted read returned %v, want a timeout", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("moving the read deadline to now did not interrupt a waiting read")
	}
	_ = relay.SetReadDeadline(time.Time{})
	go func() {
		_, _, err := relay.ReadFromUDP(make([]byte, 64))
		result <- err
	}()
	time.Sleep(20 * time.Millisecond)
	_ = relay.Close()
	select {
	case err := <-result:
		if ne, ok := err.(net.Error); err == nil || (ok && ne.Timeout()) {
			t.Fatalf("read on a closed relay returned %v, want a closed error", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("closing the relay did not end a waiting read")
	}
}

// The relay to an upstream SOCKS5 server talks to one address, so its socket
// takes that address's family rather than a dual-stack wildcard.
func TestSOCKS5UDPRelaySocketTakesTheUpstreamFamily(t *testing.T) {
	proxy, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close()
	go serveOneSOCKS5UDP(proxy, make(chan socks5.Request, 1))
	policy := DestinationPolicy{AllowPrivate: true, DialTimeout: 2 * time.Second, OutboundSOCKS5: proxy.Addr().String()}
	relay, err := policy.OpenUDPRelay(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer relay.Close()
	if local := relay.LocalAddr().(*net.UDPAddr); local.IP.To4() == nil {
		t.Fatalf("SOCKS5 relay socket %s to an IPv4 upstream is not IPv4", local)
	}
}

func TestWildcardUDPSocketFamily(t *testing.T) {
	for _, tc := range []struct{ peer, network, address string }{
		{"192.0.2.1", "udp4", "0.0.0.0:0"},
		{"::ffff:192.0.2.1", "udp4", "0.0.0.0:0"},
		{"2001:db8::1", "udp6", "[::]:0"},
	} {
		if network, address := wildcardUDPSocket(net.ParseIP(tc.peer)); network != tc.network || address != tc.address {
			t.Errorf("peer %s: got %s %s, want %s %s", tc.peer, network, address, tc.network, tc.address)
		}
	}
}
