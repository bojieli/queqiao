package pep

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/bojieli/queqiao/internal/socks5"
)

func TestPublicDestinationIP(t *testing.T) {
	for _, test := range []struct {
		ip     string
		public bool
	}{
		{"8.8.8.8", true},
		{"1.1.1.1", true},
		{"127.0.0.1", false},
		{"10.0.0.1", false},
		{"100.64.0.1", false},
		{"169.254.169.254", false},
		{"192.0.2.1", false},
		{"192.88.99.2", false},
		{"2001:4860:4860::8888", true},
		{"::1", false},
		{"64:ff9b::a00:1", false},
		{"64:ff9b:1::a00:1", false},
		{"100::1", false},
		{"100:0:0:1::1", false},
		{"2001:2::1", false},
		{"2001:10::1", false},
		{"2001:db8::1", false},
		{"2002:a00:1::1", false},
		{"3fff::1", false},
		{"5f00::1", false},
	} {
		if got := publicDestinationIP(net.ParseIP(test.ip)); got != test.public {
			t.Errorf("publicDestinationIP(%s) = %v, want %v", test.ip, got, test.public)
		}
	}
}

func TestDestinationPolicyDialsTCPThroughSOCKS5(t *testing.T) {
	destination, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer destination.Close()
	go func() {
		conn, acceptErr := destination.Accept()
		if acceptErr != nil {
			return
		}
		defer conn.Close()
		_, _ = io.Copy(conn, conn)
	}()

	proxy, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close()
	requestCh := make(chan socks5.Request, 1)
	go serveOneSOCKS5Connect(proxy, requestCh)

	policy := DestinationPolicy{AllowPrivate: true, DialTimeout: 2 * time.Second, OutboundSOCKS5: proxy.Addr().String()}
	_, destinationPort, _ := net.SplitHostPort(destination.Addr().String())
	namedDestination := net.JoinHostPort("localhost", destinationPort)
	conn, err := policy.DialContext(context.Background(), namedDestination)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.Write([]byte("through proxy")); err != nil {
		t.Fatal(err)
	}
	reply := make([]byte, len("through proxy"))
	if _, err := io.ReadFull(conn, reply); err != nil {
		t.Fatal(err)
	}
	if string(reply) != "through proxy" {
		t.Fatalf("reply = %q", reply)
	}
	if request := <-requestCh; request.Command != socks5.CommandConnect || request.Destination != namedDestination {
		t.Fatalf("upstream request = %+v", request)
	}
}

func TestSOCKS5HandshakeHonorsContextCancellation(t *testing.T) {
	proxy, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close()
	greetingRead := make(chan error, 1)
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		conn, acceptErr := proxy.Accept()
		if acceptErr != nil {
			greetingRead <- acceptErr
			return
		}
		defer conn.Close()
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		var greeting [3]byte
		_, readErr := io.ReadFull(conn, greeting[:])
		greetingRead <- readErr
		if readErr == nil {
			_, _ = io.Copy(io.Discard, conn)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		conn, _, dialErr := openSOCKS5Control(ctx, proxy.Addr().String(), socks5.CommandConnect, "example.com:443", 30*time.Second)
		if conn != nil {
			_ = conn.Close()
		}
		result <- dialErr
	}()
	if err := <-greetingRead; err != nil {
		cancel()
		t.Fatal(err)
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("SOCKS5 handshake error = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("SOCKS5 handshake did not stop after context cancellation")
	}
	select {
	case <-serverDone:
	case <-time.After(2 * time.Second):
		t.Fatal("canceled SOCKS5 handshake left its connection open")
	}
}

func TestDestinationPolicyRelaysUDPThroughSOCKS5(t *testing.T) {
	destination, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer destination.Close()
	go func() {
		packet := make([]byte, 2048)
		n, source, readErr := destination.ReadFromUDP(packet)
		if readErr == nil {
			_, _ = destination.WriteToUDP(packet[:n], source)
		}
	}()

	proxy, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close()
	requestCh := make(chan socks5.Request, 1)
	go serveOneSOCKS5UDP(proxy, requestCh)

	policy := DestinationPolicy{AllowPrivate: true, DialTimeout: 2 * time.Second, OutboundSOCKS5: proxy.Addr().String()}
	relay, err := policy.OpenUDPRelay(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer relay.Close()
	_, destinationPort, _ := net.SplitHostPort(destination.LocalAddr().String())
	namedDestination := net.JoinHostPort("localhost", destinationPort)
	_, err = policy.ResolveUDPAddr(context.Background(), namedDestination)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	_ = relay.SetReadDeadline(deadline)
	_ = relay.SetWriteDeadline(deadline)
	namedRelay, ok := relay.(namedUDPRelay)
	if !ok {
		t.Fatal("SOCKS5 UDP relay does not accept named destinations")
	}
	if _, err := namedRelay.WriteToDestination([]byte("udp through proxy"), namedDestination); err != nil {
		t.Fatal(err)
	}
	packet := make([]byte, 2048)
	n, source, err := namedRelay.ReadFromDestination(packet)
	if err != nil {
		t.Fatal(err)
	}
	if string(packet[:n]) != "udp through proxy" || source != namedDestination {
		t.Fatalf("reply = %q from %v, want %v", packet[:n], source, namedDestination)
	}
	if request := <-requestCh; request.Command != socks5.CommandUDPAssociate {
		t.Fatalf("upstream request = %+v", request)
	}
}

// TestExternalSOCKS5Upstream exercises the same client against an operator-
// supplied implementation (sing-box in the release qualification). Ordinary
// CI has no external process, so the explicit environment variable is the
// authority to run it. Both destinations remain local to the test host.
func TestExternalSOCKS5Upstream(t *testing.T) {
	upstream := os.Getenv("QUEQIAO_TEST_SOCKS5")
	if upstream == "" {
		t.Skip("QUEQIAO_TEST_SOCKS5 is not set")
	}

	tcpDestination, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer tcpDestination.Close()
	go func() {
		conn, acceptErr := tcpDestination.Accept()
		if acceptErr == nil {
			defer conn.Close()
			_, _ = io.Copy(conn, conn)
		}
	}()
	policy := DestinationPolicy{AllowPrivate: true, DialTimeout: 3 * time.Second, OutboundSOCKS5: upstream}
	_, tcpPort, _ := net.SplitHostPort(tcpDestination.Addr().String())
	tcpConn, err := policy.DialContext(context.Background(), net.JoinHostPort("localhost", tcpPort))
	if err != nil {
		t.Fatalf("TCP through external SOCKS5: %v", err)
	}
	if _, err := tcpConn.Write([]byte("external TCP")); err != nil {
		_ = tcpConn.Close()
		t.Fatal(err)
	}
	tcpReply := make([]byte, len("external TCP"))
	if _, err := io.ReadFull(tcpConn, tcpReply); err != nil {
		_ = tcpConn.Close()
		t.Fatal(err)
	}
	_ = tcpConn.Close()
	if string(tcpReply) != "external TCP" {
		t.Fatalf("external TCP reply = %q", tcpReply)
	}

	udpDestination, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer udpDestination.Close()
	go func() {
		packet := make([]byte, 2048)
		n, source, readErr := udpDestination.ReadFromUDP(packet)
		if readErr == nil {
			_, _ = udpDestination.WriteToUDP(packet[:n], source)
		}
	}()
	udpRelay, err := policy.OpenUDPRelay(context.Background())
	if err != nil {
		t.Fatalf("UDP ASSOCIATE through external SOCKS5: %v", err)
	}
	defer udpRelay.Close()
	deadline := time.Now().Add(3 * time.Second)
	_ = udpRelay.SetReadDeadline(deadline)
	_ = udpRelay.SetWriteDeadline(deadline)
	udpAddress := udpDestination.LocalAddr().(*net.UDPAddr)
	namedRelay, ok := udpRelay.(namedUDPRelay)
	if !ok {
		t.Fatal("external SOCKS5 UDP relay does not accept named destinations")
	}
	namedDestination := net.JoinHostPort("localhost", strconv.Itoa(udpAddress.Port))
	if _, err := namedRelay.WriteToDestination([]byte("external UDP"), namedDestination); err != nil {
		t.Fatal(err)
	}
	udpReply := make([]byte, 2048)
	n, source, err := namedRelay.ReadFromDestination(udpReply)
	if err != nil {
		t.Fatal(err)
	}
	sourceHost, sourcePort, splitErr := net.SplitHostPort(source)
	sourceIP := net.ParseIP(sourceHost)
	sourceMatches := sourceHost == "localhost" || sourceIP != nil && sourceIP.Equal(udpAddress.IP)
	if splitErr != nil || sourcePort != strconv.Itoa(udpAddress.Port) || !sourceMatches {
		t.Fatalf("external UDP source = %q, want %q or %v", source, namedDestination, udpAddress)
	}
	if string(udpReply[:n]) != "external UDP" {
		t.Fatalf("external UDP reply = %q", udpReply[:n])
	}
}

func serveOneSOCKS5Connect(listener net.Listener, requests chan<- socks5.Request) {
	client, err := listener.Accept()
	if err != nil {
		return
	}
	defer client.Close()
	request, err := socks5.ReadRequest(client, nil)
	if err != nil {
		return
	}
	requests <- request
	destination, err := net.Dial("tcp", request.Destination)
	if err != nil {
		_ = socks5.WriteReply(client, socks5.ReplyHostUnreachable, nil)
		return
	}
	defer destination.Close()
	if err := socks5.WriteReply(client, socks5.ReplySucceeded, destination.LocalAddr()); err != nil {
		return
	}
	var wait sync.WaitGroup
	wait.Add(2)
	go func() { defer wait.Done(); _, _ = io.Copy(destination, client) }()
	go func() { defer wait.Done(); _, _ = io.Copy(client, destination) }()
	wait.Wait()
}

func serveOneSOCKS5UDP(listener *net.TCPListener, requests chan<- socks5.Request) {
	control, err := listener.AcceptTCP()
	if err != nil {
		return
	}
	defer control.Close()
	request, err := socks5.ReadRequest(control, nil)
	if err != nil {
		return
	}
	requests <- request
	relay, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		return
	}
	defer relay.Close()
	if err := socks5.WriteReply(control, socks5.ReplySucceeded, relay.LocalAddr()); err != nil {
		return
	}
	wire := make([]byte, 65535)
	n, client, err := relay.ReadFromUDP(wire)
	if err != nil {
		return
	}
	datagram, err := socks5.ReadUDPDatagram(wire[:n])
	if err != nil {
		return
	}
	destination, err := net.ResolveUDPAddr("udp", datagram.Destination)
	if err != nil {
		return
	}
	upstream, err := net.DialUDP("udp", nil, destination)
	if err != nil {
		return
	}
	defer upstream.Close()
	_ = upstream.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := upstream.Write(datagram.Payload); err != nil {
		return
	}
	payload := make([]byte, 2048)
	n, err = upstream.Read(payload)
	if err != nil {
		return
	}
	var response bytes.Buffer
	// Returning the requested domain mirrors sing-box's domain-unmapping
	// behaviour and verifies that the Queqiao relay preserves a legal domain
	// address instead of requiring every SOCKS5 UDP response to contain an IP.
	if err := socks5.WriteUDPDatagram(&response, datagram.Destination, payload[:n]); err != nil {
		return
	}
	if _, err := relay.WriteToUDP(response.Bytes(), client); err != nil {
		return
	}
	_, _ = io.Copy(io.Discard, control)
}

func TestResolveUDPAddrUsesDestinationPolicy(t *testing.T) {
	addresses, err := (DestinationPolicy{}).ResolveUDPAddr(context.Background(), "8.8.8.8:53")
	if err != nil {
		t.Fatal(err)
	}
	if len(addresses) != 1 || !addresses[0].IP.Equal(net.ParseIP("8.8.8.8")) || addresses[0].Port != 53 {
		t.Fatalf("unexpected addresses %#v", addresses)
	}
	if _, err := (DestinationPolicy{}).ResolveUDPAddr(context.Background(), "127.0.0.1:53"); err == nil {
		t.Fatal("private UDP destination accepted")
	}
	if _, err := (DestinationPolicy{}).ResolveUDPAddr(context.Background(), "[64:ff9b::a00:1]:53"); err == nil {
		t.Fatal("NAT64-encoded private UDP destination accepted")
	}
	addresses, err = (DestinationPolicy{AllowPrivate: true}).ResolveUDPAddr(context.Background(), "127.0.0.1:53")
	if err != nil || len(addresses) != 1 {
		t.Fatalf("private destination with explicit allow failed: %v", err)
	}
	if _, err := (DestinationPolicy{OutboundSOCKS5: "127.0.0.1:1080"}).ResolveUDPAddr(context.Background(), "localhost:53"); err == nil {
		t.Fatal("private upstream-resolved UDP destination accepted")
	}
}
