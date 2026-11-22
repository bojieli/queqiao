package pep

import (
	"context"
	"net"
	"testing"
	"time"

	"golang.org/x/net/dns/dnsmessage"
)

func TestRound2QUICTriesSecondGatewayAddress(t *testing.T) {
	rig := newJoinTestRig(t, TransportQUIC, TransportQUIC, 1)
	dns, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer dns.Close()
	go func() {
		buf := make([]byte, 2048)
		for {
			n, peer, err := dns.ReadFromUDP(buf)
			if err != nil {
				return
			}
			var query dnsmessage.Message
			if query.Unpack(buf[:n]) != nil {
				continue
			}
			reply := dnsmessage.Message{Header: dnsmessage.Header{ID: query.ID, Response: true, RecursionAvailable: true}, Questions: query.Questions}
			for _, q := range query.Questions {
				if q.Type == dnsmessage.TypeA {
					for _, ip := range [][4]byte{{127, 0, 0, 2}, {127, 0, 0, 1}} {
						reply.Answers = append(reply.Answers, dnsmessage.Resource{Header: dnsmessage.ResourceHeader{Name: q.Name, Type: dnsmessage.TypeA, Class: dnsmessage.ClassINET, TTL: 60}, Body: &dnsmessage.AResource{A: ip}})
					}
				}
			}
			data, _ := reply.Pack()
			_, _ = dns.WriteToUDP(data, peer)
		}
	}()
	resolver := &net.Resolver{PreferGo: true, Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "udp", dns.LocalAddr().String())
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, port, _ := net.SplitHostPort(rig.client.cfg.RemoteAddr)
	remote := net.JoinHostPort("gateway.test", port)
	addresses, err := resolveUDPAddrs(ctx, remote, resolver)
	if err != nil || len(addresses) != 2 {
		t.Fatalf("resolve=%v %v", addresses, err)
	}
	if !addresses[0].IP.Equal(net.IPv4(127, 0, 0, 2)) {
		t.Skip("platform sorted the healthy address first")
	}
	original := net.DefaultResolver
	net.DefaultResolver = resolver
	defer func() { net.DefaultResolver = original }()
	conn, packet, err := dialQUICConnection(ctx, remote, rig.client.currentCredentials(), time.Second, "", nil, nil, rig.client.windows(), rig.client.hopDialConfig())
	if err != nil {
		t.Fatalf("healthy second address not reached: %v", err)
	}
	defer conn.CloseWithError(0, "test complete")
	defer packet.Close()
	if !conn.RemoteAddr().(*net.UDPAddr).IP.Equal(net.IPv4(127, 0, 0, 1)) {
		t.Fatal("unexpected remote")
	}
}
