package pep

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/bojieli/queqiao/internal/identity"
	"github.com/bojieli/queqiao/internal/metrics"
	"github.com/bojieli/queqiao/internal/protocol"
	"github.com/bojieli/queqiao/internal/session"
)

// wireGateway runs a real gateway on loopback that accepts the given wire
// versions over one carrier, and returns the address to dial.
func wireGateway(t *testing.T, ctx context.Context, carrier TransportKind, accept []byte, credentials identity.ServerCredentials, registry *metrics.Registry) string {
	t.Helper()
	server, err := NewServer(ServerConfig{
		ListenAddr: "127.0.0.1:0", Credentials: credentials, AcceptWire: accept,
		DestinationPolicy: DestinationPolicy{AllowPrivate: true}, EnableQUIC: carrier == TransportQUIC,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Metrics: registry,
		HandshakeTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	t.Cleanup(func() { <-done })
	if carrier == TransportTCP {
		listener, err := net.Listen("tcp4", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		go func() { defer close(done); _ = server.ServeListener(ctx, listener) }()
		return listener.Addr().String()
	}
	packetConn, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { defer close(done); _ = server.ServePacketConn(ctx, packetConn) }()
	return packetConn.LocalAddr().String()
}

func dialWireLane(ctx context.Context, carrier TransportKind, remote string, credentials identity.ClientCredentials, offer []byte) (streamConn, error) {
	if carrier == TransportTCP {
		return dialTCP(ctx, remote, credentials, offer, 2*time.Second, "", nil)
	}
	return dialQUIC(ctx, remote, credentials, offer, 2*time.Second, "", nil, nil, congestionConfig{kind: CongestionReno}, flowWindows{}, hopDialConfig{})
}

// TestNegotiatedWireVersionMatrix is the version negotiation as deployed: a
// real gateway accepting some set of versions, a real client dial offering
// some list, over both carriers. The gateway's preference decides, so the
// newest common version wins whatever order the client offers; no common
// version fails in the TLS handshake, before any frame. Whatever is
// negotiated, the lane's frames carry it in both directions.
func TestNegotiatedWireVersionMatrix(t *testing.T) {
	serverCredentials, clientCredentials := testCertificate(t)
	accepts := [][]byte{{1}, {2}, {1, 2}}
	offers := [][]byte{{1}, {2}, {2, 1}, {1, 2}}
	for _, carrier := range []TransportKind{TransportTCP, TransportQUIC} {
		for _, accept := range accepts {
			t.Run(fmt.Sprintf("%s/accept-%s", carrier, formatWireVersions(accept)), func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				registry := metrics.New()
				remote := wireGateway(t, ctx, carrier, accept, serverCredentials, registry)
				var accepted [2]uint64
				for _, offer := range offers {
					want := byte(0)
					for _, v := range offer {
						if slices.Contains(accept, v) && v > want {
							want = v
						}
					}
					dialCtx, dialCancel := context.WithTimeout(ctx, 5*time.Second)
					outer, err := dialWireLane(dialCtx, carrier, remote, clientCredentials, offer)
					if want == 0 {
						dialCancel()
						if err == nil {
							_ = outer.Close()
							t.Fatalf("offer %v: connected with no common version", offer)
						}
						if !strings.Contains(strings.ToLower(err.Error()), "no application protocol") {
							t.Fatalf("offer %v: no common version failed ambiguously: %v", offer, err)
						}
						continue
					}
					if err != nil {
						dialCancel()
						t.Fatalf("offer %v: %v", offer, err)
					}
					accepted[want-1]++
					fc := newFrameConn(outer)
					if fc.wire != want {
						t.Fatalf("offer %v: lane speaks %d, want %d", offer, fc.wire, want)
					}
					// The provider probe: an invalid JOIN answered with a
					// protocol RESET. The answer must come back on the
					// negotiated version, or the decoder refuses it.
					_ = outer.SetDeadline(time.Now().Add(3 * time.Second))
					sessionID := [16]byte{9}
					if err := fc.Write(protocol.Frame{Header: protocol.Header{
						Version: protocol.Version, Type: protocol.TypeJoin, SessionID: sessionID, Class: protocol.ClassBulk,
					}, Payload: encodeLaneID(1)}); err != nil {
						t.Fatal(err)
					}
					response, err := fc.Read()
					if err != nil {
						t.Fatalf("offer %v: probe answer on wire %d: %v", offer, want, err)
					}
					if response.Header.Version != want || response.Header.Type != protocol.TypeReset || resetCode(response.Payload) != session.ResetProtocol {
						t.Fatalf("offer %v: probe answer %+v", offer, response.Header)
					}
					_ = fc.Close()
					dialCancel()
				}
				snapshot := registry.Snapshot()
				for i := range accepted {
					if snapshot.DataConnections[i] != accepted[i] {
						t.Fatalf("wire %d connections = %d, want %d", i+1, snapshot.DataConnections[i], accepted[i])
					}
				}
			})
		}
	}
}

// TestAFrameOfTheOtherVersionIsRefusedOnANegotiatedLane holds the version to
// the connection rather than to the build: a gateway that speaks both
// versions still refuses a version-2 frame on a lane that negotiated version
// 1, because a peer that disagrees with its own handshake is not one to
// guess about.
func TestAFrameOfTheOtherVersionIsRefusedOnANegotiatedLane(t *testing.T) {
	serverCredentials, clientCredentials := testCertificate(t)
	for _, carrier := range []TransportKind{TransportTCP, TransportQUIC} {
		for _, negotiated := range []byte{1, 2} {
			t.Run(fmt.Sprintf("%s/wire-%d", carrier, negotiated), func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				remote := wireGateway(t, ctx, carrier, nil, serverCredentials, metrics.New())
				outer, err := dialWireLane(ctx, carrier, remote, clientCredentials, []byte{negotiated})
				if err != nil {
					t.Fatal(err)
				}
				defer outer.Close()
				other := 3 - negotiated
				raw, err := protocol.AppendFrame(nil, protocol.Frame{Header: protocol.Header{
					Version: other, Type: protocol.TypeJoin, SessionID: [16]byte{9}, Class: protocol.ClassBulk,
				}, Payload: encodeLaneID(1)})
				if err != nil {
					t.Fatal(err)
				}
				_ = outer.SetDeadline(time.Now().Add(3 * time.Second))
				if _, err := outer.Write(raw); err != nil {
					t.Fatal(err)
				}
				var buf [1]byte
				if n, err := outer.Read(buf[:]); err == nil || n != 0 {
					t.Fatalf("a v%d frame on a v%d lane was answered (%d bytes, %v)", other, negotiated, n, err)
				}
			})
		}
	}
}

func TestWireVersionConfiguration(t *testing.T) {
	for _, tc := range []struct {
		spec string
		want []byte
	}{
		{"1,2", []byte{1, 2}}, {"2,1", []byte{2, 1}}, {"2", []byte{2}}, {" 1 , 2 ", []byte{1, 2}},
	} {
		got, err := parseWireVersions(tc.spec)
		if err != nil || !slices.Equal(got, tc.want) {
			t.Fatalf("%q = %v, %v; want %v", tc.spec, got, err, tc.want)
		}
		if formatWireVersions(got) != strings.ReplaceAll(tc.spec, " ", "") {
			t.Fatalf("%q round-tripped as %q", tc.spec, formatWireVersions(got))
		}
	}
	for _, bad := range []string{"", "0", "3", "1,1", "1,", "two", "256"} {
		if got, err := parseWireVersions(bad); err == nil {
			t.Fatalf("%q parsed as %v", bad, got)
		}
	}
	serverCredentials, clientCredentials := testCertificate(t)
	server, err := NewServer(ServerConfig{ListenAddr: "127.0.0.1:0", Credentials: serverCredentials, AcceptWire: []byte{1, 2}})
	if err != nil {
		t.Fatal(err)
	}
	if got := server.acceptWire(); !slices.Equal(got, []byte{2, 1}) {
		t.Fatalf("gateway negotiates in order %v, want newest first", got)
	}
	if _, err := NewServer(ServerConfig{ListenAddr: "127.0.0.1:0", Credentials: serverCredentials, AcceptWire: []byte{3}}); err == nil {
		t.Fatal("gateway accepted an unknown wire version")
	}
	client, err := NewClient(ClientConfig{ListenAddr: "127.0.0.1:0", RemoteAddr: "127.0.0.1:1", Credentials: clientCredentials})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(client.cfg.WireVersions, []byte{2, 1}) {
		t.Fatalf("client offers %v by default, want [2 1]", client.cfg.WireVersions)
	}
	if got := client.dialWires(withPinnedWire(context.Background(), 1)); !slices.Equal(got, []byte{1}) {
		t.Fatalf("a dial for a version-1 flow offers %v", got)
	}
	if _, err := NewClient(ClientConfig{ListenAddr: "127.0.0.1:0", RemoteAddr: "127.0.0.1:1", Credentials: clientCredentials, WireVersions: []byte{2, 2}}); err == nil {
		t.Fatal("client accepted a repeated wire version")
	}
}
