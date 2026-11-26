package pep

import (
	"context"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/bojieli/queqiao/internal/identity"
	"github.com/bojieli/queqiao/internal/protocol"
	"github.com/bojieli/queqiao/internal/session"
)

func TestCompletedLaneJoinCarriesReplacementHint(t *testing.T) {
	local, remote := net.Pipe()
	t.Cleanup(func() { _ = local.Close(); _ = remote.Close() })
	seen := make(chan protocol.Frame, 1)
	go func() {
		frames := newFrameConn(remote)
		request, err := frames.Read()
		if err != nil {
			return
		}
		seen <- request
		_ = frames.Write(protocol.Frame{Header: protocol.Header{
			Version: protocol.Version, Type: protocol.TypeOpenOK,
			SessionID: request.Header.SessionID, FlowID: request.Header.FlowID,
			Class: protocol.ClassBulk,
		}})
	}()
	ctx := context.WithValue(context.Background(), laneReplacementContextKey{}, laneReplacementHint{id: 0})
	client := &Client{cfg: ClientConfig{HandshakeTimeout: time.Second}}
	attachTestCarriers(client)
	if _, err := client.completeLaneJoin(ctx, &authenticatedLane{
		fc: newFrameConn(local), outer: local, sessionID: [16]byte{1}, kind: TransportQUIC, laneID: 9,
	}, 7, 0); err != nil {
		t.Fatal(err)
	}
	request := <-seen
	if request.Header.Flags&protocol.FlagReplaceLane == 0 || len(request.Payload) != 16 {
		t.Fatalf("replacement JOIN flags=%#x payload=%d, want replacement flag and 16-byte payload", request.Header.Flags, len(request.Payload))
	}
	if got := binary.BigEndian.Uint64(request.Payload[8:]); got != 0 {
		t.Fatalf("replaced lane ID = %d, want the zero-numbered initial lane", got)
	}
}

// The reset code on a refused JOIN is the peer's retry policy: capacity must
// stay the transient answer, while the TCP-mode refusal is a permanent
// rejection an AUTO recovery path can also recognize and act on.
func TestCompleteLaneJoinMapsResetCodesToRetryPolicy(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	for _, test := range []struct {
		name          string
		code          session.ResetCode
		want          error
		stillRejected bool
	}{
		{name: "capacity stays transient", code: session.ResetFlowLimit, want: errLaneJoinCapacity},
		{name: "tcp mode is a recognizable rejection", code: session.ResetTransport, want: errLaneJoinTCPMode, stillRejected: true},
		{name: "protocol is permanent", code: session.ResetProtocol, want: errLaneJoinRejected},
	} {
		t.Run(test.name, func(t *testing.T) {
			local, remote := net.Pipe()
			t.Cleanup(func() { _ = local.Close(); _ = remote.Close() })
			go func() {
				frames := newFrameConn(remote)
				request, err := frames.Read()
				if err != nil {
					return
				}
				_ = frames.Write(protocol.Frame{Header: protocol.Header{
					Version: protocol.Version, Type: protocol.TypeReset,
					SessionID: request.Header.SessionID, FlowID: request.Header.FlowID,
					Class: protocol.ClassBulk,
				}, Payload: session.ResetPayload(test.code, "refused")})
			}()
			client := &Client{cfg: ClientConfig{Logger: logger, HandshakeTimeout: 2 * time.Second}}
			attachTestCarriers(client)
			_, err := client.completeLaneJoin(context.Background(), &authenticatedLane{
				fc: newFrameConn(local), outer: local, sessionID: [16]byte{1}, kind: TransportQUIC, laneID: 1,
			}, 7, 0)
			if !errors.Is(err, test.want) {
				t.Fatalf("err = %v, want %v", err, test.want)
			}
			if got := errors.Is(err, errLaneJoinRejected); got != test.stillRejected && test.want != errLaneJoinRejected {
				t.Fatalf("errors.Is(err, errLaneJoinRejected) = %t, want %t", got, test.stillRejected)
			}
		})
	}
}

// joinTestRig is a real gateway and client with the server-side flow
// registered by hand, so the test decides the admission state a rescue JOIN
// meets rather than growing one through the SOCKS path.
type joinTestRig struct {
	client     *Client
	serverFlow *multipathFlow
	serverSess *serverFlow
	cancel     context.CancelFunc
	errorsCh   chan error
}

func newJoinTestRig(t *testing.T, clientTransport, initialKind TransportKind, tcpMaxLanes int) *joinTestRig {
	t.Helper()
	certificate, roots := testCertificate(t)
	leaf, err := x509.ParseCertificate(roots.Certificate.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	principal, err := identity.PrincipalFromCertificate(leaf)
	if err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	serverListener, packetConn := listenTCPAndUDPOnOnePort(t)
	server, err := NewServer(ServerConfig{
		ListenAddr: serverListener.Addr().String(), Credentials: certificate,
		DestinationPolicy: DestinationPolicy{AllowPrivate: true},
		EnableTCP:         true, EnableQUIC: true, Logger: logger,
		HandshakeTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	serverFlow := newIsolationTestFlow(t, true)
	serverSession := newServerFlow(serverFlow, principal, initialKind, tcpMaxLanes)
	server.sessionsMu.Lock()
	server.sessions[serverFlow.sessionID] = serverSession
	server.sessionsMu.Unlock()

	client, err := NewClient(ClientConfig{
		ListenAddr: "127.0.0.1:0", RemoteAddr: serverListener.Addr().String(),
		Credentials: roots, Transport: clientTransport, FallbackDelay: 100 * time.Millisecond, FallbackGrace: 2 * time.Second,
		DialTimeout: 2 * time.Second, HandshakeTimeout: 2 * time.Second, Logger: logger,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	errorsCh := make(chan error, 2)
	go func() { errorsCh <- server.ServeListener(ctx, serverListener) }()
	go func() { errorsCh <- server.ServePacketConn(ctx, packetConn) }()
	rig := &joinTestRig{
		client: client, serverFlow: serverFlow, serverSess: serverSession,
		cancel: cancel, errorsCh: errorsCh,
	}
	t.Cleanup(func() {
		cancel()
		for range 2 {
			select {
			case <-errorsCh:
			case <-time.After(3 * time.Second):
				return
			}
		}
		_ = serverListener.Close()
		_ = packetConn.Close()
	})
	return rig
}
