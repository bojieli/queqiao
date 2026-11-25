package pep

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apernet/quic-go"
	"github.com/bojieli/queqiao/internal/identity"
	"github.com/bojieli/queqiao/internal/protocol"
	"github.com/bojieli/queqiao/internal/session"
)

// admissionGateway runs a real gateway on loopback for the admission tests and
// returns it with the address to dial.
func admissionGateway(t *testing.T, ctx context.Context, carrier TransportKind, cfg ServerConfig) (*Server, string) {
	t.Helper()
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.DestinationPolicy = DestinationPolicy{AllowPrivate: true, DialTimeout: 2 * time.Second}
	cfg.EnableQUIC = carrier == TransportQUIC
	cfg.EnableTCP = carrier == TransportTCP
	cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg.HandshakeTimeout = 3 * time.Second
	// PACKET frames stay on the stream the raw test client reads.
	cfg.UDPOnStream = true
	released := cfg.testReleasedAdmission
	server, err := NewServer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	server.cfg.testReleasedAdmission = released
	done := make(chan struct{})
	if carrier == TransportTCP {
		listener, err := net.Listen("tcp4", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		go func() { defer close(done); _ = server.ServeListener(ctx, listener) }()
		t.Cleanup(func() { <-done })
		return server, listener.Addr().String()
	}
	packetConn, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { defer close(done); _ = server.ServePacketConn(ctx, packetConn) }()
	t.Cleanup(func() { <-done })
	return server, packetConn.LocalAddr().String()
}

// rawCarrier is one authenticated client connection that opens lanes on
// demand: streams of one QUIC connection, or one TLS/TCP connection per lane.
type rawCarrier struct {
	t           *testing.T
	carrier     TransportKind
	remote      string
	credentials identity.ClientCredentials
	wire        []byte
	conn        *quic.Conn
}

func dialRawCarrier(ctx context.Context, t *testing.T, carrier TransportKind, remote string, credentials identity.ClientCredentials, wire byte) (*rawCarrier, error) {
	c := &rawCarrier{t: t, carrier: carrier, remote: remote, credentials: credentials, wire: []byte{wire}}
	if carrier == TransportQUIC {
		conn, packetConn, err := dialQUICConnection(ctx, remote, credentials, c.wire, 10*time.Second, "", nil, nil, flowWindows{}, hopDialConfig{})
		if err != nil {
			return nil, err
		}
		c.conn = conn
		t.Cleanup(func() {
			_ = conn.CloseWithError(0, "test done")
			if packetConn != nil {
				_ = packetConn.Close()
			}
		})
	}
	return c, nil
}

func (c *rawCarrier) lane(ctx context.Context) (*frameConn, streamConn, error) {
	if c.carrier == TransportTCP {
		outer, err := dialTCP(ctx, c.remote, c.credentials, c.wire, 10*time.Second, "", nil)
		if err != nil {
			return nil, nil, err
		}
		c.t.Cleanup(func() { _ = outer.Close() })
		return newFrameConn(outer), outer, nil
	}
	stream, err := c.conn.OpenStreamSync(ctx)
	if err != nil {
		return nil, nil, err
	}
	outer := &quicStreamConn{stream: stream, conn: c.conn}
	c.t.Cleanup(func() { _ = outer.Close() })
	return newFrameConn(outer), outer, nil
}

// exchange writes a lane's first frame and reads its answer within a bound.
func exchange(ctx context.Context, c *rawCarrier, frame protocol.Frame) (protocol.Frame, *frameConn, streamConn, error) {
	fc, outer, err := c.lane(ctx)
	if err != nil {
		return protocol.Frame{}, nil, nil, err
	}
	_ = outer.SetDeadline(time.Now().Add(10 * time.Second))
	if err := fc.Write(frame); err != nil {
		return protocol.Frame{}, fc, outer, err
	}
	response, err := fc.Read()
	_ = outer.SetDeadline(time.Time{})
	return response, fc, outer, err
}

func openFrame(t *testing.T, sessionID [16]byte, flowID uint64, destination string) protocol.Frame {
	t.Helper()
	payload, err := session.EncodeDestination(destination)
	if err != nil {
		t.Fatal(err)
	}
	return protocol.Frame{Header: protocol.Header{
		Version: protocol.Version, Type: protocol.TypeOpen, SessionID: sessionID, FlowID: flowID, Class: protocol.ClassNew,
	}, Payload: payload}
}

func joinFrame(sessionID [16]byte, flowID, laneID uint64) protocol.Frame {
	return protocol.Frame{Header: protocol.Header{
		Version: protocol.Version, Type: protocol.TypeJoin, SessionID: sessionID, FlowID: flowID, Class: protocol.ClassBulk,
	}, Payload: encodeLaneID(laneID)}
}

// probe runs one PROBE exchange on a new lane and closes it, as a client's
// verification does.
func probe(ctx context.Context, c *rawCarrier) (protocol.Frame, error) {
	echo, _, outer, err := exchange(ctx, c, probeFrame())
	if outer != nil {
		_ = outer.Close()
	}
	return echo, err
}

func probeFrame() protocol.Frame {
	return protocol.Frame{Header: protocol.Header{
		Version: protocol.Version, Type: protocol.TypeProbe, SessionID: [16]byte{0x70}, Class: protocol.ClassNew,
	}, Payload: []byte{1}}
}

// knownResetCode is the PROTOCOL.md §15 table, which both wire versions share
// and outside which a version-1 peer must never be sent a code.
func knownResetCode(code session.ResetCode) bool {
	return code >= session.ResetProtocol && code <= session.ResetTransport
}

// destinationKind is what the flows filling the gateway are doing.
type destinationKind int

const (
	idleFlows destinationKind = iota
	activeFlows
)

// admissionDestination is an echo server, or, for active transfers, a server
// that sends without pause for as long as its connection lives.
func admissionDestination(t *testing.T, kind destinationKind) string {
	t.Helper()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				if kind == idleFlows {
					_, _ = io.Copy(conn, conn)
					return
				}
				go func() { _, _ = io.Copy(io.Discard, conn) }()
				// Steady rather than flat out: a transfer in progress, not a
				// CPU load that starves the rest of the test on a small runner.
				chunk := bytes.Repeat([]byte{'a'}, 16*1024)
				ticker := time.NewTicker(2 * time.Millisecond)
				defer ticker.Stop()
				for range ticker.C {
					if _, err := conn.Write(chunk); err != nil {
						return
					}
				}
			}()
		}
	}()
	return listener.Addr().String()
}

// drain reads a lane until it ends, so an active transfer keeps moving.
func drain(fc *frameConn) {
	go func() {
		for {
			if _, err := fc.Read(); err != nil {
				return
			}
		}
	}()
}

// readUntilData reads frames from a lane until a DATA frame arrives.
func readUntilData(fc *frameConn, outer streamConn, within time.Duration) (protocol.Frame, error) {
	_ = outer.SetDeadline(time.Now().Add(within))
	defer func() { _ = outer.SetDeadline(time.Time{}) }()
	for {
		frame, err := fc.Read()
		if err != nil {
			return protocol.Frame{}, err
		}
		if frame.Header.Type == protocol.TypeData {
			return frame, nil
		}
		if frame.Header.Type == protocol.TypeReset {
			return frame, fmt.Errorf("RESET %d %q", resetCode(frame.Payload), frame.Payload)
		}
	}
}

type admissionCase struct {
	name    string
	carrier TransportKind
	mode    GatewayAdmission
	wire    byte
	kind    destinationKind
	// pooled puts every flow on one QUIC connection, so only the stream
	// layer is full; otherwise each flow has its own connection, as an
	// isolating client's do, and the connection bound is full as well.
	pooled bool
}

func admissionCases() []admissionCase {
	var cases []admissionCase
	for _, mode := range []GatewayAdmission{GatewayAdmissionLegacy, GatewayAdmissionOwned} {
		for _, carrier := range []TransportKind{TransportQUIC, TransportTCP} {
			for _, wire := range protocol.SupportedVersions() {
				for _, kind := range []destinationKind{idleFlows, activeFlows} {
					name := "idle"
					if kind == activeFlows {
						name = "active"
					}
					for _, pooled := range []bool{false, true} {
						if pooled && carrier != TransportQUIC {
							continue
						}
						topology := "isolated"
						if pooled {
							topology = "pooled"
						}
						cases = append(cases, admissionCase{
							name:    fmt.Sprintf("%s/%s/wire%d/%s/%s", mode, carrier, wire, name, topology),
							carrier: carrier, mode: mode, wire: wire, kind: kind, pooled: pooled,
						})
					}
				}
			}
		}
	}
	return cases
}

// Issue #170, design §10.5 and §15 test 12. Every session slot, and on QUIC
// every connection slot, is held by established flows, idle or transferring.
// One flow's lane fails. Its JOIN must still be admitted and carry the flow,
// and a PROBE on the client's standby connection must be answered; a new OPEN
// is refused. Main refused all three, so the flow waited out its grace and
// failed while holding its slot. Every RESET seen is one the lane's wire
// version knows.
func TestAFullGatewayAdmitsTheJoinAndProbeThatRecoverItsFlows(t *testing.T) {
	serverCredentials, clientCredentials := testCertificate(t)
	const maxSessions = 4
	for _, tc := range admissionCases() {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
			defer cancel()
			destination := admissionDestination(t, tc.kind)
			server, remote := admissionGateway(t, ctx, tc.carrier, ServerConfig{
				Credentials: serverCredentials, MaxSessions: maxSessions, GatewayAdmission: tc.mode,
			})
			checkReset := func(what string, frame protocol.Frame) {
				t.Helper()
				if frame.Header.Type == protocol.TypeReset && !knownResetCode(resetCode(frame.Payload)) {
					t.Fatalf("%s: RESET code %d is outside the table wire %d knows", what, resetCode(frame.Payload), tc.wire)
				}
				if frame.Header.Type != 0 && frame.Header.Version != tc.wire {
					t.Fatalf("%s: answer on wire %d, want %d", what, frame.Header.Version, tc.wire)
				}
			}

			// One flow per connection, as an isolating client does, fills the
			// connection bound as well as the session bound; a pooled client
			// fills only the session bound.
			type opened struct {
				sessionID [16]byte
				flowID    uint64
				fc        *frameConn
				outer     streamConn
			}
			flows := make([]opened, maxSessions)
			var carrier *rawCarrier
			for i := range flows {
				if carrier == nil || !tc.pooled {
					var err error
					if carrier, err = dialRawCarrier(ctx, t, tc.carrier, remote, clientCredentials, tc.wire); err != nil {
						t.Fatal(err)
					}
				}
				sessionID := [16]byte{0x5a, byte(i + 1)}
				response, fc, outer, err := exchange(ctx, carrier, openFrame(t, sessionID, uint64(100+i), destination))
				if err != nil || response.Header.Type != protocol.TypeOpenOK {
					t.Fatalf("flow %d open answered %+v, %v", i, response.Header, err)
				}
				flows[i] = opened{sessionID: sessionID, flowID: uint64(100 + i), fc: fc, outer: outer}
				if tc.kind == activeFlows && i != 0 {
					drain(fc)
				}
			}
			victim := flows[0]
			if tc.kind == activeFlows {
				// The transfer is under way before its lane fails.
				if _, err := readUntilData(victim.fc, victim.outer, 3*time.Second); err != nil {
					t.Fatalf("active flow carried no data before the failure: %v", err)
				}
			}
			waitForSessions(t, server, maxSessions)

			// The victim's lane fails; the flow enters its replacement grace.
			_ = victim.outer.Close()

			standby, err := dialRawCarrier(ctx, t, tc.carrier, remote, clientCredentials, tc.wire)
			if err != nil {
				t.Fatalf("standby connection refused: %v", err)
			}
			if tc.carrier == TransportQUIC {
				// The PROBE that verifies the standby connection.
				echo, err := probe(ctx, standby)
				if err != nil {
					t.Fatalf("PROBE on the standby connection was not answered: %v", err)
				}
				checkReset("probe", echo)
				if echo.Header.Type != protocol.TypeProbe || len(echo.Payload) != 1 || echo.Payload[0] != 1 {
					t.Fatalf("PROBE answered %+v", echo.Header)
				}
			}

			// The JOIN that rebinds the failed flow.
			joined, fc, outer, err := exchange(ctx, standby, joinFrame(victim.sessionID, victim.flowID, 1))
			if err != nil {
				t.Fatalf("JOIN of a flow the full gateway holds was not answered: %v", err)
			}
			checkReset("join", joined)
			if joined.Header.Type != protocol.TypeOpenOK {
				t.Fatalf("JOIN answered type %d %q, want OPEN_OK", joined.Header.Type, joined.Payload)
			}
			// And the flow is carried on the new lane.
			if tc.kind == idleFlows {
				if err := fc.Write(protocol.Frame{Header: protocol.Header{
					Version: protocol.Version, Type: protocol.TypeData, SessionID: victim.sessionID,
					FlowID: victim.flowID, Class: protocol.ClassInteractive,
				}, Payload: []byte("still here")}); err != nil {
					t.Fatal(err)
				}
			}
			data, err := readUntilData(fc, outer, 5*time.Second)
			if err != nil {
				t.Fatalf("rejoined flow carried nothing: %v", err)
			}
			if tc.kind == idleFlows && string(data.Payload) != "still here" {
				t.Fatalf("rejoined flow echoed %q", data.Payload)
			}

			// A new flow does not get a slot the established ones hold.
			refused, _, _, err := exchange(ctx, standby, openFrame(t, [16]byte{0x5b}, 999, destination))
			if err == nil {
				checkReset("open", refused)
				if refused.Header.Type == protocol.TypeOpenOK {
					t.Fatal("a new OPEN was admitted past the session limit")
				}
				if tc.mode == GatewayAdmissionOwned && (refused.Header.Type != protocol.TypeReset || resetCode(refused.Payload) != session.ResetFlowLimit) {
					t.Fatalf("owned admission refused the OPEN with %+v %q, want FLOW_LIMIT", refused.Header, refused.Payload)
				}
			} else if tc.mode == GatewayAdmissionOwned {
				t.Fatalf("owned admission closed the OPEN without an answer: %v", err)
			}
		})
	}
}

// waitForSessions waits until the gateway's session table holds n flows.
func waitForSessions(t *testing.T, server *Server, n int) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); ; {
		server.sessionsMu.RLock()
		got := len(server.sessions)
		server.sessionsMu.RUnlock()
		if got == n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("gateway holds %d sessions, want %d", got, n)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// A released gateway is the degraded path of design §10.2: with MaxSessions=1
// and one confirmed flow, the flow's own JOIN is refused because it needs the
// slot the flow holds. The case is stated as unrecoverable against released
// gateways, and this keeps it observed rather than hidden. The emulation is
// what lets the client-side tests of later stages run against a released
// gateway.
func TestAReleasedGatewayAtItsSessionLimitRefusesTheRescueJoin(t *testing.T) {
	serverCredentials, clientCredentials := testCertificate(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	destination := admissionDestination(t, idleFlows)
	_, remote := admissionGateway(t, ctx, TransportQUIC, ServerConfig{
		Credentials: serverCredentials, MaxSessions: 1, testReleasedAdmission: true,
	})
	carrier, err := dialRawCarrier(ctx, t, TransportQUIC, remote, clientCredentials, protocol.Version)
	if err != nil {
		t.Fatal(err)
	}
	sessionID := [16]byte{0x5c}
	response, _, outer, err := exchange(ctx, carrier, openFrame(t, sessionID, 7, destination))
	if err != nil || response.Header.Type != protocol.TypeOpenOK {
		t.Fatalf("open answered %+v, %v", response.Header, err)
	}
	_ = outer.Close()
	// The connection is alive; only the flow's stream failed.
	if answer, _, _, err := exchange(ctx, carrier, joinFrame(sessionID, 7, 1)); err == nil {
		t.Fatalf("released gateway answered the rescue JOIN at its session limit: %+v", answer.Header)
	}
	// The handshake may complete before the gateway closes the connection;
	// a stream on it must not be served.
	if standby, err := dialRawCarrier(ctx, t, TransportQUIC, remote, clientCredentials, protocol.Version); err == nil {
		if answer, err := probe(ctx, standby); err == nil {
			t.Fatalf("released gateway served a standby past its connection limit: %+v", answer.Header)
		}
	}
}

// In owned admission the session limit counts logical flows. Lanes a flow
// adds by JOIN, and PROBEs, do not consume it; a refused OPEN is told
// FLOW_LIMIT; and the slot comes back when the flow ends.
func TestOwnedAdmissionCountsFlowsNotStreams(t *testing.T) {
	serverCredentials, clientCredentials := testCertificate(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	destination := admissionDestination(t, idleFlows)
	_, remote := admissionGateway(t, ctx, TransportQUIC, ServerConfig{
		Credentials: serverCredentials, MaxSessions: 2, GatewayAdmission: GatewayAdmissionOwned,
	})
	carrier, err := dialRawCarrier(ctx, t, TransportQUIC, remote, clientCredentials, protocol.Version)
	if err != nil {
		t.Fatal(err)
	}
	// Probes first: several, sequentially, none of them a flow.
	for i := range 4 {
		echo, err := probe(ctx, carrier)
		if err != nil || echo.Header.Type != protocol.TypeProbe {
			t.Fatalf("probe %d answered %+v, %v", i, echo.Header, err)
		}
	}
	var lanes []streamConn
	for i := range 2 {
		sessionID := [16]byte{0x5d, byte(i)}
		open := openFrame(t, sessionID, uint64(10+i), destination)
		// A flow with a control lane has room for a data lane beside it.
		open.Header.Flags = protocol.FlagReserveControl
		response, _, outer, err := exchange(ctx, carrier, open)
		if err != nil || response.Header.Type != protocol.TypeOpenOK {
			t.Fatalf("flow %d open answered %+v, %v", i, response.Header, err)
		}
		lanes = append(lanes, outer)
		// A second lane for the same flow is charged to the flow.
		joined, _, _, err := exchange(ctx, carrier, joinFrame(sessionID, uint64(10+i), 1))
		if err != nil || joined.Header.Type != protocol.TypeOpenOK {
			t.Fatalf("flow %d extra lane answered %+v %q, %v", i, joined.Header, joined.Payload, err)
		}
	}
	refused, _, _, err := exchange(ctx, carrier, openFrame(t, [16]byte{0x5e}, 20, destination))
	if err != nil || refused.Header.Type != protocol.TypeReset || resetCode(refused.Payload) != session.ResetFlowLimit {
		t.Fatalf("third flow answered %+v %q, %v; want FLOW_LIMIT", refused.Header, refused.Payload, err)
	}
	echo, err := probe(ctx, carrier)
	if err != nil || echo.Header.Type != protocol.TypeProbe {
		t.Fatalf("probe at the session limit answered %+v, %v", echo.Header, err)
	}
	_ = lanes
}

func TestGatewayAdmissionConfiguration(t *testing.T) {
	credentials, _ := testCertificate(t)
	for _, value := range []string{"", "legacy", " Owned "} {
		if _, err := ParseGatewayAdmission(value); err != nil {
			t.Fatalf("%q refused: %v", value, err)
		}
	}
	server, err := NewServer(ServerConfig{ListenAddr: "127.0.0.1:0", Credentials: credentials})
	if err != nil {
		t.Fatal(err)
	}
	if server.cfg.GatewayAdmission != GatewayAdmissionLegacy {
		t.Fatalf("default admission = %q, want legacy", server.cfg.GatewayAdmission)
	}
	if _, err := NewServer(ServerConfig{ListenAddr: "127.0.0.1:0", Credentials: credentials, GatewayAdmission: "shared"}); err == nil {
		t.Fatal("an unknown gateway admission was accepted")
	}
}

// A full connection bound still admits one standby beside a principal's
// existing data connections, and only one; a principal holding none gets
// nothing past the bound.
func TestAStandbyConnectionIsAdmittedBesideAPrincipalsConnections(t *testing.T) {
	serverCredentials, clientCredentials := testCertificate(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	server, remote := admissionGateway(t, ctx, TransportQUIC, ServerConfig{Credentials: serverCredentials, MaxSessions: 1})
	first, err := dialRawCarrier(ctx, t, TransportQUIC, remote, clientCredentials, protocol.Version)
	if err != nil {
		t.Fatal(err)
	}
	if echo, err := probe(ctx, first); err != nil || echo.Header.Type != protocol.TypeProbe {
		t.Fatalf("first connection probe: %+v %v", echo.Header, err)
	}
	standby, err := dialRawCarrier(ctx, t, TransportQUIC, remote, clientCredentials, protocol.Version)
	if err != nil {
		t.Fatal(err)
	}
	if echo, err := probe(ctx, standby); err != nil || echo.Header.Type != protocol.TypeProbe {
		t.Fatalf("standby probe past a full connection bound: %+v %v", echo.Header, err)
	}
	third, err := dialRawCarrier(ctx, t, TransportQUIC, remote, clientCredentials, protocol.Version)
	if err == nil {
		if echo, err := probe(ctx, third); err == nil {
			t.Fatalf("a second standby was admitted: %+v", echo.Header)
		}
	}
	server.connMu.Lock()
	defer server.connMu.Unlock()
	for _, p := range server.principalConns {
		if p.general != 1 || p.standby == nil {
			t.Fatalf("principal holds %d general connections, standby %v; want 1 and one", p.general, p.standby != nil)
		}
	}
}

// After a promotion the client's flows live on the standby and the old
// connection is gone. The connection slot follows the connection, not the
// flows it once carried, and the standby inherits the general slot, so the
// next recovery can have a standby of its own: at MaxSessions=1 a flow can be
// moved twice.
func TestAFlowCanMoveToAStandbyTwiceAtTheConnectionLimit(t *testing.T) {
	serverCredentials, clientCredentials := testCertificate(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	destination := admissionDestination(t, idleFlows)
	server, remote := admissionGateway(t, ctx, TransportQUIC, ServerConfig{Credentials: serverCredentials, MaxSessions: 1})
	current, err := dialRawCarrier(ctx, t, TransportQUIC, remote, clientCredentials, protocol.Version)
	if err != nil {
		t.Fatal(err)
	}
	sessionID := [16]byte{0x62}
	if response, _, _, err := exchange(ctx, current, openFrame(t, sessionID, 5, destination)); err != nil || response.Header.Type != protocol.TypeOpenOK {
		t.Fatalf("open answered %+v, %v", response.Header, err)
	}
	for move := uint64(1); move <= 2; move++ {
		standby, err := dialRawCarrier(ctx, t, TransportQUIC, remote, clientCredentials, protocol.Version)
		if err != nil {
			t.Fatalf("move %d: standby dial: %v", move, err)
		}
		if echo, err := probe(ctx, standby); err != nil || echo.Header.Type != protocol.TypeProbe {
			t.Fatalf("move %d: standby probe answered %+v, %v", move, echo.Header, err)
		}
		// The old connection dies, and the flow rejoins on the standby. The
		// JOIN is retried while the gateway has yet to see the old lane end.
		_ = current.conn.CloseWithError(0, "promoted")
		var fc *frameConn
		var outer streamConn
		for attempt := 0; ; attempt++ {
			var joined protocol.Frame
			joined, fc, outer, err = exchange(ctx, standby, joinFrame(sessionID, 5, move))
			if err == nil && joined.Header.Type == protocol.TypeOpenOK {
				break
			}
			if attempt == 20 || err != nil || resetCode(joined.Payload) != session.ResetFlowLimit {
				t.Fatalf("move %d: JOIN answered %+v %q, %v", move, joined.Header, joined.Payload, err)
			}
			time.Sleep(50 * time.Millisecond)
		}
		if err := fc.Write(protocol.Frame{Header: protocol.Header{
			Version: protocol.Version, Type: protocol.TypeData, SessionID: sessionID, FlowID: 5,
			Sequence: (move - 1) * 4, Class: protocol.ClassInteractive,
		}, Payload: []byte("move")}); err != nil {
			t.Fatal(err)
		}
		if _, err := readUntilData(fc, outer, 5*time.Second); err != nil {
			t.Fatalf("move %d: flow carried nothing on its new connection: %v", move, err)
		}
		// The old connection's close has been processed: the standby holds
		// the principal's one general slot and no standby is outstanding.
		for deadline := time.Now().Add(5 * time.Second); ; {
			server.connMu.Lock()
			general, standby := 0, false
			for _, p := range server.principalConns {
				general, standby = p.general, p.standby != nil
			}
			held := len(server.connections)
			server.connMu.Unlock()
			if general == 1 && !standby && held == 1 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("move %d: principal holds %d general slots, standby %v, %d slots in the bound", move, general, standby, held)
			}
			time.Sleep(10 * time.Millisecond)
		}
		current = standby
	}
}

// openUDPAssociation opens a resumable UDP association on a lane and returns
// the lane and the token its relay will answer to next.
func openRawUDPAssociation(ctx context.Context, t *testing.T, carrier *rawCarrier, sessionID [16]byte, flowID uint64, resume []byte) (protocol.Frame, *frameConn, streamConn) {
	t.Helper()
	payload, err := session.EncodeUDPResumeOpen(resume)
	if err != nil {
		t.Fatal(err)
	}
	response, fc, outer, err := exchange(ctx, carrier, protocol.Frame{Header: protocol.Header{
		Version: protocol.Version, Type: protocol.TypeOpen, SessionID: sessionID, FlowID: flowID, Class: protocol.ClassInteractive,
	}, Payload: payload})
	if err != nil {
		t.Fatalf("UDP association open: %v", err)
	}
	return response, fc, outer
}

// udpRelaySource sends one datagram through an association and reports the
// source address the destination saw it from.
func udpRelaySource(t *testing.T, fc *frameConn, sessionID [16]byte, flowID, sequence uint64, target *net.UDPConn) string {
	t.Helper()
	payload, err := session.EncodeUDPPacket(target.LocalAddr().String(), []byte("ping"))
	if err != nil {
		t.Fatal(err)
	}
	if err := fc.Write(protocol.Frame{Header: protocol.Header{
		Version: protocol.Version, Type: protocol.TypePacket, SessionID: sessionID, FlowID: flowID,
		Sequence: sequence, Class: protocol.ClassInteractive,
	}, Payload: payload}); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 64)
	_ = target.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, from, err := target.ReadFromUDP(buf)
	if err != nil {
		t.Fatalf("destination saw no datagram: %v", err)
	}
	return from.String()
}

// Design §11.2 and §15 test 16. The client closes a UDP association's lane and
// then sends the resume OPEN, but the close may not arrive, and when it does
// the old handler parks the relay only after joining its workers. An owned
// gateway hands a relay still held by a live association to its replacement,
// with the association's session and account slots: at MaxSessions=1 the
// replacement needs no second slot, and the destination keeps seeing the same
// source address. A legacy gateway resumes only a parked relay, so with the
// close not delivered the replacement gets a fresh one (best effort, as
// documented).
func TestALiveUDPRelayIsHandedToItsReplacement(t *testing.T) {
	serverCredentials, clientCredentials := testCertificate(t)
	target, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	for _, tc := range []struct {
		name        string
		mode        GatewayAdmission
		maxSessions int
		closeFirst  bool
		wantResumed bool
	}{
		{"owned/close-lost", GatewayAdmissionOwned, 1, false, true},
		{"owned/close-arrived", GatewayAdmissionOwned, 1, true, true},
		{"legacy/close-lost", GatewayAdmissionLegacy, 2, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			server, remote := admissionGateway(t, ctx, TransportQUIC, ServerConfig{
				Credentials: serverCredentials, MaxSessions: tc.maxSessions, GatewayAdmission: tc.mode,
			})
			carrier, err := dialRawCarrier(ctx, t, TransportQUIC, remote, clientCredentials, protocol.Version)
			if err != nil {
				t.Fatal(err)
			}
			first := [16]byte{0x60}
			response, fc, outer := openRawUDPAssociation(ctx, t, carrier, first, 1, nil)
			if response.Header.Type != protocol.TypeOpenOK {
				t.Fatalf("association answered %+v %q", response.Header, response.Payload)
			}
			_, token, ok := session.DecodeUDPResumeGrant(response.Payload)
			if !ok {
				t.Fatal("no resume grant")
			}
			before := udpRelaySource(t, fc, first, 1, 0, target)
			if tc.closeFirst {
				_ = outer.Close()
			}
			second := [16]byte{0x61}
			response, fc, _ = openRawUDPAssociation(ctx, t, carrier, second, 2, token[:])
			if response.Header.Type != protocol.TypeOpenOK {
				t.Fatalf("replacement answered %+v %q", response.Header, response.Payload)
			}
			resumed, _, ok := session.DecodeUDPResumeGrant(response.Payload)
			if !ok {
				t.Fatal("no resume grant on the replacement")
			}
			if resumed != tc.wantResumed {
				t.Fatalf("resumed = %v, want %v", resumed, tc.wantResumed)
			}
			after := udpRelaySource(t, fc, second, 2, 0, target)
			if tc.wantResumed && after != before {
				t.Fatalf("relay source moved from %s to %s", before, after)
			}
			if tc.mode == GatewayAdmissionOwned {
				// Exactly one session slot is held: the one transferred.
				if got := len(server.semaphore); got != 1 {
					t.Fatalf("%d session slots held after the transfer, want 1", got)
				}
			}
		})
	}
}

// The handoff itself, against the store's race (§11.2). An association that
// ends while its replacement claims it, whether it ends because it was
// claimed or on its own, either hands the relay over or parks it with its
// lease; the token stays claimable throughout, so the replacement gets the
// relay and the slots in every interleaving, and the lease is released once.
func TestALiveRelayIsAlwaysTakenOverWhateverTheInterleaving(t *testing.T) {
	for i := range 400 {
		store := newUDPRelayStore()
		token, err := newUDPResumeToken()
		if err != nil {
			t.Fatal(err)
		}
		relay, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
		if err != nil {
			t.Fatal(err)
		}
		lease := &flowLease{}
		var released atomic.Int32
		lease.add(func() { released.Add(1) })
		stopped := make(chan struct{})
		var stopOnce sync.Once
		entry := store.publishLive(token, udpTestPrincipal, func() { stopOnce.Do(func() { close(stopped) }) })
		// The association has been carrying traffic: the client holds its grant.
		store.confirmLive(entry)
		var wg sync.WaitGroup
		var end liveEnd
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Even rounds: the owner ends because it was claimed. Odd
			// rounds: its lane failed and it ends on its own, racing the
			// claim.
			if i%2 == 0 {
				<-stopped
			}
			end = store.endLive(token, entry, relay, lease, true)
			lease.release()
		}()
		if i%4 == 1 {
			wg.Wait()
		}
		got, moved := store.transfer(context.Background(), token[:], udpTestPrincipal, time.Second)
		wg.Wait()
		if got == nil {
			t.Fatalf("round %d: the replacement got no relay (owner %v)", i, end)
		}
		if end == liveClosed {
			t.Fatalf("round %d: the owner closed a relay its replacement holds", i)
		}
		if released.Load() != 0 {
			t.Fatalf("round %d: the lease was released before the replacement ended", i)
		}
		moved.release()
		if released.Load() != 1 {
			t.Fatalf("round %d: lease released %d times, want 1", i, released.Load())
		}
		_ = relay.Close()
		store.closeAll()
	}
}

// A relay parked with a lease that nobody takes over releases the lease when
// it expires.
func TestAnUnclaimedParkedRelayReleasesItsLease(t *testing.T) {
	store := newUDPRelayStoreWith(time.Millisecond, udpRelaysRetained)
	token, err := newUDPResumeToken()
	if err != nil {
		t.Fatal(err)
	}
	relay, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	lease := &flowLease{}
	var released atomic.Int32
	lease.add(func() { released.Add(1) })
	entry := store.publishLive(token, udpTestPrincipal, func() {})
	store.confirmLive(entry)
	if end := store.endLive(token, entry, relay, lease, true); end != liveParked {
		t.Fatalf("owner end = %v, want parked", end)
	}
	lease.release()
	for deadline := time.Now().Add(2 * time.Second); released.Load() == 0; {
		if time.Now().After(deadline) {
			t.Fatal("an expired parked relay kept its lease")
		}
		time.Sleep(time.Millisecond)
	}
	if released.Load() != 1 {
		t.Fatalf("lease released %d times, want 1", released.Load())
	}
}

// Design §10.3 step 1: a RESERVE_CONTROL JOIN replaces a failed control
// generation. A released gateway with room in the lane table adds the new
// lane beside the old one, which stays schedulable until it fails; an owned
// gateway retires the previous holder of the control role once the
// replacement is committed. A holder younger than the eviction protection is
// a rescue that just won and is kept either way.
func TestAControlJoinRetiresThePreviousControlLaneInOwnedAdmission(t *testing.T) {
	for _, tc := range []struct {
		name        string
		retire      bool
		age         time.Duration
		wantRetired bool
	}{
		{"legacy", false, 2 * laneDeadPathDetection, false},
		{"owned", true, 2 * laneDeadPathDetection, true},
		{"owned/young-holder", true, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			flow := newIsolationTestFlow(t, true)
			serverSession := newServerFlow(flow, identity.Principal{}, TransportQUIC, 8)
			serverSession.retireControlOnJoin = tc.retire
			old := isolationLane(t, 0)
			old.control = true
			if err := serverSession.addLane(old); err != nil {
				t.Fatal(err)
			}
			old.admitted = time.Now().Add(-tc.age)
			replacement := isolationLane(t, 1)
			replacement.control = true
			if err := serverSession.addLane(replacement); err != nil {
				t.Fatalf("control JOIN refused: %v", err)
			}
			if old.closed.Load() {
				t.Fatal("admission alone retired the previous control lane")
			}
			serverSession.commitControlTransfer(replacement)
			if got := old.closed.Load(); got != tc.wantRetired {
				t.Fatalf("previous control lane retired = %v, want %v", got, tc.wantRetired)
			}
			if replacement.closed.Load() {
				t.Fatal("the replacement control lane was closed")
			}
		})
	}
}

// pipeQUICStream is a QUIC stream over an in-memory pipe, so the JOIN
// handler treats the lane as QUIC without a real connection.
type pipeQUICStream struct{ net.Conn }

func (p pipeQUICStream) CancelRead(quic.StreamErrorCode) {}

// Design §10.3 step 1 against design review round 5: the control role moves
// only when the replacement's OPEN_OK has been written. An OPEN_OK that
// cannot be delivered while the old holder is still healthy rolls the JOIN
// back and leaves the old lane carrying the flow; one that is delivered
// retires it. The same holds with a full lane table, where admission has to
// displace a lane -- and prefers the control holder -- to make room.
func TestAControlTransferIsCommittedOnlyAfterItsOpenOK(t *testing.T) {
	owner := identity.Principal{ProviderID: "provider", AccountID: "account", DeviceID: "owner"}
	for _, tc := range []struct{ delivered, full, named bool }{
		{false, false, false}, {true, false, false}, {false, true, false}, {true, true, false},
		// A wire-2 rescue can also name the holder with REPLACE_LANE.
		{false, false, true}, {true, false, true},
	} {
		delivered := tc.delivered
		t.Run(fmt.Sprintf("open-ok-delivered=%v/full-table=%v/replace-lane=%v", delivered, tc.full, tc.named), func(t *testing.T) {
			server, _ := pendingTestServer()
			flow := newIsolationTestFlow(t, true)
			serverSession := newServerFlow(flow, owner, TransportQUIC, 8)
			serverSession.retireControlOnJoin = true
			old := isolationLane(t, 0)
			old.control = true
			if err := serverSession.addLane(old); err != nil {
				t.Fatal(err)
			}
			old.admitted = time.Now().Add(-2 * laneDeadPathDetection)
			lanesBefore := 1
			if tc.full {
				for id := uint64(10); flow.laneCount() < serverSession.maxLanes; id++ {
					bulk := isolationLane(t, id)
					if err := serverSession.addLane(bulk); err != nil {
						t.Fatal(err)
					}
					bulk.admitted = old.admitted
				}
				lanesBefore = flow.laneCount()
			}
			if !server.registerSession(flow.sessionID, serverSession) {
				t.Fatal("register failed")
			}
			local, remote := net.Pipe()
			t.Cleanup(func() { _ = local.Close(); _ = remote.Close() })
			lane := &quicStreamConn{stream: pipeQUICStream{local}}
			request := protocol.Frame{Header: protocol.Header{
				Version: protocol.Version, Type: protocol.TypeJoin, Flags: protocol.FlagReserveControl,
				SessionID: flow.sessionID, FlowID: flow.flowID, Class: protocol.ClassBulk,
			}}
			answered := make(chan protocol.Frame, 1)
			if delivered {
				go func() {
					frame, err := newFrameConn(remote).Read()
					if err == nil {
						answered <- frame
					}
				}()
			} else {
				// The replacement's transport is gone: its OPEN_OK write fails.
				_ = remote.Close()
			}
			done := make(chan struct{})
			go func() {
				defer close(done)
				if tc.named {
					request.Header.Flags |= protocol.FlagReplaceLane
					server.handleLaneJoinOpenWithReplacement(context.Background(), lane, newFrameConn(lane), owner, flow.sessionID, 1, old.id, true, request)
					return
				}
				server.handleLaneJoinOpen(context.Background(), lane, newFrameConn(lane), owner, flow.sessionID, 1, request)
			}()
			if !delivered {
				select {
				case <-done:
				case <-time.After(3 * time.Second):
					t.Fatal("the JOIN handler did not give up on an undeliverable OPEN_OK")
				}
				if old.closed.Load() {
					t.Fatal("a JOIN whose OPEN_OK failed retired the healthy control lane")
				}
				if got := flow.laneCount(); got != lanesBefore {
					t.Fatalf("flow holds %d lanes after the failed JOIN, want the %d it had", got, lanesBefore)
				}
				return
			}
			select {
			case frame := <-answered:
				if frame.Header.Type != protocol.TypeOpenOK {
					t.Fatalf("JOIN answered type %d", frame.Header.Type)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("JOIN not answered")
			}
			for deadline := time.Now().Add(2 * time.Second); !old.closed.Load(); {
				if time.Now().After(deadline) {
					t.Fatal("the committed control transfer kept the previous holder")
				}
				time.Sleep(time.Millisecond)
			}
			if got := flow.laneCount(); got != lanesBefore {
				t.Fatalf("flow holds %d lanes after the transfer, want %d", got, lanesBefore)
			}
			_ = local.Close()
			<-done
		})
	}
}

// A replacement's lane can die before the client has sent anything on it.
// If its OPEN_OK was lost, the client holds only the token it presented; if
// it was read, it holds the new grant. Either retry must take the relay and
// the slots over; at MaxSessions=1 anything else is a FLOW_LIMIT refusal by
// the gateway's own stranded reservation.
func TestALostResumeGrantCanBeRetriedWithTheOldToken(t *testing.T) {
	t.Run("grant-lost", func(t *testing.T) { testResumeRetry(t, false) })
	t.Run("grant-read", func(t *testing.T) { testResumeRetry(t, true) })
}

func testResumeRetry(t *testing.T, grantRead bool) {
	serverCredentials, clientCredentials := testCertificate(t)
	target, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_, remote := admissionGateway(t, ctx, TransportQUIC, ServerConfig{
		Credentials: serverCredentials, MaxSessions: 1, GatewayAdmission: GatewayAdmissionOwned,
	})
	carrier, err := dialRawCarrier(ctx, t, TransportQUIC, remote, clientCredentials, protocol.Version)
	if err != nil {
		t.Fatal(err)
	}
	response, fc, _ := openRawUDPAssociation(ctx, t, carrier, [16]byte{0x63}, 1, nil)
	_, token, ok := session.DecodeUDPResumeGrant(response.Payload)
	if response.Header.Type != protocol.TypeOpenOK || !ok {
		t.Fatalf("association answered %+v", response.Header)
	}
	before := udpRelaySource(t, fc, [16]byte{0x63}, 1, 0, target)
	// The replacement's lane dies before its OPEN_OK is read.
	payload, err := session.EncodeUDPResumeOpen(token[:])
	if err != nil {
		t.Fatal(err)
	}
	lost, outer, err := carrier.lane(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := lost.Write(protocol.Frame{Header: protocol.Header{
		Version: protocol.Version, Type: protocol.TypeOpen, SessionID: [16]byte{0x64}, FlowID: 2, Class: protocol.ClassInteractive,
	}, Payload: payload}); err != nil {
		t.Fatal(err)
	}
	retryToken := token
	if grantRead {
		_ = outer.SetDeadline(time.Now().Add(3 * time.Second))
		answer, err := lost.Read()
		if err != nil || answer.Header.Type != protocol.TypeOpenOK {
			t.Fatalf("replacement answered %+v, %v", answer.Header, err)
		}
		_, retryToken, _ = session.DecodeUDPResumeGrant(answer.Payload)
	} else {
		time.Sleep(100 * time.Millisecond)
	}
	_ = outer.Close()
	// The retry presents the token the client has.
	for attempt := 0; ; attempt++ {
		response, fc, _ = openRawUDPAssociation(ctx, t, carrier, [16]byte{0x65}, 3, retryToken[:])
		if response.Header.Type == protocol.TypeOpenOK {
			break
		}
		if attempt == 10 {
			t.Fatalf("retry answered %+v %q", response.Header, response.Payload)
		}
		time.Sleep(100 * time.Millisecond)
	}
	resumed, _, _ := session.DecodeUDPResumeGrant(response.Payload)
	if !resumed {
		t.Fatal("the retry got a fresh relay")
	}
	if after := udpRelaySource(t, fc, [16]byte{0x65}, 3, 0, target); after != before {
		t.Fatalf("relay source moved from %s to %s", before, after)
	}
}

// Until the client has shown it holds a replacement's grant, the relay
// answers to both the token the replacement was opened with and its own
// grant; taking it under either takes it under both. A token reused by
// another retention drops the relay it named, with its lease, instead of
// overwriting it.
func TestAnUnconfirmedReplacementIsParkedUnderBothTokens(t *testing.T) {
	newToken := func() [session.UDPResumeTokenSize]byte {
		token, err := newUDPResumeToken()
		if err != nil {
			t.Fatal(err)
		}
		return token
	}
	newRelay := func() *net.UDPConn {
		relay, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = relay.Close() })
		return relay
	}
	countedLease := func() (*flowLease, *atomic.Int32) {
		lease := &flowLease{}
		var released atomic.Int32
		lease.add(func() { released.Add(1) })
		return lease, &released
	}
	for _, viaOld := range []bool{true, false} {
		store := newUDPRelayStore()
		old, grant := newToken(), newToken()
		lease, released := countedLease()
		entry := store.publishLive(grant, udpTestPrincipal, func() {})
		store.aliasLive(entry, old[:])
		if end := store.endLive(grant, entry, newRelay(), lease, true); end != liveParked {
			t.Fatalf("owner end = %v, want parked", end)
		}
		lease.release()
		first, second := grant, old
		if viaOld {
			first, second = old, grant
		}
		got, moved := store.transfer(context.Background(), first[:], udpTestPrincipal, time.Second)
		if got == nil {
			t.Fatalf("viaOld=%v: the relay was not reclaimed", viaOld)
		}
		if again, _ := store.transfer(context.Background(), second[:], udpTestPrincipal, time.Second); again != nil || store.retained() != 0 {
			t.Fatalf("viaOld=%v: the relay was reclaimed twice (%d left)", viaOld, store.retained())
		}
		moved.release()
		if released.Load() != 1 {
			t.Fatalf("viaOld=%v: lease released %d times", viaOld, released.Load())
		}
	}

	store := newUDPRelayStore()
	token := newToken()
	firstLease, firstReleased := countedLease()
	store.mu.Lock()
	store.retainLocked(token, udpTestPrincipal, newRelay(), firstLease, nil)
	store.retainLocked(token, udpTestPrincipal, newRelay(), &flowLease{}, nil)
	store.mu.Unlock()
	if firstReleased.Load() != 1 || store.retained() != 1 {
		t.Fatalf("a colliding retention released the first lease %d times, %d relays held", firstReleased.Load(), store.retained())
	}
	store.closeAll()
}

// A fresh association whose OPEN_OK may never have been read keeps its relay
// reclaimable but not its slots: the client's retry carries no token and must
// not be refused by them. And a relay parked under two tokens is one socket
// against the store's bound.
func TestAnUnconfirmedFreshAssociationGivesItsSlotsBack(t *testing.T) {
	store := newUDPRelayStoreWith(time.Hour, 1)
	token, err := newUDPResumeToken()
	if err != nil {
		t.Fatal(err)
	}
	relay, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer relay.Close()
	lease := &flowLease{}
	var released atomic.Int32
	lease.add(func() { released.Add(1) })
	entry := store.publishLive(token, udpTestPrincipal, func() {})
	if end := store.endLive(token, entry, relay, lease, true); end != liveParked {
		t.Fatalf("owner end = %v, want parked", end)
	}
	// Released by the time the relay is visible, not later by the caller.
	if released.Load() != 1 {
		t.Fatalf("a fresh unconfirmed association kept its lease (%d releases)", released.Load())
	}
	lease.release()
	if released.Load() != 1 {
		t.Fatalf("lease released %d times", released.Load())
	}
	if store.claim(token[:], udpTestPrincipal) == nil {
		t.Fatal("its relay is no longer reclaimable")
	}

	// One relay under two tokens fits a store bounded at one.
	old, grant := token, token
	old[0] ^= 1
	second, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	entry = store.publishLive(grant, udpTestPrincipal, func() {})
	store.aliasLive(entry, old[:])
	if end := store.endLive(grant, entry, second, &flowLease{}, true); end != liveParked {
		t.Fatalf("aliased owner end = %v, want parked", end)
	}
	if store.retained() != 1 {
		t.Fatalf("retained %d relays, want 1", store.retained())
	}
	store.closeAll()
	if store.retained() != 0 {
		t.Fatalf("retained %d relays after closeAll", store.retained())
	}
}

// A standby takes over the general slot of whichever of its principal's
// connections closes first, even when others remain, so the principal can
// have a standby again for its next recovery and the bound is never
// exceeded by more than one standby per principal.
func TestAStandbyInheritsTheSlotOfAnyClosingConnection(t *testing.T) {
	credentials, _ := testCertificate(t)
	server, err := NewServer(ServerConfig{ListenAddr: "127.0.0.1:0", Credentials: credentials, MaxSessions: 2})
	if err != nil {
		t.Fatal(err)
	}
	const key = "device"
	general := func() *connectionTicket {
		if !server.admitConnection() {
			t.Fatal("general slot refused")
		}
		server.connMu.Lock()
		p := server.principalConns[key]
		if p == nil {
			p = &principalConnections{}
			server.principalConns[key] = p
		}
		p.general++
		server.connMu.Unlock()
		return &connectionTicket{principal: key}
	}
	first, second := general(), general()
	standby := &connectionTicket{principal: key, standby: true}
	server.principalConns[key].standby = standby
	server.releaseQUICConnection(first)
	if server.isStandby(standby) || len(server.connections) != 2 || server.principalConns[key].standby != nil || server.principalConns[key].general != 2 {
		t.Fatalf("after one of two connections closed: standby=%v slots=%d general=%d", server.isStandby(standby), len(server.connections), server.principalConns[key].general)
	}
	server.releaseQUICConnection(second)
	server.releaseQUICConnection(standby)
	if len(server.connections) != 0 || len(server.principalConns) != 0 {
		t.Fatalf("%d slots and %d principals left after every connection closed", len(server.connections), len(server.principalConns))
	}
}

// A replacement arriving after a parked relay expired, but before the sweep,
// releases the slots that relay kept before it is admitted on its own.
func TestAnExpiredParkedRelayReleasesItsLeaseToTheReplacement(t *testing.T) {
	store := newUDPRelayStoreWith(time.Hour, udpRelaysRetained)
	token, err := newUDPResumeToken()
	if err != nil {
		t.Fatal(err)
	}
	relay, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	lease := &flowLease{}
	var released atomic.Int32
	lease.add(func() { released.Add(1) })
	entry := store.publishLive(token, udpTestPrincipal, func() {})
	store.confirmLive(entry)
	if end := store.endLive(token, entry, relay, lease, true); end != liveParked {
		t.Fatalf("owner end = %v, want parked", end)
	}
	store.mu.Lock()
	held := store.relays[token]
	held.expires = time.Now().Add(-time.Second)
	store.relays[token] = held
	store.mu.Unlock()
	if got, _ := store.transfer(context.Background(), token[:], udpTestPrincipal, time.Second); got != nil {
		t.Fatal("an expired relay was handed back")
	}
	if released.Load() != 1 || store.retained() != 0 {
		t.Fatalf("lease released %d times, %d relays left", released.Load(), store.retained())
	}
}

// A completed flow's replay record holds no session slot in owned admission:
// with MaxSessions=1 the next flow is admitted at once, not after the
// tombstone's linger. The records have their own bound, past which a
// completed session is not kept. Legacy admission keeps today's accounting.
func TestCompletedSessionsDoNotBlockOwnedAdmission(t *testing.T) {
	credentials, _ := testCertificate(t)
	for _, mode := range []GatewayAdmission{GatewayAdmissionLegacy, GatewayAdmissionOwned} {
		t.Run(string(mode), func(t *testing.T) {
			server, err := NewServer(ServerConfig{ListenAddr: "127.0.0.1:0", Credentials: credentials, MaxSessions: 1, GatewayAdmission: mode})
			if err != nil {
				t.Fatal(err)
			}
			session := func(id byte) (*serverFlow, [16]byte) {
				flow := newIsolationTestFlow(t, false)
				return newServerFlow(flow, identity.Principal{}, TransportQUIC, 8), [16]byte{0x70, id}
			}
			first, firstID := session(1)
			if !server.registerSession(firstID, first) {
				t.Fatal("first session refused")
			}
			server.retainCompletedSession(firstID, first)
			second, secondID := session(2)
			admitted := server.registerSession(secondID, second)
			if mode == GatewayAdmissionLegacy {
				if admitted {
					t.Fatal("legacy admission changed: a tombstone no longer counts")
				}
				return
			}
			if !admitted {
				t.Fatal("a completed flow's tombstone blocked the next flow")
			}
			if server.lookupSession(firstID) == nil {
				t.Fatal("the tombstone within its bound was not kept")
			}
			// Past the tombstone bound a completed session is not kept, and
			// is gone before its flow's slot can be reused.
			server.retainCompletedSession(secondID, second)
			if server.lookupSession(secondID) != nil {
				t.Fatal("a tombstone past its bound was kept")
			}
			third, thirdID := session(3)
			if !server.registerSession(thirdID, third) {
				t.Fatal("the next flow was refused by a record past the bound")
			}
		})
	}
}

// An association that ends for good -- its relay failed, or it was closed --
// while a replacement waits to take it over does not hand the replacement a
// broken relay: the replacement is woken with nothing and opens a fresh one.
func TestAFailedRelayIsNotHandedToAWaitingReplacement(t *testing.T) {
	store := newUDPRelayStore()
	token, err := newUDPResumeToken()
	if err != nil {
		t.Fatal(err)
	}
	relay, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer relay.Close()
	stopped := make(chan struct{})
	var stopOnce sync.Once
	entry := store.publishLive(token, udpTestPrincipal, func() { stopOnce.Do(func() { close(stopped) }) })
	store.confirmLive(entry)
	ended := make(chan liveEnd, 1)
	lease := &flowLease{}
	var released atomic.Int32
	lease.add(func() { released.Add(1) })
	go func() {
		<-stopped
		// The relay failed while the replacement waited: no retention.
		ended <- store.endLive(token, entry, relay, lease, false)
	}()
	started := time.Now()
	got, moved := store.transfer(context.Background(), token[:], udpTestPrincipal, 5*time.Second)
	if got != nil || moved != nil {
		t.Fatal("a failed relay was handed to the replacement")
	}
	// The replacement opens afresh against the slots the failed association
	// held, so they are back before it is woken.
	if released.Load() != 1 {
		t.Fatalf("the replacement was woken with the failed association's lease still held (%d releases)", released.Load())
	}
	lease.release()
	if released.Load() != 1 {
		t.Fatalf("lease released %d times", released.Load())
	}
	if waited := time.Since(started); waited > 2*time.Second {
		t.Fatalf("the replacement waited %v instead of being woken", waited)
	}
	if end := <-ended; end != liveClosed {
		t.Fatalf("owner end = %v, want closed", end)
	}
}

// The effective configuration names the stage 5 switch beside the others.
func TestBundleNamesGatewayAdmission(t *testing.T) {
	if got := Bundle(CCAPIV1, MemoryLegacy, CodedRetryLegacy, GatewayAdmissionLegacy); got != "legacy" {
		t.Fatalf("all-off bundle = %q", got)
	}
	if got := Bundle(CCAPIV2, MemoryOwned, CodedRetrySticky, GatewayAdmissionOwned); got != "cc-api=v2,coded-retry=sticky,mem=owned,gateway-admission=owned" {
		t.Fatalf("bundle = %q", got)
	}
}

// In owned admission a parked relay keeps its association's slots until its
// grace ends, and the sweeper reaches it only some time later. An OPEN that
// finds the limit full takes back the slots of relays already expired rather
// than being refused for them.
func TestAnExpiredParkedRelayDoesNotRefuseANewFlow(t *testing.T) {
	serverCredentials, clientCredentials := testCertificate(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	destination := admissionDestination(t, idleFlows)
	server, remote := admissionGateway(t, ctx, TransportQUIC, ServerConfig{
		Credentials: serverCredentials, MaxSessions: 1, GatewayAdmission: GatewayAdmissionOwned,
	})
	relay, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	token, err := newUDPResumeToken()
	if err != nil {
		t.Fatal(err)
	}
	// A parked relay holding the only session slot, past its grace.
	server.semaphore <- struct{}{}
	lease := &flowLease{}
	lease.add(func() { <-server.semaphore })
	server.udpRelays.mu.Lock()
	server.udpRelays.retainLocked(token, udpTestPrincipal, relay, lease, nil)
	held := server.udpRelays.relays[token]
	held.expires = time.Now().Add(-time.Second)
	server.udpRelays.relays[token] = held
	server.udpRelays.mu.Unlock()
	carrier, err := dialRawCarrier(ctx, t, TransportQUIC, remote, clientCredentials, protocol.Version)
	if err != nil {
		t.Fatal(err)
	}
	response, _, _, err := exchange(ctx, carrier, openFrame(t, [16]byte{0x71}, 9, destination))
	if err != nil || response.Header.Type != protocol.TypeOpenOK {
		t.Fatalf("OPEN answered %+v %q, %v; want OPEN_OK once the expired relay's slot is back", response.Header, response.Payload, err)
	}
}

// deadlineRecorder is a stream that records every deadline set on it.
type deadlineRecorder struct {
	net.Conn
	mu  sync.Mutex
	set []time.Time
}

func (d *deadlineRecorder) SetDeadline(t time.Time) error {
	d.mu.Lock()
	d.set = append(d.set, t)
	d.mu.Unlock()
	return d.Conn.SetDeadline(t)
}

func (d *deadlineRecorder) SetWriteDeadline(t time.Time) error {
	d.mu.Lock()
	d.set = append(d.set, t)
	d.mu.Unlock()
	return d.Conn.SetWriteDeadline(t)
}

// A UDP association's OPEN_OK is written under the stream's existing
// handshake deadline, in every admission mode: nothing re-arms it with a fresh
// timeout before the acknowledgement is on its way.
func TestAUDPOpenAcknowledgementKeepsItsHandshakeDeadline(t *testing.T) {
	credentials, _ := testCertificate(t)
	for _, mode := range []GatewayAdmission{GatewayAdmissionLegacy, GatewayAdmissionOwned} {
		t.Run(string(mode), func(t *testing.T) {
			server, err := NewServer(ServerConfig{
				ListenAddr: "127.0.0.1:0", Credentials: credentials, GatewayAdmission: mode,
				DestinationPolicy: DestinationPolicy{AllowPrivate: true},
				Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
			})
			if err != nil {
				t.Fatal(err)
			}
			local, remote := net.Pipe()
			defer remote.Close()
			conn := &deadlineRecorder{Conn: local}
			fc := newFrameConn(conn)
			done := make(chan struct{})
			go func() {
				defer close(done)
				server.handleUDPAssociation(context.Background(), conn, fc, udpTestPrincipal, [16]byte{0x72}, 1, nil, true, &flowLease{}, nil)
			}()
			answer, err := newFrameConn(remote).Read()
			if err != nil || answer.Header.Type != protocol.TypeOpenOK {
				t.Fatalf("association answered %+v, %v", answer.Header, err)
			}
			conn.mu.Lock()
			for _, deadline := range conn.set {
				if !deadline.IsZero() {
					conn.mu.Unlock()
					t.Fatalf("a deadline of %v was armed before the acknowledgement", time.Until(deadline))
				}
			}
			conn.mu.Unlock()
			_ = remote.Close()
			<-done
		})
	}
}

// A parked relay holds its device's place among the account's clients as
// well as a flow. Once it has expired, a new device of a MaxClients=1 account
// is admitted rather than refused for it, as for MaxFlows.
func TestAnExpiredParkedRelayFreesItsAccountCapacity(t *testing.T) {
	for _, tc := range []struct {
		name   string
		limits identity.AccountLimits
	}{
		{"flow-limit", identity.AccountLimits{MaxFlows: 1}},
		{"client-limit", identity.AccountLimits{MaxClients: 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server, principals := admissionFixture(t, tc.limits, 2)
			server.cfg.GatewayAdmission = GatewayAdmissionOwned
			server.udpRelays = newUDPRelayStore()
			old, fresh := principals[0], principals[1]
			if refusal := server.admitAccountFlow(old); refusal != nil {
				t.Fatal(refusal)
			}
			lease := &flowLease{}
			lease.add(func() { server.releaseAccountFlow(old) })
			relay, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
			if err != nil {
				t.Fatal(err)
			}
			token, err := newUDPResumeToken()
			if err != nil {
				t.Fatal(err)
			}
			server.udpRelays.mu.Lock()
			server.udpRelays.retainLocked(token, old, relay, lease, nil)
			held := server.udpRelays.relays[token]
			held.expires = time.Now().Add(-time.Second)
			server.udpRelays.relays[token] = held
			server.udpRelays.mu.Unlock()
			if refusal := server.admitAccountFlowReclaiming(fresh); refusal != nil {
				t.Fatalf("a new device was refused for an expired relay: %v", refusal)
			}
		})
	}
}

// A UDP association's OPEN_OK that cannot be written is interrupted when the
// association's context ends, in every mode and whether or not it is
// resumable, instead of holding shutdown until the handshake deadline.
func TestAStalledUDPOpenAcknowledgementEndsWithItsContext(t *testing.T) {
	credentials, _ := testCertificate(t)
	for _, mode := range []GatewayAdmission{GatewayAdmissionLegacy, GatewayAdmissionOwned} {
		for _, resumable := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/resumable=%v", mode, resumable), func(t *testing.T) {
				server, err := NewServer(ServerConfig{
					ListenAddr: "127.0.0.1:0", Credentials: credentials, GatewayAdmission: mode,
					DestinationPolicy: DestinationPolicy{AllowPrivate: true},
					Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
				})
				if err != nil {
					t.Fatal(err)
				}
				local, remote := net.Pipe()
				defer remote.Close()
				defer local.Close()
				// The peer never reads; the stream's handshake deadline is far.
				_ = local.SetDeadline(time.Now().Add(time.Minute))
				ctx, cancel := context.WithCancel(context.Background())
				done := make(chan struct{})
				go func() {
					defer close(done)
					server.handleUDPAssociation(ctx, local, newFrameConn(local), udpTestPrincipal, [16]byte{0x73}, 1, nil, resumable, &flowLease{}, nil)
				}()
				time.Sleep(50 * time.Millisecond)
				cancel()
				select {
				case <-done:
				case <-time.After(3 * time.Second):
					t.Fatal("a stalled OPEN_OK outlived its association's context")
				}
			})
		}
	}
}
