package pep

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bojieli/queqiao/internal/identity"
	"github.com/bojieli/queqiao/internal/metrics"
	"github.com/bojieli/queqiao/internal/protocol"
	"github.com/bojieli/queqiao/internal/session"
)

// wireAudit records every frame a lane accepted while a test runs, and what a
// version-1 peer would have made of each one.
//
// A v0.12 client cannot be asked after the fact what it received. What it
// can be held to is the contract it was built against: version-1 framing, no
// REPLACE_LANE, and only the reset codes that existed then. Anything outside
// that reaching a version-1 lane is a frame an old client would refuse or
// misread.
type wireAudit struct {
	mu         sync.Mutex
	frames     [3]uint64
	joins      [3]uint64
	replaces   [3]uint64
	resets     map[string]int
	violations []string
	// opened is closed when the first OPEN_OK is read: the gateway has
	// admitted a session and the client has its confirmation in hand.
	opened     chan struct{}
	openedOnce sync.Once
}

func (a *wireAudit) observe(wire byte, f protocol.Frame) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if int(wire) < len(a.frames) {
		a.frames[wire]++
		if f.Header.Type == protocol.TypeJoin {
			a.joins[wire]++
			if f.Header.Flags&protocol.FlagReplaceLane != 0 {
				a.replaces[wire]++
			}
		}
	}
	if f.Header.Type == protocol.TypeOpenOK {
		a.openedOnce.Do(func() { close(a.opened) })
	}
	if f.Header.Type == protocol.TypeReset && len(f.Payload) > 0 {
		if a.resets == nil {
			a.resets = make(map[string]int)
		}
		a.resets[fmt.Sprintf("v%d/%d/%s", wire, f.Payload[0], f.Payload[1:])]++
	}
	if wire != 1 {
		return
	}
	switch {
	case f.Header.Version != 1:
		a.violations = append(a.violations, fmt.Sprintf("version %d frame on a version-1 lane", f.Header.Version))
	case f.Header.Flags&protocol.FlagReplaceLane != 0:
		a.violations = append(a.violations, "REPLACE_LANE on a version-1 lane")
	case f.Header.Type == protocol.TypeReset && (len(f.Payload) == 0 || session.ResetCode(f.Payload[0]) < session.ResetProtocol || session.ResetCode(f.Payload[0]) > session.ResetTransport):
		a.violations = append(a.violations, fmt.Sprintf("reset code %v on a version-1 lane", f.Payload))
	}
}

func (a *wireAudit) snapshot() (frames, joins, replaces [3]uint64, violations []string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.frames, a.joins, a.replaces, append([]string(nil), a.violations...)
}

// auditWire installs a frame audit for the rest of the test. With pin set, it
// also makes every client built in the test offer only that version and every
// lane on a transport that negotiates nothing (a pipe) speak it.
func auditWire(t *testing.T, pin byte) *wireAudit {
	t.Helper()
	audit := &wireAudit{opened: make(chan struct{})}
	observe := audit.observe
	testFrameObserver.Store(&observe)
	if pin != 0 {
		wires := []byte{pin}
		testClientWires.Store(&wires)
		testFrameWire.Store(&pin)
	}
	t.Cleanup(func() {
		testFrameObserver.Store(nil)
		testClientWires.Store(nil)
		testFrameWire.Store(nil)
	})
	return audit
}

// waitOpened blocks until an OPEN_OK has been read, or fails the test after
// bound.
func (a *wireAudit) waitOpened(t *testing.T, bound time.Duration) {
	t.Helper()
	select {
	case <-a.opened:
	case <-time.After(bound):
		t.Fatalf("no OPEN_OK within %v", bound)
	}
}

func (a *wireAudit) requireClean(t *testing.T, wire byte) {
	t.Helper()
	frames, _, _, violations := a.snapshot()
	if len(violations) > 0 {
		t.Fatalf("frames a version-1 peer would refuse: %v", violations)
	}
	if frames[wire] == 0 {
		t.Fatalf("no frame was read on a version-%d lane; the scenario did not run on that wire", wire)
	}
}

// TestWire1ClientAgainstTheDualGateway replays the end-to-end scenarios that
// matter to a deployed v0.12 client with every client offering only version
// 1, against a gateway that accepts both. Each one is an existing test, run
// unchanged: the point is that the behaviour they pin is the same on the old
// wire, and that nothing the gateway sends on it is outside version 1.
//
// They cover a flow's OPEN and DATA on both carriers, UDP associations and
// their resume, a lane lost and replaced, QUIC failing over to a TCP handoff,
// the provider and pool probes, a tombstone replay, the gateway resetting a
// failed flow on every lane (#163), and a JOIN racing its OPEN (#164).
func TestWire1ClientAgainstTheDualGateway(t *testing.T) {
	scenarios := []struct {
		name string
		run  func(*testing.T)
	}{
		{"tls-open-data", TestTLSOneLaneSOCKSEndToEnd},
		{"quic-open-data", TestQUICOneLaneSOCKSEndToEnd},
		{"udp-associate-tcp", TestUDPAssociateSOCKSEndToEnd},
		{"udp-associate-quic", TestUDPAssociateQUICSOCKSEndToEnd},
		{"udp-resume-keeps-source", TestARescuedUDPAssociationKeepsItsRemoteSourceAddress},
		{"udp-rescue-to-tcp", TestUDPAssociationRescuesToTCP},
		{"quic-lane-failure", TestQUICFlowSurvivesOneLaneFailure},
		{"tcp-handoff", TestAutoFlowInstallsTCPRescueAfterAllQUICLanesFail},
		{"tcp-striping", TestTCPFallbackStripesOneFlowAcrossFourLanes},
		{"tombstone-replay", TestCompletedFlowTombstoneReplaysFinalAck},
		{"provider-probe", TestClientProbeAuthenticatesProviderWithoutOpeningFlow},
		{"pool-recovery", TestPooledFlowsRecoverThroughOneReplacementGeneration},
		{"bulk-pool-probe", TestBulkExpiredProofUsesRealProbeAndHotReuseSkipsProbe},
		{"gateway-reset-163", TestFailingGatewayFlowResetsEveryOpenLane},
		{"client-ends-on-reset-163", TestClientEndsFlowOnGatewayResetWithoutLaneFailure},
		{"staged-lane-reset-163", TestStagedLaneGetsAPermanentReset},
		{"join-waits-for-open-164", TestJoinDuringDestinationDialWaitsForTheSession},
		{"join-slow-dial-164", TestJoinOnSlowDialGetsTransientRefusal},
		{"join-failed-open-164", TestJoinOnFailedOpenGetsDestinationRefusal},
		{"open-lane-lost-164", TestOpenLaneLostDuringDialIsRescuedByJoin},
		{"join-retry-pending-164", TestJoinRetriedAfterSessionPendingFindsTheFlow},
		{"ended-session-164", TestEndedSessionIsNotNeverHeld},
		{"join-refusal-codes", TestLaneJoinRefusalCodesMatchPermanence},
	}
	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			audit := auditWire(t, 1)
			scenario.run(t)
			audit.requireClean(t, 1)
		})
	}
}

// freezeRelay is a loopback middlebox that can black-hole every connection it
// is carrying while still forwarding new ones: the shape of a stalled path,
// where the lanes a flow has stop moving and a fresh dial gets through.
type freezeRelay struct {
	t        *testing.T
	upstream string
	frozen   atomic.Uint64 // generation: connections older than this are dead
	nextGen  atomic.Uint64
	closed   chan struct{}
	wg       sync.WaitGroup
}

func newFreezeRelay(t *testing.T, upstream string) *freezeRelay {
	r := &freezeRelay{t: t, upstream: upstream, closed: make(chan struct{})}
	r.nextGen.Store(1)
	t.Cleanup(func() { close(r.closed); r.wg.Wait() })
	return r
}

// freeze black-holes every connection the relay holds now.
func (r *freezeRelay) freeze() { r.frozen.Store(r.nextGen.Add(1) - 1) }

func (r *freezeRelay) dead(gen uint64) bool { return gen <= r.frozen.Load() }

func (r *freezeRelay) serveTCP(listener net.Listener) {
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		go func() { <-r.closed; _ = listener.Close() }()
		for {
			down, err := listener.Accept()
			if err != nil {
				return
			}
			up, err := net.Dial("tcp", r.upstream)
			if err != nil {
				_ = down.Close()
				continue
			}
			gen := r.nextGen.Load()
			pipe := func(dst, src net.Conn) {
				buf := make([]byte, 32*1024)
				for {
					n, err := src.Read(buf)
					if r.dead(gen) {
						// Stop reading: the sender's window fills and the
						// lane goes silent, as on a black-holed path.
						<-r.closed
						_ = dst.Close()
						_ = src.Close()
						return
					}
					if n > 0 {
						if _, werr := dst.Write(buf[:n]); werr != nil {
							return
						}
					}
					if err != nil {
						_ = dst.Close()
						return
					}
				}
			}
			go pipe(up, down)
			go pipe(down, up)
		}
	}()
}

func (r *freezeRelay) serveUDP(conn net.PacketConn) {
	type mapping struct {
		up  *net.UDPConn
		gen uint64
	}
	var mu sync.Mutex
	mappings := make(map[string]*mapping)
	upstream, err := net.ResolveUDPAddr("udp", r.upstream)
	if err != nil {
		r.t.Fatal(err)
	}
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		go func() { <-r.closed; _ = conn.Close() }()
		buf := make([]byte, 64*1024)
		for {
			n, from, err := conn.ReadFrom(buf)
			if err != nil {
				mu.Lock()
				for _, m := range mappings {
					_ = m.up.Close()
				}
				mu.Unlock()
				return
			}
			mu.Lock()
			m := mappings[from.String()]
			if m == nil {
				up, err := net.DialUDP("udp", nil, upstream)
				if err != nil {
					mu.Unlock()
					continue
				}
				m = &mapping{up: up, gen: r.nextGen.Load()}
				mappings[from.String()] = m
				client := from
				go func() {
					back := make([]byte, 64*1024)
					for {
						n, err := up.Read(back)
						if err != nil {
							return
						}
						if !r.dead(m.gen) {
							_, _ = conn.WriteTo(back[:n], client)
						}
					}
				}()
			}
			mu.Unlock()
			if !r.dead(m.gen) {
				_, _ = m.up.Write(buf[:n])
			}
		}
	}()
}

// streamDestination sends size bytes to every connection and closes it.
func streamDestination(t *testing.T, size int) string {
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
				chunk := make([]byte, 64*1024)
				for i := range chunk {
					chunk[i] = byte(i)
				}
				for sent := 0; sent < size; sent += len(chunk) {
					if _, err := conn.Write(chunk[:min(len(chunk), size-sent)]); err != nil {
						return
					}
				}
			}()
		}
	}()
	return listener.Addr().String()
}

// countingSink reads each connection to its end and answers with the number
// of bytes it received.
func countingSink(t *testing.T) string {
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
				n, _ := io.Copy(io.Discard, conn)
				var count [8]byte
				binary.BigEndian.PutUint64(count[:], uint64(n))
				_, _ = conn.Write(count[:])
			}()
		}
	}()
	return listener.Addr().String()
}

// TestStallRescueAcrossWireVersions forces a stall on a running transfer and
// checks that the rescue keeps the flow on its version: a version-2 flow
// rescues with REPLACE_LANE, a version-1 flow with the plain JOIN a v0.12
// gateway understands, and the dual gateway admits both.
// stallRescueProgressBound is how long a stall-rescue transfer may make no
// progress at all. The rescue is what is being tested, not the transfer's
// speed: one 60 s deadline over the whole 24 MiB failed a macOS Intel runner
// with 99% of an upload delivered (issue #187). A rescue that never comes
// still fails here, at the first chunk that waits this long.
const stallRescueProgressBound = 30 * time.Second

func TestStallRescueAcrossWireVersions(t *testing.T) {
	const size = 24 << 20
	for _, carrier := range []TransportKind{TransportQUIC, TransportTCP} {
		for _, wire := range []byte{1, 2} {
			t.Run(fmt.Sprintf("%s/wire-%d", carrier, wire), func(t *testing.T) {
				audit := auditWire(t, 0)
				serverCredentials, clientCredentials := testCertificate(t)
				logger := slog.New(slog.NewTextHandler(io.Discard, nil))
				if os.Getenv("WIRE_DEBUG") != "" {
					logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
				}
				serverMetrics, clientMetrics := metrics.New(), metrics.New()
				server, err := NewServer(ServerConfig{
					ListenAddr: "127.0.0.1:0", Credentials: serverCredentials, EnableQUIC: carrier == TransportQUIC,
					DestinationPolicy: DestinationPolicy{AllowPrivate: true}, Logger: logger, Metrics: serverMetrics,
				})
				if err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				done := make(chan struct{}, 2)
				relay := (*freezeRelay)(nil)
				var remote string
				if carrier == TransportQUIC {
					gateway, err := net.ListenPacket("udp4", "127.0.0.1:0")
					if err != nil {
						t.Fatal(err)
					}
					go func() { _ = server.ServePacketConn(ctx, gateway); done <- struct{}{} }()
					front, err := net.ListenPacket("udp4", "127.0.0.1:0")
					if err != nil {
						t.Fatal(err)
					}
					relay = newFreezeRelay(t, gateway.LocalAddr().String())
					relay.serveUDP(front)
					remote = front.LocalAddr().String()
				} else {
					gateway, err := net.Listen("tcp4", "127.0.0.1:0")
					if err != nil {
						t.Fatal(err)
					}
					go func() { _ = server.ServeListener(ctx, gateway); done <- struct{}{} }()
					front, err := net.Listen("tcp4", "127.0.0.1:0")
					if err != nil {
						t.Fatal(err)
					}
					relay = newFreezeRelay(t, gateway.Addr().String())
					relay.serveTCP(front)
					remote = front.Addr().String()
				}
				clientListener, err := net.Listen("tcp4", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				// One TCP lane: the stall watchdog rescues a lone lane, and a
				// striped bundle frozen whole is a lane-failure case instead.
				client, err := NewClient(ClientConfig{
					ListenAddr: clientListener.Addr().String(), RemoteAddr: remote, Credentials: clientCredentials,
					Transport: carrier, EnableQUICPool: carrier == TransportQUIC, WireVersions: []byte{wire},
					TCPFallbackLanes: 1, Logger: logger, Metrics: clientMetrics,
				})
				if err != nil {
					t.Fatal(err)
				}
				go func() { _ = client.ServeListener(ctx, clientListener); done <- struct{}{} }()
				defer func() { cancel(); <-done; <-done }()

				// A stall is rescued by the side with work it cannot send, and
				// only the client opens lanes: the client must be the sender
				// for its watchdog to fire. A QUIC download is rescued as
				// well, by the pooled connection's liveness check; a TCP lane
				// has no such check, so its scenario is an upload.
				if carrier == TransportQUIC {
					conn := dialTestSOCKS(t, clientListener.Addr().String(), streamDestination(t, size))
					defer conn.Close()
					received := 0
					buf := make([]byte, 64*1024)
					frozen := false
					for {
						_ = conn.SetDeadline(time.Now().Add(stallRescueProgressBound))
						n, err := conn.Read(buf)
						received += n
						if !frozen && received >= 2<<20 {
							audit.waitOpened(t, stallRescueProgressBound)
							relay.freeze()
							frozen = true
						}
						if err == io.EOF {
							break
						}
						if err != nil {
							t.Fatalf("download stopped at %d of %d bytes: %v", received, size, err)
						}
					}
					if received != size {
						t.Fatalf("received %d of %d bytes", received, size)
					}
				} else {
					conn := dialTestSOCKS(t, clientListener.Addr().String(), countingSink(t))
					defer conn.Close()
					chunk := make([]byte, 64*1024)
					for sent := 0; sent < size; sent += len(chunk) {
						if sent >= 2<<20 && sent < 2<<20+len(chunk) {
							// The client accepts upload bytes before the
							// gateway has seen its OPEN, so 2 MiB written
							// says nothing about the session existing. A lane
							// frozen before the OPEN arrives is a flow that
							// was never admitted, which no JOIN can rescue
							// and the client rightly does not reopen (#159):
							// under load that was this test's failure (#202).
							audit.waitOpened(t, stallRescueProgressBound)
							relay.freeze()
						}
						_ = conn.SetDeadline(time.Now().Add(stallRescueProgressBound))
						if _, err := conn.Write(chunk); err != nil {
							t.Fatalf("upload stopped at %d of %d bytes: %v", sent, size, err)
						}
					}
					if err := conn.(*net.TCPConn).CloseWrite(); err != nil {
						t.Fatal(err)
					}
					var count [8]byte
					_ = conn.SetDeadline(time.Now().Add(stallRescueProgressBound))
					if _, err := io.ReadFull(conn, count[:]); err != nil {
						t.Fatalf("upload not confirmed: %v", err)
					}
					if got := binary.BigEndian.Uint64(count[:]); got != size {
						t.Fatalf("destination received %d of %d bytes", got, size)
					}
				}
				_, joins, replaces, violations := audit.snapshot()
				if len(violations) > 0 {
					t.Fatalf("frames a version-1 peer would refuse: %v", violations)
				}
				if joins[wire] == 0 {
					t.Fatalf("the stall was survived without a rescue JOIN on wire %d (joins %v)", wire, joins)
				}
				if wire == 1 && replaces[1] != 0 {
					t.Fatalf("a version-1 flow was rescued with REPLACE_LANE")
				}
				if wire == 2 && replaces[2] == 0 {
					t.Fatalf("a version-2 flow was rescued without REPLACE_LANE (joins %v)", joins)
				}
				if other := 3 - wire; joins[other] != 0 {
					t.Fatalf("a version-%d flow joined a version-%d lane", wire, other)
				}
				s := serverMetrics.Snapshot()
				if s.LaneJoinRefusals[metrics.LaneJoinWireMismatch] != 0 {
					t.Fatal("the gateway refused a rescue as a version mismatch")
				}
				if s.DataConnections[wire-1] == 0 || s.DataConnections[2-wire] != 0 {
					t.Fatalf("gateway connections by wire = %v, want only wire %d", s.DataConnections, wire)
				}
			})
		}
	}
}

// TestAJoinOfTheOtherVersionIsRefusedAndTheFlowSurvives is the gateway's half
// of keeping a flow on one version. A JOIN for a live flow arriving on a lane
// of the other version is refused with a protocol reset every version knows,
// counted under its own reason, and the flow carries on.
func TestAJoinOfTheOtherVersionIsRefusedAndTheFlowSurvives(t *testing.T) {
	for _, flowWire := range []byte{1, 2} {
		t.Run(fmt.Sprintf("flow-wire-%d", flowWire), func(t *testing.T) {
			audit := auditWire(t, 0)
			serverCredentials, clientCredentials := testCertificate(t)
			logger := slog.New(slog.NewTextHandler(io.Discard, nil))
			serverMetrics := metrics.New()
			server, err := NewServer(ServerConfig{
				ListenAddr: "127.0.0.1:0", Credentials: serverCredentials, EnableQUIC: true,
				DestinationPolicy: DestinationPolicy{AllowPrivate: true}, Logger: logger, Metrics: serverMetrics,
			})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			gateway, err := net.ListenPacket("udp4", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan struct{}, 2)
			go func() { _ = server.ServePacketConn(ctx, gateway); done <- struct{}{} }()
			clientListener, err := net.Listen("tcp4", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			client, err := NewClient(ClientConfig{
				ListenAddr: clientListener.Addr().String(), RemoteAddr: gateway.LocalAddr().String(), Credentials: clientCredentials,
				Transport: TransportQUIC, EnableQUICPool: true, WireVersions: []byte{flowWire}, Logger: logger,
			})
			if err != nil {
				t.Fatal(err)
			}
			go func() { _ = client.ServeListener(ctx, clientListener); done <- struct{}{} }()
			defer func() { cancel(); <-done; <-done }()

			echo, err := net.Listen("tcp4", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer echo.Close()
			go echoDestination(echo)
			conn := dialTestSOCKS(t, clientListener.Addr().String(), echo.Addr().String())
			defer conn.Close()
			roundTrip := func(message string) {
				t.Helper()
				_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
				if _, err := conn.Write([]byte(message)); err != nil {
					t.Fatal(err)
				}
				got := make([]byte, len(message))
				if _, err := io.ReadFull(conn, got); err != nil || string(got) != message {
					t.Fatalf("echo %q: %q, %v", message, got, err)
				}
			}
			roundTrip("before")

			server.sessionsMu.RLock()
			var sessionID [16]byte
			var flowID uint64
			for id, flow := range server.sessions {
				sessionID, flowID = id, flow.flow.flowID
				if flow.wireVersion() != flowWire {
					t.Errorf("gateway pinned the flow to %d, want %d", flow.wireVersion(), flowWire)
				}
			}
			server.sessionsMu.RUnlock()
			if flowID == 0 {
				t.Fatal("the gateway holds no session")
			}

			other := 3 - flowWire
			outer, err := dialWireLane(ctx, TransportQUIC, gateway.LocalAddr().String(), clientCredentials, []byte{other})
			if err != nil {
				t.Fatal(err)
			}
			defer outer.Close()
			fc := newFrameConn(outer)
			_ = outer.SetDeadline(time.Now().Add(5 * time.Second))
			var payload [8]byte
			binary.BigEndian.PutUint64(payload[:], 77)
			if err := fc.Write(protocol.Frame{Header: protocol.Header{
				Version: protocol.Version, Type: protocol.TypeJoin, SessionID: sessionID, FlowID: flowID, Class: protocol.ClassBulk,
			}, Payload: payload[:]}); err != nil {
				t.Fatal(err)
			}
			response, err := fc.Read()
			if err != nil {
				t.Fatal(err)
			}
			if response.Header.Type != protocol.TypeReset || resetCode(response.Payload) != session.ResetProtocol ||
				!strings.Contains(string(response.Payload[1:]), wireMismatchAnswer) || response.Header.Version != other {
				t.Fatalf("a v%d JOIN for a v%d flow got %+v %q", other, flowWire, response.Header, response.Payload)
			}
			if got := serverMetrics.Snapshot().LaneJoinRefusals[metrics.LaneJoinWireMismatch]; got != 1 {
				t.Fatalf("wire_mismatch refusals = %d, want 1", got)
			}
			roundTrip("after")
			_, _, _, violations := audit.snapshot()
			if len(violations) > 0 {
				t.Fatalf("frames a version-1 peer would refuse: %v", violations)
			}
		})
	}
}

// TestPinnedFlowDialsOnlyItsVersion is the client's half. A dial for an
// existing flow offers only the flow's version, so even a gateway that would
// prefer another one answers in the flow's; and a pooled connection that
// negotiated another version before the flow pinned itself is not lent to
// it.
func TestPinnedFlowDialsOnlyItsVersion(t *testing.T) {
	serverCredentials, clientCredentials := testCertificate(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	remote := wireGateway(t, ctx, TransportQUIC, nil, serverCredentials, metrics.New())
	client, err := NewClient(ClientConfig{
		ListenAddr: "127.0.0.1:0", RemoteAddr: remote, Credentials: clientCredentials,
		Transport: TransportQUIC, EnableQUICPool: true,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.closeQUICPool()
	pinned := withPinnedWire(ctx, 1)
	lane, err := client.dialJoinLane(pinned, TransportQUIC, [16]byte{1}, 3)
	if err != nil {
		t.Fatal(err)
	}
	if lane.fc.wire != 1 {
		t.Fatalf("a dedicated lane for a version-1 flow speaks %d", lane.fc.wire)
	}
	_ = lane.fc.Close()
	// The control pool is shared by every flow and negotiates the newest
	// version; a version-1 flow must not borrow it.
	pooled, err := client.dialLaneMode(ctx, TransportQUIC, [16]byte{2}, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	if pooled.fc.wire != 2 {
		t.Fatalf("the unpinned pool speaks %d, want 2", pooled.fc.wire)
	}
	_ = pooled.fc.Close()
	if _, err := client.dialLaneMode(pinned, TransportQUIC, [16]byte{3}, 4, true); err == nil || !strings.Contains(err.Error(), errPinnedWireMismatch.Error()) {
		t.Fatalf("a version-1 flow borrowed a version-2 pooled lane: %v", err)
	}

	// The bulk pool lends a connection only to flows of its version, and a
	// pinned flow's dial stays inside the pool's budget: with the one slot
	// held by an idle connection of the other version, that connection is
	// retired to make room rather than the budget being exceeded.
	client.memoryLimits.maxBulkConnections = 1
	v2, err := client.reserveBulkConn(withPinnedWire(ctx, 2))
	if err != nil {
		t.Fatal(err)
	}
	if v2.wire() != 2 {
		t.Fatalf("a bulk connection for a version-2 flow speaks %d", v2.wire())
	}
	client.releaseBulkConn(v2, false)
	v1, err := client.reserveBulkConn(pinned)
	if err != nil {
		t.Fatal(err)
	}
	if v1 == v2 || v1.wire() != 1 {
		t.Fatalf("a version-1 flow got bulk connection %p speaking %d (the version-2 one is %p)", v1, v1.wire(), v2)
	}
	if n := client.bulkConnCount(); n != 1 {
		t.Fatalf("bulk pool holds %d connections with a budget of 1", n)
	}
	if v2.conn.Context().Err() == nil {
		t.Fatal("the idle version-2 bulk connection was not retired to make room")
	}
	client.releaseBulkConn(v1, false)
	client.closeBulkQUICPool("test done")
}

// TestAJoinOfTheOtherVersionCannotClaimAPendingOpen covers the window before
// a flow exists: a JOIN of the other version for an OPEN still dialling its
// destination is refused at once as a version mismatch, without waiting on
// the OPEN or marking it as one a client asked to rescue.
func TestAJoinOfTheOtherVersionCannotClaimAPendingOpen(t *testing.T) {
	owner := identity.Principal{ProviderID: "provider", AccountID: "account", DeviceID: "owner"}
	server, registry := pendingTestServer()
	sessionID := [16]byte{8}
	pending, _ := server.beginPendingSession(sessionID, 3, owner, 1)
	defer server.endPendingSession(sessionID, pending)
	frame, ok := <-joinPendingSession(server, owner, sessionID, 3)
	if !ok || frame.Header.Type != protocol.TypeReset || resetCode(frame.Payload) != session.ResetProtocol ||
		string(frame.Payload[1:]) != wireMismatchAnswer {
		t.Fatalf("a v2 JOIN for a pending v1 OPEN got %+v %q", frame.Header, frame.Payload)
	}
	if pending.rescueRequested.Load() || pending.waiters.Load() != 0 {
		t.Fatal("a JOIN of the other version registered as the pending OPEN's rescuer")
	}
	if got := registry.Snapshot().LaneJoinRefusals[metrics.LaneJoinWireMismatch]; got != 1 {
		t.Fatalf("wire_mismatch = %d, want 1", got)
	}
}
