package pep

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/netip"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/apernet/quic-go"
	quiccongestion "github.com/apernet/quic-go/congestion"
	"github.com/bojieli/queqiao/internal/coded"
	wancongestion "github.com/bojieli/queqiao/internal/congestion"
	"github.com/bojieli/queqiao/internal/identity"
	"github.com/bojieli/queqiao/internal/metrics"
	"github.com/bojieli/queqiao/internal/netbind"
	"github.com/bojieli/queqiao/internal/pathmodel"
	"github.com/bojieli/queqiao/internal/portmux"
	"github.com/bojieli/queqiao/internal/protocol"
)

type TransportKind string

const (
	TransportTCP  TransportKind = "tcp"
	TransportQUIC TransportKind = "quic"
	TransportAuto TransportKind = "auto"
)

// defaultALPN is the data plane's preferred protocol. It lives with the wire
// version in internal/protocol, because bumping one without the other is the
// failure the pairing exists to prevent.
const defaultALPN = protocol.DataALPN

// parseWireVersions reads a comma-separated list of wire versions, such as
// "2,1", keeping its order. Every entry must be a version this build speaks,
// and none may repeat.
func parseWireVersions(spec string) ([]byte, error) {
	var out []byte
	for _, field := range strings.Split(spec, ",") {
		field = strings.TrimSpace(field)
		parsed, err := strconv.ParseUint(field, 10, 8)
		version := byte(parsed)
		if err != nil || !protocol.Supported(version) {
			return nil, fmt.Errorf("unsupported wire version %q (this build speaks %s)", field, formatWireVersions(protocol.SupportedVersions()))
		}
		if slices.Contains(out, version) {
			return nil, fmt.Errorf("wire version %d is listed twice", version)
		}
		out = append(out, version)
	}
	return out, nil
}

// ParseWireVersions is parseWireVersions for the command line.
func ParseWireVersions(spec string) ([]byte, error) { return parseWireVersions(spec) }

// formatWireVersions renders versions as the comma-separated list the flags
// take and --version prints.
func formatWireVersions(versions []byte) string {
	parts := make([]string, len(versions))
	for i, v := range versions {
		parts[i] = strconv.Itoa(int(v))
	}
	return strings.Join(parts, ",")
}

// FormatWireVersions is formatWireVersions for the command line.
func FormatWireVersions(versions []byte) string { return formatWireVersions(versions) }

// resolveWireVersions validates a configured version list and returns it in
// the given order, or every version this build speaks, most preferred first,
// when none was configured.
func resolveWireVersions(configured []byte) ([]byte, error) {
	if len(configured) == 0 {
		return protocol.SupportedVersions(), nil
	}
	out := make([]byte, 0, len(configured))
	for _, v := range configured {
		if !protocol.Supported(v) {
			return nil, fmt.Errorf("unsupported wire version %d (this build speaks %s)", v, formatWireVersions(protocol.SupportedVersions()))
		}
		if slices.Contains(out, v) {
			return nil, fmt.Errorf("wire version %d is listed twice", v)
		}
		out = append(out, v)
	}
	return out, nil
}

func wireALPNs(versions []byte) []string {
	alpns := make([]string, len(versions))
	for i, v := range versions {
		alpns[i] = protocol.ALPN(v)
	}
	return alpns
}

// checkNegotiatedWire maps the ALPN a data connection negotiated to its wire
// version, refusing one that the local end did not offer. TLS already refuses
// a protocol nobody offered; this also refuses a peer that negotiated none.
func checkNegotiatedWire(alpn string, offered []byte) (byte, bool) {
	wire, ok := protocol.VersionForALPN(alpn)
	if !ok || !slices.Contains(offered, wire) {
		return 0, false
	}
	return wire, true
}

const (
	defaultAdaptiveMinBytesPerSec = 64 * 1024
	defaultAdaptiveMaxBytesPerSec = 200 * 1024 * 1024
	maxConfiguredSessions         = 1 << 16
)

// CongestionControlKind selects the QUIC sender.
//
// Erasure is the default and the only one that should normally be chosen. Reno
// leaves the apNet quic-go default untouched and is the safe control. BBR is
// the original queqiao controller. BBRTUIC is a faithful Go port of TUIC's
// quinn-congestions BBR model. Adaptive is a conservative rate-estimating
// controller for unknown paths. Brutal is a fixed-rate mode for controlled
// experiments where the operator knows the per-lane budget.
type CongestionControlKind string

const (
	CongestionReno     CongestionControlKind = "reno"
	CongestionBBR      CongestionControlKind = "bbr"
	CongestionBBRTUIC  CongestionControlKind = "bbr-tuic"
	CongestionAdaptive CongestionControlKind = "adaptive"
	CongestionBrutal   CongestionControlKind = "brutal"
	// CongestionErasure is BBR corrected for a path that erases packets for
	// reasons unrelated to congestion. It is the right choice on a long-haul
	// path with a loss floor; on a clean path it reduces to BBR, because the
	// floor it measures is zero and the correction it applies is one.
	CongestionErasure CongestionControlKind = "erasure"
)

// defaultCongestion is the controller used when none is configured. It is a
// function rather than a constant so the choice has one home and a test can
// name it.
func defaultCongestion() CongestionControlKind { return CongestionErasure }

type congestionConfig struct {
	kind                   CongestionControlKind
	brutalBytesPerSecond   uint64
	adaptiveMinBytesPerSec uint64
	adaptiveMaxBytesPerSec uint64
	// hierarchicalPath models the path as a chain of segments rather than a
	// single endpoint pair, so that flows to different peers pool what they
	// share -- the uplink -- while keeping separate what they do not.
	//
	// It is off by default because the deployment this project's published
	// results describe has one peer, where the two models are identical by
	// construction and the extra node measures nothing. It earns its keep when
	// one client serves several providers, which share the client's uplink and
	// nothing else.
	hierarchicalPath bool
	// discoverGrouping lets the tree's shape follow the evidence rather than
	// the static hierarchy. It does nothing without hierarchicalPath.
	discoverGrouping bool
	// ccAPI selects how the controller is driven; see CCAPIVersion.
	ccAPI CCAPIVersion
	// sched selects the class scheduler; see SchedMode. It applies only
	// with ccAPI v2.
	sched SchedMode
	// scope selects what one controller governs; see CongestionScope. It
	// applies only with ccAPI v2 and the erasure law.
	scope CongestionScope
	// provisional says the connection's peer is not yet known to be an
	// authorised data connection (a gateway installs a controller before it
	// knows the ALPN or the principal). Under congestion=path such a
	// connection gets a standalone controller and attaches to its path's
	// controller only once it is known; see attachPathController.
	provisional bool
	// principal names the sending endpoint's identity in its path key under
	// congestion=path, read at installation and at each migration; see
	// Client.pathPrincipalNow.
	principal func() string
	// generation, if set, reads the endpoint's uplink generation, which
	// principal includes; see PathAttachment.SetGeneration.
	generation func() uint64
}

type udpHealth struct {
	mu        sync.Mutex
	failures  int
	threshold int
	cooldown  time.Duration
	blockedTo time.Time
}

// hopDialConfig carries port-hopping configuration for dialQUICConnection.
// A zero value disables port hopping.
type hopDialConfig struct {
	portCount  int    // 0 or 1 = disabled; ≥2 = enabled
	providerID string // for deterministic HopPorts derivation
	// walk is the client-wide port selection state, shared by all dials so
	// each attempt continues where the previous one left off instead of
	// restarting on the primary port.
	walk    *portmux.HopWalk
	metrics *metrics.Registry
	logger  *slog.Logger
}

// quicPathEvidence is deliberately narrower than "a QUIC operation ended".
// Only observations about network reachability belong in the global cooldown;
// peer shutdown, stream cancellation, authentication, protocol, destination,
// and caller-lifecycle outcomes are endpoint state and remain neutral.
type quicPathEvidence uint8

const (
	quicPathNeutral quicPathEvidence = iota
	quicPathAvailable
	quicPathUnavailable
)

// peerResponseError marks an error decided from a syntactically complete peer
// frame. The application operation failed, but the QUIC path demonstrably
// carried a round trip.
type peerResponseError struct{ err error }

func (e *peerResponseError) Error() string { return e.err.Error() }
func (e *peerResponseError) Unwrap() error { return e.err }

func peerResponse(err error) error {
	if err == nil {
		return nil
	}
	return &peerResponseError{err: err}
}

func peerResponded(err error) bool {
	if errors.Is(err, errDestinationUnavailable) {
		return true
	}
	var response *peerResponseError
	return errors.As(err, &response)
}

// classifyQUICPathEvidence converts typed transport outcomes into path
// evidence. Unknown errors stay neutral: a false negative costs one future
// race, while a false positive forces every new flow onto TCP for a cooldown.
func classifyQUICPathEvidence(err error) quicPathEvidence {
	if err == nil || peerResponded(err) {
		return quicPathAvailable
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, errCarrierAdmissionWait) {
		// A wait for local admission (recovery=carrier) is no evidence about
		// the network.
		return quicPathNeutral
	}

	// Explicit peer/implementation outcomes are not reachability evidence.
	var applicationError *quic.ApplicationError
	var streamError *quic.StreamError
	var statelessReset *quic.StatelessResetError
	var versionError *quic.VersionNegotiationError
	if errors.As(err, &applicationError) || errors.As(err, &streamError) ||
		errors.As(err, &statelessReset) || errors.As(err, &versionError) {
		return quicPathNeutral
	}
	var transportError *quic.TransportError
	if errors.As(err, &transportError) {
		if !transportError.Remote && transportError.ErrorCode == quic.NoViablePathError {
			return quicPathUnavailable
		}
		return quicPathNeutral
	}

	var handshakeTimeout *quic.HandshakeTimeoutError
	var idleTimeout *quic.IdleTimeoutError
	if errors.As(err, &handshakeTimeout) || errors.As(err, &idleTimeout) ||
		errors.Is(err, context.DeadlineExceeded) {
		return quicPathUnavailable
	}
	if unreachableRouteErrno(err) {
		return quicPathUnavailable
	}
	var networkError net.Error
	if errors.As(err, &networkError) && networkError.Timeout() {
		return quicPathUnavailable
	}
	return quicPathNeutral
}

// differentialQUICPathEvidence requires the TCP control to reach the same
// endpoint before a negative QUIC observation becomes global. A pending QUIC
// attempt is not evidence at all: TCP can authenticate sooner on a lossy path
// and still be orders of magnitude worse once it carries data. If TCP also
// failed, the endpoint or whole uplink may be down and QUIC is not singled out.
func differentialQUICPathEvidence(quicEvidence, tcpEvidence quicPathEvidence) quicPathEvidence {
	if tcpEvidence != quicPathAvailable {
		return quicPathNeutral
	}
	if quicEvidence == quicPathUnavailable {
		return quicPathUnavailable
	}
	return quicPathNeutral
}

func newUDPHealth(threshold int, cooldown time.Duration) *udpHealth {
	if threshold <= 0 {
		threshold = 3
	}
	if cooldown <= 0 {
		cooldown = 30 * time.Second
	}
	return &udpHealth{threshold: threshold, cooldown: cooldown}
}

func (h *udpHealth) allow(now time.Time) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return !now.Before(h.blockedTo)
}

func (h *udpHealth) reset() {
	h.mu.Lock()
	h.failures = 0
	h.blockedTo = time.Time{}
	h.mu.Unlock()
}

func (h *udpHealth) failure(now time.Time) {
	h.mu.Lock()
	h.failures++
	if h.failures >= h.threshold {
		h.blockedTo = now.Add(h.cooldown)
		h.failures = 0
	}
	h.mu.Unlock()
}

func (h *udpHealth) observe(evidence quicPathEvidence, now time.Time) {
	switch evidence {
	case quicPathAvailable:
		h.reset()
	case quicPathUnavailable:
		h.failure(now)
	}
}

type streamConn interface {
	io.ReadWriteCloser
	SetDeadline(time.Time) error
}

// quicBidiStream is the part of quic.Stream used by a lane. Keeping this
// narrow makes the two-direction close contract independently testable.
type quicBidiStream interface {
	io.ReadWriteCloser
	SetDeadline(time.Time) error
	SetWriteDeadline(time.Time) error
	CancelRead(quic.StreamErrorCode)
}

// laneTransportStats is intentionally a small internal projection of QUIC's
// connection counters.  Keeping the QUIC type out of the flow and metrics
// packages lets TCP rescue lanes remain dependency-independent.
type laneTransportStats struct {
	latestRTT, smoothedRTT   time.Duration
	bytesSent, bytesReceived uint64
	// Coded receive-direction symbol outcomes. Every source symbol the peer
	// sent ends in exactly one of the three, which is what makes them a
	// denominator: arrived, reconstructed by the code, or left the window
	// still missing and re-issued by the session a round trip later.
	codedSources, codedRecovered, codedLost uint64
	packetsSent, packetsReceived            uint64
	controller                              wancongestion.ControllerTelemetry
}

type laneStatsProvider interface {
	transportStats() laneTransportStats
}

// handshakeRoundTrips is how many round trips a handshake gets, rather than
// how many seconds.
//
// A wall-clock constant is a constant sized for whichever path it was chosen
// on. On the channel this project targets each exchange needs 1/(1-p)
// transmissions with a long tail -- at 42% erasure, one exchange in a hundred
// still needs seven -- so a five-second bound expires while the peer's open is
// being retransmitted, and the stream is closed under a flow that was working.
// Thirty round trips is generous on any path and finite on all of them.
const handshakeRoundTrips = 30

// handshakeBound is the configured handshake timeout, extended to what the
// measured path needs. It never shortens the configured value: an operator who
// asked for longer gets longer.
func handshakeBound(conn streamConn, configured time.Duration) time.Duration {
	rtt := transportRTT(conn)
	if rtt <= 0 {
		return configured
	}
	if scaled := handshakeRoundTrips * rtt; scaled > configured {
		return scaled
	}
	return configured
}

// transportRTT is the round trip the lane's transport has measured, or zero
// when it measures none. It is a round trip and nothing else: a dial's duration
// includes the handshake, the authentication and, for a fresh connection,
// every round trip those took.
func transportRTT(conn streamConn) time.Duration {
	provider, ok := conn.(laneStatsProvider)
	if !ok {
		return 0
	}
	stats := provider.transportStats()
	if stats.smoothedRTT > 0 {
		return stats.smoothedRTT
	}
	return stats.latestRTT
}

type quicStreamConn struct {
	stream     quicBidiStream
	conn       *quic.Conn
	packet     net.PacketConn
	controller wancongestion.TelemetryProvider
	// closeConn is true for a dedicated lane. Streams obtained from the
	// client pool and streams accepted by the server must only close their
	// stream; closing the connection would tear down unrelated flows.
	closeConn bool
	once      sync.Once
	writeOnce sync.Once
	writeErr  error
	// bulk carries this lane's data frames as coded datagrams when the path
	// erases enough to make that worth doing. It belongs to the connection
	// rather than the stream, so a pooled connection's streams share it.
	bulk *coded.Path
}

// bulkPath lets the framing discover the coded substrate without every call
// site that builds a lane having to thread it through.
func (c *quicStreamConn) bulkPath() *coded.Path { return c.bulk }

// pathModel is the connection's path model reference; it follows a rekey.
func (c *quicStreamConn) pathModel() pathmodel.Model { return pathModelOf(c.conn) }

func (c *quicStreamConn) transportStats() laneTransportStats {
	if c == nil || c.conn == nil {
		return laneTransportStats{}
	}
	s := c.conn.ConnectionStats()
	stats := laneTransportStats{
		latestRTT: s.LatestRTT, smoothedRTT: s.SmoothedRTT,
		bytesSent: s.BytesSent, bytesReceived: s.BytesReceived,
		packetsSent: s.PacketsSent, packetsReceived: s.PacketsReceived,
	}
	if c.controller != nil {
		stats.controller = c.controller.Telemetry()
	}
	if c.bulk != nil {
		// The coded path belongs to the connection, not the stream, so these
		// are connection-scoped like the counters above and fold once however
		// many lanes share it.
		coded := c.bulk.Stats()
		stats.codedSources = coded.Sources
		stats.codedRecovered = coded.Recovered
		stats.codedLost = coded.Lost
	}
	return stats
}

func (c *quicStreamConn) Read(p []byte) (int, error)  { return c.stream.Read(p) }
func (c *quicStreamConn) Write(p []byte) (int, error) { return c.stream.Write(p) }

// classedStream is a QUIC stream that records the send class of what it is
// given.
type classedStream interface {
	WriteClass(p []byte, class quiccongestion.SendClass) (int, error)
	SetBoost(on bool)
}

// classScheduled says whether this lane's connection schedules classes
// (sched=class), and so whether class tags and boost mean anything on it.
func (c *quicStreamConn) classScheduled() (classedStream, bool) {
	if c.conn == nil || !isClassScheduled(c.conn) {
		return nil, false
	}
	s, ok := c.stream.(classedStream)
	return s, ok
}

// writeClass writes p as bytes of class when the connection schedules
// classes, and as an ordinary write otherwise.
func (c *quicStreamConn) writeClass(p []byte, class quiccongestion.SendClass) (int, error) {
	if s, ok := c.classScheduled(); ok {
		return s.WriteClass(p, class)
	}
	return c.stream.Write(p)
}

// classed says whether this lane's connection schedules classes.
func (c *quicStreamConn) classed() bool {
	_, ok := c.classScheduled()
	return ok
}

// setBoost sets the stream's boost property, and reports whether the
// connection schedules classes at all.
func (c *quicStreamConn) setBoost(on bool) bool {
	s, ok := c.classScheduled()
	if ok {
		s.SetBoost(on)
	}
	return ok
}

func (c *quicStreamConn) SetDeadline(t time.Time) error {
	return c.stream.SetDeadline(t)
}
func (c *quicStreamConn) SetWriteDeadline(t time.Time) error {
	return c.stream.SetWriteDeadline(t)
}

// CloseWrite finishes one side of a pooled probe stream while leaving its read
// side and the shared QUIC connection alive. The client uses that half-close
// to delimit a bounded bidirectional prewarm without inventing another wire
// flag or closing connections which already carry flows.
func (c *quicStreamConn) CloseWrite() error {
	c.writeOnce.Do(func() { c.writeErr = c.stream.Close() })
	return c.writeErr
}

func (c *quicStreamConn) Close() error {
	var err error
	c.once.Do(func() {
		// quic.Stream.Close closes only the send direction. CancelRead releases
		// a blocked reader and its flow-control credit; omitting it retained
		// aborted pooled streams indefinitely even though the logical lane was
		// already gone.
		c.stream.CancelRead(0)
		err = c.CloseWrite()
		if c.closeConn {
			// Dedicated lanes own their QUIC connection and socket. Pooled
			// streams deliberately leave both alive for other flows.
			if closeErr := c.conn.CloseWithError(0, "queqiao lane closed"); closeErr != nil {
				err = closeErr
			}
			if c.packet != nil {
				_ = c.packet.Close()
			}
		}
	})
	return err
}

func tlsClientConfig(credentials identity.ClientCredentials, wires []byte) (*tls.Config, error) {
	if len(wires) == 0 {
		wires = protocol.SupportedVersions()
	}
	return identity.ClientTLSConfigProtocols(credentials, wireALPNs(wires))
}

// negotiatedWire reports the version the lane's QUIC connection negotiated.
// Every stream on a connection shares it.
func (c *quicStreamConn) negotiatedWire() (byte, bool) {
	if c == nil || c.conn == nil {
		return 0, false
	}
	return protocol.VersionForALPN(c.conn.ConnectionState().TLS.NegotiatedProtocol)
}

func dialTCP(ctx context.Context, remote string, credentials identity.ClientCredentials, wires []byte, dialTimeout time.Duration, localAddress string, control func(string, string, syscall.RawConn) error) (streamConn, error) {
	var localAddr net.Addr
	composed := control
	// LocalAddr and Control apply to the connection this dialer makes, never to
	// the lookup that turns the endpoint name into the address it connects to.
	// An endpoint named rather than numbered therefore asked its question over
	// an unbound socket, which capture is free to claim and send to the very
	// gateway the answer is needed to reach. Built from the resolution the
	// binding already needed, so a dial does not enumerate interfaces twice.
	resolver := net.DefaultResolver
	if localAddress != "" {
		result, err := netbind.ResolveWithInterface(localAddress)
		if err != nil {
			return nil, err
		}
		localAddr = &net.TCPAddr{IP: result.Addr.AsSlice()}
		composed = composeSocketControls(netbind.InterfaceControl(result.InterfaceName), control)
		resolver = netbind.ResolverForResult(result, control)
	}
	tlsConfig, err := tlsClientConfig(credentials, wires)
	if err != nil {
		return nil, err
	}
	conn, err := (&tls.Dialer{
		NetDialer: &net.Dialer{
			Timeout:   dialTimeout,
			KeepAlive: 30 * time.Second,
			LocalAddr: localAddr,
			Control:   composed,
			Resolver:  resolver,
		},
		Config: tlsConfig,
	}).DialContext(ctx, "tcp", remote)
	if err != nil {
		return nil, explainDataHandshakeError(remote, "TCP", wires, err)
	}
	tlsConn := conn.(*tls.Conn)
	if _, ok := checkNegotiatedWire(tlsConn.ConnectionState().NegotiatedProtocol, offeredWires(wires)); !ok {
		_ = tlsConn.Close()
		return nil, fmt.Errorf("gateway %q did not negotiate Queqiao protocol %s over TCP; check that the endpoint and server version match", remote, formatWireVersions(offeredWires(wires)))
	}
	return tlsConn, nil
}

// Flow-control windows are the single largest measured determinant of
// long-haul goodput for this transport, so they are named constants rather
// than inline literals.
//
// quic-go auto-tunes its receive windows upward from an initial value, but the
// growth heuristic requires the receiver to consume a large fraction of the
// window within a small multiple of the RTT. On a 200 ms path with a few
// percent packet loss, loss recovery delays consumption enough that the
// window stops growing, and the *receive window* rather than congestion
// control becomes the binding constraint. Measured with cmd/queqiaobench at
// 200 ms RTT and 1--5% loss, a 512 KiB initial stream window cost 30--40%
// goodput against an otherwise identical TUIC-shaped reference.
//
// TUIC (via quinn) instead uses a fixed 8 MiB stream receive window and a
// 16 MiB connection send window with no ramp at all. These constants match it
// exactly, initial and maximum alike, so no ramp remains.
//
// Allowing the window to auto-tune *above* that point was measured and
// rejected. It bought a little bulk goodput (58.5--64.8 against 55.4--58.5
// Mbit/s on a 50 MiB transfer) by letting the sender hold a deeper standing
// queue at the bottleneck, and it cost far more than it bought at the tail:
// interactive requests issued during that transfer went from a 489--701 ms
// 95th percentile to 976--1062 ms, and from an 883 ms worst case to 1339 ms.
// Protecting interactive latency under bulk load is the point of this
// transport, so the ceiling stays where TUIC puts it.
//
// The connection window is what bounds per-connection receive memory: it caps
// the aggregate across all streams, so a large per-stream window does not
// multiply by the stream limit.
const (
	initialStreamReceiveWindow     = 8 * 1024 * 1024
	initialConnectionReceiveWindow = 16 * 1024 * 1024
	// The ceilings are what let quic-go auto-tune the receive window up to the
	// path's bandwidth-delay product. Setting the initial value equal to the
	// maximum, which this did, switches auto-tuning off and makes the window a
	// constant -- and a receive window is a hard bound on throughput at
	// rwnd/RTT, so a constant one caps a single stream at a rate that falls as
	// the round trip grows.
	//
	// Measured with raw QUIC through the emulator, no proxy on either side, at
	// 400 ms and a 400 Mbit/s link: a fixed 8 MiB window delivers 104 Mbit/s
	// and a 64 MiB ceiling delivers 190, which is 1.8 times as much. At 50 ms
	// on the same link the two are 341 and 365, because there the product is
	// small enough that 8 MiB was never the binding constraint. That is the
	// shape of a flow-control limit rather than a congestion-control one, and
	// it is why link utilisation collapsed with the product across every cell
	// of the benchmark grid for both this transport and the reference.
	maxStreamReceiveWindow     = 64 * 1024 * 1024
	maxConnectionReceiveWindow = 128 * 1024 * 1024
	// A bounded stream fan-out lets one QUIC connection carry multiple
	// independent PEP flows, like TUIC, without an unbounded stream commitment.
	// 1,024 is the mobile upper bound; stream state and retained payload remain
	// bounded separately by the endpoint's memory budgets.
	maxIncomingStreams = 1024
)

// flowWindows selects the QUIC receive windows. A zero field takes the
// default. They are configurable because they are the single largest measured
// determinant of long-haul goodput and because their correct value is a
// property of the path, not of the code: the defaults match TUIC, which is the
// right answer for the paths this project targets, but a much fatter or much
// thinner path wants a different one.
type flowWindows struct {
	stream        uint64
	connection    uint64
	maxStream     uint64
	maxConnection uint64
	maxStreams    int64
	// codedQueue is the connection-level coded path's send and receive
	// mailbox depth. Zero keeps the desktop default; mobile supplies the same
	// tiny depth as its other frame queues.
	codedQueue int
}

func (w flowWindows) resolved() (stream, connection, streamMax, connectionMax uint64, streams int64) {
	stream, connection = w.stream, w.connection
	if stream == 0 {
		stream = initialStreamReceiveWindow
	}
	if connection == 0 {
		connection = initialConnectionReceiveWindow
	}
	streamMax, connectionMax = w.maxStream, w.maxConnection
	if streamMax == 0 {
		streamMax = maxStreamReceiveWindow
	}
	if connectionMax == 0 {
		connectionMax = maxConnectionReceiveWindow
	}
	streams = w.maxStreams
	if streams == 0 {
		streams = maxIncomingStreams
	}
	return stream, connection, streamMax, connectionMax, streams
}

func (w flowWindows) validate() error {
	stream, connection, streamMax, connectionMax, streams := w.resolved()
	if streamMax < stream {
		return errors.New("maximum stream receive window is below its initial value")
	}
	if connectionMax < connection {
		return errors.New("maximum connection receive window is below its initial value")
	}
	if connection < stream || connectionMax < streamMax {
		return errors.New("connection receive window must not be smaller than stream receive window")
	}
	if streams < 1 || streams > maxIncomingStreams {
		return fmt.Errorf("maximum incoming streams must be between 1 and %d", maxIncomingStreams)
	}
	return nil
}

func quicConfig(windows flowWindows) *quic.Config {
	streamWindow, connectionWindow, streamMax, connectionMax, incomingStreams := windows.resolved()
	return &quic.Config{
		// The handshake gets as long as an erasing path needs.
		//
		// On the channel this targets the handshake itself takes about five
		// seconds: its packets are large, they are lost 42% of the time, and
		// the probe timeouts that recover them double. Ten seconds made the
		// first connection a coin flip, and it is the one connection
		// everything else is built on. This is the one place a long wall-clock
		// constant is right, because there is no measurement of the path yet
		// to scale anything by.
		HandshakeIdleTimeout: 30 * time.Second,
		// Existing-flow TCP rescue cannot begin until QUIC declares the
		// black-holed lane dead. Keep this bound well below application-level
		// request timeouts while allowing several PTOs on a 200 ms WAN.
		//
		// laneReplacementWait is derived from this, because a flow's grace has
		// to cover the time its peer spends reaching this same conclusion
		// before it can even begin opening a replacement. Changing the number
		// here changes what a flow waits; it is not a knob local to one
		// connection.
		MaxIdleTimeout:                 laneDeadPathDetection,
		KeepAlivePeriod:                5 * time.Second,
		InitialStreamReceiveWindow:     streamWindow,
		MaxStreamReceiveWindow:         streamMax,
		InitialConnectionReceiveWindow: connectionWindow,
		MaxConnectionReceiveWindow:     connectionMax,
		MaxIncomingStreams:             incomingStreams,
		MaxIncomingUniStreams:          0,
		// The China path has a smaller effective UDP MTU than this host's
		// interface. Disable probing until path-specific MTU discovery is
		// available; otherwise a successful probe can raise packets above the
		// path MTU and stall a long response.
		DisablePathMTUDiscovery: true,
		InitialPacketSize:       1200,
		// Datagrams are negotiated on every connection so a coded lane is
		// possible without a second handshake. Negotiating costs one transport
		// parameter; a connection that never sends one pays nothing more.
		EnableDatagrams: true,
	}
}

func dialQUIC(ctx context.Context, remote string, credentials identity.ClientCredentials, wires []byte, dialTimeout time.Duration, localAddress string, control func(string, string, syscall.RawConn) error, observeTransientWrite func(error), ccfg congestionConfig, windows flowWindows, hop hopDialConfig) (streamConn, error) {
	conn, packetConn, err := dialQUICConnection(ctx, remote, credentials, wires, dialTimeout, localAddress, control, observeTransientWrite, windows, hop)
	if err != nil {
		return nil, err
	}
	controller := configureQUICController(conn, ccfg)
	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		_ = conn.CloseWithError(0, "unable to open queqiao stream")
		if packetConn != nil {
			_ = packetConn.Close()
		}
		return nil, err
	}
	return &quicStreamConn{
		stream: stream, conn: conn, packet: packetConn, controller: controller,
		closeConn: true, bulk: connBulkPath(conn, windows.codedQueue),
	}, nil
}

// transientRoutePacketConn gives a short local route outage the same meaning
// as a datagram lost in the network.
//
// A Wi-Fi reassociation can make sendmsg return ENETUNREACH or EHOSTUNREACH
// for a few hundred milliseconds even though the socket, local address and
// peer are all still valid. quic-go quite reasonably treats an arbitrary
// PacketConn write error as terminal, because it cannot know whether the
// application supplied a permanently broken socket. For a client-owned UDP
// socket these particular errors are different: closing the connection turns
// one lost packet into the loss of every multiplexed stream. Reporting the
// datagram as accepted lets QUIC's normal loss detection and PTO retransmit it
// after the route returns. The negotiated idle timeout remains the finite
// bound for a path that does not return.
//
// The wrapper deliberately recognizes only errors a socket can outlive.
// EADDRNOTAVAIL / WSAEADDRNOTAVAIL reaches quic-go: a socket explicitly bound
// to a DHCP address which disappeared cannot migrate to its replacement, and
// pretending its writes succeeded leaves every stream on it stalled until a
// process restart. Permission, descriptor, message-size and peer/protocol
// errors likewise close the unusable transport.
type transientRoutePacketConn struct {
	net.PacketConn
	observe func(error)
}

func (c *transientRoutePacketConn) WriteTo(payload []byte, addr net.Addr) (int, error) {
	n, err := c.PacketConn.WriteTo(payload, addr)
	if err == nil || !transientRouteWriteError(err) {
		return n, err
	}
	if c.observe != nil {
		c.observe(err)
	}
	return len(payload), nil
}

// transientRouteOOBPacketConn preserves quic-go's UDP fast path. Hiding
// ReadMsgUDP / WriteMsgUDP behind a plain net.PacketConn would disable ECN,
// batched I/O and GSO on Linux, turning a resilience fix into a throughput
// regression.
type transientRouteOOBPacketConn struct {
	*transientRoutePacketConn
	oob    quic.OOBCapablePacketConn
	stream net.Conn
}

// x/net's OOB adapter unwraps PacketConn through net.Conn. The socket behind
// quic-go's OOB interface is a UDPConn, so preserve that part of its method
// set as well as the packet methods exposed by the embedded wrapper.
func (c *transientRouteOOBPacketConn) Read(payload []byte) (int, error) {
	return c.stream.Read(payload)
}

func (c *transientRouteOOBPacketConn) Write(payload []byte) (int, error) {
	return c.stream.Write(payload)
}

func (c *transientRouteOOBPacketConn) RemoteAddr() net.Addr {
	return c.stream.RemoteAddr()
}

func (c *transientRouteOOBPacketConn) SyscallConn() (syscall.RawConn, error) {
	return c.oob.SyscallConn()
}

func (c *transientRouteOOBPacketConn) SetReadBuffer(bytes int) error {
	return c.oob.SetReadBuffer(bytes)
}

func (c *transientRouteOOBPacketConn) ReadMsgUDP(payload, oob []byte) (int, int, int, *net.UDPAddr, error) {
	return c.oob.ReadMsgUDP(payload, oob)
}

func (c *transientRouteOOBPacketConn) WriteMsgUDP(payload, oob []byte, addr *net.UDPAddr) (int, int, error) {
	n, oobn, err := c.oob.WriteMsgUDP(payload, oob, addr)
	if err == nil || !transientRouteWriteError(err) {
		return n, oobn, err
	}
	if c.observe != nil {
		c.observe(err)
	}
	return len(payload), len(oob), nil
}

func tolerateTransientRouteErrors(conn net.PacketConn, observe func(error)) net.PacketConn {
	base := &transientRoutePacketConn{PacketConn: conn, observe: observe}
	if oob, ok := conn.(quic.OOBCapablePacketConn); ok {
		if stream, ok := conn.(net.Conn); ok {
			return &transientRouteOOBPacketConn{transientRoutePacketConn: base, oob: oob, stream: stream}
		}
	}
	return base
}

// transientRouteWriteError reports a local send failure that a QUIC connection
// should see as one lost packet rather than a dead socket. The codes it names
// are per-platform, because the constants that spell them are: see
// sockerr_windows.go.
func transientRouteWriteError(err error) bool {
	return transientRouteWriteErrno(err)
}

// dialQUICConnection establishes only the QUIC connection. Keeping this
// separate from stream creation allows the client to pool one connection and
// open a stream for each logical flow without paying another handshake.
//
// When hop.portCount ≥ 2, the raw UDP socket is wrapped in a ClientPortMux
// before being handed to quic-go, and a HopController goroutine is started to
// monitor for the per-port blocking pattern caused by GFW and trigger port
// hops reactively. The mux's context is cancelled when the PacketConn is
// closed, which stops the goroutine.
func dialQUICConnection(ctx context.Context, remote string, credentials identity.ClientCredentials, wires []byte, dialTimeout time.Duration, localAddress string, control func(string, string, syscall.RawConn) error, observeTransientWrite func(error), windows flowWindows, hop hopDialConfig) (*quic.Conn, net.PacketConn, error) {
	dialCtx := ctx
	var cancel context.CancelFunc
	if dialTimeout > 0 {
		dialCtx, cancel = context.WithTimeout(ctx, dialTimeout)
		defer cancel()
	}
	tlsCfg, err := tlsClientConfig(credentials, wires)
	if err != nil {
		return nil, nil, err
	}
	listenAddress := ":0"
	composed := control
	// See dialTCP: the lookup that turns a named endpoint into an address does
	// not inherit the socket's binding, so it needs a resolver that carries it.
	resolver := net.DefaultResolver
	if localAddress != "" {
		result, parseErr := netbind.ResolveWithInterface(localAddress)
		if parseErr != nil {
			return nil, nil, parseErr
		}
		listenAddress = net.JoinHostPort(result.Addr.String(), "0")
		composed = composeSocketControls(netbind.InterfaceControl(result.InterfaceName), control)
		resolver = netbind.ResolverForResult(result, control)
	}

	addresses, err := resolveUDPAddrs(dialCtx, remote, resolver)
	if err != nil {
		return nil, nil, err
	}
	localHost, _, _ := net.SplitHostPort(listenAddress)
	localIP, _ := netip.ParseAddr(localHost)
	candidates := addresses[:0]
	for _, addr := range addresses {
		if !localIP.IsValid() || localIP.Unmap().Is4() == (addr.IP.To4() != nil) {
			candidates = append(candidates, addr)
		}
	}
	if len(candidates) == 0 {
		return nil, nil, fmt.Errorf("no gateway address compatible with local binding %q", listenAddress)
	}
	raceCtx, cancelRace := context.WithCancel(dialCtx)
	defer cancelRace()
	type result struct {
		conn   *quic.Conn
		packet net.PacketConn
		err    error
	}
	results := make(chan result)
	next, active := 0, 0
	launch := func() {
		addr := candidates[next]
		waves := (len(candidates) - next + 1) / 2
		next++
		active++
		go func() {
			attemptCtx := raceCtx
			if deadline, ok := raceCtx.Deadline(); ok && waves > 1 {
				var stop context.CancelFunc
				attemptCtx, stop = context.WithTimeout(raceCtx, time.Until(deadline)/time.Duration(waves))
				defer stop()
			}
			conn, packet, err := dialQUICAddress(attemptCtx, remote, addr, tlsCfg, listenAddress, composed, observeTransientWrite, windows, hop)
			select {
			case results <- result{conn, packet, err}:
			case <-raceCtx.Done():
				if conn != nil {
					_ = conn.CloseWithError(0, "unused address candidate")
				}
				if packet != nil {
					_ = packet.Close()
				}
			}
		}()
	}
	launch()
	delay := 250 * time.Millisecond
	if deadline, ok := raceCtx.Deadline(); ok {
		delay = min(delay, max(time.Millisecond, time.Until(deadline)/2))
	}
	stagger := time.NewTicker(delay)
	defer stagger.Stop()
	var failures []error
	for active > 0 {
		select {
		case got := <-results:
			active--
			if got.err == nil {
				return got.conn, got.packet, nil
			}
			failures = append(failures, got.err)
			if next < len(candidates) {
				launch()
			}
		case <-stagger.C:
			if active < 2 && next < len(candidates) {
				launch()
			}
		case <-raceCtx.Done():
			return nil, nil, raceCtx.Err()
		}
	}
	return nil, nil, errors.Join(failures...)
}

// quicDialSocket names the socket a QUIC dial to remoteAddr listens on. A dial
// with no local binding gets the wildcard of the gateway's own address family,
// not a dual-stack socket. On macOS the kernel can give a dual-stack socket an
// ephemeral port that an IPv4 socket elsewhere on the host already holds, and
// the older IPv4 socket then receives every datagram sent to that port: the
// gateway answers the handshake, the answers go to the other socket, and the
// dial retransmits its first flight into silence until it times out. A socket
// of the remote's own family is allocated from the same table as those IPv4
// sockets, so it cannot be given a port one of them holds.
func quicDialSocket(listenAddress string, remoteAddr *net.UDPAddr) (network, address string) {
	if listenAddress != ":0" {
		// A local binding names an address, which fixes the family.
		return "udp", listenAddress
	}
	if remoteAddr.IP.To4() != nil {
		return "udp4", "0.0.0.0:0"
	}
	return "udp6", "[::]:0"
}

func dialQUICAddress(dialCtx context.Context, remote string, remoteAddr *net.UDPAddr, tlsCfg *tls.Config, listenAddress string, composed func(string, string, syscall.RawConn) error, observeTransientWrite func(error), windows flowWindows, hop hopDialConfig) (*quic.Conn, net.PacketConn, error) {
	network, address := quicDialSocket(listenAddress, remoteAddr)
	packetConn, err := (&net.ListenConfig{Control: composed}).ListenPacket(dialCtx, network, address)
	if err != nil {
		return nil, nil, err
	}
	if hop.portCount >= 2 {
		// Wrap the raw UDP socket with bidirectional port rewriting. The mux
		// is transparent to quic-go: outgoing packets are redirected to the
		// active hop port and incoming packets appear to originate from the
		// primary port.
		rawUDP := packetConn.(*net.UDPConn)
		ports := portmux.HopPorts(hop.providerID, remoteAddr.Port, hop.portCount)
		mux := portmux.NewClientPortMux(rawUDP, remoteAddr, ports)
		if hop.walk != nil {
			// Start the dial on the next walked port rather than the
			// primary: the primary is the port a blocker watches, and
			// restarting there every dial strands the pool's other ports.
			idx := hop.walk.Next()
			from, to := mux.Hop(idx)
			if hop.logger != nil {
				hop.logger.Debug("QUIC dial starting on walked hop port", "from_port", from, "to_port", to)
			}
		}
		ctrl := portmux.NewHopController(mux, portmux.HopConfig{
			Walk:    hop.walk,
			Metrics: hop.metrics,
			Logger:  hop.logger,
		})
		go ctrl.Run(mux.Context())
		packetConn = mux
	}
	packetConn = tolerateTransientRouteErrors(packetConn, observeTransientWrite)
	offered := make([]byte, 0, len(tlsCfg.NextProtos))
	for _, alpn := range tlsCfg.NextProtos {
		if v, ok := protocol.VersionForALPN(alpn); ok {
			offered = append(offered, v)
		}
	}
	conn, err := quic.Dial(dialCtx, packetConn, remoteAddr, tlsCfg, quicConfig(windows))
	if err != nil {
		_ = packetConn.Close()
		return nil, nil, explainDataHandshakeError(remote, "QUIC", offered, err)
	}
	if _, ok := checkNegotiatedWire(conn.ConnectionState().TLS.NegotiatedProtocol, offered); !ok {
		_ = conn.CloseWithError(0, "queqiao protocol not negotiated")
		_ = packetConn.Close()
		return nil, nil, fmt.Errorf("gateway %q did not negotiate Queqiao protocol %s over QUIC; check that the endpoint and server version match", remote, formatWireVersions(offered))
	}
	return conn, packetConn, nil
}

func explainDataHandshakeError(remote, transport string, wires []byte, err error) error {
	if err != nil && strings.Contains(strings.ToLower(err.Error()), "no application protocol") {
		return fmt.Errorf("gateway %q rejected Queqiao protocol %s over %s; the endpoint may run a gateway that accepts none of these wire versions, an incompatible development server, or another TLS service: %w", remote, formatWireVersions(offeredWires(wires)), transport, err)
	}
	return err
}

// validateLocalAddressSpec checks syntax without requiring the address or
// interface to be present at process startup. DHCP and interface state can
// change after startup; resolution is therefore repeated for every outer
// dial by netbind.ResolveWithInterface.
func validateLocalAddressSpec(spec string) error {
	return netbind.Validate(spec)
}

// resolveUDPAddr resolves a UDP endpoint through the given resolver.
//
// net.ResolveUDPAddr would use the default one, whose sockets carry neither the
// local address nor the interface binding the lane is about to use. A literal
// address needs no lookup and takes the same path it always did.
func resolveUDPAddr(ctx context.Context, remote string, resolver *net.Resolver) (*net.UDPAddr, error) {
	addresses, err := resolveUDPAddrs(ctx, remote, resolver)
	if err != nil {
		return nil, err
	}
	return addresses[0], nil
}

func resolveUDPAddrs(ctx context.Context, remote string, resolver *net.Resolver) ([]*net.UDPAddr, error) {
	host, port, err := net.SplitHostPort(remote)
	if err != nil {
		return nil, err
	}
	portNumber, err := net.LookupPort("udp", port)
	if err != nil {
		return nil, err
	}
	if addr, err := netip.ParseAddr(host); err == nil {
		return []*net.UDPAddr{{IP: net.IP(addr.AsSlice()), Zone: addr.Zone(), Port: portNumber}}, nil
	}
	addresses, err := resolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, err
	}
	if len(addresses) == 0 {
		return nil, fmt.Errorf("no addresses for %q", host)
	}
	result := make([]*net.UDPAddr, 0, len(addresses))
	for _, address := range addresses {
		result = append(result, &net.UDPAddr{IP: address.IP, Zone: address.Zone, Port: portNumber})
	}
	return result, nil
}

// composeSocketControls returns a control function that calls a then b. Either
// may be nil; if both are nil the result is nil. The first non-nil error aborts
// the chain.
func composeSocketControls(a, b func(string, string, syscall.RawConn) error) func(string, string, syscall.RawConn) error {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	return func(network, address string, conn syscall.RawConn) error {
		if err := a(network, address, conn); err != nil {
			return err
		}
		return b(network, address, conn)
	}
}

func configureQUICController(conn *quic.Conn, cfg congestionConfig) wancongestion.TelemetryProvider {
	if conn == nil {
		return nil
	}
	switch cfg.kind {
	case CongestionBBR:
		controller := wancongestion.NewBBRSender(conn.InitialPacketSize())
		conn.SetCongestionControl(controller)
		return controller
	case CongestionBBRTUIC:
		controller := wancongestion.NewTUICBBRSender(conn.InitialPacketSize())
		conn.SetCongestionControl(controller)
		return controller
	case CongestionAdaptive:
		controller := wancongestion.NewAdaptiveSender(conn.InitialPacketSize(), cfg.adaptiveMinBytesPerSec, cfg.adaptiveMaxBytesPerSec)
		conn.SetCongestionControl(controller)
		return controller
	case CongestionErasure:
		// Every lane to the same peer shares one model. Deciding alone is what
		// made lanes cost more than they earn on this path: each measured the
		// erasure floor from only its own packets and discovered the
		// bottleneck from only its own delivered rate, so the aggregate
		// overshot by however many lanes there were. Live, four lanes
		// delivered about 8 Mbit/s where one delivered 11.
		if cfg.ccAPI == CCAPIV2 && cfg.scope == CongestionScopePath {
			if cfg.provisional {
				return provisionalPathController(conn)
			}
			principal := cfg.principal
			if principal == nil {
				principal = func() string { return clientPathPrincipal }
			}
			return attachPathController(conn, cfg, principal)
		}
		if cfg.ccAPI == CCAPIV2 {
			// One resolution of the path names both the model and the key a
			// migration is compared against.
			egress, key := resolvePath(conn.LocalAddr(), conn.RemoteAddr())
			controller := wancongestion.NewErasureSenderOn(
				conn.InitialPacketSize(), pathModelForKey(key, egress, cfg.hierarchicalPath, cfg.discoverGrouping))
			attached := wancongestion.NewErasurePathSender(controller, erasureRekey(conn, cfg, key))
			attached.SetAddresses(func() (net.Addr, net.Addr) { return conn.LocalAddr(), conn.RemoteAddr() })
			if cfg.sched == SchedClass {
				attached.EnableClassScheduling(wancongestion.DefaultClassWeights)
				markClassScheduled(conn)
			}
			conn.SetCongestionControl(attached)
			return attached
		}
		controller := wancongestion.NewErasureSenderOn(
			conn.InitialPacketSize(), pathModelFor(conn, cfg.hierarchicalPath, cfg.discoverGrouping))
		conn.SetCongestionControl(controller)
		return controller
	case CongestionBrutal:
		if cfg.brutalBytesPerSecond > 0 {
			controller := wancongestion.NewBrutalSender(cfg.brutalBytesPerSecond, false)
			conn.SetCongestionControl(controller)
			return controller
		}
	case CongestionReno, "":
		// Keep the controller selected by the QUIC implementation.
	default:
		// Configuration is validated before dialing. Fail-safe to the stock
		// controller if a future caller constructs an invalid config directly.
	}
	return nil
}

// erasureRekey decides, after a connection migrated, whether it moved to
// another path. A NAT rebinding that changes only the port, or a local
// rebinding onto the same uplink, keeps the path key and so keeps the
// controller with everything it has measured. A new remote host or a new
// uplink is another path, and gets a fresh controller for it, which is what
// re-attaching to that path's state means while each connection still owns
// its controller.
//
// The comparison is against the key resolved when the current controller was
// chosen, not one recomputed from the old addresses: a wildcard-bound socket's
// key comes from the routing table, which after an uplink change resolves the
// old addresses to the new uplink too.
func erasureRekey(conn *quic.Conn, cfg congestionConfig, current string) func(oldLocal, newLocal, oldRemote, newRemote net.Addr) *wancongestion.ErasureSender {
	return func(_, newLocal, _, newRemote net.Addr) *wancongestion.ErasureSender {
		egress, next := resolvePath(newLocal, newRemote)
		if next == current {
			return nil
		}
		current = next
		// The connection's other readers of the path follow it too.
		rekeyPathModel(conn, next)
		return wancongestion.NewErasureSenderOn(conn.InitialPacketSize(), pathModelForKey(next, egress, cfg.hierarchicalPath, cfg.discoverGrouping))
	}
}

// peerKey identifies the endpoint pair a connection belongs to. It is the
// peer's address without its port: a second lane to the same server opens a
// new port and must still share, because the bottleneck it will contend for is
// the same one.
// peerKey identifies the path a connection runs over, which is a pair: the
// uplink it leaves by and the peer it reaches.
//
// The peer alone is not the path. The same server over Wi-Fi and over a
// cellular link erases differently, is bottlenecked differently and has a
// different minimum round trip, and a measurement of one is worse than no
// measurement of the other -- it is a confident wrong answer, and everything
// downstream is sized from it. The local address is what distinguishes them:
// changing uplink changes it, so the model for the new path starts empty
// rather than inheriting the old one's conclusions.
// pathModelFor returns the model this connection contributes to and reads.
//
// Flat, that is one model per endpoint pair, which is what every deployment
// had before the tree existed and what the WAN results were measured on.
// Hierarchical, it is the uplink's model and the peer's, and a flow may do
// only what the tighter of the two permits -- so a second provider reached
// over the same uplink starts from what the first measured about it, and a
// congested peer does not throttle traffic to a healthy one.
func pathModelFor(conn *quic.Conn, hierarchical, discover bool) pathmodel.Model {
	egress, key := resolvePath(conn.LocalAddr(), conn.RemoteAddr())
	return pathModelForKey(key, egress, hierarchical, discover)
}

// pathModelForKey selects the model for a resolved path key and egress, so a
// caller that also remembers the key resolves the route once.
func pathModelForKey(key, egress string, hierarchical, discover bool) pathmodel.Model {
	if !hierarchical {
		return pathmodel.Shared(key)
	}
	k := pathmodel.Key{Egress: egress, Dest: key}
	if discover {
		return pathmodel.SharedChainRegrouping(k)
	}
	return pathmodel.SharedChain(k)
}

func peerKey(conn *quic.Conn) string {
	return pathKey(conn.LocalAddr(), conn.RemoteAddr())
}

// pathKey names the uplink and peer a connection runs between.
//
// A socket's own local address is not reliably the uplink. Bound to the
// wildcard, as these are, it reports :: or 0.0.0.0 whatever route the kernel
// actually chose -- so keying on it would give Wi-Fi and cellular the same
// name, which is the one thing this key exists to prevent. When it is
// unspecified the routing table is asked instead, which is where the answer
// was all along.
// resolvePath resolves a path's egress and key with one routing lookup.
func resolvePath(local, remote net.Addr) (egress, key string) {
	egress = addressHost(local)
	if isUnspecifiedHost(egress) {
		egress = routeSource(remote)
	}
	return egress, egress + "->" + addressHost(remote)
}

func pathKey(local, remote net.Addr) string {
	source := addressHost(local)
	if isUnspecifiedHost(source) {
		source = routeSource(remote)
	}
	return source + "->" + addressHost(remote)
}

func isUnspecifiedHost(host string) bool {
	if host == "" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsUnspecified()
}

// routeSource asks the routing table which source address this destination
// gets. Dialling a UDP socket sends no packets; it only binds and resolves the
// route, which is exactly the question.
func routeSource(remote net.Addr) string {
	if remote == nil {
		return ""
	}
	conn, err := net.Dial("udp", remote.String())
	if err != nil {
		return ""
	}
	defer conn.Close()
	return addressHost(conn.LocalAddr())
}

func addressHost(addr net.Addr) string {
	if addr == nil {
		return ""
	}
	if host, _, err := net.SplitHostPort(addr.String()); err == nil {
		return host
	}
	return addr.String()
}

func quicServerConfig(windows flowWindows) *quic.Config {
	cfg := quicConfig(windows)
	return cfg
}

func acceptQUICStream(ctx context.Context, conn *quic.Conn, controller wancongestion.TelemetryProvider) (streamConn, error) {
	stream, err := conn.AcceptStream(ctx)
	if err != nil {
		return nil, err
	}
	return &quicStreamConn{
		stream: stream, conn: conn, controller: controller,
		closeConn: false, bulk: connBulkPath(conn, 0),
	}, nil
}

func offeredWires(wires []byte) []byte {
	if len(wires) == 0 {
		return protocol.SupportedVersions()
	}
	return wires
}

func transportError(kind TransportKind, err error) error {
	return fmt.Errorf("%s lane: %w", kind, err)
}
