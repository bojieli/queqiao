package pep

import (
	"context"
	"encoding/binary"
	"io"
	"log/slog"
	"math"
	"net"
	"sort"
	"testing"
	"time"

	"github.com/bojieli/queqiao/internal/metrics"
	"github.com/bojieli/queqiao/internal/pathmodel"
	"github.com/bojieli/queqiao/internal/pathsim"
	"github.com/bojieli/queqiao/internal/socks5"
)

// The deterministic workload tests of docs/WORKLOAD-MODEL.md ("Minimal live
// plan": W8, W9b, W10, W11 and M5) replace live runs. Each runs a workload
// end to end, through a real client and gateway, across a seeded
// internal/pathsim path, once per arm: the legacy bundle, which is the
// control, and the bundle the default flip ships (design §15.3, §15.4). The
// arms differ only in their switches.

// workloadArm is one arm of a workload test: a set of switches applied to an
// otherwise identical client and gateway.
type workloadArm struct {
	name   string
	client func(*ClientConfig)
	server func(*ServerConfig)
}

// legacyArm is every switch off: today's behaviour and the registered
// control of the stage-10 gate.
var legacyArm = workloadArm{name: "legacy", client: func(*ClientConfig) {}, server: func(*ServerConfig) {}}

// flipArm is every switch of the path-authority migration that is merged,
// on: what the default flip measures. A stage that adds a switch adds it
// here.
var flipArm = workloadArm{
	name: "flip",
	client: func(c *ClientConfig) {
		c.CCAPI, c.Sched, c.CongestionScope = CCAPIV2, SchedClass, CongestionScopePath
		c.Topology, c.Classifier = TopologySingle, ClassifierPhased
		c.CodedRetry, c.Memory = CodedRetrySticky, MemoryOwned
	},
	server: func(s *ServerConfig) {
		s.CCAPI, s.Sched, s.CongestionScope = CCAPIV2, SchedClass, CongestionScopePath
		s.Classifier, s.CodedRetry, s.Memory = ClassifierPhased, CodedRetrySticky, MemoryOwned
		s.GatewayAdmission = GatewayAdmissionOwned
	},
}

// workloadArms are the two arms, control first.
func workloadArms() []workloadArm { return []workloadArm{legacyArm, flipArm} }

// workloadRig is a client and a gateway with an emulated path between them.
type workloadRig struct {
	t             *testing.T
	socks         string
	relay         *pathsim.Relay
	client        *Client
	server        *Server
	clientMetrics *metrics.Registry
	serverMetrics *metrics.Registry
}

// newWorkloadRig starts a client and a gateway for arm across path. setup,
// when given, runs on the client and the gateway before they serve.
func newWorkloadRig(t *testing.T, path pathsim.Config, arm workloadArm, setup func(*Client, *Server)) *workloadRig {
	t.Helper()
	pathmodel.Reset()
	t.Cleanup(pathmodel.Reset)

	certificate, roots := testCertificate(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	packetConn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serverMetrics, clientMetrics := metrics.New(), metrics.New()
	serverCfg := ServerConfig{
		ListenAddr: "127.0.0.1:0", Credentials: certificate,
		DestinationPolicy: DestinationPolicy{AllowPrivate: true}, EnableQUIC: true, Logger: logger,
		Metrics: serverMetrics, HandshakeTimeout: 30 * time.Second,
	}
	arm.server(&serverCfg)
	server, err := NewServer(serverCfg)
	if err != nil {
		t.Fatalf("%s gateway: %v", arm.name, err)
	}
	relay, err := pathsim.New("127.0.0.1:0", packetConn.LocalAddr().String(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = relay.Close() })

	clientListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	clientCfg := ClientConfig{
		ListenAddr: clientListener.Addr().String(), RemoteAddr: relay.LocalAddr(),
		Credentials: roots, Transport: TransportQUIC, EnableQUICPool: true,
		Logger: logger, Metrics: clientMetrics,
	}
	arm.client(&clientCfg)
	client, err := NewClient(clientCfg)
	if err != nil {
		t.Fatalf("%s client: %v", arm.name, err)
	}
	if setup != nil {
		setup(client, server)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{}, 2)
	go func() { _ = server.ServePacketConn(ctx, packetConn); done <- struct{}{} }()
	go func() { _ = client.ServeListener(ctx, clientListener); done <- struct{}{} }()
	t.Cleanup(func() {
		cancel()
		for range 2 {
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Errorf("%s: the client or gateway did not stop", arm.name)
				return
			}
		}
	})
	return &workloadRig{
		t: t, socks: clientListener.Addr().String(), relay: relay,
		client: client, server: server, clientMetrics: clientMetrics, serverMetrics: serverMetrics,
	}
}

// dial opens a SOCKS CONNECT to destination through the tunnel.
func (r *workloadRig) dial(destination string) net.Conn {
	r.t.Helper()
	conn := dialTestSOCKS(r.t, r.socks, destination)
	r.t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// wireBytes is every byte the emulated path carried, both directions.
func (r *workloadRig) wireBytes() uint64 {
	up, down := r.relay.Stats()
	return up.BytesIn + down.BytesIn
}

// socksAssociation is one SOCKS UDP ASSOCIATE: the control connection that
// holds it open, the local socket and the relay address to send to.
type socksAssociation struct {
	control net.Conn
	local   *net.UDPConn
	relay   *net.UDPAddr
}

// associate opens a UDP association through the tunnel.
func (r *workloadRig) associate() (*socksAssociation, error) {
	control, err := net.DialTimeout("tcp", r.socks, 5*time.Second)
	if err != nil {
		return nil, err
	}
	fail := func(err error) (*socksAssociation, error) {
		_ = control.Close()
		return nil, err
	}
	_ = control.SetDeadline(time.Now().Add(30 * time.Second))
	if _, err := control.Write([]byte{5, 1, 0}); err != nil {
		return fail(err)
	}
	var method [2]byte
	if _, err := io.ReadFull(control, method[:]); err != nil {
		return fail(err)
	}
	if _, err := control.Write([]byte{5, socks5.CommandUDPAssociate, 0, 1, 0, 0, 0, 0, 0, 0}); err != nil {
		return fail(err)
	}
	var reply [10]byte
	if _, err := io.ReadFull(control, reply[:]); err != nil {
		return fail(err)
	}
	if reply[1] != socks5.ReplySucceeded {
		return fail(io.ErrUnexpectedEOF)
	}
	_ = control.SetDeadline(time.Time{})
	local, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		return fail(err)
	}
	return &socksAssociation{
		control: control, local: local,
		relay: &net.UDPAddr{IP: net.IP(reply[4:8]), Port: int(binary.BigEndian.Uint16(reply[8:10]))},
	}, nil
}

func (a *socksAssociation) Close() error {
	_ = a.local.Close()
	return a.control.Close()
}

// send sends payload to destination through the association.
func (a *socksAssociation) send(destination *net.UDPAddr, payload []byte) error {
	_, err := a.local.WriteToUDP(socksUDPRequest(destination, payload), a.relay)
	return err
}

// receive reads one datagram's payload, waiting until deadline.
func (a *socksAssociation) receive(buf []byte, deadline time.Time) ([]byte, error) {
	_ = a.local.SetReadDeadline(deadline)
	n, _, err := a.local.ReadFromUDP(buf)
	if err != nil {
		return nil, err
	}
	datagram, err := socks5.ReadUDPDatagram(buf[:n])
	if err != nil {
		return nil, err
	}
	return datagram.Payload, nil
}

// socksPacketConn is a net.PacketConn through a SOCKS UDP association, to a
// single remote: what a QUIC client inside the tunnel sends on. It wraps the
// socket rather than embedding it, so quic-go sees a plain PacketConn and
// cannot reach the socket's own batch or ECN paths around the SOCKS header.
type socksPacketConn struct {
	association *socksAssociation
	remote      *net.UDPAddr
	buf         []byte
}

func (c *socksPacketConn) ReadFrom(p []byte) (int, net.Addr, error) {
	for {
		n, _, err := c.association.local.ReadFromUDP(c.buf)
		if err != nil {
			return 0, nil, err
		}
		datagram, err := socks5.ReadUDPDatagram(c.buf[:n])
		if err != nil {
			continue
		}
		return copy(p, datagram.Payload), c.remote, nil
	}
}

func (c *socksPacketConn) WriteTo(p []byte, _ net.Addr) (int, error) {
	if err := c.association.send(c.remote, p); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (c *socksPacketConn) Close() error                  { return c.association.Close() }
func (c *socksPacketConn) LocalAddr() net.Addr           { return c.association.local.LocalAddr() }
func (c *socksPacketConn) SetDeadline(t time.Time) error { return c.association.local.SetDeadline(t) }
func (c *socksPacketConn) SetReadDeadline(t time.Time) error {
	return c.association.local.SetReadDeadline(t)
}
func (c *socksPacketConn) SetWriteDeadline(t time.Time) error {
	return c.association.local.SetWriteDeadline(t)
}

// judgeComparisons reports whether a workload test should apply its bounds
// against legacy: not under the race detector, which slows the endpoints
// several-fold and unequally. Hard assertions apply either way.
func judgeComparisons(t *testing.T) bool {
	t.Helper()
	if workloadRaceEnabled {
		t.Log("race detector on: comparative bounds are reported, not judged")
		return false
	}
	return true
}

// durationQuantile is the nearest-rank q-quantile of samples; zero when there
// are none.
func durationQuantile(samples []time.Duration, q float64) time.Duration {
	if len(samples) == 0 {
		return 0
	}
	sorted := append([]time.Duration(nil), samples...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	rank := int(math.Ceil(q*float64(len(sorted)))) - 1
	return sorted[max(0, min(rank, len(sorted)-1))]
}

// floatQuantile is durationQuantile for plain numbers.
func floatQuantile(samples []float64, q float64) float64 {
	if len(samples) == 0 {
		return 0
	}
	sorted := append([]float64(nil), samples...)
	sort.Float64s(sorted)
	rank := int(math.Ceil(q*float64(len(sorted)))) - 1
	return sorted[max(0, min(rank, len(sorted)-1))]
}
