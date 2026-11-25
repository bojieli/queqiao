package pep

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apernet/quic-go"
	quiccongestion "github.com/apernet/quic-go/congestion"

	wancongestion "github.com/bojieli/queqiao/internal/congestion"
	"github.com/bojieli/queqiao/internal/identity"
	"github.com/bojieli/queqiao/internal/pathsim"
)

var pathScopeConfig = congestionConfig{kind: CongestionErasure, ccAPI: CCAPIV2, sched: SchedClass, scope: CongestionScopePath}

// pathBed is a gateway-side listener behind one emulated bottleneck, which
// any number of client connections can be dialled through: they share the
// bottleneck, and under congestion=path they share one controller.
type pathBed struct {
	t         *testing.T
	listener  *quic.Listener
	relay     *pathsim.Relay
	clientTLS func() *quic.Conn
	// received counts the bytes the gateway side read, per client
	// connection, in dial order.
	mu       sync.Mutex
	received map[string]*atomic.Int64
	// principal is this bed's client identity, so each bed starts from a
	// cold controller and a cold path model.
	principal string
}

func newPathBed(t *testing.T, cfg pathsim.Config) *pathBed {
	t.Helper()
	serverCredentials, clientCredentials := testCertificate(t)
	serverTLS, err := identity.ServerTLSConfig(serverCredentials, defaultALPN, false)
	if err != nil {
		t.Fatal(err)
	}
	clientTLS, err := tlsClientConfig(clientCredentials, nil)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := quic.ListenAddr("127.0.0.1:0", serverTLS, quicConfig(flowWindows{}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	relay, err := pathsim.New("127.0.0.1:0", listener.Addr().String(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = relay.Close() })
	b := &pathBed{t: t, listener: listener, relay: relay, received: map[string]*atomic.Int64{}, principal: newClientPathPrincipal()}
	go func() {
		for {
			conn, err := listener.Accept(context.Background())
			if err != nil {
				return
			}
			t.Cleanup(func() { _ = conn.CloseWithError(0, "") })
			go func() {
				for {
					stream, err := conn.AcceptStream(context.Background())
					if err != nil {
						return
					}
					go func() {
						// Each stream starts by naming the client connection
						// it belongs to: the relay hides the client's address.
						var n [1]byte
						if _, err := io.ReadFull(stream, n[:]); err != nil {
							return
						}
						name := make([]byte, n[0])
						if _, err := io.ReadFull(stream, name); err != nil {
							return
						}
						counter := b.counter(string(name))
						buf := make([]byte, 64<<10)
						for {
							n, err := stream.Read(buf)
							counter.Add(int64(n))
							if err != nil {
								return
							}
						}
					}()
				}
			}()
		}
	}()
	b.clientTLS = func() *quic.Conn {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		conn, err := quic.DialAddr(ctx, relay.LocalAddr(), clientTLS, quicConfig(flowWindows{}))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.CloseWithError(0, "") })
		return conn
	}
	return b
}

func (b *pathBed) counter(remote string) *atomic.Int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	c, ok := b.received[remote]
	if !ok {
		c = &atomic.Int64{}
		b.received[remote] = c
	}
	return c
}

// dial opens a client connection through the bottleneck, attached to the
// path's controller.
func (b *pathBed) dial() (*quic.Conn, *wancongestion.PathAttachment) {
	conn := b.clientTLS()
	cfg := pathScopeConfig
	cfg.principal = func() string { return b.principal }
	att, ok := configureQUICController(conn, cfg).(*wancongestion.PathAttachment)
	if !ok {
		b.t.Fatal("congestion=path did not install a path attachment")
	}
	return conn, att
}

// bulk writes to one stream of conn until ctx ends.
func bulk(ctx context.Context, conn *quic.Conn) {
	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return
	}
	go func() {
		<-ctx.Done()
		stream.CancelWrite(0)
	}()
	if _, err := stream.Write(streamName(conn)); err != nil {
		return
	}
	chunk := make([]byte, 32<<10)
	for ctx.Err() == nil {
		if _, err := stream.Write(chunk); err != nil {
			return
		}
	}
}

// streamName is the header naming conn to the bed's reader.
func streamName(conn *quic.Conn) []byte {
	name := conn.LocalAddr().String()
	return append([]byte{byte(len(name))}, name...)
}

// waitAttached waits for n attachments, which are installed on each
// connection's run loop.
func waitAttached(t *testing.T, ctl *wancongestion.PathController, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for ctl.Attachments() != n {
		if time.Now().After(deadline) {
			t.Fatalf("%d attachments, want %d", ctl.Attachments(), n)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// checkFlight asserts that the path's flight is the sum of its counted
// gauges.
func checkFlight(t *testing.T, s wancongestion.PathControllerStats) {
	t.Helper()
	var sum quiccongestion.ByteCount
	for epoch, g := range s.Gauges {
		if g < 0 {
			t.Fatalf("attachment %d gauge %d", epoch, g)
		}
		if s.Counted[epoch] {
			sum += g
		}
	}
	if s.Flight != sum {
		t.Fatalf("path flight %d, sum of counted gauges %d", s.Flight, sum)
	}
}

// Test 1 of §15 on real fork send loops: k = 1, 2 and 4 connections over one
// emulated bottleneck, each attached to the same PathController and each
// saturated. They deliver together what one connection delivers, and share
// it: no connection is starved, and none takes a multiple of another's.
func TestConnectionsOnOnePathShareOneController(t *testing.T) {
	if testing.Short() {
		t.Skip("emulated-path timing test")
	}
	path := pathsim.Config{OneWayDelay: 10 * time.Millisecond, RateBytesPerSec: 1_500_000, Seed: 11}
	const run = 4 * time.Second
	aggregate := map[int]float64{}
	for _, k := range []int{1, 2, 4} {
		pathAuthorities.forget()
		bed := newPathBed(t, path)
		conns := make([]*quic.Conn, k)
		var ctl *wancongestion.PathController
		for i := range conns {
			var att *wancongestion.PathAttachment
			conns[i], att = bed.dial()
			if ctl == nil {
				ctl = att.Controller()
			} else if att.Controller() != ctl {
				t.Fatalf("k=%d: connection %d attached to another controller", k, i)
			}
		}
		waitAttached(t, ctl, k)
		ctx, cancel := context.WithTimeout(context.Background(), run)
		for _, c := range conns {
			go bulk(ctx, c)
		}
		// The accounting holds throughout.
		for ctx.Err() == nil {
			time.Sleep(50 * time.Millisecond)
			checkFlight(t, ctl.Stats())
		}
		cancel()
		var total float64
		var per []float64
		for _, c := range conns {
			got := float64(bed.counter(c.LocalAddr().String()).Load())
			per = append(per, got)
			total += got
		}
		aggregate[k] = total / run.Seconds()
		s := ctl.Stats()
		t.Logf("k=%d: aggregate %.2f MB/s, per connection %v; refusals window %d pacing %d turn %d, lapsed %d",
			k, aggregate[k]/1e6, per, s.RefusedWindow, s.RefusedPacing, s.RefusedTurn, s.LapsedClaims)
		if total == 0 {
			t.Fatalf("k=%d: nothing was delivered", k)
		}
		for i, b := range per {
			if b <= 0 || b < 0.4*total/float64(k) {
				t.Errorf("k=%d: connection %d delivered %.0f of %.0f bytes; starved", k, i, b, total)
			}
		}
		if m := ctl.Telemetry().StateMisses; m != 0 {
			t.Errorf("k=%d: %d acknowledgements found no sampler state", k, m)
		}
	}
	for _, k := range []int{2, 4} {
		if r := aggregate[k] / aggregate[1]; r < 0.75 || r > 1.3 {
			t.Errorf("k=%d connections delivered %.2fx what one did through one controller", k, r)
		}
	}
}

// A connection that closes mid-transfer detaches: its flight leaves the
// path's sum and its records are discarded, and the connection left on the
// path takes the whole of it.
func TestClosingAConnectionReconcilesThePathsFlight(t *testing.T) {
	if testing.Short() {
		t.Skip("emulated-path timing test")
	}
	pathAuthorities.forget()
	bed := newPathBed(t, pathsim.Config{OneWayDelay: 10 * time.Millisecond, RateBytesPerSec: 1_500_000, Seed: 12})
	a, attA := bed.dial()
	b, _ := bed.dial()
	ctl := attA.Controller()
	waitAttached(t, ctl, 2)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ctxA, cancelA := context.WithCancel(ctx)
	go bulk(ctxA, a)
	go bulk(ctx, b)
	time.Sleep(2 * time.Second)
	before := float64(bed.counter(b.LocalAddr().String()).Load())
	beforeA := float64(bed.counter(a.LocalAddr().String()).Load())
	if ctl.Stats().Flight == 0 {
		t.Fatal("nothing in flight mid-transfer")
	}
	cancelA()
	_ = a.CloseWithError(0, "")
	waitAttached(t, ctl, 1)
	s := ctl.Stats()
	checkFlight(t, s)
	if len(s.Gauges) != 1 {
		t.Fatalf("%d gauges after one connection of two closed", len(s.Gauges))
	}
	if s.Discarded == 0 {
		t.Fatal("the closed connection's records were not discarded")
	}
	start := time.Now()
	time.Sleep(2 * time.Second)
	after := (float64(bed.counter(b.LocalAddr().String()).Load()) - before) / time.Since(start).Seconds()
	shared := (before + beforeA) / 2
	t.Logf("two connections %.2f MB/s together; the one left %.2f MB/s", shared/1e6, after/1e6)
	if after < 0.7*shared {
		t.Errorf("the connection left on the path delivered %.2f MB/s, against %.2f MB/s for the two together", after/1e6, shared/1e6)
	}
	checkFlight(t, ctl.Stats())
}

// Test 17 of §15, NAT rebinding: a migration that changes only the remote
// port keeps the connection on its path's controller with its flight; one
// that changes the remote host moves it, with its flight, to the other
// path's controller.
func TestRebindingKeepsTheAuthorityAndAPathChangeMovesIt(t *testing.T) {
	pathAuthorities.forget()
	bed := newPathBed(t, pathsim.Config{OneWayDelay: 5 * time.Millisecond, Seed: 13})
	conn, att := bed.dial()
	ctl := att.Controller()
	waitAttached(t, ctl, 1)
	local, remote := conn.LocalAddr().(*net.UDPAddr), conn.RemoteAddr().(*net.UDPAddr)
	rebound := &net.UDPAddr{IP: remote.IP, Port: remote.Port + 1}
	att.OnPathMigrated(0, local, local, remote, rebound)
	if att.Controller() != ctl || ctl.Attachments() != 1 {
		t.Fatal("a port-only rebinding moved the connection off its path's controller")
	}
	elsewhere := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 2), Port: remote.Port}
	att.OnPathMigrated(0, local, local, rebound, elsewhere)
	moved := att.Controller()
	if moved == ctl || ctl.Attachments() != 0 || moved.Attachments() != 1 {
		t.Fatalf("a new remote host did not move the connection: same=%v, attachments %d and %d", moved == ctl, ctl.Attachments(), moved.Attachments())
	}
}

// §4.1: a gateway connection starts on a standalone controller, and only an
// authorised data connection attaches to its path's controller. The
// provisional controller reports to no path model and attaches to nothing.
func TestAProvisionalConnectionAttachesToNothing(t *testing.T) {
	pathAuthorities.forget()
	bed := newPathBed(t, pathsim.Config{OneWayDelay: time.Millisecond, Seed: 14})
	conn := bed.clientTLS()
	cfg := pathScopeConfig
	cfg.provisional = true
	c := configureQUICController(conn, cfg)
	if _, ok := c.(*wancongestion.ErasurePathSender); !ok {
		t.Fatalf("a provisional connection got %T", c)
	}
	pathAuthorities.mu.Lock()
	n := len(pathAuthorities.controllers)
	pathAuthorities.mu.Unlock()
	if n != 0 {
		t.Fatalf("a provisional connection created %d path controllers", n)
	}
	if !isClassScheduled(conn) {
		t.Fatal("the provisional controller does not schedule classes")
	}
	// Its data, sent through the provisional controller, moves.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Write(append(streamName(conn), make([]byte, 64<<10)...)); err != nil {
		t.Fatal(err)
	}
	_ = stream.Close()
	for bed.counter(conn.LocalAddr().String()).Load() < 64<<10 {
		if ctx.Err() != nil {
			t.Fatal("the provisional connection's data did not arrive")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// The old-client case (§12): a client that isolates opens several data
// connections to a gateway running congestion=path. The gateway attaches
// every one of that device's data connections to one controller, keyed by
// its principal, and a closed one detaches.
func TestAGatewayAttachesEveryDataConnectionOfADeviceToOneController(t *testing.T) {
	pathAuthorities.forget()
	certificate, roots := testCertificate(t)
	packetConn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(ServerConfig{
		ListenAddr: "127.0.0.1:0", Credentials: certificate, EnableQUIC: true,
		DestinationPolicy: DestinationPolicy{AllowPrivate: true},
		CCAPI:             CCAPIV2, Sched: SchedClass, CongestionScope: CongestionScopePath,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = server.ServePacketConn(ctx, packetConn) }()
	clientTLS, err := tlsClientConfig(roots, nil)
	if err != nil {
		t.Fatal(err)
	}
	const lanes = 3
	conns := make([]*quic.Conn, lanes)
	for i := range conns {
		dialCtx, done := context.WithTimeout(ctx, 10*time.Second)
		conns[i], err = quic.DialAddr(dialCtx, packetConn.LocalAddr().String(), clientTLS, quicConfig(flowWindows{}))
		done()
		if err != nil {
			t.Fatal(err)
		}
		defer conns[i].CloseWithError(0, "")
	}
	gateway := func() (controllers, attachments int) {
		pathAuthorities.mu.Lock()
		defer pathAuthorities.mu.Unlock()
		for name, c := range pathAuthorities.controllers {
			if len(name) > len("principal:") && name[:len("principal:")] == "principal:" {
				controllers++
				attachments += c.Attachments()
			}
		}
		return
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		c, a := gateway()
		if c == 1 && a == lanes {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the gateway has %d controllers with %d attachments for one device's %d connections", c, a, lanes)
		}
		time.Sleep(10 * time.Millisecond)
	}
	_ = conns[0].CloseWithError(0, "")
	for {
		if _, a := gateway(); a == lanes-1 {
			break
		}
		if time.Now().After(deadline.Add(5 * time.Second)) {
			_, a := gateway()
			t.Fatalf("%d attachments after one of %d connections closed", a, lanes)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Two clients in one daemon -- two credential profiles -- are two
// principals: their connections to the same gateway over the same uplink
// attach to different controllers, with different path models.
func TestTwoClientsInOneProcessHaveTheirOwnControllers(t *testing.T) {
	pathAuthorities.forget()
	bed := newPathBed(t, pathsim.Config{OneWayDelay: time.Millisecond, Seed: 15})
	one, two := newClientPathPrincipal(), newClientPathPrincipal()
	if one == two {
		t.Fatal("two clients share a principal")
	}
	var atts []*wancongestion.PathAttachment
	for _, p := range []string{one, two} {
		cfg := pathScopeConfig
		cfg.principal = func() string { return p }
		att, ok := configureQUICController(bed.clientTLS(), cfg).(*wancongestion.PathAttachment)
		if !ok {
			t.Fatal("no path attachment")
		}
		atts = append(atts, att)
	}
	if atts[0].Controller() == atts[1].Controller() {
		t.Fatal("two clients' connections share one controller")
	}
}

// Under congestion=path a connection's coded readers are wired to the
// path controller: its window sizes from its own source rate, and its burst
// statistic is the controller's datagram one once that has evidence (until
// then, the model's).
func TestCodedReadersFollowThePathController(t *testing.T) {
	pathAuthorities.forget()
	bed := newPathBed(t, pathsim.Config{OneWayDelay: time.Millisecond, Seed: 16})
	conn, att := bed.dial()
	waitAttached(t, att.Controller(), 1)
	if !isPathScoped(conn) {
		t.Fatal("a path-scoped connection is not marked for source-rate sizing")
	}
	m := pathModelOf(conn)
	if m.datagramBurst.Load() == nil {
		t.Fatal("the connection's model has no datagram burst source")
	}
	if _, ok := att.Controller().DatagramBurstFactor(); ok {
		t.Fatal("a fresh controller claims datagram burst evidence")
	}
	if got, want := m.Current().BurstFactor, m.model.Load().Current().BurstFactor; got != want {
		t.Fatalf("without datagram evidence the reader sees burst %.2f, want the model's %.2f", got, want)
	}
	plain := bed.clientTLS()
	if isPathScoped(plain) {
		t.Fatal("a connection with no path controller is marked path-scoped")
	}
}

// An uplink change starts a new path controller even when the new network
// hands out the same source address as the old one: the client's path key
// carries its uplink generation. A connection dialled before the change
// keeps its controller.
func TestAnUplinkChangeWithTheSameAddressStartsANewController(t *testing.T) {
	pathAuthorities.forget()
	_, credentials := testCertificate(t)
	client, err := NewClient(ClientConfig{
		ListenAddr: "127.0.0.1:0", RemoteAddr: "127.0.0.1:1", Credentials: credentials,
		CCAPI: CCAPIV2, Sched: SchedClass, CongestionScope: CongestionScopePath,
	})
	if err != nil {
		t.Fatal(err)
	}
	bed := newPathBed(t, pathsim.Config{OneWayDelay: time.Millisecond, Seed: 17})
	dial := func() *wancongestion.PathAttachment {
		cfg := pathScopeConfig
		cfg.principal = client.pathPrincipalNow
		att, ok := configureQUICController(bed.clientTLS(), cfg).(*wancongestion.PathAttachment)
		if !ok {
			t.Fatal("no path attachment")
		}
		return att
	}
	before, again := dial(), dial()
	if before.Controller() != again.Controller() {
		t.Fatal("two connections on one uplink got different controllers")
	}
	// A dial whose configuration was taken before the change and that is
	// installed after it -- an attempt that outlived an uplink reset.
	captured := pathScopeConfig
	captured.principal = client.pathPrincipalNow
	late := bed.clientTLS()
	waitAttached(t, before.Controller(), 2)
	oldPrincipal := client.pathPrincipalNow()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	client.onUplinkChanged(ctx)
	after := dial()
	if after.Controller() == before.Controller() {
		t.Fatal("after an uplink change with the same source address the new connection reuses the old network's controller")
	}
	if att := configureQUICController(late, captured).(*wancongestion.PathAttachment); att.Controller() != after.Controller() {
		t.Fatal("a connection installed after the uplink change resolved the old generation's controller")
	}
	// A migration after the change -- with the addresses unchanged --
	// moves a connection to the current generation's controller, the one a
	// connection dialled after the change got: the whole key is compared.
	// The rekey is exercised on its own, with the client's principal before
	// the change, as the run loop would call it.
	conn := bed.clientTLS()
	local, remote := conn.LocalAddr(), conn.RemoteAddr()
	_, addrs := resolvePath(local, remote)
	var followed string
	rekey := pathRekey(client.pathPrincipalNow, oldPrincipal+"|"+addrs, 1200, func(next string) { followed = next })
	if c := rekey(local, local, remote, remote); c != after.Controller() || followed != client.pathPrincipalNow()+"|"+addrs {
		t.Fatalf("a migration after the uplink change moved the connection to %p, readers following %q; want the current generation's %p", c, followed, after.Controller())
	}
	same := pathRekey(client.pathPrincipalNow, client.pathPrincipalNow()+"|"+addrs, 1200, func(string) {})
	if same(local, local, remote, remote) != nil {
		t.Fatal("a migration that kept the whole key moved the connection")
	}
}

// A data connection whose principal is no longer authorised when the
// gateway would attach it to its path controller is closed, rather than left
// to carry data later outside the device's path budget.
func TestAGatewayClosesADataConnectionItCannotAttach(t *testing.T) {
	pathAuthorities.forget()
	saved := authorizePathPrincipal
	authorizePathPrincipal = func(*identity.Store, identity.Principal, time.Time) error { return errors.New("account disabled") }
	t.Cleanup(func() { authorizePathPrincipal = saved })
	certificate, roots := testCertificate(t)
	packetConn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(ServerConfig{
		ListenAddr: "127.0.0.1:0", Credentials: certificate, EnableQUIC: true,
		CCAPI: CCAPIV2, Sched: SchedClass, CongestionScope: CongestionScopePath,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = server.ServePacketConn(ctx, packetConn) }()
	clientTLS, err := tlsClientConfig(roots, nil)
	if err != nil {
		t.Fatal(err)
	}
	dialCtx, done := context.WithTimeout(ctx, 10*time.Second)
	defer done()
	conn, err := quic.DialAddr(dialCtx, packetConn.LocalAddr().String(), clientTLS, quicConfig(flowWindows{}))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseWithError(0, "")
	select {
	case <-conn.Context().Done():
	case <-time.After(5 * time.Second):
		t.Fatal("the gateway kept a data connection it could not attach to its path controller")
	}
}

// A resume and a change to a network with the same address seen on one
// tick still start a new uplink generation before the path is prewarmed.
func TestAResumeWithAnUplinkChangeStartsANewGeneration(t *testing.T) {
	_, credentials := testCertificate(t)
	client, err := NewClient(ClientConfig{
		ListenAddr: "127.0.0.1:0", RemoteAddr: "127.0.0.1:1", Credentials: credentials,
		CCAPI: CCAPIV2, Sched: SchedClass, CongestionScope: CongestionScopePath,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	before := client.pathPrincipalNow()
	client.onUplinkTick(ctx, true, false, "", "")
	if client.pathPrincipalNow() != before {
		t.Fatal("a resume without a change started a new generation")
	}
	client.onUplinkTick(ctx, true, true, "10.0.0.2|en0|1|10.0.0.1", "10.0.0.2|en0|1|10.0.0.254")
	if client.pathPrincipalNow() == before {
		t.Fatal("a resume that saw a same-address network change kept the old generation")
	}
}
