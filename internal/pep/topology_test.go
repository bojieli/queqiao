package pep

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bojieli/queqiao/internal/classifier"
	"github.com/bojieli/queqiao/internal/metrics"
)

// newBundleClient is the stage-7 client configuration under test: the
// congestion=path bundle with classifier=phased, as the stage 6/7 campaigns
// run it (design §15.3), and the given topology.
func newBundleClientConfig(topology Topology) ClientConfig {
	return ClientConfig{
		Transport: TransportQUIC, EnableQUICPool: true,
		CCAPI: CCAPIV2, Sched: SchedClass, CongestionScope: CongestionScopePath,
		Classifier: ClassifierPhased, Topology: topology,
	}
}

func TestTopologyValidator(t *testing.T) {
	for _, c := range []struct {
		topology Topology
		scope    CongestionScope
		fanOut   int
		ok       bool
	}{
		{"", "", 0, true},
		{TopologyIsolated, CongestionScopeConnection, 0, true},
		{TopologyIsolated, CongestionScopePath, 1, true},
		{TopologySingle, CongestionScopePath, 0, true},
		{TopologySingle, CongestionScopePath, maxTopologyFanOut, true},
		// One connection per path needs the path controller's arbitration.
		{TopologySingle, CongestionScopeConnection, 0, false},
		{TopologySingle, "", 0, false},
		// A fan-out is a policy of the single topology only.
		{TopologyIsolated, CongestionScopePath, 2, false},
		{TopologySingle, CongestionScopePath, maxTopologyFanOut + 1, false},
		{TopologySingle, CongestionScopePath, -1, false},
		{"mesh", CongestionScopePath, 0, false},
	} {
		err := ValidateTopology(c.topology, c.scope, c.fanOut)
		if (err == nil) != c.ok {
			t.Errorf("ValidateTopology(%q, %q, %d) = %v, want ok=%v", c.topology, c.scope, c.fanOut, err, c.ok)
		}
	}
	if got := Bundle(CCAPIV2, MemoryLegacy, CodedRetryLegacy, GatewayAdmissionLegacy, SchedClass, ClassifierPhased, CongestionScopePath, TopologySingle); got != "cc-api=v2,sched=class,congestion=path,topology=single,classifier=phased" {
		t.Errorf("bundle %q", got)
	}
	if got := Bundle(CCAPIV2, MemoryLegacy, CodedRetryLegacy, GatewayAdmissionLegacy, SchedClass, ClassifierLegacy, CongestionScopePath, TopologyIsolated); got != "cc-api=v2,sched=class,congestion=path" {
		t.Errorf("isolated topology is the default and not named: %q", got)
	}

	_, roots := testCertificate(t)
	base := func(mutate func(*ClientConfig)) error {
		cfg := newBundleClientConfig(TopologySingle)
		cfg.ListenAddr, cfg.RemoteAddr, cfg.Credentials = "127.0.0.1:0", "127.0.0.1:1", roots
		cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
		mutate(&cfg)
		_, err := NewClient(cfg)
		return err
	}
	if err := base(func(*ClientConfig) {}); err != nil {
		t.Fatalf("the stage-7 bundle was rejected: %v", err)
	}
	for name, mutate := range map[string]func(*ClientConfig){
		"without the path controller": func(c *ClientConfig) { c.CongestionScope = CongestionScopeConnection },
		"without the pool":            func(c *ClientConfig) { c.EnableQUICPool = false },
		"fan-out beyond the iOS pool": func(c *ClientConfig) {
			c.TopologyFanOut = 3
			c.MemoryLimits = iosMemoryLimits()
			c.ChunkSize = iosChunkSize
		},
		"fan-out without single": func(c *ClientConfig) { c.Topology = TopologyIsolated; c.TopologyFanOut = 2 },
	} {
		if err := base(mutate); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	// iOS has one secondary connection: a fan-out of two fits.
	if err := base(func(c *ClientConfig) {
		c.TopologyFanOut = 2
		c.MemoryLimits = iosMemoryLimits()
		c.ChunkSize = iosChunkSize
	}); err != nil {
		t.Fatalf("a fan-out within the iOS pool was rejected: %v", err)
	}
}

// The placement decision itself: under topology=single nothing moves a flow
// to a second connection unless a fan-out is configured, and then the
// trigger is exactly isolation's -- a bulk flow sharing the preferred
// connection -- within the fan-out's connection budget.
func TestSingleTopologyIsolatesOnlyForAnExplicitFanOut(t *testing.T) {
	bulk := flowSnapshot{Class: classifier.ClassBulk}
	quiet := flowSnapshot{Class: classifier.ClassInteractive}
	for _, c := range []struct {
		name     string
		topology Topology
		fanOut   int
		shared   int64
		snapshot flowSnapshot
		want     bool
		budget   int
	}{
		{"isolated: bulk sharing the pool isolates", TopologyIsolated, 0, 2, bulk, true, isolatedBulkConns},
		{"isolated: a lone bulk flow stays", TopologyIsolated, 0, 1, bulk, false, isolatedBulkConns},
		{"isolated: interactive stays", TopologyIsolated, 0, 2, quiet, false, isolatedBulkConns},
		{"single: bulk sharing the connection stays", TopologySingle, 0, 8, bulk, false, 0},
		{"single, fan-out 1 is off", TopologySingle, 1, 8, bulk, false, 0},
		{"single, fan-out 3: bulk sharing fans out", TopologySingle, 3, 2, bulk, true, 2},
		{"single, fan-out 3: a lone bulk flow stays", TopologySingle, 3, 1, bulk, false, 2},
		{"single, fan-out 3: interactive stays", TopologySingle, 3, 2, quiet, false, 2},
	} {
		c := c
		t.Run(c.name, func(t *testing.T) {
			client := &Client{cfg: ClientConfig{Topology: c.topology, TopologyFanOut: c.fanOut}}
			client.memoryLimits.maxBulkConnections = isolatedBulkConns
			client.quicPoolActive.Store(c.shared)
			if got := client.secondConnectionWanted(c.snapshot); got != c.want {
				t.Fatalf("secondConnectionWanted = %v, want %v", got, c.want)
			}
			if got := client.maxBulkConns(); got != c.budget {
				t.Fatalf("bulk pool budget %d, want %d", got, c.budget)
			}
		})
	}
}

// Connection-level flow control is not the new bottleneck. Under isolation
// each bulk flow on a secondary connection had a connection receive window
// of its own; on one connection they share one. With the desktop windows the
// shared window is far above any path's product and is left alone; with the
// mobile profiles' fixed windows the one connection's ceiling covers what the
// pooled connection and the isolated ones could hold together, for any
// number of bulk flows, and never more than the isolated topology committed.
func TestSingleConnectionWindowsKeepIsolatedCapacity(t *testing.T) {
	const (
		kib = 1024
		mib = 1024 * kib
	)
	for _, p := range []struct {
		name    string
		windows flowWindows
		maxBulk int
	}{
		{"desktop", flowWindows{}, isolatedBulkConns},
		// mobile/core/resources.go.
		{"ios", flowWindows{stream: 64 * kib, maxStream: 64 * kib, connection: 2 * mib, maxConnection: 2 * mib, maxStreams: 32}, 1},
		{"android", flowWindows{stream: mib, maxStream: mib, connection: 4 * mib, maxConnection: 4 * mib, maxStreams: 64}, 2},
	} {
		t.Run(p.name, func(t *testing.T) {
			single := singleConnectionWindows(p.windows, p.maxBulk)
			if err := single.validate(); err != nil {
				t.Fatal(err)
			}
			_, conn, streamMax, connMax, _ := p.windows.resolved()
			sInitial, sConn, _, sConnMax, _ := single.resolved()
			if sInitial != func() uint64 { s, _, _, _, _ := p.windows.resolved(); return s }() {
				t.Fatal("the stream window changed")
			}
			if conn == connMax && sConn != sConnMax {
				t.Fatalf("a fixed connection window became auto-tuned: %d..%d", sConn, sConnMax)
			}
			for flows := 1; flows <= 64; flows++ {
				// Isolated: the first flow and any beyond the pool stay on
				// the pooled connection; up to maxBulk have their own.
				own := min(flows-1, p.maxBulk)
				pooled := flows - own
				isolated := min(uint64(pooled)*streamMax, connMax) + uint64(own)*min(streamMax, connMax)
				one := min(uint64(flows)*streamMax, sConnMax)
				if one < isolated && one < maxConnectionReceiveWindow {
					t.Errorf("%d bulk flows: one connection can hold %d bytes in flight, isolated %d", flows, one, isolated)
				}
			}
			if commitment := connMax * uint64(1+p.maxBulk); sConnMax > commitment {
				t.Errorf("one connection's window %d exceeds the isolated topology's commitment %d", sConnMax, commitment)
			}
			t.Logf("connection window %d..%d -> %d..%d", conn, connMax, sConn, sConnMax)
		})
	}
	if w := singleConnectionWindows(flowWindows{}, isolatedBulkConns); w != (flowWindows{}) {
		t.Fatalf("desktop windows changed: %+v", w)
	}
}

// topologyRig is a gateway and a client on loopback, with a destination that
// streams each flow a paced download, so a flow lives long enough to be
// classified BULK and, under isolation, to be isolated.
type topologyRig struct {
	t           *testing.T
	server      *Server
	client      *Client
	clientAddr  string
	destination string
	cancel      context.CancelFunc
	done        chan error
}

const (
	rigDownload = 1536 * 1024
	rigRate     = 512 * 1024 // bytes per second per flow: three seconds each
)

func newTopologyRig(t *testing.T, serverCfg ServerConfig, clientCfg ClientConfig) *topologyRig {
	t.Helper()
	destinationListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = destinationListener.Close() })
	go pacedDownloadDestination(destinationListener, rigDownload, rigRate)

	certificate, roots := testCertificate(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	packetConn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serverCfg.ListenAddr, serverCfg.Credentials = "127.0.0.1:0", certificate
	serverCfg.DestinationPolicy = DestinationPolicy{AllowPrivate: true}
	serverCfg.EnableQUIC, serverCfg.Logger = true, logger
	if serverCfg.Metrics == nil {
		serverCfg.Metrics = metrics.New()
	}
	server, err := NewServer(serverCfg)
	if err != nil {
		t.Fatal(err)
	}
	clientListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	clientCfg.ListenAddr, clientCfg.RemoteAddr, clientCfg.Credentials = clientListener.Addr().String(), packetConn.LocalAddr().String(), roots
	clientCfg.Logger = logger
	if clientCfg.Metrics == nil {
		clientCfg.Metrics = metrics.New()
	}
	client, err := NewClient(clientCfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &topologyRig{
		t: t, server: server, client: client, clientAddr: clientListener.Addr().String(),
		destination: destinationListener.Addr().String(), cancel: cancel, done: make(chan error, 2),
	}
	go func() { r.done <- server.ServePacketConn(ctx, packetConn) }()
	go func() { r.done <- client.ServeListener(ctx, clientListener) }()
	t.Cleanup(r.stop)
	return r
}

func (r *topologyRig) stop() {
	if r.cancel == nil {
		return
	}
	r.cancel()
	r.cancel = nil
	for range 2 {
		select {
		case <-r.done:
		case <-time.After(5 * time.Second):
			r.t.Error("service shutdown timeout")
			return
		}
	}
}

// download opens n flows at once, each reading its whole paced download, and
// returns the first error.
func (r *topologyRig) download(n int, during func()) error {
	var wg sync.WaitGroup
	errs := make(chan error, n)
	conns := make([]net.Conn, n)
	for i := range conns {
		conns[i] = dialTestSOCKS(r.t, r.clientAddr, r.destination)
	}
	for i, conn := range conns {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer conn.Close()
			_ = conn.SetDeadline(time.Now().Add(40 * time.Second))
			got, err := io.ReadAll(conn)
			if err == nil {
				err = checkDownload(got)
			}
			if err != nil {
				errs <- fmt.Errorf("flow %d: %w", i, err)
			}
		}()
	}
	if during != nil {
		during()
	}
	wg.Wait()
	close(errs)
	return <-errs
}

func checkDownload(got []byte) error {
	if len(got) != rigDownload {
		return fmt.Errorf("downloaded %d bytes, want %d", len(got), rigDownload)
	}
	for i, b := range got {
		if b != byte(i%251) {
			return fmt.Errorf("byte %d is %d, want %d", i, b, byte(i%251))
		}
	}
	return nil
}

// pacedDownloadDestination sends every connection size bytes of a known
// pattern at rate bytes per second, then closes it.
func pacedDownloadDestination(listener net.Listener, size, rate int) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		go func() {
			defer conn.Close()
			const piece = 16 * 1024
			buf := make([]byte, piece)
			start := time.Now()
			for sent := 0; sent < size; {
				n := min(piece, size-sent)
				for i := range n {
					buf[i] = byte((sent + i) % 251)
				}
				if err := writeFull(conn, buf[:n]); err != nil {
					return
				}
				sent += n
				if ahead := time.Duration(float64(sent)/float64(rate)*float64(time.Second)) - time.Since(start); ahead > 0 {
					time.Sleep(ahead)
				}
			}
		}()
	}
}

// pathAttachments sums the attachments of every controller whose key starts
// with prefix: a client's own path, or a gateway's paths for its clients.
func pathAttachments(prefix string) (controllers, attachments int) {
	pathAuthorities.mu.Lock()
	defer pathAuthorities.mu.Unlock()
	for name, c := range pathAuthorities.controllers {
		if strings.HasPrefix(name, prefix) {
			controllers++
			attachments += c.Attachments()
		}
	}
	return controllers, attachments
}

// The stage-7 behaviour end to end, with classifier=phased on. Four
// concurrent bulk downloads share the pooled connection. Under isolation
// (the stage-6 bundle) they leave it for the bulk pool; under
// topology=single the same flows stay streams on the one connection, every
// byte arrives, and the client's path controller has that one attachment.
func TestSingleTopologyKeepsConcurrentBulkFlowsOnOneConnection(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-second end-to-end transfer")
	}
	for _, arm := range []struct {
		topology Topology
		fanOut   int
	}{{TopologyIsolated, 0}, {TopologySingle, 0}, {TopologySingle, 2}} {
		topology := arm.topology
		t.Run(fmt.Sprintf("%s fan-out %d", topology, arm.fanOut), func(t *testing.T) {
			pathAuthorities.forget()
			clientMetrics := metrics.New()
			cfg := newBundleClientConfig(topology)
			cfg.Metrics = clientMetrics
			cfg.TopologyFanOut = arm.fanOut
			rig := newTopologyRig(t, ServerConfig{
				CCAPI: CCAPIV2, Sched: SchedClass, CongestionScope: CongestionScopePath, Classifier: ClassifierPhased,
			}, cfg)
			maxAttached := 0
			started := time.Now()
			defer func() { t.Logf("downloads took %v", time.Since(started)) }()
			if err := rig.download(4, func() {
				deadline := time.Now().Add(2500 * time.Millisecond)
				for time.Now().Before(deadline) {
					_, n := pathAttachments(rig.client.pathPrincipal + "|")
					maxAttached = max(maxAttached, n)
					time.Sleep(20 * time.Millisecond)
				}
			}); err != nil {
				t.Fatal(err)
			}
			isolations := clientMetrics.Snapshot().BulkIsolations
			t.Logf("bulk isolations %d, bulk connections %d, most client attachments %d", isolations, rig.client.bulkConnCount(), maxAttached)
			switch topology {
			case TopologyIsolated:
				// The scenario does provoke isolation, so the single arm's
				// result is the switch's doing.
				if isolations == 0 || maxAttached < 2 {
					t.Fatalf("isolated topology did not isolate: %d isolations, %d attachments", isolations, maxAttached)
				}
			case TopologySingle:
				if arm.fanOut > 1 {
					// The explicit fan-out triggers on the same condition
					// and stays inside its budget: the preferred
					// connection plus fanOut-1, all one controller's.
					controllers, _ := pathAttachments(rig.client.pathPrincipal + "|")
					if isolations == 0 || rig.client.bulkConnCount() > arm.fanOut-1 || maxAttached > arm.fanOut || controllers != 1 {
						t.Fatalf("fan-out %d: %d isolations, %d bulk connections, %d attachments over %d controllers",
							arm.fanOut, isolations, rig.client.bulkConnCount(), maxAttached, controllers)
					}
					return
				}
				if isolations != 0 || rig.client.bulkConnCount() != 0 {
					t.Fatalf("topology=single isolated %d flows onto %d bulk connections", isolations, rig.client.bulkConnCount())
				}
				if maxAttached != 1 {
					t.Fatalf("topology=single used %d connections on its path, want 1", maxAttached)
				}
			}
		})
	}
}

// Rescue still works under topology=single. One of two concurrent downloads
// loses its only lane (the gateway closes the stream); the client's per-flow
// rescue opens a replacement, which on its own connection would be an
// attachment of the same path controller, the flow's bytes all arrive, and
// its sibling is undisturbed.
func TestSingleTopologyRescuesAFlowThatLosesItsLane(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-second end-to-end transfer")
	}
	pathAuthorities.forget()
	clientMetrics := metrics.New()
	cfg := newBundleClientConfig(TopologySingle)
	cfg.Metrics = clientMetrics
	rig := newTopologyRig(t, ServerConfig{
		CCAPI: CCAPIV2, Sched: SchedClass, CongestionScope: CongestionScopePath, Classifier: ClassifierPhased,
	}, cfg)
	if err := rig.download(2, func() {
		time.Sleep(800 * time.Millisecond)
		var failed *mpLane
		rig.server.sessionsMu.RLock()
		for _, s := range rig.server.sessions {
			if lanes := s.flow.healthyLanes(); len(lanes) > 0 {
				failed = lanes[0]
				break
			}
		}
		rig.server.sessionsMu.RUnlock()
		if failed == nil {
			t.Error("no flow to fail")
			return
		}
		if err := failed.fc.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			t.Error(err)
		}
	}); err != nil {
		t.Fatal(err)
	}
	s := clientMetrics.Snapshot()
	controllers, _ := pathAttachments(rig.client.pathPrincipal + "|")
	t.Logf("lane replacements %d, bulk isolations %d, client controllers %d", s.LaneReplacements, s.BulkIsolations, controllers)
	if s.LaneReplacements == 0 {
		t.Fatal("the flow completed without a rescue; the lane was not lost")
	}
	if s.BulkIsolations != 0 {
		t.Fatalf("%d isolations under topology=single", s.BulkIsolations)
	}
	if controllers != 1 {
		t.Fatalf("the client's connections are spread over %d controllers, want one path authority", controllers)
	}
}

// Old-client compatibility (design §12, §15.1 test 17): a released client
// that isolates (the legacy bundle) against a gateway running the stage-7
// gateway bundle. Its flows isolate and complete, and the gateway attaches
// every data connection of that device -- pooled and isolated -- to one
// path controller. And the reverse: a topology=single client against a
// legacy gateway, which sees one connection.
func TestTopologyAcrossOldAndNewPeers(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-second end-to-end transfer")
	}
	t.Run("isolating old client, new gateway", func(t *testing.T) {
		pathAuthorities.forget()
		clientMetrics := metrics.New()
		rig := newTopologyRig(t, ServerConfig{
			CCAPI: CCAPIV2, Sched: SchedClass, CongestionScope: CongestionScopePath, Classifier: ClassifierPhased,
		}, ClientConfig{Transport: TransportQUIC, EnableQUICPool: true, Metrics: clientMetrics})
		maxGateway := 0
		if err := rig.download(4, func() {
			deadline := time.Now().Add(2500 * time.Millisecond)
			for time.Now().Before(deadline) {
				controllers, n := pathAttachments("principal:")
				if controllers > 1 {
					t.Errorf("the gateway split one device over %d controllers", controllers)
					return
				}
				maxGateway = max(maxGateway, n)
				time.Sleep(20 * time.Millisecond)
			}
		}); err != nil {
			t.Fatal(err)
		}
		isolations := clientMetrics.Snapshot().BulkIsolations
		t.Logf("old client isolations %d; gateway attachments at most %d on one controller", isolations, maxGateway)
		if isolations == 0 || maxGateway < 2 {
			t.Fatalf("old client isolated %d flows; gateway saw at most %d attachments", isolations, maxGateway)
		}
	})
	t.Run("single client, legacy gateway", func(t *testing.T) {
		pathAuthorities.forget()
		clientMetrics := metrics.New()
		cfg := newBundleClientConfig(TopologySingle)
		cfg.Metrics = clientMetrics
		rig := newTopologyRig(t, ServerConfig{}, cfg)
		if err := rig.download(4, nil); err != nil {
			t.Fatal(err)
		}
		if n := clientMetrics.Snapshot().BulkIsolations; n != 0 {
			t.Fatalf("%d isolations", n)
		}
	})
}

// Memory on the iOS profile (design §11.5, §15.1 test 14) under
// topology=single: 200 idle flows and four bulk downloads at once, against
// the same load under isolation. Every byte arrives in both; the single
// client holds one QUIC connection where isolation also takes the profile's
// one secondary connection, so its QUIC receive commitment (connections times
// connection window) is lower even with the grown window; the shared budgets
// are never exceeded; and the heap the load adds is no more than isolation's.
// The heap is the test process's, client and gateway together, so it is
// compared between the arms rather than against the 28 MiB device limit.
func TestSingleTopologyStaysInsideTheIOSMemoryProfile(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-second end-to-end transfer")
	}
	growth := map[Topology]uint64{}
	commitment := map[Topology]uint64{}
	for _, topology := range []Topology{TopologyIsolated, TopologySingle} {
		t.Run(string(topology), func(t *testing.T) {
			pathAuthorities.forget()
			cfg := newBundleClientConfig(topology)
			cfg.MemoryLimits, cfg.ChunkSize, cfg.Memory = iosMemoryLimits(), iosChunkSize, MemoryOwned
			cfg.MaxSessions, cfg.MaxPendingOpens = 1024, 128
			cfg.StreamReceiveWindow, cfg.MaxStreamReceiveWindow = 64*1024, 64*1024
			cfg.ConnectionReceiveWindow, cfg.MaxConnectionReceiveWindow = 2*1024*1024, 2*1024*1024
			cfg.MaxIncomingStreams = 32
			rig := newTopologyRig(t, ServerConfig{
				CCAPI: CCAPIV2, Sched: SchedClass, CongestionScope: CongestionScopePath, Classifier: ClassifierPhased,
				MaxSessions: 2048,
			}, cfg)
			w := rig.client.preferredWindows()
			if topology == TopologySingle && rig.client.windows().connection != 2*1024*1024 {
				t.Fatal("a secondary connection would get the preferred connection's grown window")
			}
			if topology == TopologySingle && (w.connection != 2*1024*1024+64*1024 || w.maxConnection != w.connection) {
				// Grown by the one stream window isolation's second
				// connection would have added.
				t.Fatalf("iOS single-connection windows %+v", w)
			}
			idleListener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer idleListener.Close()
			go echoDestination(idleListener)
			idle := make([]net.Conn, 0, 200)
			defer func() {
				for _, c := range idle {
					_ = c.Close()
				}
			}()
			for range cap(idle) {
				idle = append(idle, dialTestSOCKS(t, rig.clientAddr, idleListener.Addr().String()))
			}
			runtime.GC()
			var before runtime.MemStats
			runtime.ReadMemStats(&before)
			var peak uint64
			maxAttached := 0
			if err := rig.download(4, func() {
				deadline := time.Now().Add(3 * time.Second)
				for time.Now().Before(deadline) {
					var m runtime.MemStats
					runtime.ReadMemStats(&m)
					peak = max(peak, m.HeapInuse)
					_, n := pathAttachments(rig.client.pathPrincipal + "|")
					maxAttached = max(maxAttached, n)
					time.Sleep(50 * time.Millisecond)
				}
			}); err != nil {
				t.Fatal(err)
			}
			stats := rig.client.MemoryStats()
			if peak > before.HeapInuse {
				growth[topology] = peak - before.HeapInuse
			}
			commitment[topology] = uint64(maxAttached) * w.maxConnection
			t.Logf("heap growth %d KiB; QUIC receive commitment %d KiB over %d connections; send %+v; receive %+v",
				growth[topology]>>10, commitment[topology]>>10, maxAttached, stats.Send, stats.Receive)
			if stats.Send.Peak > stats.Send.Capacity || stats.Receive.Peak > stats.Receive.Capacity {
				t.Fatalf("shared budgets exceeded: %+v", stats)
			}
			if topology == TopologySingle && (maxAttached != 1 || rig.client.bulkConnCount() != 0) {
				t.Fatalf("client held %d attached connections and %d bulk connections, want one", maxAttached, rig.client.bulkConnCount())
			}
		})
	}
	if commitment[TopologySingle] >= commitment[TopologyIsolated] {
		t.Errorf("one connection commits %d receive bytes, isolation %d", commitment[TopologySingle], commitment[TopologyIsolated])
	}
	if growth[TopologySingle] > growth[TopologyIsolated]+4<<20 {
		t.Errorf("the load grew the heap by %d KiB on one connection, %d KiB isolated", growth[TopologySingle]>>10, growth[TopologyIsolated]>>10)
	}
}
