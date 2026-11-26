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

	"github.com/bojieli/queqiao/internal/metrics"
)

// newBundleClientConfig is the client configuration under test: QUIC, one
// connection per path (design §7).
func newBundleClientConfig() ClientConfig {
	return ClientConfig{Transport: TransportQUIC}
}

// Connection-level flow control is not the new bottleneck. Under isolation
// each bulk flow on a secondary connection had a connection receive window
// of its own; on one connection they share one. With the desktop windows the
// shared window is far above any path's product and is left alone; with the
// mobile profiles' fixed windows the one connection's ceiling covers what the
// pooled connection and the isolated ones could hold together, for any
// number of bulk flows, and never more than the isolated topology committed.
// (The bound over several live generations is
// TestPreferredWindowCreditIsBoundedOverEveryLiveGeneration.)
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
		{"desktop", flowWindows{}, maxWindowCreditConnections},
		// mobile/core/resources.go.
		{"ios", flowWindows{stream: 64 * kib, maxStream: 64 * kib, connection: 2 * mib, maxConnection: 2 * mib, maxStreams: 32}, 1},
		{"android", flowWindows{stream: mib, maxStream: mib, connection: 4 * mib, maxConnection: 4 * mib, maxStreams: 64}, 2},
		// A stream allowed the whole connection window.
		{"stream equals connection", flowWindows{stream: mib, maxStream: mib, connection: mib, maxConnection: mib, maxStreams: 64}, 1},
	} {
		t.Run(p.name, func(t *testing.T) {
			client := &Client{cfg: ClientConfig{
				StreamReceiveWindow: p.windows.stream, MaxStreamReceiveWindow: p.windows.maxStream,
				ConnectionReceiveWindow: p.windows.connection, MaxConnectionReceiveWindow: p.windows.maxConnection,
				MaxIncomingStreams: p.windows.maxStreams,
			}}
			client.memoryLimits.maxBulkConnections = p.maxBulk
			single, hold := client.reservePreferredWindows()
			defer hold.release()
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
	desktop := &Client{cfg: ClientConfig{}}
	desktop.memoryLimits.maxBulkConnections = maxWindowCreditConnections
	if w, hold := desktop.reservePreferredWindows(); w != desktop.windows() {
		t.Fatalf("desktop windows changed: %+v", w)
	} else {
		hold.release()
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
			if err := checkDownload(conn); err != nil {
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

// checkDownload reads a whole download through a fixed buffer, checking the
// pattern as it goes, so verification retains nothing the memory test would
// count.
func checkDownload(r io.Reader) error {
	buf := make([]byte, 16*1024)
	n := 0
	for {
		k, err := r.Read(buf)
		for i, b := range buf[:k] {
			if want := byte((n + i) % 251); b != want {
				return fmt.Errorf("byte %d is %d, want %d", n+i, b, want)
			}
		}
		n += k
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
	}
	if n != rigDownload {
		return fmt.Errorf("downloaded %d bytes, want %d", n, rigDownload)
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
	pathAuthorities.forget()
	clientMetrics := metrics.New()
	cfg := newBundleClientConfig()
	cfg.Metrics = clientMetrics
	rig := newTopologyRig(t, ServerConfig{}, cfg)
	maxAttached := 0
	if err := rig.download(4, func() {
		deadline := time.Now().Add(2500 * time.Millisecond)
		for time.Now().Before(deadline) {
			_, n := pathAttachments(rig.client.pathPrincipalNow() + "|")
			maxAttached = max(maxAttached, n)
			time.Sleep(20 * time.Millisecond)
		}
	}); err != nil {
		t.Fatal(err)
	}
	isolations := clientMetrics.Snapshot().BulkIsolations
	t.Logf("bulk isolations %d, most client attachments %d", isolations, maxAttached)
	if isolations != 0 {
		t.Fatalf("the client isolated %d flows", isolations)
	}
	if maxAttached != 1 {
		t.Fatalf("the client used %d connections on its path, want 1", maxAttached)
	}
}

// A flow that loses its only lane on one connection per path: one of two
// concurrent downloads has its stream closed by the gateway, and the carrier
// manager rebinds the flow by a JOIN on the live carrier. Its bytes all
// arrive, its sibling is undisturbed, and the client never has more than one
// path controller.
func TestSingleTopologyRescuesAFlowThatLosesItsLane(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-second end-to-end transfer")
	}
	pathAuthorities.forget()
	clientMetrics := metrics.New()
	cfg := newBundleClientConfig()
	cfg.Metrics = clientMetrics
	rig := newTopologyRig(t, ServerConfig{}, cfg)
	var maxControllers int
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
		deadline := time.Now().Add(1500 * time.Millisecond)
		for time.Now().Before(deadline) {
			controllers, _ := pathAttachments(rig.client.pathPrincipalNow() + "|")
			maxControllers = max(maxControllers, controllers)
			time.Sleep(5 * time.Millisecond)
		}
	}); err != nil {
		t.Fatal(err)
	}
	s := clientMetrics.Snapshot()
	m := rig.client.carriers
	joins := m.sameCarrierJoins.Load() + m.newCarrierJoins.Load()
	controllers, _ := pathAttachments(rig.client.pathPrincipalNow() + "|")
	t.Logf("rebinding JOINs %d, bulk isolations %d, client controllers %d (at most %d)",
		joins, s.BulkIsolations, controllers, maxControllers)
	if maxControllers != 1 || controllers != 1 {
		t.Fatalf("the client's connections spread over %d controllers (at most %d), want one path authority", controllers, maxControllers)
	}
	if joins == 0 {
		t.Fatal("the flow completed without a rebinding JOIN; the lane was not lost")
	}
	if s.BulkIsolations != 0 {
		t.Fatalf("%d isolations", s.BulkIsolations)
	}
	if n := s.LaneRescueAttempts; n != 0 {
		t.Fatalf("%d per-flow rescue dials", n)
	}
}

// Old-peer compatibility (design §12, §15.1 test 17): a client against a
// gateway that accepts only wire 1, as a released gateway does, sees one
// connection and isolates nothing. An isolating released client against a
// new gateway can no longer be built in-process; the released-binary interop
// gate (scripts/wire_interop.py) covers it, and the gateway still attaches
// every data connection of one device to one controller.
func TestTopologyAcrossOldAndNewPeers(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-second end-to-end transfer")
	}
	pathAuthorities.forget()
	clientMetrics := metrics.New()
	cfg := newBundleClientConfig()
	cfg.Metrics = clientMetrics
	rig := newTopologyRig(t, ServerConfig{AcceptWire: []byte{1}}, cfg)
	if err := rig.download(4, nil); err != nil {
		t.Fatal(err)
	}
	if n := clientMetrics.Snapshot().BulkIsolations; n != 0 {
		t.Fatalf("%d isolations", n)
	}
}

// Memory on the iOS profile (design §11.5, §15.1 test 14): 200 idle flows
// and four bulk downloads at once. Every byte arrives; the client holds one
// QUIC connection, with the preferred connection's window grown by the one
// stream window the profile's secondary connection would have added; and the
// shared budgets are never exceeded. Until stage 11 the heap the load added
// was also compared with the isolated topology's under the same load.
func TestSingleTopologyStaysInsideTheIOSMemoryProfile(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-second end-to-end transfer")
	}
	pathAuthorities.forget()
	cfg := newBundleClientConfig()
	cfg.MemoryLimits, cfg.ChunkSize = iosMemoryLimits(), iosChunkSize
	cfg.MaxSessions, cfg.MaxPendingOpens = 1024, 128
	cfg.StreamReceiveWindow, cfg.MaxStreamReceiveWindow = 64*1024, 64*1024
	cfg.ConnectionReceiveWindow, cfg.MaxConnectionReceiveWindow = 2*1024*1024, 2*1024*1024
	cfg.MaxIncomingStreams = 32
	rig := newTopologyRig(t, ServerConfig{
		MaxSessions: 2048,
	}, cfg)
	w, hold := rig.client.reservePreferredWindows()
	hold.release()
	if rig.client.windows().connection != 2*1024*1024 {
		t.Fatal("a secondary connection would get the preferred connection's grown window")
	}
	if w.connection != 2*1024*1024+64*1024 || w.maxConnection != w.connection {
		t.Fatalf("iOS single-connection windows %+v", w)
	}
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	var peak uint64
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
	maxAttached := 0
	if err := rig.download(4, func() {
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			peak = max(peak, m.HeapInuse)
			_, n := pathAttachments(rig.client.pathPrincipalNow() + "|")
			maxAttached = max(maxAttached, n)
			time.Sleep(50 * time.Millisecond)
		}
	}); err != nil {
		t.Fatal(err)
	}
	stats := rig.client.MemoryStats()
	var growth uint64
	if peak > before.HeapInuse {
		growth = peak - before.HeapInuse
	}
	t.Logf("heap growth %d KiB; QUIC receive commitment %d KiB over %d connections; send %+v; receive %+v",
		growth>>10, uint64(maxAttached)*w.maxConnection>>10, maxAttached, stats.Send, stats.Receive)
	if stats.Send.Peak > stats.Send.Capacity || stats.Receive.Peak > stats.Receive.Capacity {
		t.Fatalf("shared budgets exceeded: %+v", stats)
	}
	if maxAttached != 1 {
		t.Fatalf("client held %d attached connections, want one", maxAttached)
	}
}

// The receive-window credit topology=single gives the preferred connection
// is bounded over every live connection on the path, not per connection. A
// retired generation drains for as long as a flow still borrows it, so
// repeated supersessions can leave several generations alive at once. Their
// connection windows together, beside the base window each connection
// carries anyway, must not exceed the credit of the bulk connections the
// topology does not open: what isolation could have held on those same
// connections plus its bulk pool. The configuration is the one where the
// cap matters most, a stream allowed the whole connection window. Once the
// drained generations close, their credit returns.
func TestPreferredWindowCreditIsBoundedOverEveryLiveGeneration(t *testing.T) {
	pathAuthorities.forget()
	const mib = 1024 * 1024
	certificate, roots := testCertificate(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	packetConn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(ServerConfig{
		ListenAddr: "127.0.0.1:0", Credentials: certificate, EnableQUIC: true, Logger: logger,
		DestinationPolicy: DestinationPolicy{AllowPrivate: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = server.ServePacketConn(ctx, packetConn) }()
	cfg := newBundleClientConfig()
	cfg.ListenAddr, cfg.RemoteAddr, cfg.Credentials, cfg.Logger = "127.0.0.1:0", packetConn.LocalAddr().String(), roots, logger
	cfg.StreamReceiveWindow, cfg.MaxStreamReceiveWindow = mib, mib
	cfg.ConnectionReceiveWindow, cfg.MaxConnectionReceiveWindow = mib, mib
	limits := iosMemoryLimits() // one bulk connection: one connection's credit
	cfg.MemoryLimits, cfg.ChunkSize = limits, iosChunkSize
	client, err := NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer client.closeQUICPool()
	ccfg := congestionConfig{principal: client.pathPrincipalNow}

	const generations = 3
	var live []*controlQUICGeneration
	for i := range generations {
		g, err := client.acquireControlQUICGeneration(ctx, ccfg)
		if err != nil {
			t.Fatal(err)
		}
		live = append(live, g)
		if i < generations-1 {
			// Its borrow is a flow that keeps it draining after it is
			// superseded.
			retirePooledGenerationForTest(client, g)
		}
	}
	var total uint64
	for i, g := range live {
		if g.conn.Context().Err() != nil {
			t.Fatalf("generation %d closed; it should still be draining", i)
		}
		w := g.conn.Config().MaxConnectionReceiveWindow
		t.Logf("generation %d: connection window %d KiB", i, w>>10)
		total += w
	}
	bound := uint64(generations)*mib + uint64(limits.MaxBulkConnections)*mib
	if total > bound {
		t.Fatalf("%d live generations advertise %d KiB of connection window, over the %d KiB isolation could hold on them and its bulk pool",
			generations, total>>10, bound>>10)
	}

	// The drained generations close when their flows leave, and their credit
	// is available to the next preferred connection.
	for _, g := range live {
		g.releaseBorrow()
	}
	client.retireControlQUICGeneration(live[generations-1], "test")
	for i, g := range live {
		waitFor(t, 5*time.Second, fmt.Sprintf("generation %d to close", i), func() bool { return g.conn.Context().Err() != nil })
	}
	fresh, err := client.acquireControlQUICGeneration(ctx, ccfg)
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.releaseBorrow()
	if w := fresh.conn.Config().MaxConnectionReceiveWindow; w != 2*mib {
		t.Fatalf("after the drained generations closed the preferred connection got %d KiB, want the whole credit (%d KiB)", w>>10, 2*mib>>10)
	}
}

// A replacement dialled the moment its predecessor's connection ends gets
// the predecessor's credit back even if the callback that removes the hold
// has not run yet: an ended connection holds nothing.
func TestEndedConnectionHoldsNoWindowCredit(t *testing.T) {
	const mib = 1024 * 1024
	client := &Client{cfg: ClientConfig{
		StreamReceiveWindow: mib, MaxStreamReceiveWindow: mib,
		ConnectionReceiveWindow: mib, MaxConnectionReceiveWindow: mib,
	}}
	client.memoryLimits.maxBulkConnections = 1
	first, hold := client.reservePreferredWindows()
	if first.maxConnection != 2*mib || hold == nil {
		t.Fatalf("first preferred connection got %d", first.maxConnection)
	}
	// The predecessor's connection ends; its removal callback is not run
	// (as if it were still scheduled).
	ended, cancel := context.WithCancel(context.Background())
	cancel()
	client.windowCredit.mu.Lock()
	hold.conn = ended
	client.windowCredit.mu.Unlock()
	second, next := client.reservePreferredWindows()
	defer next.release()
	if second.maxConnection != 2*mib {
		t.Fatalf("a replacement dialled after its predecessor ended got %d KiB, want the whole credit", second.maxConnection>>10)
	}
	// While a holder is live, the credit is not granted twice.
	third, none := client.reservePreferredWindows()
	if third.maxConnection != mib || none != nil {
		t.Fatalf("a second live preferred connection got %d KiB beside a live holder", third.maxConnection>>10)
	}
}
