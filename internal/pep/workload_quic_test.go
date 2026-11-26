package pep

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/apernet/quic-go"

	"github.com/bojieli/queqiao/internal/identity"
	"github.com/bojieli/queqiao/internal/pathsim"
)

// innerALPN is the protocol of the QUIC server inside the tunnel: an
// application's own, which the tunnel carries as UDP and never reads.
const innerALPN = "w8-inner"

// innerQUICServer is the far side of W8: a QUIC server that answers each
// stream's 4-byte size with that many bytes, as an HTTP/3 server answers a
// GET with its body. It runs quic-go's defaults: its own congestion control
// and loss recovery, nested inside the tunnel's.
type innerQUICServer struct {
	addr  string
	roots identity.ClientCredentials
	mu    sync.Mutex
	conns []*quic.Conn
}

func newInnerQUICServer(t *testing.T) *innerQUICServer {
	t.Helper()
	serverCredentials, roots := testCertificate(t)
	tlsConfig, err := identity.ServerTLSConfig(serverCredentials, innerALPN, false)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := quic.ListenAddr("127.0.0.1:0", tlsConfig, &quic.Config{MaxIdleTimeout: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	s := &innerQUICServer{addr: listener.Addr().String(), roots: roots}
	go func() {
		for {
			conn, err := listener.Accept(context.Background())
			if err != nil {
				return
			}
			s.mu.Lock()
			s.conns = append(s.conns, conn)
			s.mu.Unlock()
			go func() {
				for {
					stream, err := conn.AcceptStream(context.Background())
					if err != nil {
						return
					}
					go serveInnerStream(stream)
				}
			}()
		}
	}()
	return s
}

func serveInnerStream(stream *quic.Stream) {
	defer stream.Close()
	var request [4]byte
	if _, err := io.ReadFull(stream, request[:]); err != nil {
		return
	}
	body := make([]byte, 32*1024)
	for left := int(binary.BigEndian.Uint32(request[:])); left > 0; {
		n, err := stream.Write(body[:min(left, len(body))])
		if err != nil {
			return
		}
		left -= n
	}
}

// stats is the server connection's own count, which for a download is the
// sender's.
func (s *innerQUICServer) stats() quic.ConnectionStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	var total quic.ConnectionStats
	for _, conn := range s.conns {
		st := conn.ConnectionStats()
		total.PacketsSent += st.PacketsSent
		total.PacketsLost += st.PacketsLost
	}
	return total
}

// waitInnerQuiet waits, for at most 10 s, until the inner server has sent
// nothing for 500 ms.
func waitInnerQuiet(s *innerQUICServer) {
	last := s.stats().PacketsSent
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); {
		time.Sleep(500 * time.Millisecond)
		now := s.stats().PacketsSent
		if now == last {
			return
		}
		last = now
	}
}

// dialInnerQUIC dials the inner server through a UDP association of rig, as
// a QUIC application behind a SOCKS client does.
func dialInnerQUIC(ctx context.Context, t *testing.T, rig *workloadRig, server *innerQUICServer) (*quic.Conn, error) {
	tlsConfig, err := identity.ClientTLSConfigProtocols(server.roots, []string{innerALPN})
	if err != nil {
		return nil, err
	}
	association, err := rig.associate()
	if err != nil {
		return nil, err
	}
	remote, err := net.ResolveUDPAddr("udp", server.addr)
	if err != nil {
		_ = association.Close()
		return nil, err
	}
	// The transport does not close a PacketConn it was handed.
	t.Cleanup(func() { _ = association.Close() })
	transport := &quic.Transport{Conn: &socksPacketConn{association: association, remote: remote, buf: make([]byte, 64*1024)}}
	t.Cleanup(func() { _ = transport.Close() })
	return transport.Dial(ctx, remote, tlsConfig, &quic.Config{MaxIdleTimeout: time.Minute, HandshakeIdleTimeout: 20 * time.Second})
}

// innerGet fetches size bytes on a new stream of conn.
func innerGet(ctx context.Context, conn *quic.Conn, size int) error {
	n, _, err := innerGetFor(ctx, conn, size, 0)
	if err == nil && int(n) != size {
		err = fmt.Errorf("stream delivered %d of %d bytes", n, size)
	}
	return err
}

// innerGetFor fetches size bytes on a new stream of conn, stopping after
// limit if that is positive. It reports the bytes read and the time taken.
func innerGetFor(ctx context.Context, conn *quic.Conn, size int, limit time.Duration) (int64, time.Duration, error) {
	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return 0, 0, err
	}
	var request [4]byte
	binary.BigEndian.PutUint32(request[:], uint32(size))
	start := time.Now()
	if _, err := stream.Write(request[:]); err != nil {
		return 0, 0, err
	}
	_ = stream.Close()
	if limit > 0 {
		_ = stream.SetReadDeadline(start.Add(limit))
	} else if deadline, ok := ctx.Deadline(); ok {
		_ = stream.SetReadDeadline(deadline)
	}
	n, err := io.Copy(io.Discard, stream)
	elapsed := time.Since(start)
	var timeout net.Error
	if limit > 0 && errors.As(err, &timeout) && timeout.Timeout() {
		stream.CancelRead(0)
		return n, elapsed, nil
	}
	return n, elapsed, err
}

// pageObjects is W3's page: a 30 KB document, then 40 objects of log-normal
// size (median 15 KB, p95 300 KB, at most 2 MB), drawn from a fixed seed so
// every arm loads the same page.
func pageObjects(seed int64) (document int, objects []int) {
	r := rand.New(rand.NewSource(seed))
	sigma := math.Log(300.0/15.0) / 1.645
	for range 40 {
		size := int(15000 * math.Exp(sigma*r.NormFloat64()))
		objects = append(objects, min(max(size, 200), 2_000_000))
	}
	return 30_000, objects
}

// w8Outcome is one arm's W8 measures.
type w8Outcome struct {
	goodput       float64 // download bytes per second
	innerLoss     float64 // the inner sender's lost packets over sent
	wirePerUseful float64 // emulated-path bytes, both directions, per downloaded byte
	queueing      time.Duration
	pages         []time.Duration
	// The same two measures over the page phase.
	pageLoss          float64
	pageWirePerUseful float64
}

// W8. Tunnelled QUIC (docs/WORKLOAD-MODEL.md): a QUIC application inside the
// tunnel, its packets SOCKS UDP datagrams the tunnel carries in its one
// ordered symbol stream at INTERACTIVE weight (design Q5). The inner
// connection runs quic-go's own congestion control and loss recovery, so two
// control loops are nested and the inner one reads the tunnel's repair delay
// and any overflow drop as congestion. Plain QUIC streams stand in for
// HTTP/3: the framing is not what is measured.
//
// (a) A download on one stream, 20 MB or 30 s, whichever is first; (b) W3
// pages, each one's objects as concurrent streams on the same connection.
// Bounds (model W8): download goodput ≥ 0.90 × legacy; inner loss rate ≤
// legacy + 1 pp; wire bytes per useful byte ≤ legacy × 1.15; the loss and
// overhead bounds hold over the page phase too. Legacy's values are frozen
// below (w8Legacy), as measured on these paths at stage 10 before the legacy
// bundle was deleted. Page load time and the inner smoothed RTT above its
// minimum are reported.
//
// Two paths, both 120 ms round trip at 20 Mbit/s behind a drop-tail queue.
// On the clean one the inner flow fills the bottleneck through the tunnel;
// on the lossy one (3 % random loss each way) the tunnel's coding sits
// under an inner protocol that also repairs, and the inner flow is held to
// what the residual loss allows it. A policed bottleneck is left out: there
// the inner controller's response to the policer's drops varied the goodput
// by a quarter from run to run, which no 0.90 bound survives.
// -short runs the clean path with a 4 MB download and one page; the lossy
// path takes a minute and runs in the full suite.
func TestTunnelledQUICDownloadOnAnEmulatedPath(t *testing.T) {
	requireStableImpairmentClock(t)
	t.Run("clean", func(t *testing.T) {
		download, pages := 20_000_000, 4
		if testing.Short() {
			download, pages = 4_000_000, 1
		}
		legacy := w8LegacyClean
		if testing.Short() {
			legacy = w8LegacyCleanShort
		}
		outcome := runTunnelledQUIC(t, pathsim.Config{OneWayDelay: 60 * time.Millisecond, Seed: 8, RateBytesPerSec: 20e6 / 8},
			download, pages)
		checkTunnelledQUIC(t, outcome, legacy)
	})
	t.Run("lossy", func(t *testing.T) {
		if testing.Short() {
			t.Skip("a minute of a nested controller held down by residual loss; the full suite runs it")
		}
		outcome := runTunnelledQUIC(t, pathsim.Config{OneWayDelay: 60 * time.Millisecond, LossRate: 0.03, Seed: 8, RateBytesPerSec: 20e6 / 8},
			20_000_000, 1)
		checkTunnelledQUIC(t, outcome, w8LegacyLossy)
	})
}

// runTunnelledQUIC runs W8 across path: a download of at most size bytes and
// 30 s, then pages W3 pages.
func runTunnelledQUIC(t *testing.T, path pathsim.Config, size, pages int) w8Outcome {
	t.Helper()
	const limit = 30 * time.Second
	rig := newWorkloadRig(t, path, pathAuthorityArm, nil)
	server := newInnerQUICServer(t)
	var o w8Outcome
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	conn, err := dialInnerQUIC(ctx, t, rig, server)
	if err != nil {
		t.Fatalf("inner handshake: %v", err)
	}
	defer conn.CloseWithError(0, "")

	sentBefore, wireBefore := server.stats(), rig.wireBytes()
	n, elapsed, err := innerGetFor(ctx, conn, size, limit)
	if err != nil {
		t.Fatalf("download: %v", err)
	}
	sentAfter, wireAfter := server.stats(), rig.wireBytes()
	if n == 0 || (int(n) < size && elapsed < limit) {
		t.Fatalf("the download ended after %d of %d bytes in %v", n, size, elapsed.Round(time.Millisecond))
	}
	o.goodput = float64(n) / elapsed.Seconds()
	if sent := sentAfter.PacketsSent - sentBefore.PacketsSent; sent > 0 {
		o.innerLoss = float64(sentAfter.PacketsLost-sentBefore.PacketsLost) / float64(sent)
	}
	o.wirePerUseful = float64(wireAfter-wireBefore) / float64(n)
	st := conn.ConnectionStats()
	o.queueing = st.SmoothedRTT - st.MinRTT

	// A capped download's sender learns of the cancellation a round trip
	// later; let it drain so none of it is charged to the pages.
	waitInnerQuiet(server)
	pageSentBefore, pageWireBefore := server.stats(), rig.wireBytes()
	pageBytes := 0
	for p := range pages {
		document, objects := pageObjects(int64(p))
		pageBytes += document
		for _, size := range objects {
			pageBytes += size
		}
		start := time.Now()
		if err := innerGet(ctx, conn, document); err != nil {
			t.Fatalf("page %d document: %v", p, err)
		}
		var objectsWG sync.WaitGroup
		objectErrs := make(chan error, len(objects))
		for _, size := range objects {
			objectsWG.Add(1)
			go func() {
				defer objectsWG.Done()
				if err := innerGet(ctx, conn, size); err != nil {
					objectErrs <- err
				}
			}()
		}
		objectsWG.Wait()
		close(objectErrs)
		if err := <-objectErrs; err != nil {
			t.Fatalf("page %d: %v", p, err)
		}
		o.pages = append(o.pages, time.Since(start))
	}
	if pages > 0 {
		pageSentAfter, pageWireAfter := server.stats(), rig.wireBytes()
		if sent := pageSentAfter.PacketsSent - pageSentBefore.PacketsSent; sent > 0 {
			o.pageLoss = float64(pageSentAfter.PacketsLost-pageSentBefore.PacketsLost) / float64(sent)
		}
		o.pageWirePerUseful = float64(pageWireAfter-pageWireBefore) / float64(pageBytes)
	}
	t.Logf("download goodput %.2f Mbit/s, inner loss %.2f %%, wire bytes per useful byte %.3f, inner queueing %v; "+
		"pages: load p50 %v, inner loss %.2f %%, wire bytes per useful byte %.3f",
		o.goodput*8/1e6, 100*o.innerLoss, o.wirePerUseful, o.queueing.Round(time.Millisecond),
		durationQuantile(o.pages, 0.5).Round(time.Millisecond), 100*o.pageLoss, o.pageWirePerUseful)
	return o
}

// w8Legacy is what the legacy bundle measured on one of W8's paths at stage
// 10, before it was deleted: the control the model's bounds are against.
type w8Legacy struct {
	goodputMbps                 float64
	innerLoss, wirePerUseful    float64
	pageLoss, pageWirePerUseful float64
}

// Medians of four runs on a 32-core Linux host. The clean path was the same
// to three figures every run; on the lossy path goodput ran from 1.33 to
// 1.47 Mbit/s and the inner loss from 1.20 to 1.43 %. The path controller's
// own figures in the same runs were 17.0 and 14.6 Mbit/s on the clean path
// and 1.45 to 1.54 Mbit/s with 1.13 to 1.31 % inner loss on the lossy one.
var (
	w8LegacyClean      = w8Legacy{goodputMbps: 17.03, innerLoss: 0, wirePerUseful: 1.188, pageLoss: 0, pageWirePerUseful: 1.186}
	w8LegacyCleanShort = w8Legacy{goodputMbps: 14.61, innerLoss: 0, wirePerUseful: 1.200, pageLoss: 0, pageWirePerUseful: 1.189}
	w8LegacyLossy      = w8Legacy{goodputMbps: 1.45, innerLoss: 0.0133, wirePerUseful: 1.90, pageLoss: 0.0131, pageWirePerUseful: 1.89}
)

// checkTunnelledQUIC applies W8's bounds against legacy's frozen measures.
func checkTunnelledQUIC(t *testing.T, o w8Outcome, legacy w8Legacy) {
	t.Helper()
	if !judgeBounds(t) {
		return
	}
	if got := o.goodput * 8 / 1e6; got < 0.90*legacy.goodputMbps {
		t.Errorf("tunnelled QUIC goodput %.2f Mbit/s, below 0.90 × legacy's %.2f", got, legacy.goodputMbps)
	}
	if o.innerLoss > legacy.innerLoss+0.01 {
		t.Errorf("the inner connection lost %.2f %% of its packets; the bound is legacy's %.2f %% + 1 pp",
			100*o.innerLoss, 100*legacy.innerLoss)
	}
	if o.wirePerUseful > 1.15*legacy.wirePerUseful {
		t.Errorf("%.3f wire bytes per useful byte; the bound is legacy's %.3f × 1.15", o.wirePerUseful, legacy.wirePerUseful)
	}
	// The page phase, whose concurrent streams the download does not
	// exercise, is held to the same loss and overhead bounds.
	if o.pageLoss > legacy.pageLoss+0.01 {
		t.Errorf("pages: the inner connection lost %.2f %% of its packets; the bound is legacy's %.2f %% + 1 pp",
			100*o.pageLoss, 100*legacy.pageLoss)
	}
	if o.pageWirePerUseful > 1.15*legacy.pageWirePerUseful {
		t.Errorf("pages: %.3f wire bytes per useful byte; the bound is legacy's %.3f × 1.15", o.pageWirePerUseful, legacy.pageWirePerUseful)
	}
}
