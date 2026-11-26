package pep

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bojieli/queqiao/internal/metrics"
	"github.com/bojieli/queqiao/internal/pathmodel"
	"github.com/bojieli/queqiao/internal/pathsim"
	"github.com/bojieli/queqiao/internal/profile"
)

// dc-long-haul on the path-authority architecture (docs/DESIGN-PATH-AUTHORITY.md
// §11.3), on real fork send loops over emulated paths: one path authority per
// leaf, the tree used only as knowledge. Until stage 11 each trial also ran
// the legacy bundle the profile had run before (per-connection erasure
// senders under the tree's joint limit), and the stage-9 gate's contrasts
// were computed against it here and live (scripts/dc_gate.py). The legacy
// arm went with the legacy code once the gate passed; what is left are the
// absolute guarantees: requests complete, traffic is sustained, and the
// completion spread across loaded leaves stays out of the independent-relay
// regime.
const (
	// dcSpreadCeiling: the slowest loaded leaf's median request completion
	// over the fastest's never reaches the independent-relay regime.
	dcSpreadCeiling = 3.5
)

// dcArm names the bundle a bed runs under the dc-long-haul profile.
type dcArm struct {
	name string
}

var dcNewArm = dcArm{name: "path-authority"}

// dcRPC is the destination's protocol: an eight-byte header, the bytes the
// client sends and the bytes it wants back, then the request. An upload of
// dcUnbounded is read until EOF; a download of dcUnbounded is written until
// the client goes away. Every byte read is counted.
const dcUnbounded = math.MaxUint32

type dcDestination struct {
	listener net.Listener
	received atomic.Int64
}

func newDCDestination(t *testing.T) *dcDestination {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	d := &dcDestination{listener: ln}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go d.serve(c)
		}
	}()
	return d
}

func (d *dcDestination) serve(c net.Conn) {
	defer c.Close()
	var header [8]byte
	if _, err := io.ReadFull(c, header[:]); err != nil {
		return
	}
	up, down := binary.BigEndian.Uint32(header[:4]), binary.BigEndian.Uint32(header[4:])
	buf := make([]byte, 64<<10)
	for read := int64(0); up == dcUnbounded || read < int64(up); {
		want := len(buf)
		if up != dcUnbounded && int64(want) > int64(up)-read {
			want = int(int64(up) - read)
		}
		n, err := c.Read(buf[:want])
		read += int64(n)
		d.received.Add(int64(n))
		if err != nil {
			return
		}
	}
	for sent := int64(0); down == dcUnbounded || sent < int64(down); {
		n := int64(len(buf))
		if down != dcUnbounded && n > int64(down)-sent {
			n = int64(down) - sent
		}
		if _, err := c.Write(buf[:n]); err != nil {
			return
		}
		sent += n
	}
}

// dcLeaf is one leaf of the tree: a client to its own gateway (a provider of
// its own), across its own relay, which may share a bottleneck with others.
type dcLeaf struct {
	name        string
	socks       string
	destination *dcDestination
	relay       *pathsim.Relay
}

// dcBed brings up leaves for one arm in one process, which shares the path
// tree and the path authorities exactly as one multi-provider client process
// does.
type dcBed struct {
	t      *testing.T
	arm    dcArm
	ctx    context.Context
	cancel context.CancelFunc
}

func newDCBed(t *testing.T, arm dcArm) *dcBed {
	t.Helper()
	// Each arm starts cold: no model, tree node or authority survives from
	// the previous trial (in one process every leaf would otherwise inherit
	// what the last arm measured).
	pathmodel.Reset()
	pathmodel.ResetTree()
	pathAuthorities.forget()
	ctx, cancel := context.WithCancel(context.Background())
	return &dcBed{t: t, arm: arm, ctx: ctx, cancel: cancel}
}

func (b *dcBed) close() {
	b.cancel()
	pathmodel.Reset()
	pathmodel.ResetTree()
	pathAuthorities.forget()
}

// dcHost is a loopback address of its own for leaf i, so that each leaf is
// its own endpoint pair: the path key ignores the port, so two leaves on
// 127.0.0.1 would be one leaf.
func dcHost(t *testing.T, base, i int) string {
	t.Helper()
	host := fmt.Sprintf("127.0.0.%d", base+i)
	probe, err := net.ListenPacket("udp", host+":0")
	if err != nil {
		t.Skipf("this host has no %s loopback address for a leaf of its own: %v", host, err)
	}
	_ = probe.Close()
	return host
}

// leaf starts leaf i. relay builds its emulated path from the relay's listen
// address to the gateway's.
func (b *dcBed) leaf(i int, relay func(listen, target string) (*pathsim.Relay, error)) *dcLeaf {
	t := b.t
	t.Helper()
	dc, err := profile.ByName("dc-long-haul")
	if err != nil {
		t.Fatal(err)
	}
	certificate, roots := testCertificate(t)
	// The gateway listens on a loopback address of its own too, so that each
	// gateway's per-peer model is its own, as on separate hosts.
	gatewayConn, err := net.ListenPacket("udp", dcHost(t, 40, i)+":0")
	if err != nil {
		t.Fatal(err)
	}
	discard := slog.New(slog.NewTextHandler(io.Discard, nil))
	scfg := ServerConfig{
		ListenAddr: "127.0.0.1:0", Credentials: certificate, Profile: dc,
		DestinationPolicy: DestinationPolicy{AllowPrivate: true}, EnableQUIC: true,
		Logger: discard, Metrics: metrics.New(), HandshakeTimeout: 30 * time.Second,
	}
	server, err := NewServer(scfg)
	if err != nil {
		t.Fatal(err)
	}
	r, err := relay(dcHost(t, 20, i)+":0", gatewayConn.LocalAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	clientListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ccfg := ClientConfig{
		ListenAddr: clientListener.Addr().String(), RemoteAddr: r.LocalAddr(), Credentials: roots,
		Transport: TransportQUIC, Profile: dc, Metrics: metrics.New(), Logger: discard,
	}
	client, err := NewClient(ccfg)
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = server.ServePacketConn(b.ctx, gatewayConn) }()
	go func() { _ = client.ServeListener(b.ctx, clientListener) }()
	context.AfterFunc(b.ctx, func() { _ = clientListener.Close(); _ = gatewayConn.Close() })
	return &dcLeaf{name: fmt.Sprintf("leaf-%d", i), socks: clientListener.Addr().String(), destination: newDCDestination(t), relay: r}
}

// dial opens a flow and sends its header.
func (l *dcLeaf) dial(up, down uint32, budget time.Duration) (net.Conn, error) {
	conn, err := trySocksDial(l.socks, l.destination.listener, budget)
	if err != nil {
		return nil, err
	}
	var header [8]byte
	binary.BigEndian.PutUint32(header[:4], up)
	binary.BigEndian.PutUint32(header[4:], down)
	if _, err := conn.Write(header[:]); err != nil {
		conn.Close()
		return nil, err
	}
	return conn, nil
}

// request is one transactional exchange on a new application connection:
// up bytes sent, down bytes read back. It returns the time from dial to the
// last byte back.
func (l *dcLeaf) request(up, down uint32, budget time.Duration) (time.Duration, error) {
	began := time.Now()
	conn, err := l.dial(up, down, budget)
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	if up > 0 {
		if _, err := conn.Write(make([]byte, up)); err != nil {
			return 0, err
		}
	}
	if _, err := io.CopyN(io.Discard, conn, int64(down)); err != nil {
		return 0, err
	}
	return time.Since(began), nil
}

// upload keeps one backlogged upload running until stop.
func (l *dcLeaf) upload(stop <-chan struct{}) error {
	conn, err := l.dial(dcUnbounded, 0, 30*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()
	buf := make([]byte, 32<<10)
	for {
		select {
		case <-stop:
			return nil
		default:
		}
		_ = conn.SetWriteDeadline(time.Now().Add(time.Second))
		if _, err := conn.Write(buf); err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			return err
		}
	}
}

// download keeps one backlogged download running until stop, counting what
// arrives.
func (l *dcLeaf) download(stop <-chan struct{}, got *atomic.Int64) error {
	conn, err := l.dial(0, dcUnbounded, 30*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()
	context.AfterFunc(contextFromStop(stop), func() { _ = conn.Close() })
	buf := make([]byte, 64<<10)
	for {
		_ = conn.SetReadDeadline(time.Now().Add(30 * time.Second))
		n, err := conn.Read(buf)
		got.Add(int64(n))
		if err != nil {
			select {
			case <-stop:
				return nil
			default:
				return err
			}
		}
	}
}

func contextFromStop(stop <-chan struct{}) context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	go func() { <-stop; cancel() }()
	return ctx
}

func dcMedian(d []time.Duration) time.Duration { return quantileOf(d, 0.5) }

func quantileOf(d []time.Duration, p float64) time.Duration {
	if len(d) == 0 {
		return 0
	}
	s := append([]time.Duration(nil), d...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	return s[int(math.Ceil(p*float64(len(s))))-1]
}

func jainIndex(x []float64) float64 {
	var sum, squares float64
	for _, v := range x {
		sum += v
		squares += v * v
	}
	if squares == 0 {
		return 0
	}
	return sum * sum / (float64(len(x)) * squares)
}

func requireDCBed(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("compares two bundles across emulated 200 ms paths")
	}
	if raceDetectorEnabled {
		t.Skip("a latency and fairness comparison means nothing under race instrumentation")
	}
	requireStableImpairmentClock(t)
}

// dcFairness is one arm's result on the multi-leaf workload.
type dcFairness struct {
	leafBytes   []float64       // sustained bytes per loaded leaf in the window
	leafMedians []time.Duration // median request completion per loaded leaf
	requests    []time.Duration // every loaded leaf's request completions
	control     []time.Duration // the uncongested sibling's request completions
	failed      int
}

func (f dcFairness) worst() float64 {
	w := math.Inf(1)
	for _, v := range f.leafBytes {
		w = math.Min(w, v)
	}
	return w
}

func (f dcFairness) aggregate() float64 {
	var s float64
	for _, v := range f.leafBytes {
		s += v
	}
	return s
}

func (f dcFairness) spread() float64 {
	lo, hi := time.Duration(math.MaxInt64), time.Duration(0)
	for _, m := range f.leafMedians {
		lo, hi = min(lo, m), max(hi, m)
	}
	if lo <= 0 {
		return math.Inf(1)
	}
	return float64(hi) / float64(lo)
}

const (
	dcLoadedLeaves     = 3
	dcBulkFlowsPerLeaf = 2
	// The request phase, as the live gate's minimal size: on every loaded
	// leaf at once, one request at a time, ten of them 2 s apart, ASR and
	// TTS alternating.
	dcLeafRequests   = 10
	dcRequestSpacing = 2 * time.Second
	dcRequestTimeout = 30 * time.Second
	// The sustained phase: two backlogged uploads per loaded leaf, measured
	// after a warm-up, while the uncongested sibling fetches a 300 KB
	// request every second.
	dcSustainedWindow  = 14 * time.Second
	dcSustainedWarmup  = 2 * time.Second
	dcControlBytes     = 300 << 10
	dcControlEvery     = time.Second
	dcAncestorRateByte = 1_500_000 // 12 Mbit/s, shared by the loaded leaves
)

// dcASRorTTS is request i of the W4 alternation: ASR (355 KB up, 200 B back)
// on even i, TTS (300 B up, 300 KB back) on odd.
func dcASRorTTS(i int) (up, down uint32) {
	if i%2 == 1 {
		return dcTTSUpBytes, dcTTSDownBytes
	}
	return dcASRUpBytes, dcASRDownBytes
}

// runDCFairness is the §11.3 multi-leaf workload at deterministic size, in
// the live gate's two phases: three loaded leaves behind one binding common
// ancestor -- an emulated bottleneck all three cross -- and one uncongested
// sibling on a path of its own, all in one client process.
//
//   - Requests: every loaded leaf at once, ten W4 requests each, one at a
//     time, 2 s apart. Request latency and the completion spread (the
//     slowest leaf's median over the fastest's) come from here.
//   - Sustained: two backlogged uploads per loaded leaf -- the direction its
//     client sends, where the legacy tree's shared node caps every member --
//     while the sibling fetches a request every second. The worst leaf's
//     bytes, Jain's index, the aggregate and the sibling's p50 come from
//     here.
//
// A solo run is the sustained phase of one loaded leaf alone on the same
// ancestor: the calibration that says whether the ancestor binds.
func runDCFairness(t *testing.T, arm dcArm, solo bool) dcFairness {
	t.Helper()
	bed := newDCBed(t, arm)
	defer bed.close()
	leaves := dcLoadedLeaves
	if solo {
		leaves = 1
	}
	shared := pathsim.DCLongHaul()
	shared.RateBytesPerSec = dcAncestorRateByte
	ancestor := pathsim.NewBottleneck(shared)
	var loaded []*dcLeaf
	for i := 0; i < leaves; i++ {
		loaded = append(loaded, bed.leaf(i+1, func(listen, target string) (*pathsim.Relay, error) {
			return ancestor.Attach(listen, target, shared)
		}))
	}
	var control *dcLeaf
	if !solo {
		control = bed.leaf(dcLoadedLeaves+1, func(listen, target string) (*pathsim.Relay, error) {
			return pathsim.New(listen, target, pathsim.DCLongHaul())
		})
		if _, err := control.request(dcControlBytes, 1, dcRequestTimeout); err != nil {
			t.Fatalf("%s control: warm-up request: %v", arm.name, err)
		}
	}
	var out dcFairness
	var mu sync.Mutex
	var wg sync.WaitGroup

	if !solo {
		// The request phase. The first request on each leaf is on a cold
		// app connection, as in the live gate.
		perLeaf := make([][]time.Duration, len(loaded))
		for i, l := range loaded {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for r := 0; r < dcLeafRequests; r++ {
					began := time.Now()
					up, down := dcASRorTTS(r)
					d, err := l.request(up, down, dcRequestTimeout)
					mu.Lock()
					if err != nil {
						out.failed++
						d = dcRequestTimeout
					}
					perLeaf[i] = append(perLeaf[i], d)
					out.requests = append(out.requests, d)
					mu.Unlock()
					time.Sleep(time.Until(began.Add(dcRequestSpacing)))
				}
			}()
		}
		wg.Wait()
		for _, d := range perLeaf {
			out.leafMedians = append(out.leafMedians, dcMedian(d))
		}
	}

	// The sustained phase.
	stop := make(chan struct{})
	var bulkErrors atomic.Int64
	for _, l := range loaded {
		for j := 0; j < dcBulkFlowsPerLeaf; j++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := l.upload(stop); err != nil {
					bulkErrors.Add(1)
				}
			}()
		}
	}
	if control != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tick := time.NewTicker(dcControlEvery)
			defer tick.Stop()
			var inner sync.WaitGroup
			defer inner.Wait()
			for {
				select {
				case <-stop:
					return
				case <-tick.C:
				}
				inner.Add(1)
				go func() {
					defer inner.Done()
					d, err := control.request(dcControlBytes, 1, dcRequestTimeout)
					mu.Lock()
					defer mu.Unlock()
					if err != nil {
						out.failed++
						d = dcRequestTimeout
					}
					out.control = append(out.control, d)
				}()
			}
		}()
	}
	time.Sleep(dcSustainedWarmup)
	start := make([]int64, len(loaded))
	for i, l := range loaded {
		start[i] = l.destination.received.Load()
	}
	time.Sleep(dcSustainedWindow)
	for i, l := range loaded {
		out.leafBytes = append(out.leafBytes, float64(l.destination.received.Load()-start[i]))
	}
	close(stop)
	wg.Wait()
	if n := bulkErrors.Load(); n > 0 {
		t.Errorf("%s: %d backlogged uploads failed", arm.name, n)
	}
	window := dcSustainedWindow.Seconds()
	if solo {
		t.Logf("%-14s solo leaf %.1f Mbit/s", arm.name, out.aggregate()*8/window/1e6)
		return out
	}
	t.Logf("%-14s leaf Mbit/s %v  worst %.1f  aggregate %.1f  jain %.3f  spread %.2f  request p50 %v p99 %v  control p50 %v  failed %d",
		arm.name, mbits(out.leafBytes, window), out.worst()*8/window/1e6, out.aggregate()*8/window/1e6, jainIndex(out.leafBytes),
		out.spread(), dcMedian(out.requests).Round(time.Millisecond), quantileOf(out.requests, 0.99).Round(time.Millisecond),
		dcMedian(out.control).Round(time.Millisecond), out.failed)
	return out
}

func mbits(bytes []float64, seconds float64) []string {
	out := make([]string, len(bytes))
	for i, b := range bytes {
		out[i] = fmt.Sprintf("%.1f", b*8/seconds/1e6)
	}
	return out
}

// TestDCMultiLeafFairnessHoldsWithAKnowledgeOnlyTree runs §11.3's fairness
// workload, deterministic: a binding common ancestor and an uncongested
// sibling, three trials. No request fails, every loaded leaf is served, and
// the completion spread stays below the independent-relay ceiling; the
// gate's contrasts against the legacy bundle were decided at stage 9.
func TestDCMultiLeafFairnessHoldsWithAKnowledgeOnlyTree(t *testing.T) {
	requireDCBed(t)
	var results []dcFairness
	for range 3 {
		results = append(results, runDCFairness(t, dcNewArm, false))
	}
	window := dcSustainedWindow.Seconds()
	for _, r := range results {
		if r.failed > 0 {
			t.Errorf("%d requests failed", r.failed)
		}
		if r.worst() <= 0 {
			t.Errorf("a loaded leaf was not served: %v", mbits(r.leafBytes, window))
		}
		if spread := r.spread(); spread > dcSpreadCeiling {
			t.Errorf("completion spread %.2f reaches the independent-relay regime (ceiling %.1f)", spread, dcSpreadCeiling)
		}
		t.Logf("worst %.2f Mbit/s, aggregate %.2f, jain %.3f, spread %.2f, request p50 %v p99 %v, control p50 %v",
			r.worst()*8/window/1e6, r.aggregate()*8/window/1e6, jainIndex(r.leafBytes), r.spread(),
			dcMedian(r.requests), quantileOf(r.requests, 0.99), dcMedian(r.control))
	}
}

// dcShape is one arm's result on the single-leaf request and sustained
// workload.
type dcShape struct {
	cold      time.Duration   // the first request, on a cold app connection
	requests  []time.Duration // all ten
	sustained float64         // bytes per second over the sustained window
}

const (
	dcShapeRequests  = 10
	dcASRUpBytes     = 355 << 10
	dcASRDownBytes   = 200
	dcTTSUpBytes     = 300
	dcTTSDownBytes   = 300 << 10
	dcShapeSpacing   = 500 * time.Millisecond
	dcSustainedFlows = 2
)

// runDCShape is the DC trial at deterministic size on the characterised path
// (pathsim.DCLongHaul: 200 ms, 14% downstream erasure, clean upstream, a
// 333 Mbit/s knee): ten transactional requests, ASR (355 KB up, 200 B back)
// and TTS (300 B up, 300 KB back) alternating, the first on a cold app
// connection; then two backlogged downloads, the gateway sending.
func runDCShape(t *testing.T, arm dcArm) dcShape {
	t.Helper()
	bed := newDCBed(t, arm)
	defer bed.close()
	l := bed.leaf(1, func(listen, target string) (*pathsim.Relay, error) {
		return pathsim.New(listen, target, pathsim.DCLongHaul())
	})
	var out dcShape
	for i := 0; i < dcShapeRequests; i++ {
		up, down := dcASRorTTS(i)
		d, err := l.request(up, down, dcRequestTimeout)
		if err != nil {
			t.Fatalf("%s request %d: %v", arm.name, i, err)
		}
		if i == 0 {
			out.cold = d
		}
		out.requests = append(out.requests, d)
		time.Sleep(dcShapeSpacing)
	}
	stop := make(chan struct{})
	var got atomic.Int64
	var wg sync.WaitGroup
	var failed atomic.Int64
	for j := 0; j < dcSustainedFlows; j++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := l.download(stop, &got); err != nil {
				failed.Add(1)
			}
		}()
	}
	time.Sleep(dcSustainedWarmup)
	from := got.Load()
	time.Sleep(dcSustainedWindow)
	out.sustained = float64(got.Load()-from) / dcSustainedWindow.Seconds()
	close(stop)
	wg.Wait()
	if n := failed.Load(); n > 0 {
		t.Errorf("%s: %d sustained downloads failed", arm.name, n)
	}
	t.Logf("%-14s cold %v  requests p50 %v p99 %v  sustained %.1f Mbit/s", arm.name, out.cold.Round(time.Millisecond),
		dcMedian(out.requests).Round(time.Millisecond), quantileOf(out.requests, 0.99).Round(time.Millisecond), out.sustained*8/1e6)
	return out
}

// TestDCRequestLatencyAndSustainedGoodputShape is the DC trial's request and
// sustained shape on the emulated characterised path, twice: every request
// completes (a failure is fatal in runDCShape) and traffic is sustained. The
// gate's latency and goodput contrasts against the legacy bundle were
// decided at stage 9.
func TestDCRequestLatencyAndSustainedGoodputShape(t *testing.T) {
	requireDCBed(t)
	for range 2 {
		if r := runDCShape(t, dcNewArm); r.sustained <= 0 {
			t.Fatalf("no sustained goodput: %.0f B/s", r.sustained)
		}
	}
}

// Under dc-long-haul the new architecture's authority reports through the
// tree: its own model is the leaf, and the uplink's and the endpoint pair's
// nodes hear it, so they are priors for the next leaf (§11.3). Nothing
// reports to them as a legacy chain member would (a delivered rate), so no
// node computes a share.
func TestDCProfileRunsOnThePathAuthorityWithTheTreeAsKnowledge(t *testing.T) {
	requireStableImpairmentClock(t)
	bed := newDCBed(t, dcNewArm)
	defer bed.close()
	l := bed.leaf(1, func(listen, target string) (*pathsim.Relay, error) {
		return pathsim.New(listen, target, pathsim.Config{OneWayDelay: 5 * time.Millisecond, LossRate: 0.05, Seed: 9})
	})
	for i := 0; i < 4; i++ {
		if _, err := l.request(dcASRUpBytes, dcASRDownBytes, dcRequestTimeout); err != nil {
			t.Fatal(err)
		}
	}
	var egress, endpoint *pathmodel.NodeReport
	nodes := pathmodel.InspectTree()
	for i := range nodes {
		n := &nodes[i]
		switch {
		case len(n.Key) > 7 && n.Key[:7] == "egress|":
			egress = n
		case len(n.Key) > 5 && n.Key[:5] == "dest|":
			endpoint = n
		}
	}
	if egress == nil || endpoint == nil {
		t.Fatalf("the authority did not report through the tree: %+v", nodes)
	}
	for _, n := range []*pathmodel.NodeReport{egress, endpoint} {
		if n.State.ObservedSamples == 0 {
			t.Errorf("node %s heard nothing: %+v", n.Key, n.State)
		}
	}
}
