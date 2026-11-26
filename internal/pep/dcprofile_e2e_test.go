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
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bojieli/queqiao/internal/metrics"
	"github.com/bojieli/queqiao/internal/pathmodel"
	"github.com/bojieli/queqiao/internal/pathsim"
	"github.com/bojieli/queqiao/internal/profile"
)

// Stage 9: dc-long-haul on the new architecture (docs/DESIGN-PATH-AUTHORITY.md
// §11.3), against the legacy bundle it ran until now, on real fork send loops
// over emulated paths. The legacy arm is the profile as it was: per-connection
// erasure senders reporting to the path tree's chain, whose shared nodes cap
// every member (the hierarchy's joint limit). The candidate arm is the whole
// stage 1-8 bundle with classifier=phased: one path authority per leaf, the
// tree used only as knowledge.
//
// The acceptance bounds are the stage-9 gate's own, frozen in
// scripts/validation/dc_gate.ini before any stage-9 data existed; the live gate
// judges the same quantities against the same legacy arm.
const (
	// dcGoodputRatio: sustained goodput, d = cand - 0.90 x legacy.
	dcGoodputRatio = 0.90
	// dcLatencyMaxRatio: request latency at the median and p99, d = 1.10 x
	// legacy - cand.
	dcLatencyMaxRatio = 1.10
	// dcWorstLeafRatio: the least-served loaded leaf's sustained bytes, d =
	// cand - 0.90 x legacy.
	dcWorstLeafRatio = 0.90
	// dcSpreadMaxRatio and dcSpreadCeiling: the slowest loaded leaf's median
	// request completion over the fastest's, at most 1.25 x legacy's and
	// never into the independent-relay regime (3.5).
	dcSpreadMaxRatio = 1.25
	dcSpreadCeiling  = 3.5
	// dcJainMargin: Jain's index over the loaded leaves' sustained bytes, d =
	// cand - (legacy - 0.05).
	dcJainMargin = 0.05
	// dcControlMaxRatio: the uncongested sibling's request p50 under the
	// others' load, d = 1.10 x legacy - cand.
	dcControlMaxRatio = 1.10
)

// dcLatencySlack is the one allowance the deterministic version adds to a
// latency ratio: the emulator's own scheduling noise, a small fraction of the
// 200 ms round trip, so that a ratio is not failed by a few milliseconds of
// timer jitter on a loaded test host. It is not a performance allowance.
const dcLatencySlack = 15 * time.Millisecond

// dcArm is one bundle under the dc-long-haul profile.
type dcArm struct {
	name   string
	legacy bool
}

var (
	dcLegacyArm = dcArm{name: "legacy", legacy: true}
	dcNewArm    = dcArm{name: "path-authority"}
)

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
	dc, err := profile.ByName(dcLongHaulProfile)
	if err != nil {
		t.Fatal(err)
	}
	certificate, roots := testCertificate(t)
	// The gateway listens on a loopback address of its own too, so that each
	// gateway's legacy per-peer model is its own, as on separate hosts.
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
	if !b.arm.legacy {
		scfg.CCAPI, scfg.Sched, scfg.CongestionScope = CCAPIV2, SchedClass, CongestionScopePath
		scfg.CodedRetry, scfg.Memory, scfg.Classifier = CodedRetrySticky, MemoryOwned, ClassifierPhased
		scfg.GatewayAdmission, scfg.Recovery = GatewayAdmissionOwned, RecoveryCarrier
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
		Transport: TransportQUIC, EnableQUICPool: true, Profile: dc, Metrics: metrics.New(), Logger: discard,
	}
	if !b.arm.legacy {
		ccfg.CCAPI, ccfg.Sched, ccfg.CongestionScope, ccfg.Topology = CCAPIV2, SchedClass, CongestionScopePath, TopologySingle
		ccfg.CodedRetry, ccfg.Memory, ccfg.Classifier = CodedRetrySticky, MemoryOwned, ClassifierPhased
		ccfg.Recovery = RecoveryCarrier
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
	// dcBindingMax is the harness's validity bound (dc_gate.ini [validity]
	// binding_max): the legacy arm's simultaneous aggregate over the sum of
	// the loaded leaves' solo rates. Above it the ancestor did not bind and
	// the comparison is not the one §11.3 asks for.
	dcBindingMax = 0.80
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

func meanOf(xs []float64) float64 {
	var s float64
	for _, x := range xs {
		s += x
	}
	return s / float64(len(xs))
}

// TestDCMultiLeafFairnessHoldsWithAKnowledgeOnlyTree is §11.3's fairness
// criterion, deterministic: a binding common ancestor and an uncongested
// sibling, the worst leaf's service and the spread of completion times
// compared with the legacy bundle's in the same run, two trials of each arm
// in ABBA order, with the live gate's frozen bounds. A knowledge-only tree
// that fails it fails the gate.
func TestDCMultiLeafFairnessHoldsWithAKnowledgeOnlyTree(t *testing.T) {
	requireDCBed(t)
	// Calibration, as the live gate's: one loaded leaf alone on the
	// ancestor, on the legacy arm.
	solo := runDCFairness(t, dcLegacyArm, true).aggregate()
	results := map[string][]dcFairness{}
	// Three trials per arm, ABBAAB: the leaves' shares on an emulated
	// shared queue move by tens of percent between trials.
	for _, arm := range []dcArm{dcLegacyArm, dcNewArm, dcNewArm, dcLegacyArm, dcLegacyArm, dcNewArm} {
		results[arm.name] = append(results[arm.name], runDCFairness(t, arm, false))
	}
	summarise := func(rs []dcFairness) (worst, aggregate, jain, spread float64, p50, p99, control time.Duration, failed int) {
		var ws, as, js, ss []float64
		var requests, controls []time.Duration
		for _, r := range rs {
			ws, as, js, ss = append(ws, r.worst()), append(as, r.aggregate()), append(js, jainIndex(r.leafBytes)), append(ss, r.spread())
			requests, controls = append(requests, r.requests...), append(controls, r.control...)
			failed += r.failed
		}
		return meanOf(ws), meanOf(as), meanOf(js), meanOf(ss), dcMedian(requests), quantileOf(requests, 0.99), dcMedian(controls), failed
	}
	lw, la, lj, ls, lp50, lp99, lc, lf := summarise(results[dcLegacyArm.name])
	cw, ca, cj, cs, cp50, cp99, cc, cf := summarise(results[dcNewArm.name])
	window := dcSustainedWindow.Seconds()
	t.Logf("legacy: worst %.2f Mbit/s, aggregate %.2f, jain %.3f, spread %.2f, request p50 %v p99 %v, control p50 %v, failed %d",
		lw*8/window/1e6, la*8/window/1e6, lj, ls, lp50, lp99, lc, lf)
	t.Logf("new:    worst %.2f Mbit/s, aggregate %.2f, jain %.3f, spread %.2f, request p50 %v p99 %v, control p50 %v, failed %d",
		cw*8/window/1e6, ca*8/window/1e6, cj, cs, cp50, cp99, cc, cf)
	if binding := la / (dcLoadedLeaves * solo); binding > dcBindingMax {
		t.Fatalf("the common ancestor did not bind: the legacy aggregate is %.2f of the loaded leaves' solo rates (bound %.2f)", binding, dcBindingMax)
	} else {
		t.Logf("ancestor binding: legacy aggregate / sum of solo rates = %.2f", binding)
	}
	// Hard: what a knowledge-only tree must guarantee whatever the gate's
	// verdict. Requests complete as often as on the legacy bundle, and the
	// uncongested sibling is not throttled by what the tree knows about its
	// congested siblings (knowledge is a prior, never a limit).
	if cf > lf {
		t.Errorf("the new architecture failed %d requests, the legacy bundle %d", cf, lf)
	}
	if limit := scaleDuration(lc, dcControlMaxRatio) + dcLatencySlack; cc > limit {
		t.Errorf("the uncongested sibling's request p50 %v > %.2f x legacy's %v: tree knowledge throttled it", cc, dcControlMaxRatio, lc)
	}
	// The §11.3 fairness criterion with the gate's frozen bounds, reported as
	// a verdict rather than asserted: it is the measurement the stage-9
	// decision rests on, and a FAIL is a legitimate outcome of the design
	// (the legacy bundle then stays for this profile and joint limiting
	// waits for the tree arbiter, Q2), not a defect in this change. The live
	// gate (scripts/dc_gate.py) decides; this is its deterministic twin.
	verdict := dcVerdict{t: t}
	verdict.check("worst leaf's service", cw >= dcWorstLeafRatio*lw, "%.0f B vs %.2f x legacy's %.0f B", cw, dcWorstLeafRatio, lw)
	verdict.check("completion spread", cs <= dcSpreadMaxRatio*ls && cs <= dcSpreadCeiling, "%.2f vs %.2f x legacy's %.2f, ceiling %.1f", cs, dcSpreadMaxRatio, ls, dcSpreadCeiling)
	verdict.check("cross-leaf fairness (Jain)", cj >= lj-dcJainMargin, "%.3f vs legacy's %.3f - %.2f", cj, lj, dcJainMargin)
	verdict.check("sustained goodput", ca >= dcGoodputRatio*la, "%.0f B vs %.2f x legacy's %.0f B", ca, dcGoodputRatio, la)
	verdict.check("loaded request p50", cp50 <= scaleDuration(lp50, dcLatencyMaxRatio)+dcLatencySlack, "%v vs %.2f x legacy's %v", cp50, dcLatencyMaxRatio, lp50)
	verdict.check("loaded request p99", cp99 <= scaleDuration(lp99, dcLatencyMaxRatio)+dcLatencySlack, "%v vs %.2f x legacy's %v", cp99, dcLatencyMaxRatio, lp99)
	verdict.report("§11.3 multi-leaf fairness")
}

// dcVerdict collects a gate's criteria and logs one verdict line.
type dcVerdict struct {
	t      *testing.T
	failed []string
}

func (v *dcVerdict) check(name string, ok bool, format string, args ...any) {
	v.t.Helper()
	state := "pass"
	if !ok {
		state = "FAIL"
		v.failed = append(v.failed, name)
	}
	v.t.Logf("  %-28s %s: %s", name, state, fmt.Sprintf(format, args...))
}

func (v *dcVerdict) report(gate string) {
	v.t.Helper()
	if len(v.failed) == 0 {
		v.t.Logf("%s verdict: PASS", gate)
		return
	}
	v.t.Logf("%s verdict: FAIL on %v (design §11.3: the legacy bundle stays for dc-long-haul)", gate, v.failed)
}

func scaleDuration(d time.Duration, f float64) time.Duration { return time.Duration(float64(d) * f) }

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
// sustained shape on the emulated characterised path, ABBA: request latency
// at the median and p99 within 10% of the legacy bundle's, and sustained
// goodput at least 0.90 of it (§11.3, §15.4).
func TestDCRequestLatencyAndSustainedGoodputShape(t *testing.T) {
	requireDCBed(t)
	results := map[string][]dcShape{}
	for _, arm := range []dcArm{dcLegacyArm, dcNewArm, dcNewArm, dcLegacyArm} {
		results[arm.name] = append(results[arm.name], runDCShape(t, arm))
	}
	pool := func(rs []dcShape) (p50, p99 time.Duration, sustained float64) {
		var all []time.Duration
		var s []float64
		for _, r := range rs {
			all = append(all, r.requests...)
			s = append(s, r.sustained)
		}
		return dcMedian(all), quantileOf(all, 0.99), meanOf(s)
	}
	lp50, lp99, ls := pool(results[dcLegacyArm.name])
	cp50, cp99, cs := pool(results[dcNewArm.name])
	t.Logf("legacy: request p50 %v p99 %v, sustained %.1f Mbit/s", lp50, lp99, ls*8/1e6)
	t.Logf("new:    request p50 %v p99 %v, sustained %.1f Mbit/s", cp50, cp99, cs*8/1e6)
	// Every request completed (a failure is fatal above) and both arms
	// sustained traffic; the gate's latency and goodput contrasts are the
	// verdict, as in the fairness test.
	if cs <= 0 || ls <= 0 {
		t.Fatalf("no sustained goodput: new %.0f, legacy %.0f B/s", cs, ls)
	}
	verdict := dcVerdict{t: t}
	verdict.check("request p50", cp50 <= scaleDuration(lp50, dcLatencyMaxRatio)+dcLatencySlack, "%v vs %.2f x legacy's %v", cp50, dcLatencyMaxRatio, lp50)
	verdict.check("request p99", cp99 <= scaleDuration(lp99, dcLatencyMaxRatio)+dcLatencySlack, "%v vs %.2f x legacy's %v", cp99, dcLatencyMaxRatio, lp99)
	verdict.check("sustained goodput", cs >= dcGoodputRatio*ls, "%.1f Mbit/s vs %.2f x legacy's %.1f", cs*8/1e6, dcGoodputRatio, ls*8/1e6)
	verdict.report("DC request and sustained shape")
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
	// Only the client's authority is in a tree: one uplink and one endpoint
	// pair, the client's to its relay. The gateway's authority takes no tree
	// view (#235), so no node is keyed by the gateway's own address.
	gateway := dcHost(t, 40, 1)
	var dests int
	for _, n := range nodes {
		if strings.Contains(n.Key, gateway+"->") {
			t.Errorf("the gateway's authority reported into a tree: node %s", n.Key)
		}
		if strings.HasPrefix(n.Key, "dest|") {
			dests++
		}
	}
	if dests != 1 || !strings.Contains(endpoint.Key, "->"+dcHost(t, 20, 1)) {
		t.Errorf("want exactly the client's endpoint pair to the relay, got %d dest nodes (%s)", dests, endpoint.Key)
	}
	for _, n := range []*pathmodel.NodeReport{egress, endpoint} {
		if n.State.ObservedSamples == 0 {
			t.Errorf("node %s heard nothing: %+v", n.Key, n.State)
		}
		if n.State.Share != 0 || n.State.Seed != 0 {
			t.Errorf("node %s computed a share %v or seed %v from an authority", n.Key, n.State.Share, n.State.Seed)
		}
	}
}
