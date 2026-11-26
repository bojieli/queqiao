package pep

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bojieli/queqiao/internal/pathsim"
)

// audioInterval and audioBytes are W7's audio: 160 B every 20 ms.
const (
	audioInterval = 20 * time.Millisecond
	audioBytes    = 160
)

// audioDirection is one direction of a call's audio: each packet's one-way
// delay, zero for one that never arrived.
type audioDirection struct {
	delays []time.Duration
}

// judge returns how many packets were late (lost, or more than playout
// past the direction's best one-way delay) and the lateness of each that
// arrived. Both ends of the test share one clock, so one-way delays are
// exact.
func (a audioDirection) judge(playout time.Duration) (late int, lateness []time.Duration) {
	var best time.Duration
	for _, d := range a.delays {
		if d > 0 && (best == 0 || d < best) {
			best = d
		}
	}
	for _, d := range a.delays {
		if d == 0 {
			late++
			continue
		}
		l := d - best
		lateness = append(lateness, l)
		if l > playout {
			late++
		}
	}
	return late, lateness
}

// audioPacket stamps a packet with its sequence and send time.
func audioPacket(seq int) []byte {
	p := make([]byte, audioBytes)
	binary.BigEndian.PutUint32(p, uint32(seq))
	binary.BigEndian.PutUint64(p[4:], uint64(time.Now().UnixNano()))
	return p
}

// audioDelay reads a stamped packet's sequence and one-way delay.
func audioDelay(p []byte) (int, time.Duration, bool) {
	if len(p) < 12 {
		return 0, 0, false
	}
	sent := time.Unix(0, int64(binary.BigEndian.Uint64(p[4:])))
	return int(binary.BigEndian.Uint32(p)), time.Since(sent), true
}

// audioFarEnd is the far side of a call: it records the uplink's arrivals
// and, once the first uplink packet names the call's return address, sends
// its own open-loop downlink of count packets. Uplink arrivals never pace it.
type audioFarEnd struct {
	conn  *net.UDPConn
	count int
	mu    sync.Mutex
	up    audioDirection
	// downSent is closed when the downlink's last packet has been sent.
	downSent chan struct{}
}

func newAudioFarEnd(t *testing.T, count int) *audioFarEnd {
	t.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	f := &audioFarEnd{conn: conn, count: count, up: audioDirection{delays: make([]time.Duration, count)}, downSent: make(chan struct{})}
	go func() {
		buf := make([]byte, 2048)
		started := false
		for {
			n, from, err := conn.ReadFromUDP(buf)
			if err != nil {
				return
			}
			seq, delay, ok := audioDelay(buf[:n])
			if !ok {
				continue
			}
			f.mu.Lock()
			if seq < count && f.up.delays[seq] == 0 {
				f.up.delays[seq] = delay
			}
			f.mu.Unlock()
			if !started {
				started = true
				go func() {
					defer close(f.downSent)
					start := time.Now()
					for seq := range count {
						if wait := time.Until(start.Add(time.Duration(seq) * audioInterval)); wait > 0 {
							time.Sleep(wait)
						}
						_, _ = conn.WriteToUDP(audioPacket(seq), from)
					}
				}()
			}
		}
	}()
	return f
}

func (f *audioFarEnd) uplink() audioDirection {
	f.mu.Lock()
	defer f.mu.Unlock()
	return audioDirection{delays: append([]time.Duration(nil), f.up.delays...)}
}

// runCall sends the uplink for count packets and records the downlink until
// the far end has sent its last packet and a drain period has passed,
// reading on across any gap. A downlink that never starts ends the wait at
// the call's length plus a minute.
func runCall(a *socksAssociation, far *audioFarEnd, count int, drain time.Duration) (down audioDirection) {
	down.delays = make([]time.Duration, count)
	stop := make(chan struct{})
	go func() {
		select {
		case <-far.downSent:
			time.Sleep(drain)
		case <-time.After(time.Duration(count)*audioInterval + time.Minute):
		}
		close(stop)
	}()
	done := make(chan struct{})
	go func() {
		defer close(done)
		buf := make([]byte, 2048)
		for {
			select {
			case <-stop:
				return
			default:
			}
			payload, err := a.receive(buf, time.Now().Add(100*time.Millisecond))
			if err != nil {
				if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
					continue
				}
				return
			}
			if seq, delay, ok := audioDelay(payload); ok && seq < count && down.delays[seq] == 0 {
				down.delays[seq] = delay
			}
		}
	}()
	start := time.Now()
	target := far.conn.LocalAddr().(*net.UDPAddr)
	for seq := range count {
		if wait := time.Until(start.Add(time.Duration(seq) * audioInterval)); wait > 0 {
			time.Sleep(wait)
		}
		_ = a.send(target, audioPacket(seq))
	}
	<-done
	return down
}

// m5Direction is one audio direction's M5 measures.
type m5Direction struct {
	sent     int
	late     int
	lateness []time.Duration
}

// m5Outcome is one block's M5 measures.
type m5Outcome struct {
	audio        [2]m5Direction // uplink, downlink
	keystrokes   []time.Duration
	heavyGoodput float64       // during the thin measurements
	heavyGap     time.Duration // the longest the heavy download moved nothing during them
	pathDrops    uint64        // the emulated bottleneck's own drops, both directions
}

// M5. Heavy UDP beside thin UDP and coded stream chunks
// (docs/WORKLOAD-MODEL.md): a tunnelled QUIC download (W8) fills the path
// while a call's audio (W7, 160 B every 20 ms each way on its own
// association) and terminal keystrokes (W6, on a TCP flow small enough to be
// coded) run beside it. All three share the tunnel's one ordered symbol
// stream (design Q5), so a heavy association that the tunnel lets queue
// delays the others by its queue.
//
// The audio is two open-loop streams, one per direction, each on its own
// clock: the far end starts its downlink when the first uplink packet names
// the call. Both ends share the test's clock, so each packet's one-way delay
// is exact, and its lateness is that delay less the direction's best. A
// packet is late when its lateness exceeds the 150 ms playout budget or it
// never arrives. Bounds, per direction (models W7, W6): audio late loss ≤
// legacy + 0.5 pp, with one packet of allowance at this size; audio lateness
// p99 ≤ legacy × 1.10 + 10 ms; keystroke echo p99 ≤ legacy × 1.10 + 20 ms.
// The heavy download must move data throughout the measurements. The model
// also reports the coded FIFO's sojourn p99 and the UDP PACKET frames
// dropped at overflow; no build exports either yet, so the emulated
// bottleneck's own drops and the heavy flow's goodput are reported in their
// place.
//
// A block is one client, gateway and emulated path: 120 ms round trip,
// 20 Mbit/s behind a drop-tail queue, for 30 s. The measures follow the inner
// controller's sawtooth, so one block's tail moves by a third from run to
// run in either direction, as the design expects of a tail (§15.4 judges
// tails over blocks). The full suite runs §15.4's sequential gate on each
// measure's per-block contrast: 6 blocks, then one at a time to 12, passing
// when every lower bound clears zero, failing when any upper bound falls
// below it, and failing as inconclusive at the cap. Until stage 11 each block
// also ran the legacy bundle and the contrast was paired with it; the
// contrast is now against legacy's mean over the blocks measured at stage 10,
// before the legacy bundle was deleted (m5Check.legacy). -short runs one 8 s
// block as a smoke test: every keystroke echoed, audio measured both ways,
// the heavy flow moving.
func TestHeavyUDPDoesNotDelayThinUDPOrCodedChunks(t *testing.T) {
	requireStableImpairmentClock(t)
	const playout = 150 * time.Millisecond
	measure, minBlocks, maxBlocks := 30*time.Second, 6, 12
	if testing.Short() {
		measure, minBlocks, maxBlocks = 8*time.Second, 1, 1
	}
	echoListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = echoListener.Close() })
	go echoDestination(echoListener)

	checks := m5Checks
	// measures holds, per block: late fraction up, late fraction down,
	// lateness p99 up, lateness p99 down, keystroke echo p99.
	var measures [5][]float64
	for block := range maxBlocks {
		// A subtest per block, so each block's clients, gateways and paths
		// are torn down before the next starts.
		var outcome *m5Outcome
		if !t.Run(fmt.Sprintf("block%d", block), func(t *testing.T) {
			outcome = runM5Block(t, echoListener.Addr().String(), measure, playout)
		}) {
			t.FailNow()
		}
		if outcome == nil {
			continue // not selected by -run
		}
		o := outcome
		for d, name := range []string{"uplink", "downlink"} {
			a := o.audio[d]
			t.Logf("block %d, %s audio: %d of %d late or lost, lateness p99 %v", block, name, a.late, a.sent,
				durationQuantile(a.lateness, 0.99).Round(time.Millisecond))
			if len(a.lateness) == 0 {
				t.Fatalf("no %s audio arrived", name)
			}
			measures[d] = append(measures[d], float64(a.late)/float64(a.sent))
			measures[2+d] = append(measures[2+d], durationQuantile(a.lateness, 0.99).Seconds())
		}
		t.Logf("block %d: keystroke echo p99 %v over %d; heavy goodput %.2f Mbit/s; bottleneck drops %d",
			block, durationQuantile(o.keystrokes, 0.99).Round(time.Millisecond), len(o.keystrokes),
			o.heavyGoodput*8/1e6, o.pathDrops)
		measures[4] = append(measures[4], durationQuantile(o.keystrokes, 0.99).Seconds())
		// A heavy flow that stalled would leave the thin flows nothing to
		// compete with: the measurement would not be M5.
		if o.heavyGoodput < 1e6/8 || o.heavyGap > 2*time.Second {
			t.Fatalf("the heavy download moved %.2f Mbit/s during the measurements and stopped for up to %v",
				o.heavyGoodput*8/1e6, o.heavyGap.Round(time.Millisecond))
		}
		n := len(measures[0])
		if testing.Short() || n < minBlocks || n != block+1 {
			continue // too few blocks to look, or -run selected some only
		}
		if !judgeBounds(t) {
			return
		}
		// Design §15.4's sequential gate, at its frozen look levels: pass
		// when every measure's lower bound clears zero, fail when any upper
		// bound is below it, and otherwise run one more block.
		verdict := judgeM5(t, checks, measures, n, n == maxBlocks)
		if verdict != m5Undecided {
			if verdict == m5Fail {
				t.Fail()
			}
			return
		}
	}
	if n := len(measures[0]); !testing.Short() && n > 0 && n < minBlocks {
		t.Logf("%d of %d blocks selected by -run: not judged", n, minBlocks)
	}
}

// m5Check is one M5 measure and its bound: at most m × legacy + a, where
// legacy is the legacy bundle's mean over the blocks measured at stage 10.
type m5Check struct {
	name    string
	measure int
	m, a    float64
	unit    float64 // for the message: 100 for %, 1000 for ms
	legacy  float64
}

// m5Checks are the model's bounds (W7, W6) against legacy's means over 21
// blocks measured at stage 10, on a 32-core Linux host: 2.19 % and 2.60 % of
// audio late or lost, lateness p99 of 224 ms and 289 ms, keystroke echo p99
// of 341 ms. Paired with those blocks, the path-authority bundle was not
// decided either way at the cap on the audio measures: its uplink lateness
// p99 ran at 223 to 249 ms, beside a bound of 257.
var m5Checks = []m5Check{
	{"uplink audio late loss (%)", 0, 1, 0.005, 100, 0.0219},
	{"downlink audio late loss (%)", 1, 1, 0.005, 100, 0.0260},
	{"uplink audio lateness p99 (ms)", 2, 1.10, 0.010, 1000, 0.2242},
	{"downlink audio lateness p99 (ms)", 3, 1.10, 0.010, 1000, 0.2892},
	{"keystroke echo p99 (ms)", 4, 1.10, 0.020, 1000, 0.3406},
}

type m5Verdict int

const (
	m5Undecided m5Verdict = iota
	m5Pass
	m5Fail
)

// m5LookT is the one-sided t quantile of each look, 6 to 12 blocks: the
// harness's frozen Lan-DeMets Pocock-type levels (0.031, then 0.0185 to
// 0.0162; scripts/ab_stats.py spending_levels) at n − 1 degrees of freedom.
var m5LookT = map[int]float64{6: 2.395, 7: 2.669, 8: 2.637, 9: 2.581, 10: 2.526, 11: 2.478, 12: 2.433}

// judgeM5 applies the per-block contrast d = (m × legacy + a) − measured of
// each check at the look after n blocks.
func judgeM5(t testing.TB, checks []m5Check, measures [5][]float64, n int, last bool) m5Verdict {
	t.Helper()
	tq := m5LookT[n]
	allPass, anyFail := true, false
	for _, c := range checks {
		d := make([]float64, n)
		for b := range n {
			d[b] = c.m*c.legacy + c.a - measures[c.measure][b]
		}
		mean, se := meanAndSE(d)
		lower, upper := mean-tq*se, mean+tq*se
		t.Logf("look %d, %s: legacy %.1f, measured %v; contrast mean %.3g, bounds [%.3g, %.3g]",
			n, c.name, c.legacy*c.unit, scaled(measures[c.measure], c.unit),
			mean*c.unit, lower*c.unit, upper*c.unit)
		if upper < 0 {
			anyFail = true
			t.Errorf("%s: exceeds legacy × %.2f + %.3g (contrast upper bound %.3g after %d blocks)",
				c.name, c.m, c.a*c.unit, upper*c.unit, n)
		}
		if lower <= 0 {
			allPass = false
			if last && upper >= 0 {
				t.Errorf("%s: no evidence after %d blocks that it is within legacy × %.2f + %.3g (contrast lower bound %.3g)",
					c.name, n, c.m, c.a*c.unit, lower*c.unit)
			}
		}
	}
	switch {
	case anyFail:
		return m5Fail
	case allPass:
		return m5Pass
	case last:
		return m5Fail
	}
	return m5Undecided
}

// meanAndSE is the mean of d and its standard error.
func meanAndSE(d []float64) (mean, se float64) {
	n := float64(len(d))
	for _, v := range d {
		mean += v
	}
	mean /= n
	if len(d) < 2 {
		return mean, 0
	}
	var ss float64
	for _, v := range d {
		ss += (v - mean) * (v - mean)
	}
	return mean, math.Sqrt(ss/(n-1)) / math.Sqrt(n)
}

func scaled(values []float64, unit float64) []float64 {
	out := make([]float64, len(values))
	for i, v := range values {
		out[i] = math.Round(v*unit*10) / 10
	}
	return out
}

// runM5Block runs one M5 block on a fresh client, gateway, emulated path
// and far ends.
func runM5Block(t *testing.T, echo string, measure, playout time.Duration) *m5Outcome {
	t.Helper()
	path := pathsim.Config{OneWayDelay: 60 * time.Millisecond, Seed: 5, RateBytesPerSec: 20e6 / 8}
	rig := newWorkloadRig(t, path, pathAuthorityArm, nil)
	server := newInnerQUICServer(t)
	far := newAudioFarEnd(t, int(measure/audioInterval))
	var o m5Outcome
	if err := runM5Arm(t, rig, server, far, echo, measure, playout, &o); err != nil {
		t.Fatal(err)
	}
	return &o
}

// runM5Arm runs one block's M5: the thin endpoints are set up first, then the
// heavy download starts and fills the path, then the call and the keystrokes
// run while the download's progress is counted. Every worker it starts has
// ended when it returns.
func runM5Arm(t *testing.T, rig *workloadRig, server *innerQUICServer, far *audioFarEnd, echo string, measure, playout time.Duration, o *m5Outcome) error {
	ctx, cancel := context.WithTimeout(context.Background(), measure+2*time.Minute)
	defer cancel()

	keys := rig.dial(echo)
	audio, err := rig.associate()
	if err != nil {
		return fmt.Errorf("audio association: %w", err)
	}
	defer audio.Close()
	conn, err := dialInnerQUIC(ctx, t, rig, server)
	if err != nil {
		return fmt.Errorf("inner handshake: %w", err)
	}
	defer conn.CloseWithError(0, "")
	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return err
	}
	var request [4]byte
	binary.BigEndian.PutUint32(request[:], 1<<30) // outlasts the block, and fits an int on 32-bit hosts
	if _, err := stream.Write(request[:]); err != nil {
		return err
	}
	_ = stream.Close()

	// The heavy download runs until the measurements end; heavyDone is
	// waited on whatever happens below.
	var heavy atomic.Int64
	heavyCtx, stopHeavy := context.WithCancel(ctx)
	heavyDone := make(chan struct{})
	go func() {
		defer close(heavyDone)
		go func() {
			<-heavyCtx.Done()
			stream.CancelRead(0)
		}()
		buf := make([]byte, 64*1024)
		for {
			n, err := stream.Read(buf)
			heavy.Add(int64(n))
			if err != nil {
				return
			}
		}
	}()
	defer func() {
		stopHeavy()
		<-heavyDone
	}()
	time.Sleep(3 * time.Second) // let it fill the path

	upBefore, downBefore := rig.relay.Stats()
	heavyBefore, start := heavy.Load(), time.Now()
	// Sample the heavy download's progress throughout, so a download that
	// stalled part way cannot pass for competition.
	sampling := make(chan struct{})
	sampled := make(chan time.Duration, 1)
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		last, lastMoved, longest := heavy.Load(), time.Now(), time.Duration(0)
		for {
			select {
			case <-sampling:
				sampled <- max(longest, time.Since(lastMoved))
				return
			case now := <-ticker.C:
				if n := heavy.Load(); n != last {
					last, lastMoved = n, now
				} else {
					longest = max(longest, now.Sub(lastMoved))
				}
			}
		}
	}()
	var thin sync.WaitGroup
	var keyErr error
	thin.Add(2)
	go func() {
		defer thin.Done()
		down := runCall(audio, far, int(measure/audioInterval), time.Second)
		o.audio[1] = summarizeAudio(down, playout)
	}()
	go func() {
		defer thin.Done()
		o.keystrokes, keyErr = runKeystrokes(keys, measure)
	}()
	thin.Wait()
	close(sampling)
	o.heavyGap = <-sampled
	o.heavyGoodput = float64(heavy.Load()-heavyBefore) / time.Since(start).Seconds()
	upAfter, downAfter := rig.relay.Stats()
	o.pathDrops = upAfter.PacketsDropped - upBefore.PacketsDropped + downAfter.PacketsDropped - downBefore.PacketsDropped
	o.audio[0] = summarizeAudio(far.uplink(), playout)
	if keyErr != nil {
		return fmt.Errorf("keystrokes: %w", keyErr)
	}
	return nil
}

func summarizeAudio(a audioDirection, playout time.Duration) m5Direction {
	late, lateness := a.judge(playout)
	return m5Direction{sent: len(a.delays), late: late, lateness: lateness}
}

// runKeystrokes types a 36-byte keystroke every 50 ms for d on conn, which
// echoes, without waiting for each echo, as a person types; it returns each
// echo's time. The stream is ordered, so echoes are matched by position.
func runKeystrokes(conn net.Conn, d time.Duration) ([]time.Duration, error) {
	const interval = 50 * time.Millisecond
	count := int(d / interval)
	sent := make(chan time.Time, count)
	_ = conn.SetDeadline(time.Now().Add(d + 30*time.Second))
	writeErr := make(chan error, 1)
	go func() {
		key := make([]byte, 36)
		start := time.Now()
		for i := range count {
			if wait := time.Until(start.Add(time.Duration(i) * interval)); wait > 0 {
				time.Sleep(wait)
			}
			sent <- time.Now()
			if _, err := conn.Write(key); err != nil {
				writeErr <- err
				return
			}
		}
		writeErr <- nil
	}()
	echo := make([]byte, 36)
	echoes := make([]time.Duration, 0, count)
	for range count {
		if _, err := io.ReadFull(conn, echo); err != nil {
			return echoes, err
		}
		echoes = append(echoes, time.Since(<-sent))
	}
	return echoes, <-writeErr
}

// The M5 gate passes only on evidence: a clear pass, a clear failure, and
// noise that never settles (which fails at the cap) all come out as §15.4
// says.
func TestM5SequentialGateNeedsEvidenceToPass(t *testing.T) {
	checks := []m5Check{{"lateness (ms)", 0, 1.10, 0.010, 1000, 0.200}}
	series := func(measured []float64) (m [5][]float64) {
		m[0] = measured
		return m
	}
	same := []float64{0.200, 0.210, 0.190, 0.205, 0.195, 0.200}
	better := []float64{0.150, 0.160, 0.155, 0.150, 0.158, 0.152}
	worse := []float64{0.300, 0.310, 0.290, 0.305, 0.295, 0.300}
	// Legacy's own level clears the 10 % + 10 ms margin.
	if v := judgeM5(t, checks, series(same), 6, false); v != m5Pass {
		t.Errorf("at legacy's level: verdict %d, want pass", v)
	}
	if v := judgeM5(t, checks, series(better), 6, false); v != m5Pass {
		t.Errorf("better than legacy: verdict %d, want pass", v)
	}
	inner := &quietTB{TB: t}
	if v := judgeM5(inner, checks, series(worse), 6, false); v != m5Fail {
		t.Errorf("clearly worse than legacy: verdict %d, want fail", v)
	}
	// Right at the bound with block-to-block noise is undecided before the
	// cap and fails at it.
	edge := []float64{0.260, 0.180, 0.290, 0.170, 0.250, 0.200}
	if v := judgeM5(inner, checks, series(edge), 6, false); v != m5Undecided {
		t.Errorf("noisy at the bound: verdict %d, want undecided", v)
	}
	if v := judgeM5(inner, checks, series(edge), 6, true); v != m5Fail {
		t.Errorf("noisy at the cap: verdict %d, want fail", v)
	}
}

// quietTB records failures instead of failing, for testing a judge.
type quietTB struct {
	testing.TB
	failed bool
}

func (q *quietTB) Helper()               {}
func (q *quietTB) Logf(string, ...any)   {}
func (q *quietTB) Errorf(string, ...any) { q.failed = true }
