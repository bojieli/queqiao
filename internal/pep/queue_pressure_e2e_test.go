package pep

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bojieli/queqiao/internal/pathsim"
)

// Queue pressure at a shared bottleneck: two senders, each a leaf with its
// own client, gateway and controller, each with two backlogged downloads
// through one 300 Mbit/s, 53 ms link with a buffer of one bandwidth-delay
// product. Competing traffic on a real path includes other users' flows, so a
// sender may take its share of the link but must not hold the shared queue
// full. Until stage 11 this also reported a path authority beside a legacy
// sender and beside a loss-based (NewReno) one; an endpoint can no longer be
// configured as either.

const (
	qpFlowsPerSender = 2
	qpWarmup         = 6 * time.Second
	qpWindow         = 14 * time.Second
)

func qpLeg() pathsim.Config {
	return pathsim.Config{OneWayDelay: 26500 * time.Microsecond, RateBytesPerSec: 300_000_000 / 8, QueueBytes: 2 << 20, Seed: 91}
}

type qpResult struct {
	arms      [2]string
	mbps      [2]float64
	queue     []time.Duration // the shared queue's delay, sampled
	drops     uint64          // tail drops at the shared queue in the window
	delivered uint64          // packets out of the shared queue in the window
	telemetry []string        // path controllers' state, sampled
}

func (r qpResult) share(i int) float64 { return r.mbps[i] / (r.mbps[0] + r.mbps[1]) }

func runQueuePressure(t *testing.T) qpResult {
	t.Helper()
	a, b := dcNewArm, dcNewArm
	bed := newDCBed(t, a)
	defer bed.close()
	leg := qpLeg()
	link := pathsim.NewBottleneck(leg)
	attach := func(listen, target string) (*pathsim.Relay, error) { return link.Attach(listen, target, leg) }
	leaves := [2]*dcLeaf{bed.leaf(1, attach), bed.leaf(2, attach)}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var got [2]atomic.Int64
	for i, l := range leaves {
		for range qpFlowsPerSender {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_ = l.download(stop, &got[i])
			}()
		}
	}
	time.Sleep(qpWarmup)
	res := qpResult{arms: [2]string{a.name, b.name}}
	var start [2]int64
	for i := range got {
		start[i] = got[i].Load()
	}
	_, down0 := link.Stats()
	began := time.Now()
	nextTelemetry := began
	window := qpWindow
	if d, err := time.ParseDuration(os.Getenv("QQ_QP_WINDOW")); err == nil && d > 0 {
		window = d
	}
	for time.Since(began) < window {
		_, q := link.QueueDelay()
		res.queue = append(res.queue, q)
		if time.Now().After(nextTelemetry) {
			nextTelemetry = nextTelemetry.Add(qpTelemetryEvery())
			res.telemetry = append(res.telemetry, qpTelemetry(time.Since(began), q))
		}
		time.Sleep(50 * time.Millisecond)
	}
	elapsed := time.Since(began).Seconds()
	_, down1 := link.Stats()
	for i := range got {
		res.mbps[i] = float64(got[i].Load()-start[i]) * 8 / elapsed / 1e6
	}
	res.drops = down1.PacketsDropped - down0.PacketsDropped
	res.delivered = down1.PacketsOut - down0.PacketsOut
	close(stop)
	wg.Wait()
	return res
}

// qpTelemetry is one line of every live path controller's state: window and
// flight against the product of its own estimate and minimum round trip.
func qpTelemetry(at, queue time.Duration) string {
	var b strings.Builder
	fmt.Fprintf(&b, "t=%4.1fs queue %3dms", at.Seconds(), queue.Milliseconds())
	pathAuthorities.mu.Lock()
	defer pathAuthorities.mu.Unlock()
	names := make([]string, 0, len(pathAuthorities.controllers))
	for name, c := range pathAuthorities.controllers {
		if c.Attachments() > 0 {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	for _, name := range names {
		c := pathAuthorities.controllers[name]
		tel, st := c.Telemetry(), c.Stats()
		bdp := float64(tel.MaxBandwidth) * tel.MinRTT.Seconds()
		ratio := func(v float64) float64 {
			if bdp <= 0 {
				return 0
			}
			return v / bdp
		}
		side := "gw"
		if strings.HasPrefix(name, clientPathPrincipal) {
			side = "cl"
		}
		if side == "cl" {
			continue // the client's controllers carry only acknowledgements here
		}
		fmt.Fprintf(&b, " | %s m%d samp %d/%d app bw %.0fMb minrtt %dms cwnd %.2fbdp hi %.2fbdp flight %.2fbdp pace %.0fMb brake %.2f eras %.3f",
			side, tel.Mode, st.AppSamples+st.NonAppSamples, st.AppSamples, float64(tel.MaxBandwidth)*8/1e6, tel.MinRTT.Milliseconds(),
			ratio(float64(tel.CongestionWindow)), ratio(float64(tel.InflightBound)), ratio(float64(st.Flight)), float64(tel.PacingRate)*8/1e6, tel.DelayBrake, tel.Erasure)
	}
	return b.String()
}

func qpTelemetryEvery() time.Duration {
	if d, err := time.ParseDuration(os.Getenv("QQ_QP_TELEMETRY")); err == nil && d > 0 {
		return d
	}
	return time.Second
}

func qpQuantile(d []time.Duration, p float64) time.Duration {
	if len(d) == 0 {
		return 0
	}
	s := append([]time.Duration(nil), d...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	return s[min(len(s)-1, int(p*float64(len(s))))]
}

func (r qpResult) dropRate() float64 {
	if r.delivered+r.drops == 0 {
		return 0
	}
	return float64(r.drops) / float64(r.delivered+r.drops)
}

func (r qpResult) String() string {
	return fmt.Sprintf("%-14s vs %-14s | %6.1f vs %6.1f Mbit/s (share %.2f) | queue p50 %3dms p90 %3dms | drop %.2f%%",
		r.arms[0], r.arms[1], r.mbps[0], r.mbps[1], r.share(0),
		qpQuantile(r.queue, 0.5).Milliseconds(), qpQuantile(r.queue, 0.9).Milliseconds(), 100*r.dropRate())
}

// qpSelfDropMax is the most a pair of path authorities may lose to the
// queue they share. Before the loss bound they held it full and lost 16 to
// 24% there; with it, 0.1 to 0.3%.
const qpSelfDropMax = 0.05

// TestSharedBottleneckQueuePressure reports the standing queue two path
// authorities hold at a shared link and the share each takes, and fails if
// they overflow the queue they share.
//
// QQ_QP_REPEAT=n repeats the run, QQ_QP_WINDOW sets the measured window and
// QQ_QP_TELEMETRY how often the controllers' state is sampled for go test -v.
func TestSharedBottleneckQueuePressure(t *testing.T) {
	requireDCBed(t)
	repeat := 1
	if n, err := strconv.Atoi(os.Getenv("QQ_QP_REPEAT")); err == nil && n > 1 {
		repeat = n
	}
	for range repeat {
		r := runQueuePressure(t)
		t.Logf("PAIR %s", r)
		if testing.Verbose() {
			for _, line := range r.telemetry {
				t.Log("  " + line)
			}
		}
		if r.dropRate() > qpSelfDropMax {
			t.Errorf("two path authorities lost %.1f%% of their packets to the queue they share (bound %.0f%%)",
				100*r.dropRate(), 100*qpSelfDropMax)
		}
	}
}
