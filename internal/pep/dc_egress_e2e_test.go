package pep

import (
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bojieli/queqiao/internal/pathsim"
)

// The gateway's joint egress limit (docs/DESIGN-EGRESS-JOINT-LIMIT.md §5.3).
//
// One gateway port serves several DC leaves, each its own client, gateway
// object and path authority. Past the port is a point that drops what it
// cannot pass and holds no queue: a policer. Two leaves carry backlogged
// downloads and a light control leaf fetches 300 KB every half second, all
// across the one policer. Without a joint limit each authority reads its share
// of the policer's drops as channel erasure and makes it up, so together they
// overdrive the policer, and the control leaf pays an extra round trip for
// every packet of its fetch that is lost.
//
// The bed is the DC gate's: a 53 ms leg, the legacy arm beside the path
// authority, ABBAAB.

const (
	egressLoadedLeaves = 2
	egressFetchBytes   = 300 << 10
	egressFetchEvery   = 500 * time.Millisecond
	egressWarmup       = 6 * time.Second
	egressWindow       = 12 * time.Second
	// egressMbit is the policed egress's rate. QQ_EGRESS_BED_MBIT scales the
	// bed down on a host that cannot carry 1 Gbit/s through one process.
	egressMbit = 1000
)

func egressBedMbit(t *testing.T) int {
	t.Helper()
	if v := os.Getenv("QQ_EGRESS_BED_MBIT"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			t.Fatalf("QQ_EGRESS_BED_MBIT=%q", v)
		}
		return n
	}
	return egressMbit
}

// egressPolicedLeg is a policed leg: a token bucket refilled every 2 ms, no
// queue, so nothing stands in the sender's round trip.
func egressPolicedLeg(mbit int, loss float64, seed int64) pathsim.Config {
	return pathsim.Config{
		OneWayDelay: 26500 * time.Microsecond, RateBytesPerSec: uint64(mbit) * 1_000_000 / 8,
		PolicerRefillPeriod: 2 * time.Millisecond, LossRate: loss, Seed: seed,
	}
}

type egressResult struct {
	control    []time.Duration
	loadedMbps float64
	dropRate   float64 // of the policed leg's downstream packets
	failed     int
}

func runEgress(t *testing.T, arm dcArm, mbit int) egressResult {
	t.Helper()
	bed := newDCBed(t, arm)
	defer bed.close()
	leg := egressPolicedLeg(mbit, 0, 91)
	policer := pathsim.NewBottleneck(leg)
	attach := func(listen, target string) (*pathsim.Relay, error) { return policer.Attach(listen, target, leg) }
	var loaded []*dcLeaf
	for i := range egressLoadedLeaves {
		loaded = append(loaded, bed.leaf(i+1, attach))
	}
	control := bed.leaf(egressLoadedLeaves+1, attach)
	var res egressResult
	fetch := func() time.Duration {
		d, err := control.request(0, egressFetchBytes, dcRequestTimeout)
		if err != nil {
			res.failed++
			return dcRequestTimeout
		}
		return d
	}
	fetch()
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var got atomic.Int64
	for _, l := range loaded {
		for range 2 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_ = l.download(stop, &got)
			}()
		}
	}
	time.Sleep(egressWarmup)
	_, down0 := policer.Stats()
	start, began := got.Load(), time.Now()
	for time.Since(began) < egressWindow {
		at := time.Now()
		res.control = append(res.control, fetch())
		time.Sleep(time.Until(at.Add(egressFetchEvery)))
	}
	res.loadedMbps = float64(got.Load()-start) * 8 / time.Since(began).Seconds() / 1e6
	_, down1 := policer.Stats()
	if in := down1.PacketsIn - down0.PacketsIn; in > 0 {
		res.dropRate = float64((down1.PacketsDropped+down1.PacketsLost)-(down0.PacketsDropped+down0.PacketsLost)) / float64(in)
	}
	close(stop)
	wg.Wait()
	return res
}

func TestDCControlLeafBesidePolicedEgress(t *testing.T) {
	requireDCBed(t)
	mbit := egressBedMbit(t)
	pooled := map[string][]time.Duration{}
	rates := map[string][]float64{}
	drops := map[string][]float64{}
	for _, arm := range []dcArm{dcLegacyArm, dcNewArm, dcNewArm, dcLegacyArm, dcLegacyArm, dcNewArm} {
		r := runEgress(t, arm, mbit)
		t.Logf("%-14s control p50 %v p90 %v max %v | loaded leaves %.0f Mbit/s | policer drop %.1f%% | failed %d",
			arm.name, q(r.control, 0.5).Round(time.Millisecond), q(r.control, 0.9).Round(time.Millisecond),
			q(r.control, 1).Round(time.Millisecond), r.loadedMbps, 100*r.dropRate, r.failed)
		pooled[arm.name] = append(pooled[arm.name], r.control...)
		rates[arm.name] = append(rates[arm.name], r.loadedMbps)
		drops[arm.name] = append(drops[arm.name], r.dropRate)
	}
	mean := func(v []float64) float64 {
		s := 0.0
		for _, x := range v {
			s += x
		}
		return s / float64(len(v))
	}
	for _, arm := range []dcArm{dcLegacyArm, dcNewArm} {
		t.Logf("POOLED %-14s control p50 %v p90 %v (n=%d) | loaded leaves mean %.0f Mbit/s | policer drop %.1f%%",
			arm.name, q(pooled[arm.name], 0.5).Round(time.Millisecond), q(pooled[arm.name], 0.9).Round(time.Millisecond),
			len(pooled[arm.name]), mean(rates[arm.name]), 100*mean(drops[arm.name]))
	}
	legacy, cand := q(pooled[dcLegacyArm.name], 0.5), q(pooled[dcNewArm.name], 0.5)
	if limit := time.Duration(dcControlMaxRatio*float64(legacy)) + dcLatencySlack; cand > limit {
		t.Errorf("control leaf p50 %v exceeds %.2fx legacy's %v (+%v slack)", cand, dcControlMaxRatio, legacy, dcLatencySlack)
	}
	if lm, cm := mean(rates[dcLegacyArm.name]), mean(rates[dcNewArm.name]); cm < lm {
		t.Errorf("loaded leaves' aggregate %.0f Mbit/s is below legacy's %.0f", cm, lm)
	}
}

// A single leaf behind a policer, with and without channel loss. Part A
// withholds compensation for loss that rises with the sender's rate, which on
// a policed path is intended, so this is where it would show if it cost the
// leaf anything: the policed leaf's sustained goodput and a fetch beside its
// download, against the legacy arm, by the DC gate's own bounds.

type policedResult struct {
	mbps    float64
	fetches []time.Duration
	failed  int
}

func runPolicedLeaf(t *testing.T, arm dcArm, mbit int, loss float64) policedResult {
	t.Helper()
	bed := newDCBed(t, arm)
	defer bed.close()
	leg := egressPolicedLeg(mbit, loss, 93)
	leaf := bed.leaf(1, func(listen, target string) (*pathsim.Relay, error) { return pathsim.New(listen, target, leg) })
	var res policedResult
	fetch := func() time.Duration {
		d, err := leaf.request(0, 64<<10, dcRequestTimeout)
		if err != nil {
			res.failed++
			return dcRequestTimeout
		}
		return d
	}
	fetch()
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var got atomic.Int64
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = leaf.download(stop, &got)
	}()
	time.Sleep(egressWarmup)
	start, began := got.Load(), time.Now()
	for time.Since(began) < egressWindow {
		at := time.Now()
		res.fetches = append(res.fetches, fetch())
		time.Sleep(time.Until(at.Add(egressFetchEvery)))
	}
	res.mbps = float64(got.Load()-start) * 8 / time.Since(began).Seconds() / 1e6
	close(stop)
	wg.Wait()
	return res
}

func TestDCPolicedLeafGoodputAndLatency(t *testing.T) {
	requireDCBed(t)
	for _, tc := range []struct {
		name string
		mbit int
		loss float64
	}{
		{"policed 100 Mbit/s", 100, 0},
		{"policed 100 Mbit/s, 5% channel loss", 100, 0.05},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pooled := map[string][]time.Duration{}
			rates := map[string][]float64{}
			for _, arm := range []dcArm{dcLegacyArm, dcNewArm, dcNewArm, dcLegacyArm, dcLegacyArm, dcNewArm} {
				r := runPolicedLeaf(t, arm, tc.mbit, tc.loss)
				t.Logf("%-14s goodput %.1f Mbit/s | fetch p50 %v p90 %v | failed %d", arm.name, r.mbps,
					q(r.fetches, 0.5).Round(time.Millisecond), q(r.fetches, 0.9).Round(time.Millisecond), r.failed)
				pooled[arm.name] = append(pooled[arm.name], r.fetches...)
				rates[arm.name] = append(rates[arm.name], r.mbps)
			}
			mean := func(v []float64) float64 {
				s := 0.0
				for _, x := range v {
					s += x
				}
				return s / float64(len(v))
			}
			for _, arm := range []dcArm{dcLegacyArm, dcNewArm} {
				t.Logf("POOLED %-14s goodput mean %.1f Mbit/s | fetch p50 %v p90 %v (n=%d)", arm.name, mean(rates[arm.name]),
					q(pooled[arm.name], 0.5).Round(time.Millisecond), q(pooled[arm.name], 0.9).Round(time.Millisecond), len(pooled[arm.name]))
			}
			if lm, cm := mean(rates[dcLegacyArm.name]), mean(rates[dcNewArm.name]); cm < dcGoodputRatio*lm {
				t.Errorf("goodput %.1f Mbit/s is below %.2fx legacy's %.1f", cm, dcGoodputRatio, lm)
			}
			legacy, cand := q(pooled[dcLegacyArm.name], 0.5), q(pooled[dcNewArm.name], 0.5)
			if limit := time.Duration(dcLatencyMaxRatio*float64(legacy)) + dcLatencySlack; cand > limit {
				t.Errorf("fetch p50 %v exceeds %.2fx legacy's %v (+%v slack)", cand, dcLatencyMaxRatio, legacy, dcLatencySlack)
			}
		})
	}
}
