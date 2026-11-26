package pep

import (
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bojieli/queqiao/internal/pathsim"
)

// The DC gate's control leaf beside other leaves' downloads (§11.3, Q2): a
// 53 ms, 300 Mbit/s leg; three loaded leaves, each its own client, gateway
// and path authority, each with two backlogged downloads, and a control leaf
// fetching 300 KB on a fresh flow every half second, as the live gate's
// probe. Each leaf's authority is independent and nothing limits them
// jointly. The control leaf either shares the leaves' bottleneck (the joint
// queue is the question) or has a path of its own (only its own flow's
// startup is). Until stage 11 each trial also ran the legacy bundle, whose
// path tree capped its members; that arm went with the legacy code.

const (
	dcCtlLoadedLeaves = 3
	dcCtlFetchBytes   = 300 << 10
	dcCtlEvery        = 500 * time.Millisecond
	dcCtlWarmup       = 4 * time.Second
	dcCtlWindow       = 12 * time.Second
)

func dcCtlLeg() pathsim.Config {
	return pathsim.Config{OneWayDelay: 26500 * time.Microsecond, RateBytesPerSec: 300_000_000 / 8, QueueBytes: 2 << 20, Seed: 81}
}

type dcCtlResult struct {
	idle, loaded []time.Duration // control fetches alone, and under load
	loadedMbps   float64         // the loaded leaves' aggregate download
	failed       int
}

func runDCControlLeaf(t *testing.T, arm dcArm, shared bool) dcCtlResult {
	t.Helper()
	bed := newDCBed(t, arm)
	defer bed.close()
	leg := dcCtlLeg()
	ancestor := pathsim.NewBottleneck(leg)
	attach := func(listen, target string) (*pathsim.Relay, error) { return ancestor.Attach(listen, target, leg) }
	var loaded []*dcLeaf
	for i := range dcCtlLoadedLeaves {
		loaded = append(loaded, bed.leaf(i+1, attach))
	}
	controlPath := attach
	if !shared {
		controlPath = func(listen, target string) (*pathsim.Relay, error) { return pathsim.New(listen, target, leg) }
	}
	control := bed.leaf(dcCtlLoadedLeaves+1, controlPath)
	var res dcCtlResult
	fetch := func() time.Duration {
		d, err := control.request(0, dcCtlFetchBytes, dcRequestTimeout)
		if err != nil {
			res.failed++
			return dcRequestTimeout
		}
		return d
	}
	// Warm the control leaf's carrier, then measure it alone.
	fetch()
	for range 8 {
		res.idle = append(res.idle, fetch())
		time.Sleep(dcCtlEvery)
	}
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
	time.Sleep(dcCtlWarmup)
	start, began := got.Load(), time.Now()
	for time.Since(began) < dcCtlWindow {
		at := time.Now()
		res.loaded = append(res.loaded, fetch())
		time.Sleep(time.Until(at.Add(dcCtlEvery)))
	}
	res.loadedMbps = float64(got.Load()-start) * 8 / time.Since(began).Seconds() / 1e6
	close(stop)
	wg.Wait()
	return res
}

func q(d []time.Duration, p float64) time.Duration {
	s := append([]time.Duration(nil), d...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	return s[min(len(s)-1, int(p*float64(len(s))))]
}

func TestDCControlLeafBesideLoadedDownloads(t *testing.T) {
	requireDCBed(t)
	for _, shared := range []bool{true, false} {
		where, order := "own path", []dcArm{dcNewArm}
		if shared {
			// Three trials: on a shared emulated queue the control leaf's
			// latency moves by a factor of two or three between trials.
			where, order = "shared bottleneck", []dcArm{dcNewArm, dcNewArm, dcNewArm}
		}
		pooled := map[string][]time.Duration{}
		rates := map[string][]float64{}
		for _, arm := range order {
			r := runDCControlLeaf(t, arm, shared)
			t.Logf("%-17s %-14s control fetch alone p50 %v | under load p50 %v p90 %v max %v | loaded leaves %.0f Mbit/s | failed %d",
				where, arm.name, q(r.idle, 0.5).Round(time.Millisecond), q(r.loaded, 0.5).Round(time.Millisecond),
				q(r.loaded, 0.9).Round(time.Millisecond), q(r.loaded, 1).Round(time.Millisecond), r.loadedMbps, r.failed)
			pooled[arm.name] = append(pooled[arm.name], r.loaded...)
			rates[arm.name] = append(rates[arm.name], r.loadedMbps)
		}
		for _, arm := range []dcArm{dcNewArm} {
			sum := 0.0
			for _, v := range rates[arm.name] {
				sum += v
			}
			t.Logf("POOLED %-17s %-14s control under load p50 %v p90 %v (n=%d) | loaded leaves mean %.0f Mbit/s",
				where, arm.name, q(pooled[arm.name], 0.5).Round(time.Millisecond), q(pooled[arm.name], 0.9).Round(time.Millisecond),
				len(pooled[arm.name]), sum/float64(len(rates[arm.name])))
		}
	}
}
