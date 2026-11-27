package pep

import (
	"encoding/binary"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bojieli/queqiao/internal/pathsim"
)

// W6 beside a bulk download behind a token-bucket policer -- the live
// campaign's "policed" scenario in miniature (4 Mbit/s, a 12 KB bucket, +150
// ms round trip). A policer does not queue: a packet over the bucket is
// dropped. That is the one place the live campaign measured the new bundle
// worse than main: the keystroke echo p50 was unchanged and its p99 was about
// twice main's, with the slow keys arriving in bursts. This test is the
// deterministic form of that measurement (it reproduces in one 20 s run), so
// the tail can be reasoned about without a live campaign.
//
// Where the delay is not: it is not queueing. On a queueing shaper of the
// same rate (the same path with the policer's refill period removed) the
// bundles swap places -- the flip arm's echo p99 is about half the legacy
// arm's -- and with no bulk on the path both are clean. What the policer
// changes is that a small record can be dropped at the bucket, and the new
// bundle recovers a dropped interactive record later than main does: about
// 500 ms, on the same leg the bulk is running on.
//
// W6's bound (as M5 judges it) is keystroke echo p99 at most 1.10 x legacy's
// plus 20 ms. The flip arm does not meet it behind a policer and this test
// reports the comparison without gating on it, as W10's comparative bounds do
// on platforms where they are not judged; it fails only on a runaway (p99
// past the ceiling below) or on a measurement that produced nothing. The
// issue this measured is #282.
func TestKeystrokeEchoBehindAPolicerBesideABulkDownload(t *testing.T) {
	requireStableImpairmentClock(t)
	measure := 20 * time.Second
	if testing.Short() {
		measure = 8 * time.Second
	}
	path := pathsim.Config{
		OneWayDelay: 75 * time.Millisecond, Seed: 12,
		RateBytesPerSec: 4e6 / 8, PolicerRefillPeriod: 8 * time.Millisecond,
		PolicerBurstBytes: 12000,
	}

	echoListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = echoListener.Close() })
	go echoDestination(echoListener)
	bulk := segmentServer(t)

	arms := workloadArms()
	echoes := make([][]time.Duration, len(arms))
	drops := make([]uint64, len(arms))
	errs := make(chan error, len(arms))
	var wg sync.WaitGroup
	for i := range arms {
		wg.Add(1)
		go func() {
			defer wg.Done()
			e, d, err := policedKeystrokeArm(t, path, arms[i], echoListener.Addr().String(), bulk, measure)
			echoes[i], drops[i] = e, d
			if err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	over := func(samples []time.Duration, limit time.Duration) (n int) {
		for _, d := range samples {
			if d > limit {
				n++
			}
		}
		return n
	}
	for i, arm := range arms {
		e := echoes[i]
		if len(e) == 0 {
			t.Fatalf("%s: no keystroke echoed", arm.name)
		}
		frac := float64(over(e, time.Second)) / float64(len(e))
		t.Logf("%s: keystrokes %d, echo p50 %v p95 %v p99 %v max %v; over 1 s: %.1f %%; path drops %d",
			arm.name, len(e), durationQuantile(e, 0.50).Round(time.Millisecond),
			durationQuantile(e, 0.95).Round(time.Millisecond), durationQuantile(e, 0.99).Round(time.Millisecond),
			durationQuantile(e, 1).Round(time.Millisecond), 100*frac, drops[i])
	}
	legacy, flip := durationQuantile(echoes[0], 0.99), durationQuantile(echoes[1], 0.99)
	if bound := time.Duration(float64(legacy)*1.10) + 20*time.Millisecond; flip > bound {
		t.Logf("REPORT ONLY (#282): keystroke echo p99 %v behind a policer, above W6's bound of 1.10 x legacy (%v) + 20 ms (%v)",
			flip.Round(time.Millisecond), legacy.Round(time.Millisecond), bound.Round(time.Millisecond))
	}
	// A ceiling, not the bound: the tail is a known open case and gates
	// nothing, but a recovery that stops working altogether (a record lost
	// until the flow times out) is not that case.
	if ceiling := 2 * time.Second; flip > ceiling {
		t.Errorf("keystroke echo p99 %v behind a policer, past the %v ceiling", flip.Round(time.Millisecond), ceiling)
	}
}

// policedKeystrokeArm runs one arm: a bulk download fills the path, then the
// keystrokes type at W6's cadence while it does. The echoes are returned as
// measured.
// policedKeystrokeArmsLike is policedKeystrokeArm with the live workload's
// background: several short downloads cycling on the shaper rather than one
// endless stream, which is what the impaired campaign's fetch workload does
// and what its slow episodes coincide with.
func policedKeystrokeArmsLike(t *testing.T, path pathsim.Config, arm workloadArm, echo, bulk string,
	loads int, measure time.Duration) ([]time.Duration, uint64, error) {
	t.Helper()
	rig := newWorkloadRig(t, path, arm, nil)
	keys := rig.dial(echo)
	stop := make(chan struct{})
	done := make(chan struct{})
	var moved int64
	if loads > 0 {
		var wg sync.WaitGroup
		for i := 0; i < loads; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-stop:
						return
					default:
					}
					conn := rig.dial(bulk)
					var request [4]byte
					binary.BigEndian.PutUint32(request[:], 1<<20) // a 1 MiB fetch
					if _, err := conn.Write(request[:]); err != nil {
						_ = conn.Close()
						return
					}
					// The server keeps the connection for the next request, so
					// read exactly the response rather than to EOF.
					_ = conn.SetDeadline(time.Now().Add(time.Minute))
					n, _ := io.CopyN(io.Discard, conn, 1<<20)
					atomic.AddInt64(&moved, n)
					_ = conn.Close()
				}
			}()
		}
		defer func() { close(stop); wg.Wait(); close(done) }()
		time.Sleep(2 * time.Second)
	} else {
		close(done)
	}
	upBefore, downBefore := rig.relay.Stats()
	echoes, err := runKeystrokes(keys, measure)
	upAfter, downAfter := rig.relay.Stats()
	drops := upAfter.PacketsDropped - upBefore.PacketsDropped + downAfter.PacketsDropped - downBefore.PacketsDropped
	if err != nil {
		return nil, drops, err
	}
	if loads > 0 && atomic.LoadInt64(&moved) < 250*1024 {
		t.Errorf("%s: the background downloads moved only %d bytes", arm.name, moved)
	}
	return echoes, drops, nil
}

func policedKeystrokeArm(t *testing.T, path pathsim.Config, arm workloadArm, echo, bulk string,
	measure time.Duration) ([]time.Duration, uint64, error) {
	t.Helper()
	rig := newWorkloadRig(t, path, arm, nil)
	keys := rig.dial(echo)
	done := make(chan struct{})
	var moved int64
	if bulk != "" {
		conn := rig.dial(bulk)
		var request [4]byte
		binary.BigEndian.PutUint32(request[:], 1<<30) // outlasts the block
		if _, err := conn.Write(request[:]); err != nil {
			return nil, 0, err
		}
		go func() {
			defer close(done)
			buf := make([]byte, 64*1024)
			for {
				n, err := conn.Read(buf)
				moved += int64(n)
				if err != nil {
					return
				}
			}
		}()
		defer func() {
			_ = conn.Close()
			<-done
		}()
		time.Sleep(2 * time.Second) // let the download fill the policer
	} else {
		close(done)
	}
	upBefore, downBefore := rig.relay.Stats()
	echoes, err := runKeystrokes(keys, measure)
	upAfter, downAfter := rig.relay.Stats()
	drops := upAfter.PacketsDropped - upBefore.PacketsDropped + downAfter.PacketsDropped - downBefore.PacketsDropped
	if err != nil {
		return nil, drops, err
	}
	if bulk != "" && moved < 250*1024 {
		t.Errorf("%s: the bulk download moved only %d bytes beside the keystrokes", arm.name, moved)
	}
	return echoes, drops, nil
} // The same comparison under the live workload's background: four 1 MiB
// downloads cycling on the shaper instead of one endless stream. This is the
// regime the impaired campaign runs (#282), where its slow episodes are.
func TestKeystrokeEchoBehindAPolicerBesideCyclingDownloads(t *testing.T) {
	requireStableImpairmentClock(t)
	measure := 20 * time.Second
	if testing.Short() {
		measure = 8 * time.Second
	}
	path := pathsim.Config{
		OneWayDelay: 75 * time.Millisecond, Seed: 12,
		RateBytesPerSec: 4e6 / 8, PolicerRefillPeriod: 8 * time.Millisecond,
		PolicerBurstBytes: 12000,
	}
	echoListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = echoListener.Close() })
	go echoDestination(echoListener)
	bulk := segmentServer(t)

	arms := workloadArms()
	echoes := make([][]time.Duration, len(arms))
	drops := make([]uint64, len(arms))
	errs := make(chan error, len(arms))
	var wg sync.WaitGroup
	for i := range arms {
		wg.Add(1)
		go func() {
			defer wg.Done()
			e, d, err := policedKeystrokeArmsLike(t, path, arms[i], echoListener.Addr().String(), bulk, 4, measure)
			echoes[i], drops[i] = e, d
			if err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	for i, arm := range arms {
		e := echoes[i]
		if len(e) == 0 {
			t.Fatalf("%s: no keystroke echoed", arm.name)
		}
		over := 0
		for _, d := range e {
			if d > time.Second {
				over++
			}
		}
		t.Logf("%s: keystrokes %d, echo p50 %v p95 %v p99 %v max %v; over 1 s: %.1f %%; path drops %d",
			arm.name, len(e), durationQuantile(e, 0.50).Round(time.Millisecond),
			durationQuantile(e, 0.95).Round(time.Millisecond), durationQuantile(e, 0.99).Round(time.Millisecond),
			durationQuantile(e, 1).Round(time.Millisecond), 100*float64(over)/float64(len(e)), drops[i])
	}
}
