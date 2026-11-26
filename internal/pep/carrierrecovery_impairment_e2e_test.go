package pep

import (
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bojieli/queqiao/internal/pathsim"
)

// Carrier recovery under the impairments of the live campaign's netem arm,
// on pathsim. The workload is one bulk download beside one interactive echo
// flow. Hard assertions: no flow dies silently (each either keeps making
// progress or ends with an error the application sees) and no false
// replacement of a carrier that is only slow or lossy. Until stage 11 each ran
// against legacy recovery too; that arm went with the legacy code, so what is
// left are the absolute assertions.

// downloadMarker opens a download on muxDestination instead of an echo.
const downloadMarker = 0xFF

// muxDestination serves a download to a connection whose first byte is
// downloadMarker, and echoes every other connection, first byte included.
func muxDestination(l net.Listener) {
	for {
		conn, err := l.Accept()
		if err != nil {
			return
		}
		go func() {
			defer conn.Close()
			var first [1]byte
			if _, err := io.ReadFull(conn, first[:]); err != nil {
				return
			}
			if first[0] == downloadMarker {
				go func() { _, _ = io.Copy(io.Discard, conn) }()
				buf := make([]byte, 32<<10)
				for {
					if _, err := conn.Write(buf); err != nil {
						return
					}
				}
			}
			if _, err := conn.Write(first[:]); err != nil {
				return
			}
			_, _ = io.Copy(conn, conn)
		}()
	}
}

// downloadStream reads one bulk download and records its progress.
type downloadStream struct {
	conn     net.Conn
	received atomic.Int64
	lastAt   atomic.Int64 // unix nanos of the last read that returned bytes
	err      atomic.Pointer[error]
}

func startDownload(t *testing.T, p *carrierPair) *downloadStream {
	t.Helper()
	conn, err := trySocksDial(p.socks, p.destination, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.SetDeadline(time.Time{})
	if _, err := conn.Write([]byte{downloadMarker}); err != nil {
		t.Fatal(err)
	}
	d := &downloadStream{conn: conn}
	d.lastAt.Store(time.Now().UnixNano())
	t.Cleanup(func() { _ = conn.Close() })
	go func() {
		buf := make([]byte, 64<<10)
		for {
			n, err := conn.Read(buf)
			if n > 0 {
				d.received.Add(int64(n))
				d.lastAt.Store(time.Now().UnixNano())
			}
			if err != nil {
				d.err.CompareAndSwap(nil, &err)
				return
			}
		}
	}()
	return d
}

func (d *downloadStream) failure() error {
	if p := d.err.Load(); p != nil {
		return *p
	}
	return nil
}

// waitReceived waits for more than want bytes, or returns false at budget.
func (d *downloadStream) waitReceived(want int64, budget time.Duration) bool {
	deadline := time.Now().Add(budget)
	for d.received.Load() <= want {
		if d.failure() != nil || time.Now().After(deadline) {
			return false
		}
		time.Sleep(20 * time.Millisecond)
	}
	return true
}

// impairmentRun is one arm's outcome.
type impairmentRun struct {
	// downloaded is the bulk bytes delivered over the measured window.
	downloaded int64
	// echoed is the interactive bytes echoed over it.
	echoed int64
	// resumed is how long after the impairment ended both flows were seen
	// delivering again; negative if they never did.
	resumed    time.Duration
	promotions int64
	silent     []string
}

// checkNotSilent records a flow that neither progressed within quiet nor
// reported an error: a silent death.
func (r *impairmentRun) checkNotSilent(name string, lastProgress time.Time, failed error, quiet time.Duration) {
	if failed == nil && time.Since(lastProgress) > quiet {
		r.silent = append(r.silent, name)
	}
}

func impairmentPair(t *testing.T, path pathsim.Config, recovery CarrierRecoveryConfig) *carrierPair {
	return newCarrierPair(t, carrierPairOptions{path: path, serve: muxDestination, recovery: recovery})
}

func (r impairmentRun) promotionsOf(p *carrierPair) int64 {
	return p.client.carriers.promotions.Load()
}

// measureSteady runs the workload over a steady impairment for window.
func measureSteady(t *testing.T, path pathsim.Config, window time.Duration) impairmentRun {
	t.Helper()
	p := impairmentPair(t, path, CarrierRecoveryConfig{})
	run := impairmentRun{}
	d := startDownload(t, p)
	e := startEchoStream(t, p, 64, 50*time.Millisecond)
	if !d.waitReceived(64<<10, 60*time.Second) {
		t.Fatalf("the download never started: %v", d.failure())
	}
	d0, e0 := d.received.Load(), e.echoed.Load()
	time.Sleep(window)
	run.downloaded, run.echoed = d.received.Load()-d0, e.echoed.Load()-e0
	run.promotions = run.promotionsOf(p)
	run.checkNotSilent("download", time.Unix(0, d.lastAt.Load()), d.failure(), 10*time.Second)
	if e.failure() != nil || run.echoed == 0 {
		run.silent = append(run.silent, "echo")
	}
	return run
}

// checkSteady runs the workload and applies the common assertions.
func checkSteady(t *testing.T, path pathsim.Config, window time.Duration) {
	if testing.Short() {
		t.Skip("wall-clock impairment campaign")
	}
	run := measureSteady(t, path, window)
	t.Logf("%d bytes down, %d echoed, %d promotions", run.downloaded, run.echoed, run.promotions)
	if len(run.silent) > 0 {
		t.Fatalf("flows died silently: %v", run.silent)
	}
	if run.promotions != 0 {
		t.Fatalf("the carrier manager replaced a carrier that was only impaired (%d promotions)", run.promotions)
	}
	if run.downloaded == 0 {
		t.Fatal("the download made no progress over the window")
	}
}

// 3 % bursty loss at 5 Mbit/s with +250 ms of round trip: a slow, lossy
// path is waited out and never replaced.
func TestCarrierRecoveryOnASlowLossyPath(t *testing.T) {
	checkSteady(t, pathsim.Config{
		OneWayDelay: 125 * time.Millisecond, RateBytesPerSec: 5_000_000 / 8,
		LossRate: 0.03, LossBurstPackets: 3, QueueBytes: 256 << 10, Seed: 31,
	}, 15*time.Second)
}

// A 4 Mbit/s policer: drops at the bucket are loss on a live carrier, which
// is no evidence against it.
func TestCarrierRecoveryUnderA4MbitPolicer(t *testing.T) {
	checkSteady(t, pathsim.Config{
		OneWayDelay: 30 * time.Millisecond, RateBytesPerSec: 4_000_000 / 8,
		PolicerRefillPeriod: 8 * time.Millisecond, Seed: 32,
	}, 15*time.Second)
}

// measureBlackhole drops everything, from every source, for outage, then
// restores the path, and reports how soon both flows deliver again.
func measureBlackhole(t *testing.T, outage time.Duration) impairmentRun {
	t.Helper()
	p := impairmentPair(t, pathsim.Config{OneWayDelay: 40 * time.Millisecond, RateBytesPerSec: 10_000_000 / 8, QueueBytes: 256 << 10, Seed: 33}, CarrierRecoveryConfig{})
	run := impairmentRun{resumed: -1}
	d := startDownload(t, p)
	e := startEchoStream(t, p, 64, 50*time.Millisecond)
	if !d.waitReceived(64<<10, 30*time.Second) {
		t.Fatal("the download never started")
	}
	e.waitEchoed(t, 1024, 20*time.Second)
	p.relay.SetLossRate(1, 1)
	time.Sleep(outage)
	p.relay.SetLossRate(0, 0)
	restored := time.Now()
	d0, e0 := d.received.Load(), e.echoed.Load()
	budget := 60 * time.Second
	for time.Since(restored) < budget {
		if d.received.Load() > d0+64<<10 && e.echoed.Load() > e0+512 {
			run.resumed = time.Since(restored)
			break
		}
		if d.failure() != nil || e.failure() != nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	run.promotions = run.promotionsOf(p)
	// A flow that did not resume must have said so.
	if run.resumed < 0 {
		run.checkNotSilent("download", time.Unix(0, d.lastAt.Load()), d.failure(), 10*time.Second)
		if e.failure() == nil {
			run.silent = append(run.silent, "echo")
		}
	}
	return run
}

// A whole-path blackhole of 8 s (inside QUIC's idle timeout, so the carrier
// survives it) and of 25 s (past it, so the carrier dies and the flows are
// rebound onto a new one inside the gateway's grace). The flows resume.
func TestCarrierRecoveryAcrossBlackholes(t *testing.T) {
	if testing.Short() {
		t.Skip("wall-clock impairment campaign")
	}
	for _, outage := range []time.Duration{8 * time.Second, 25 * time.Second} {
		t.Run(outage.String(), func(t *testing.T) {
			carrier := measureBlackhole(t, outage)
			t.Logf("%v blackhole: resumed %v after the restore (%d promotions)", outage, carrier.resumed, carrier.promotions)
			if len(carrier.silent) > 0 {
				t.Fatalf("flows died silently: %v", carrier.silent)
			}
			if carrier.resumed < 0 {
				t.Fatal("the carrier manager did not resume the flows after the path came back")
			}
			if outage < laneDeadPathDetection && carrier.promotions != 0 {
				// A path that comes back inside the idle timeout returns the
				// carrier to ACTIVE; the standby could not be verified
				// through the same outage.
				t.Fatalf("%d promotions across an outage the carrier survived", carrier.promotions)
			}
		})
	}
}

// Idle and wake under loss (D idle-wake, W9b). Three idle flows on a lossy,
// slow path sit through a real idle longer than QUIC's idle timeout, which
// the keep-alive bridges; the carrier manager's clock is moved to 1, 5 and
// 16 minutes of idle before each wake (crossing the 15-minute boundary),
// which a real campaign covers in wall time. Every wake is delivered within
// 5 s, and nothing is replaced.
func TestCarrierRecoveryIdleWakeUnderLoss(t *testing.T) {
	if testing.Short() {
		t.Skip("wall-clock impairment campaign")
	}
	path := pathsim.Config{OneWayDelay: 125 * time.Millisecond, RateBytesPerSec: 5_000_000 / 8, LossRate: 0.03, LossBurstPackets: 3, Seed: 34}
	wakes := []time.Duration{time.Minute, 5 * time.Minute, 16 * time.Minute}
	var skew atomic.Int64
	cfg := CarrierRecoveryConfig{clock: func() time.Time { return time.Now().Add(time.Duration(skew.Load())) }}
	p := impairmentPair(t, path, cfg)
	var flows []*echoStream
	for range wakes {
		e := startEchoStream(t, p, 200, 20*time.Millisecond)
		e.waitEchoed(t, 2<<10, 60*time.Second)
		e.pause.Store(true)
		flows = append(flows, e)
	}
	time.Sleep(laneDeadPathDetection + 3*time.Second)
	for i, idle := range wakes {
		skew.Store(int64(idle))
		e := flows[i]
		before := e.echoed.Load()
		e.pause.Store(false)
		took := e.waitEchoed(t, before+200, 5*time.Second)
		e.pause.Store(true)
		t.Logf("wake after %v idle delivered in %v", idle, took)
	}
	m := p.client.carriers
	if got := m.promotions.Load() + m.deadlineFailures.Load(); got != 0 {
		t.Fatalf("idle flows on a lossy path were replaced or failed (%d):\n%s", got, p.clientLogs.String())
	}
}
