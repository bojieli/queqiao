package pep

import (
	"encoding/binary"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/bojieli/queqiao/internal/pathsim"
)

// segmentServer answers each 4-byte request with that many bytes: an HTTP
// server's segment responses with the framing taken out.
func segmentServer(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				var request [4]byte
				body := make([]byte, 64*1024)
				for {
					if _, err := io.ReadFull(conn, request[:]); err != nil {
						return
					}
					for left := int(binary.BigEndian.Uint32(request[:])); left > 0; {
						n, err := conn.Write(body[:min(left, len(body))])
						if err != nil {
							return
						}
						left -= n
					}
				}
			}()
		}
	}()
	return listener.Addr().String()
}

// fetchSegment requests size bytes on conn and reads them.
func fetchSegment(conn net.Conn, size int) (time.Duration, error) {
	var request [4]byte
	binary.BigEndian.PutUint32(request[:], uint32(size))
	start := time.Now()
	if _, err := conn.Write(request[:]); err != nil {
		return 0, err
	}
	if _, err := io.CopyN(io.Discard, conn, int64(size)); err != nil {
		return 0, err
	}
	return time.Since(start), nil
}

// segmentOutcome is one arm's W10 measures.
type segmentOutcome struct {
	times      []time.Duration
	throughput []float64 // bytes per second, as an ABR sees each segment
	rebuffer   float64
}

// W10. Segment fetch (docs/WORKLOAD-MODEL.md): an adaptive-bitrate player on
// one warm connection fetches a 1.5 MB segment every 4 s. Between segments
// the connection idles, so every fetch restarts from idle, and a classifier
// that is no longer sticky sees the duty cycle flip it. A segment that takes
// longer than its period stalls playback by the difference.
//
// Measures: each segment's download time d_k, its throughput as the player
// sees it (size / d_k), and the rebuffer ratio Σ max(0, d_k − period) over
// the session. Bounds (model W10): p10 throughput ≥ 0.90 × legacy; rebuffer
// ratio ≤ legacy + 1 pp.
//
// The arms run at once, each on its own client, gateway and emulated path,
// so whatever else the host is doing slows both alike. The full form is the
// model's: 15 segments over 60 s on a path with 1 % loss. -short halves the
// segment and its period, keeping the bitrate and an idle gap of ten credit
// rounds before each fetch, on the same path without the loss, fetches 11,
// and holds the median throughput to the 0.90 bound: at that size p10 moves
// by a tenth with one slow segment in either arm. The full suite applies the
// model's p10 and rebuffer bounds.
//
// This test found that mem=owned's retention bound decayed through every
// pause to its startup allowance, so each segment began again from it; see
// stripe's TestRetentionBoundIsKeptAcrossAnApplicationPause.
func TestSegmentFetchOnAnEmulatedPath(t *testing.T) {
	requireStableImpairmentClock(t)
	size, period, segments := 1500*1000, 4*time.Second, 15
	path := pathsim.Config{
		OneWayDelay: 60 * time.Millisecond, LossRate: 0.01, Seed: 10,
		RateBytesPerSec: 20e6 / 8, PolicerRefillPeriod: 8 * time.Millisecond,
	}
	if testing.Short() {
		size, period, segments = 750*1000, 2*time.Second, 11
		path.LossRate = 0
	}

	arms := workloadArms()
	outcomes := make([]segmentOutcome, len(arms))
	rigs := make([]*workloadRig, len(arms))
	for i, arm := range arms {
		rigs[i] = newWorkloadRig(t, path, arm, nil)
	}
	destination := segmentServer(t)
	var wg sync.WaitGroup
	errs := make(chan error, len(arms))
	for i := range arms {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn := rigs[i].dial(destination)
			_ = conn.SetDeadline(time.Now().Add(time.Duration(segments)*period + time.Minute))
			// Warm: the player has the manifest before the first segment.
			if _, err := fetchSegment(conn, 4096); err != nil {
				errs <- err
				return
			}
			start := time.Now()
			var stalled time.Duration
			o := &outcomes[i]
			for k := range segments {
				if wait := time.Until(start.Add(time.Duration(k) * period)); wait > 0 {
					time.Sleep(wait)
				}
				d, err := fetchSegment(conn, size)
				if err != nil {
					errs <- err
					return
				}
				o.times = append(o.times, d)
				o.throughput = append(o.throughput, float64(size)/d.Seconds())
				stalled += max(0, d-period)
			}
			o.rebuffer = stalled.Seconds() / (time.Duration(segments) * period).Seconds()
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	for i, arm := range arms {
		o := outcomes[i]
		rounded := make([]time.Duration, len(o.times))
		for k, d := range o.times {
			rounded[k] = d.Round(time.Millisecond)
		}
		t.Logf("%s: segment times %v; p10 throughput %.2f Mbit/s; rebuffer %.2f %%",
			arm.name, rounded, floatQuantile(o.throughput, 0.10)*8/1e6, 100*o.rebuffer)
	}
	if !judgeComparisons(t) {
		return
	}
	legacy, flip := outcomes[0], outcomes[1]
	if testing.Short() {
		// Eleven segments on a lossless path: one or two slow segments in
		// either arm move the p10 by a tenth from run to run, so -short
		// holds the median to the 0.90 bound, which still catches a flow
		// that restarts slowly after every pause, and leaves p10 and the
		// rebuffer ratio to the full suite's model form.
		if got, floor := floatQuantile(flip.throughput, 0.50), 0.90*floatQuantile(legacy.throughput, 0.50); got < floor {
			t.Errorf("median segment throughput %.2f Mbit/s under the flip bundle, below 0.90 × legacy's (%.2f)",
				got*8/1e6, floor*8/1e6)
		}
		return
	}
	if got, floor := floatQuantile(flip.throughput, 0.10), 0.90*floatQuantile(legacy.throughput, 0.10); got < floor {
		t.Errorf("p10 segment throughput %.2f Mbit/s under the flip bundle, below 0.90 × legacy's (%.2f)",
			got*8/1e6, floor*8/1e6)
	}
	if flip.rebuffer > legacy.rebuffer+0.01 {
		t.Errorf("rebuffer ratio %.2f %% under the flip bundle against %.2f %% under legacy; the bound is legacy + 1 pp",
			100*flip.rebuffer, 100*legacy.rebuffer)
	}
}

// W10's player under recovery=carrier, which the flip arm above does not yet
// carry. An idle carrier still carries the odd small frame, and the pause
// between segments (3 s) outlives the bandwidth estimate's window (ten round
// trips, 1.2 s). A lone small packet acknowledged in the pause used to
// replace the aged-out estimate with its own few hundred bytes a second, and
// the next segment was paced from that: it took 2.4 to 4.8 s instead of 1 s,
// in 9 sessions of 21. With the evidence rule (congestion's
// TestALonePacketInAPauseDoesNotBecomeTheEstimate) none of 20 did.
func TestSegmentFetchAfterAPauseUnderCarrierRecovery(t *testing.T) {
	requireStableImpairmentClock(t)
	if testing.Short() {
		t.Skip("a minute of wall-clock playback; the full suite runs it")
	}
	const size, period, segments = 1500 * 1000, 4 * time.Second, 15
	path := pathsim.Config{
		OneWayDelay: 60 * time.Millisecond, LossRate: 0.01, Seed: 10,
		RateBytesPerSec: 20e6 / 8, PolicerRefillPeriod: 8 * time.Millisecond,
	}
	arm := workloadArm{
		name:   "flip with recovery=carrier",
		client: func(c *ClientConfig) { flipArm.client(c); c.Recovery = RecoveryCarrier },
		server: func(s *ServerConfig) { flipArm.server(s); s.Recovery = RecoveryCarrier },
	}
	rig := newWorkloadRig(t, path, arm, nil)
	conn := rig.dial(segmentServer(t))
	_ = conn.SetDeadline(time.Now().Add(segments*period + time.Minute))
	if _, err := fetchSegment(conn, 4096); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	var times []time.Duration
	for k := range segments {
		if wait := time.Until(start.Add(time.Duration(k) * period)); wait > 0 {
			time.Sleep(wait)
		}
		d, err := fetchSegment(conn, size)
		if err != nil {
			t.Fatal(err)
		}
		times = append(times, d.Round(time.Millisecond))
	}
	t.Logf("segment times %v", times)
	if !judgeComparisons(t) {
		return
	}
	// A segment is 0.6 s of the path's rate and takes about 1 s; one that
	// restarted from a collapsed estimate took at least 2.4 s.
	for k, d := range times {
		if d > 2*time.Second {
			t.Errorf("segment %d took %v after its pause; the others take about 1 s", k, d)
		}
	}
}
