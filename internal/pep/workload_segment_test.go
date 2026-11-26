package pep

import (
	"encoding/binary"
	"io"
	"net"
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

// segmentOutcome is W10's measures.
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
// ratio ≤ legacy + 1 pp. Legacy's values are frozen below, as measured on
// this path at stage 10 before the legacy bundle was deleted.
//
// The full form is the model's: 15 segments over 60 s on a path with 1 %
// loss. -short halves the segment and its period, keeping the bitrate and an
// idle gap of ten credit rounds before each fetch, on the same path without
// the loss, fetches 11, and holds the median throughput to the 0.90 bound:
// at that size p10 moves by a tenth with one slow segment. The full suite
// applies the model's p10 and rebuffer bounds.
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

	rig := newWorkloadRig(t, path, pathAuthorityArm, nil)
	conn := rig.dial(segmentServer(t))
	_ = conn.SetDeadline(time.Now().Add(time.Duration(segments)*period + time.Minute))
	// Warm: the player has the manifest before the first segment.
	if _, err := fetchSegment(conn, 4096); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	var stalled time.Duration
	var o segmentOutcome
	for k := range segments {
		if wait := time.Until(start.Add(time.Duration(k) * period)); wait > 0 {
			time.Sleep(wait)
		}
		d, err := fetchSegment(conn, size)
		if err != nil {
			t.Fatal(err)
		}
		o.times = append(o.times, d)
		o.throughput = append(o.throughput, float64(size)/d.Seconds())
		stalled += max(0, d-period)
	}
	o.rebuffer = stalled.Seconds() / (time.Duration(segments) * period).Seconds()

	rounded := make([]time.Duration, len(o.times))
	for k, d := range o.times {
		rounded[k] = d.Round(time.Millisecond)
	}
	t.Logf("segment times %v; median throughput %.2f Mbit/s, p10 %.2f Mbit/s; rebuffer %.2f %%",
		rounded, floatQuantile(o.throughput, 0.50)*8/1e6, floatQuantile(o.throughput, 0.10)*8/1e6, 100*o.rebuffer)
	if !judgeBounds(t) {
		return
	}
	if testing.Short() {
		// Eleven segments on a lossless path: one or two slow segments move
		// the p10 by a tenth from run to run, so -short holds the median to
		// the 0.90 bound, which still catches a flow that restarts slowly
		// after every pause, and leaves p10 and the rebuffer ratio to the
		// full suite's model form.
		if got, floor := floatQuantile(o.throughput, 0.50)*8/1e6, 0.90*segmentLegacyShortMedianMbps; got < floor {
			t.Errorf("median segment throughput %.2f Mbit/s, below 0.90 × legacy's %.2f (%.2f)", got, segmentLegacyShortMedianMbps, floor)
		}
		return
	}
	if got, floor := floatQuantile(o.throughput, 0.10)*8/1e6, 0.90*segmentLegacyP10Mbps; got < floor {
		t.Errorf("p10 segment throughput %.2f Mbit/s, below 0.90 × legacy's %.2f (%.2f)", got, segmentLegacyP10Mbps, floor)
	}
	if o.rebuffer > segmentLegacyRebuffer+0.01 {
		t.Errorf("rebuffer ratio %.2f %%; the bound is legacy's %.2f %% + 1 pp", 100*o.rebuffer, 100*segmentLegacyRebuffer)
	}
}

// What the legacy bundle measured on W10's paths at stage 10, before it was
// deleted, on a 32-core Linux host: the median of five full runs (p10 of
// 11.79, 10.70, 10.39, 9.60 and 7.14 Mbit/s; no rebuffering) and of four
// -short runs (medians of 10.02, 9.63, 9.54 and 9.42 Mbit/s). The path
// controller's own figures in the same runs were a p10 of 10.8 to 11.4 and a
// -short median of 9.7 to 10.2.
const (
	segmentLegacyP10Mbps         = 10.39
	segmentLegacyShortMedianMbps = 9.6
	segmentLegacyRebuffer        = 0.0
)
