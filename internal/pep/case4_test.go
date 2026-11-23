package pep

import (
	"io"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bojieli/queqiao/internal/pathsim"
)

// Falsification case 4 from docs/CONTROL-REDESIGN.md.
//
// A policer drops what it cannot pass and holds nothing, so overload produces
// loss and no delay. Neither brake can act: there is no queue for the delay
// bound, and the loss is sustained enough to read as the channel's floor. This
// used to be a characterization of the overdrive that follows. Since the
// pooled seed was put in delivery units (#73) the overdrive is bounded, so
// the test now asserts both sides of that: no runaway, which current main
// fails at thousands of times the policer rate, and no collapse. It still
// asserts that the path is unbraked, because that part is not fixed.
//
// It matters more than a hypothetical, because internal/pathsim records that
// the live path this project targets is a policer -- "at twice the bottleneck
// rate it shows arrival runs averaging 2.3 packets and loss runs averaging
// 5.7 ... a limiter which passes everything for a while and then drops
// everything for a while".
func TestCase4APolicedPathIsBoundedNotRunaway(t *testing.T) {
	if testing.Short() {
		t.Skip("brings up QUIC across an emulated 300 ms path")
	}
	requireStableImpairmentClock(t)
	const shaped = 250_000
	path := pathsim.Config{
		OneWayDelay:         150 * time.Millisecond,
		RateBytesPerSec:     shaped,
		PolicerRefillPeriod: 8 * time.Millisecond,
		Seed:                53,
	}
	socks, destination := codedPair(t, false, &path)
	conn := socksDial(t, socks, destination, erasureChannelBudget(180*time.Second))
	defer conn.Close()

	payload := make([]byte, 256*1024)
	rand.New(rand.NewSource(29)).Read(payload)
	// echoed counts bytes as they come back and exchanges counts whole
	// round trips, so the collapse check below can read what was delivered
	// late in the run rather than the controller's peaks, which a sender that
	// cleared them at startup and then stalled would still pass.
	var echoed, exchanges atomic.Int64
	done := make(chan struct{})
	go func() {
		defer close(done)
		echo := countingReader{r: conn, n: &echoed}
		for i := 0; i < 8; i++ {
			if _, err := conn.Write(payload); err != nil {
				return
			}
			got := make([]byte, len(payload))
			if _, err := io.ReadFull(echo, got); err != nil {
				return
			}
			exchanges.Add(1)
		}
	}()
	type progress struct {
		at     time.Duration
		echoed int64
	}
	start := time.Now()
	samples := []progress{{0, 0}}

	var peakPacing, peakBandwidth uint64
	var lanes int64
	var lastMode uint32
	var maxQueue, maxQueueSmoothed, maxQueueMin time.Duration
	var validQueueSamples, pendingQueueSamples int
	var maxBrake float64
	var lastLoss float64
	deadline := time.After(24 * time.Second)
	for measuring := true; measuring; {
		select {
		case <-done:
			measuring = false
		case <-deadline:
			measuring = false
		case <-time.After(2 * time.Second):
		}
		samples = append(samples, progress{time.Since(start), echoed.Load()})
		s := lastClient.Metrics().Snapshot()
		if s.QUICControllerPacingRate > peakPacing {
			peakPacing = s.QUICControllerPacingRate
		}
		if s.QUICControllerMaxBandwidth > peakBandwidth {
			peakBandwidth = s.QUICControllerMaxBandwidth
		}
		if q, valid := policerQueueDelay(s.QUICSmoothedRTT, s.QUICControllerMinRTT); valid {
			validQueueSamples++
			if validQueueSamples == 1 || q > maxQueue {
				maxQueue = q
				maxQueueSmoothed, maxQueueMin = s.QUICSmoothedRTT, s.QUICControllerMinRTT
			}
		} else {
			pendingQueueSamples++
		}
		if s.QUICDelayBrake > maxBrake {
			maxBrake = s.QUICDelayBrake
		}
		lanes = s.QUICLanes
		lastMode = s.QUICControllerMode
		if s.QUICPacketsSent > 0 {
			lastLoss = 100 * float64(s.QUICLossObservedPackets) / float64(s.QUICPacketsSent)
		}
	}

	t.Logf("policed at %d B/s: peak pacing %d (%.1fx), peak bandwidth estimate %d (%.1fx), "+
		"worst queue %v, strongest brake %.4f, loss %.1f%%, lanes %d, controller mode %d",
		shaped, peakPacing, float64(peakPacing)/shaped,
		peakBandwidth, float64(peakBandwidth)/shaped,
		maxQueue.Round(time.Millisecond), maxBrake, lastLoss, lanes, lastMode)
	// Goodput over the later half of the run: from the first sample at or
	// after its midpoint to its end, whether that end was the deadline or the
	// last exchange completing.
	end := samples[len(samples)-1]
	mid := end
	for _, sample := range samples {
		if sample.at >= end.at/2 {
			mid = sample
			break
		}
	}
	var lateGoodput float64
	if end.at > mid.at {
		lateGoodput = float64(end.echoed-mid.echoed) / (end.at - mid.at).Seconds()
	}
	t.Logf("echoed %d bytes in %d of 8 exchanges over %v; from %v on, %.0f B/s (%.2fx)",
		end.echoed, exchanges.Load(), end.at.Round(time.Millisecond), mid.at.Round(time.Millisecond),
		lateGoodput, lateGoodput/shaped)
	t.Logf("queue observation: smoothed RTT %v, controller minimum %v; %d valid samples, %d pending baselines",
		maxQueueSmoothed, maxQueueMin, validQueueSamples, pendingQueueSamples)
	if validQueueSamples == 0 {
		t.Fatal("the flow never supplied a measured RTT baseline, so queue delay was not characterized")
	}

	if peakPacing == 0 {
		t.Skip("the flow never got going, so this run measured nothing")
	}
	// Runaway is a property of the controller's peaks. Measured locally over
	// 32 runs (docs/CONTROL-REDESIGN.md) the estimate peaked at 1.0-2.0x the
	// policer and pacing at 3.1-8.6x, pacing being the estimate times the
	// startup gain and any erasure compensation. The bounds leave headroom
	// for scheduling noise and still fail main's runaway by three orders of
	// magnitude.
	estimate, pacing := float64(peakBandwidth)/shaped, float64(peakPacing)/shaped
	if estimate > 3 || pacing > 12 {
		t.Errorf("controller rates ran away on a %d B/s policer: estimate %.1fx, pacing %.1fx", shaped, estimate, pacing)
	}
	// Collapse is not: a sender can clear any peak threshold at startup and
	// then stall. So it is judged by what came back over the later half of
	// the run. The exchanges are sequential, a write and then its echo, so
	// this never approaches the policer's rate: over 24 local runs it was
	// 0.17-0.35x, two to three exchanges' worth. 0.1x fails a sender that
	// finished fewer than about one and a half exchanges in that half.
	if lateGoodput < 0.1*shaped {
		t.Errorf("delivery collapsed on a %d B/s policer: %.0f B/s (%.2fx) echoed from %v to %v",
			shaped, lateGoodput, lateGoodput/shaped, mid.at.Round(time.Millisecond), end.at.Round(time.Millisecond))
	}
	if maxQueue > 50*time.Millisecond || maxBrake > 0 {
		t.Errorf("a policer produced %v of queue and a brake of %.4f; if it now queues, this "+
			"is no longer the unbraked case and the design document should say so",
			maxQueue, maxBrake)
	}
}

// countingReader counts the bytes read through it.
type countingReader struct {
	r io.Reader
	n *atomic.Int64
}

func (c countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n.Add(int64(n))
	return n, err
}
