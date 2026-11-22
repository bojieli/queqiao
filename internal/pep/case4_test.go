package pep

import (
	"io"
	"math/rand"
	"testing"
	"time"

	"github.com/bojieli/queqiao/internal/pathsim"
)

// Falsification case 4 from docs/CONTROL-REDESIGN.md. A policer drops
// excess traffic without queueing. The delay brake therefore remains inactive.
// Bound the peak controller rates to catch runaway feedback across lane joins.
func TestCase4APolicedPathIsStillUnbraked(t *testing.T) {
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
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 8; i++ {
			if _, err := conn.Write(payload); err != nil {
				return
			}
			got := make([]byte, len(payload))
			if _, err := io.ReadFull(conn, got); err != nil {
				return
			}
		}
	}()

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
	t.Logf("queue observation: smoothed RTT %v, controller minimum %v; %d valid samples, %d pending baselines",
		maxQueueSmoothed, maxQueueMin, validQueueSamples, pendingQueueSamples)
	if validQueueSamples == 0 {
		t.Fatal("the flow never supplied a measured RTT baseline, so queue delay was not characterized")
	}

	if peakPacing == 0 {
		t.Skip("the flow never got going, so this run measured nothing")
	}
	// Bucket bursts and startup gain can exceed the sustained rate. With the
	// two pooled-rate fixes the local peaks were 1.0–1.3x and 3.6x. Allow
	// headroom for host scheduling while rejecting feedback runaway.
	if peakBandwidth > shaped*8 || peakPacing > shaped*24 {
		t.Errorf("controller rates ran away on a %d B/s policer: estimated %d, pacing %d", shaped, peakBandwidth, peakPacing)
	}
	if maxQueue > 50*time.Millisecond || maxBrake > 0 {
		t.Errorf("a policer produced %v of queue and a brake of %.4f; if it now queues, this "+
			"is no longer the unbraked case and the design document should say so",
			maxQueue, maxBrake)
	}
}
