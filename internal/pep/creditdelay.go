package pep

import (
	"sync"
	"sync/atomic"
	"time"
)

// creditSampler measures sender-side credit delay (docs/DESIGN-PATH-
// AUTHORITY.md §15.3, §15.4): the time from a chunk's last byte being taken
// by the carrier to the flow acknowledgement that covers the chunk, minus the
// path's minimum round trip, on this endpoint's clock alone.
//
// It is passive telemetry behind no switch, so its cost is kept to an atomic
// load per DATA frame written and per acknowledgement that advances. A flow
// holds at most one outstanding sample, the way a TCP sender times one
// segment per round trip: the next chunk the carrier takes after a sample
// completes starts the next one. A chunk the flow later re-issues keeps its
// first take time, so a repair's cost is part of the delay it caused.
type creditSampler struct {
	pending atomic.Bool
	mu      sync.Mutex
	start   uint64
	end     uint64
	taken   time.Time
	minRTT  time.Duration
}

// noteCarrierTook is called when a lane's carrier has taken a DATA frame.
func (f *multipathFlow) noteCarrierTook(lane *mpLane, sequence uint64, n int) {
	if f.metrics == nil || n <= 0 || f.credit.pending.Load() {
		return
	}
	minRTT := time.Duration(f.minRTTNS.Load())
	if minRTT <= 0 {
		// The flow has not refreshed its telemetry yet: ask this lane.
		if provider, ok := lane.fc.transport().(laneStatsProvider); ok {
			minRTT = provider.transportStats().controller.MinRTT
		}
	}
	if minRTT <= 0 {
		// No path minimum to subtract (a TCP lane, or no round trip yet):
		// a sample would not mean the same thing, so none is taken.
		return
	}
	c := &f.credit
	c.mu.Lock()
	if !c.pending.Load() {
		c.start, c.end, c.taken, c.minRTT = sequence, sequence+uint64(n), time.Now(), minRTT
		c.pending.Store(true)
	}
	c.mu.Unlock()
}

// noteCreditCovered is called with replayMu held after an acknowledgement
// advanced the flow's coverage. It completes the outstanding sample if the
// acknowledgement covers the sampled chunk. It takes no other flow lock.
func (f *multipathFlow) noteCreditCovered() {
	c := &f.credit
	if !c.pending.Load() {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.pending.Load() {
		return
	}
	if f.acked < c.end && (f.ackTrack == nil || !f.ackTrack.Covered(c.start, c.end)) {
		return
	}
	f.metrics.CreditDelay(time.Since(c.taken) - c.minRTT)
	c.pending.Store(false)
}
