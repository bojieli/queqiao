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
//
// Only a QUIC stream write of bytes no carrier had before is timed. Its
// return is the moment the carrier took the bytes; a coded write returns
// when the frame is queued for the code, before its symbols are registered
// or even sent; a TCP lane has no path minimum round trip to subtract; and
// a re-issue would time the repair instead of the delay that caused it (it
// may still complete a sample already armed).
type creditSampler struct {
	pending atomic.Bool
	// takenEnd is the highest offset any carrier has taken, on any lane and
	// substrate: a write that does not start at or above it re-issues bytes
	// a carrier already had, and must not start a sample, which would time
	// the repair and not the delay that caused it.
	takenEnd atomic.Uint64
	mu       sync.Mutex
	start    uint64
	end      uint64
	taken    time.Time
	minRTT   time.Duration
}

// noteTaken advances takenEnd to end and reports whether [sequence, end)
// lies wholly above what any carrier had taken before: an original
// transmission.
func (c *creditSampler) noteTaken(sequence, end uint64) bool {
	for {
		high := c.takenEnd.Load()
		if end <= high {
			return false
		}
		if c.takenEnd.CompareAndSwap(high, end) {
			return sequence >= high
		}
	}
}

// noteCarrierTook is called when a lane's carrier has taken a DATA frame;
// reliable says it went on the QUIC stream rather than the code.
func (f *multipathFlow) noteCarrierTook(lane *mpLane, sequence uint64, n int, reliable bool) {
	if f.metrics == nil || n <= 0 {
		return
	}
	end := sequence + uint64(n)
	original := f.credit.noteTaken(sequence, end)
	if !original || !reliable || lane.kind != TransportQUIC || f.credit.pending.Load() {
		return
	}
	taken := time.Now()
	// The baseline is the path's own minimum round trip, as the transport
	// measures it, whatever controller runs: Reno installs none, and
	// another controller's figure can be a smoothed or fallback value that
	// would subtract the very queueing this measures.
	var minRTT time.Duration
	if provider, ok := lane.fc.transport().(laneStatsProvider); ok {
		minRTT = provider.transportStats().minRTT
	}
	if minRTT <= 0 {
		// No round trip measured yet: nothing to subtract, so no sample.
		return
	}
	// Armed under replayMu, the lock acknowledgements are applied under,
	// so an acknowledgement is either already reflected here or will find
	// the sample armed. A chunk already covered has no covering
	// acknowledgement still to come.
	f.replayMu.Lock()
	defer f.replayMu.Unlock()
	if f.acked >= end || f.ackTrack != nil && f.ackTrack.Covered(sequence, end) {
		return
	}
	c := &f.credit
	c.mu.Lock()
	if !c.pending.Load() {
		c.start, c.end, c.taken, c.minRTT = sequence, end, taken, minRTT
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
