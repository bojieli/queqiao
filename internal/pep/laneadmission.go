package pep

import (
	"context"
	"time"
)

// Lane admission, and why there is no flow-level congestion window above it.
//
// RFC 6356's linked increase used to sit here, bounding what a flow could
// commit across all its lanes so that a multipath flow took no more of a
// shared bottleneck than a single-path one would. It went when the fairness
// obligation did, and the QUIC lanes it coupled went after it: a QUIC flow's
// data is carried by one connection. The explicit TCP-only bundle below does
// not recreate that controller; each socket remains kernel paced. See
// docs/DESIGN.md.
//
// What is left is per-lane, and is not a congestion window. The transport
// already has one of those, and two nested controllers would be a mistake.

const (
	// laneSampleInterval is how often lane congestion state is read. It is
	// well inside a round trip on the paths this targets, so a loss episode is
	// seen while it still means something.
	laneSampleInterval = 50 * time.Millisecond
	// tcpStripingOutstandingBytes limits byte-offset commitment, not network
	// flight. A TLS write returns when the kernel socket buffer accepts bytes,
	// so without this bound the first TCP lane can own the whole read-ahead
	// window before a joined lane gets work. One MiB is several BDPs on the
	// measured 200-250 ms fallback path and keeps BBR application-fed while
	// leaving ready chunks for every other lane.
	tcpStripingOutstandingBytes = 1024 * 1024
	// laneOutstandingRounds and minLaneOutstandingBytes size a QUIC lane's
	// commitment; see quicLaneOutstanding. The floor is a few chunks, so a
	// lane whose estimate is still a trickle can grow it.
	laneOutstandingRounds   = 2
	minLaneOutstandingBytes = 4 * defaultChunkSize
)

// laneAdmission answers the scheduler's admission questions for one flow.
//
// A lane's write-ahead allowance is what its own transport says the path can
// hold. Negotiated TCP striping adds an acknowledgement-side commitment bound
// because a kernel TCP write is buffered rather than congestion-paced from the
// caller's perspective.
type laneAdmission struct {
	flow *multipathFlow
}

func (w *laneAdmission) Lane(laneID uint64, _ uint64) int {
	allowance := minLaneQueueBytes
	if lane := w.flow.laneByID(laneID); lane != nil {
		allowance = lane.windowBytes()
	}
	return allowance
}

func (w *laneAdmission) Outstanding(laneID uint64, _ uint64) int {
	lane := w.flow.laneByID(laneID)
	if lane == nil {
		return 0
	}
	if w.flow.tcpStriping.Load() && lane.kind == TransportTCP {
		return tcpStripingOutstandingBytes
	}
	return quicLaneOutstanding(lane.sendRate())
}

// quicLaneOutstanding bounds what a QUIC lane may hold unacknowledged by what
// it can put through in laneOutstandingRounds round trips at its own pacing
// rate: one in flight, one waiting behind it.
//
// Admission otherwise counts only what the lane's writer has not yet handed to
// the transport, and a QUIC stream takes a write as soon as its buffer has
// room, so a lane's commitment was bounded by nothing short of the flow's
// memory. A lane paced well below the others -- capped at its pool share, or a
// spare that had carried nothing and is still at a cold estimate -- went on
// taking chunks at the rate the fast lanes freed them and sent them at its own:
// measured live, 1.2-1.5 MB queued behind a lane pacing 0.7-1 MB/s, so a chunk
// taken there waited one to two seconds before it left. Every one of them
// outlived the two-round-trip reissue deadline and was copied to another lane,
// which is where four in ten chunks on the fast path were going twice; and the
// receiver's contiguous point waited on that lane, so no acknowledgement moved
// for three round trips and the stall watchdog demoted the lanes that were
// working. Bounded by its own rate, a slow lane carries what it can and leaves
// the rest ready for lanes that can carry it sooner.
//
// No bound while the lane has no rate or round trip to size one from.
func quicLaneOutstanding(pacing float64, rtt time.Duration) int {
	if pacing <= 0 || rtt <= 0 {
		return 0
	}
	bound := int(pacing * rtt.Seconds() * laneOutstandingRounds)
	if bound < minLaneOutstandingBytes {
		return minLaneOutstandingBytes
	}
	return bound
}

// Total is zero: independent TCP congestion controllers must not be coupled by
// another congestion window. Memory remains bounded by scheduler retention.
func (w *laneAdmission) Total() int { return 0 }

// laneByID returns a lane by identifier, or nil.
func (f *multipathFlow) laneByID(laneID uint64) *mpLane {
	f.lanesMu.RLock()
	defer f.lanesMu.RUnlock()
	return f.lanes[laneID]
}

// sampleLaneCongestion reads each lane's transport for the telemetry the path
// model and the lane trace are built from.
func (f *multipathFlow) sampleLaneCongestion(ctx context.Context) {
	ticker := time.NewTicker(laneSampleInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-f.done:
			return
		case <-ticker.C:
		}
		for _, lane := range f.healthyLanes() {
			provider, ok := lane.fc.transport().(laneStatsProvider)
			if !ok {
				continue
			}
			stats := provider.transportStats()
			traceLane(f, lane.id, stats)
		}
	}
}
