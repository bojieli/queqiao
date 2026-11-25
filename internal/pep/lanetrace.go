package pep

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bojieli/queqiao/internal/stripe"
)

// laneTrace prints one line per lane per congestion sample when
// QUEQIAO_LANE_TRACE is set. It exists because every question this transport has
// had to answer -- why a window grew, why a bottleneck dropped, why one trial
// was twice another -- is a question about per-lane state over time, and a
// throughput number at the end of a transfer cannot answer any of them.
//
// It is off unless asked for and writes typed records through the runtime
// logger. It is not an aggregate metric: this is deliberately raw per-lane
// state, but it shares the bounded file and schema envelope with other logs.
var laneTrace atomic.Bool

func init() { laneTrace.Store(os.Getenv("QUEQIAO_LANE_TRACE") == "1") }

var traceStart = time.Now()

// traceLane records one lane's transport and scheduler state. The flow is
// identified by its address rather than its wire identifier because both
// endpoints share the latter, and in an in-process benchmark both endpoints
// trace to the same stream.
func traceLane(f *multipathFlow, laneID uint64, stats laneTransportStats) {
	if !laneTrace.Load() {
		return
	}
	window := 0
	if lane := f.laneByID(laneID); lane != nil {
		window = lane.windowBytes()
	}
	c := stats.controller
	var held, queued uint64
	var chunks, ready int
	var flowHeld uint64
	var issued, reissued, sourceBytes, reissuedBytes, laneFailures uint64
	var reissue stripe.ReissueStats
	if sched := f.scheduler.Load(); sched != nil {
		chunks, held, queued = sched.LaneOutstanding(laneID)
		ready, flowHeld = sched.Pending()
		st := sched.Stats()
		// issued against source bytes is the check that found a completed chunk
		// being sent twice: a 320-chunk object cannot be issued 481 times.
		issued, reissued = st.ChunksIssued, st.ChunksRetransmit
		sourceBytes, reissuedBytes = st.SourceBytes, st.BytesRetransmit
		laneFailures = st.LaneFailures
		reissue = st.Reissue
	}
	meanResidency, maxResidency := f.takeResidency()
	if f.logger == nil {
		return
	}
	f.logger.Info("lane performance snapshot",
		"type", "lane_metrics", "telemetry_schema", 1,
		"t", time.Since(traceStart).Seconds(), "flow", fmt.Sprintf("%p", f), "flow_id", f.flowID, "lane", laneID,
		"cwnd", c.CongestionWindow, "inflight", c.BytesInFlight,
		"minrtt", float64(c.MinRTT.Microseconds())/1000, "srtt", float64(stats.smoothedRTT.Microseconds())/1000,
		"pacing", c.PacingRate, "maxbw", c.MaxBandwidth, "erasure", c.Erasure,
		"round", c.Round, "appsamp", c.AppSamples, "nonapp", c.NonAppSamples,
		"window", window, "held", held, "queued", queued, "chunks", chunks, "ready", ready, "flowheld", flowHeld,
		"issued", issued, "reissued", reissued, "source", sourceBytes, "breissued", reissuedBytes, "lanefail", laneFailures,
		"resid", float64(meanResidency.Microseconds())/1000, "residmax", float64(maxResidency.Microseconds())/1000,
		"acksin", f.acksIn.Swap(0), "acksout", f.acksOut.Swap(0), "acksched", f.acksSched.Swap(0),
		"ackwr", float64(f.ackWriteNS.Swap(0))/1e6,
		"sent", stats.bytesSent, "pktsent", stats.packetsSent, "pktrecv", stats.packetsReceived,
		// What the path did, not what the controller was charged. quic-go's own
		// loss counters are absent because this transport replaces its
		// congestion controller, so they never leave zero.
		"lost", c.PacketsLostObserved, "lostcong", c.PacketsLost, "mode", c.Mode,
		// How the share and the pacer treated this connection; see
		// congestion.ControllerTelemetry.
		"share", c.Share, "capped", c.ShareCapped, "upace", c.UncappedPacing, "applimited", c.AppLimited,
		"aobytes", c.AckOnlyBytes, "aocharged", c.AckOnlyCharged, "ackedb", c.AckedBytes,
		"pdeny", c.PacingDenials, "wdeny", c.WindowDenials, "held", c.HeldSends,
		// Reissued bytes by cause and by the replaced attempt's substrate.
		"rs_lane", reissue.Bytes[stripe.ReissueLane], "rs_timer", reissue.Bytes[stripe.ReissueTimer],
		"rs_gap", reissue.Bytes[stripe.ReissueGap], "rs_rescue", reissue.Bytes[stripe.ReissueRescue],
		"rs_rel", reissue.FromReliable, "rs_coded", reissue.FromCoded)
}

// residency records how long a chunk stays in the scheduler's window: from the
// moment a lane takes it to the moment the peer's range acknowledgement frees
// it. It is the denominator of a lane's throughput -- window divided by
// residency -- so it is the number that says whether a window is too small or
// the loop through it is too slow, and no throughput figure distinguishes
// those.
type residency struct {
	mu    sync.Mutex
	total time.Duration
	count int
	max   time.Duration
}

func (f *multipathFlow) observeResidency(d time.Duration) {
	if !laneTrace.Load() {
		return
	}
	f.residency.mu.Lock()
	f.residency.total += d
	f.residency.count++
	if d > f.residency.max {
		f.residency.max = d
	}
	f.residency.mu.Unlock()
}

// takeResidency returns the mean and maximum since the last call.
func (f *multipathFlow) takeResidency() (mean, max time.Duration) {
	f.residency.mu.Lock()
	defer f.residency.mu.Unlock()
	if f.residency.count > 0 {
		mean = f.residency.total / time.Duration(f.residency.count)
	}
	max = f.residency.max
	f.residency.total, f.residency.count, f.residency.max = 0, 0, 0
	return mean, max
}

// ackTiming rides the context of a traced flow-acknowledgement write, so the
// lane that carried it and where its time went can be recorded without
// changing the write path's signatures. Only the ACK loop attaches one, and
// only while the trace is on.
type ackTiming struct {
	pendingSinceNS  int64
	lockNS, writeNS int64
	lane            uint64
	carrier         *mpLane
}

type ackTimingKey struct{}

// traceACKSent records one flow acknowledgement as written: which lane's
// connection actually carried it -- writeACKPreferred can use any healthy
// lane, so the flow's data lane does not identify it -- how long it waited to
// be coalesced, for the frame writer's lock and in the stream write, and that
// connection's controller state at the moment, so a delay can be pinned on
// the pacer, the share or the window.
func (f *multipathFlow) traceACKSent(sequence uint64, gap, repeat bool, start time.Time, t *ackTiming, err error) {
	if f.logger == nil {
		return
	}
	coalesce := 0.0
	if t.pendingSinceNS > 0 {
		coalesce = float64(start.UnixNano()-t.pendingSinceNS) / 1e6
	}
	attrs := []any{
		"type", "flow_ack", "telemetry_schema", 1, "flow_id", f.flowID, "seq", sequence,
		"gap", gap, "repeat", repeat, "carrier", t.lane, "failed", err != nil,
		"coalesce_ms", coalesce, "lock_ms", float64(t.lockNS) / 1e6, "write_ms", float64(t.writeNS) / 1e6,
		"total_ms", float64(time.Since(start).Microseconds()) / 1000,
	}
	if t.carrier != nil && t.carrier.fc != nil {
		if provider, ok := t.carrier.fc.transport().(laneStatsProvider); ok {
			c := provider.transportStats().controller
			attrs = append(attrs, "pacing", c.PacingRate, "upace", c.UncappedPacing, "share", c.Share,
				"capped", c.ShareCapped, "inflight", c.BytesInFlight, "cwnd", c.CongestionWindow,
				"pdeny", c.PacingDenials, "wdeny", c.WindowDenials, "held", c.HeldSends,
				"aobytes", c.AckOnlyBytes, "aocharged", c.AckOnlyCharged, "ackedb", c.AckedBytes,
				"applimited", c.AppLimited)
		}
	}
	f.logger.Info("flow ack sent", attrs...)
}

// traceACKReceived records a flow acknowledgement's arrival, with the lane it
// arrived on; matched against traceACKSent by flow and sequence it is the
// acknowledgement's transit.
func (f *multipathFlow) traceACKReceived(event inboundEvent) {
	if f.logger == nil {
		return
	}
	var lane uint64
	if event.lane != nil {
		lane = event.lane.id
	}
	f.logger.Info("flow ack received", "type", "flow_ack", "telemetry_schema", 1, "flow_id", f.flowID,
		"seq", event.frame.Header.Sequence, "flags", event.frame.Header.Flags, "lane", lane)
}
