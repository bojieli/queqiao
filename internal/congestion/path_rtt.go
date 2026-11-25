package congestion

import (
	"time"

	quiccongestion "github.com/apernet/quic-go/congestion"
)

// pathRTT is a path controller's round-trip state: the minimum and an
// exponentially weighted mean over the samples that accompany its
// attachments' acknowledgement events (docs/DESIGN-PATH-AUTHORITY.md §4.2).
// Each connection keeps its own RTT and PTO in quic-go; this is the path's,
// and it is what the control law's delay bound and ProbeRTT read.
//
// Until the path has a sample it reports what fallback, the first attached
// connection's own statistics, reports, so the law starts from the
// handshake's measurement exactly as it did on one connection.
//
// It is accessed only under the controller's lock.
type pathRTT struct {
	min, latest, smoothed, deviation time.Duration
	fallback                         quiccongestion.RTTStatsProvider
}

var _ quiccongestion.RTTStatsProvider = (*pathRTT)(nil)

// observe adds one sample; a zero sample is none.
func (r *pathRTT) observe(sample time.Duration) {
	if sample <= 0 {
		return
	}
	r.latest = sample
	if r.min == 0 || sample < r.min {
		r.min = sample
	}
	if r.smoothed == 0 {
		r.smoothed = sample
		r.deviation = sample / 2
		return
	}
	diff := r.smoothed - sample
	if diff < 0 {
		diff = -diff
	}
	r.deviation = (3*r.deviation + diff) / 4
	r.smoothed = (7*r.smoothed + sample) / 8
}

func (r *pathRTT) sampled() bool { return r.smoothed > 0 }

func (r *pathRTT) MinRTT() time.Duration {
	if !r.sampled() && r.fallback != nil {
		return r.fallback.MinRTT()
	}
	return r.min
}

func (r *pathRTT) LatestRTT() time.Duration {
	if !r.sampled() && r.fallback != nil {
		return r.fallback.LatestRTT()
	}
	return r.latest
}

func (r *pathRTT) SmoothedRTT() time.Duration {
	if !r.sampled() && r.fallback != nil {
		return r.fallback.SmoothedRTT()
	}
	return r.smoothed
}

func (r *pathRTT) MeanDeviation() time.Duration {
	if !r.sampled() && r.fallback != nil {
		return r.fallback.MeanDeviation()
	}
	return r.deviation
}

func (r *pathRTT) MaxAckDelay() time.Duration {
	if r.fallback != nil {
		return r.fallback.MaxAckDelay()
	}
	return 0
}

func (r *pathRTT) PTO(includeMaxAckDelay bool) time.Duration {
	if !r.sampled() && r.fallback != nil {
		return r.fallback.PTO(includeMaxAckDelay)
	}
	pto := r.smoothed + max(4*r.deviation, time.Millisecond)
	if includeMaxAckDelay {
		pto += r.MaxAckDelay()
	}
	return pto
}

// The mutators belong to the connections' own statistics; the path's are
// fed by observe.
func (r *pathRTT) UpdateRTT(time.Duration, time.Duration) {}
func (r *pathRTT) SetMaxAckDelay(time.Duration)           {}
func (r *pathRTT) SetInitialRTT(time.Duration)            {}
