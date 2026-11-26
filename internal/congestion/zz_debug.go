package congestion

import (
	"fmt"
	"sync/atomic"
	"time"
)

// DebugCompensation turns on the #238 diagnostic log (temporary).
var DebugCompensation atomic.Bool

type debugEntry struct {
	at                        time.Time
	round                     uint64
	want, applied             float64
	delivered, deliveredAt    float64
	sampleMax                 uint64
	sampleMaxInterval         time.Duration
	ackRate, sendRate, pacing uint64
	minRTT, srtt              time.Duration
	erasure                   float64
}

func (e *ErasureSender) debugNote(want float64) {
	if !DebugCompensation.Load() {
		return
	}
	var srtt time.Duration
	if e.rttStats != nil {
		srtt = e.rttStats.SmoothedRTT()
	}
	est := &e.inner.estimator
	entry := debugEntry{
		at: time.Now(), round: e.inner.round, want: want, applied: e.appliedArrival,
		delivered: float64(est.estimate()), deliveredAt: e.deliveredAtCompensation,
		sampleMax: est.sampleRateMax, sampleMaxInterval: est.sampleMaxInterval,
		ackRate: est.latestAckRate, sendRate: est.latestSendRate, pacing: e.inner.pacingRate,
		minRTT: e.inner.minRoundTrip(), srtt: srtt, erasure: float64(e.erasure.Load()) / partsPerMillion,
	}
	if n := len(e.debugLog); n > 0 && e.debugLog[n-1].round == entry.round && e.debugLog[n-1].applied == entry.applied {
		e.debugLog[n-1] = entry
		return
	}
	if len(e.debugLog) >= 4000 {
		e.debugLog = e.debugLog[1:]
	}
	e.debugLog = append(e.debugLog, entry)
}

// DebugLog returns the diagnostic log of the path's law.
func (c *PathController) DebugLog() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []string
	for _, d := range c.law.debugLog {
		out = append(out, fmt.Sprintf("%s r%d want %.3f applied %.3f delivered %.2f@%.2f MB/s smax %.2f/%v ack %.2f send %.2f pacing %.2f minrtt %v srtt %v erasure %.3f",
			d.at.Format("05.000"), d.round, d.want, d.applied, d.delivered/1e6, d.deliveredAt/1e6, float64(d.sampleMax)/1e6, d.sampleMaxInterval.Round(time.Millisecond),
			float64(d.ackRate)/1e6, float64(d.sendRate)/1e6, float64(d.pacing)/1e6, d.minRTT.Round(time.Millisecond), d.srtt.Round(time.Millisecond), d.erasure))
	}
	return out
}
