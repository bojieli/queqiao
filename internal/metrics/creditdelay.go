package metrics

import (
	"fmt"
	"io"
	"sync/atomic"
	"time"
)

// Sender-side credit delay (docs/DESIGN-PATH-AUTHORITY.md §15.3, §15.4): the
// time from a chunk's last byte being taken by the carrier to the flow
// acknowledgement that covers it, minus the path's minimum round trip, on
// the sender's clock alone. It is a generic secondary of the live gate. A
// healthy path keeps it near zero; queueing, a slow reverse ACK channel and
// repairs all add to it.
//
// It is passive: every endpoint records it, behind no switch. A flow takes
// at most one sample at a time (one per round trip or so), and the record is
// an atomic increment into a fixed histogram.

// CreditDelayBounds are the histogram's upper bucket bounds. A last, implicit
// bucket holds everything above the final bound (+Inf). They are fine below
// half a second, where the gate's margins are, and coarse above it.
var CreditDelayBounds = [...]time.Duration{
	time.Millisecond, 2 * time.Millisecond, 5 * time.Millisecond, 10 * time.Millisecond,
	15 * time.Millisecond, 20 * time.Millisecond, 30 * time.Millisecond, 40 * time.Millisecond,
	50 * time.Millisecond, 75 * time.Millisecond, 100 * time.Millisecond, 150 * time.Millisecond,
	200 * time.Millisecond, 300 * time.Millisecond, 400 * time.Millisecond, 500 * time.Millisecond,
	750 * time.Millisecond, time.Second, 1500 * time.Millisecond, 2 * time.Second,
	3 * time.Second, 5 * time.Second, 10 * time.Second, 30 * time.Second,
}

// CreditDelayBuckets is the number of histogram buckets, +Inf included.
const CreditDelayBuckets = len(CreditDelayBounds) + 1

type creditDelayHistogram struct {
	buckets [CreditDelayBuckets]atomic.Uint64
	sumNS   atomic.Uint64
}

// CreditDelaySnapshot is the histogram's part of a Snapshot. Buckets are not
// cumulative: Buckets[i] counts samples above bound i-1 and at most bound i.
type CreditDelaySnapshot struct {
	Buckets [CreditDelayBuckets]uint64
	Sum     time.Duration
}

// Count is the number of samples.
func (s CreditDelaySnapshot) Count() uint64 {
	var n uint64
	for _, v := range s.Buckets {
		n += v
	}
	return n
}

// CreditDelay records one sample. A negative delay (an acknowledgement
// faster than the path's recorded minimum) counts as zero.
func (r *Registry) CreditDelay(d time.Duration) {
	if r == nil {
		return
	}
	if d < 0 {
		d = 0
	}
	i := 0
	for i < len(CreditDelayBounds) && d > CreditDelayBounds[i] {
		i++
	}
	r.creditDelay.buckets[i].Add(1)
	r.creditDelay.sumNS.Add(uint64(d.Nanoseconds())) // #nosec G115 -- d is clamped to >= 0 above
}

func (r *Registry) creditDelaySnapshot() CreditDelaySnapshot {
	var s CreditDelaySnapshot
	for i := range s.Buckets {
		s.Buckets[i] = r.creditDelay.buckets[i].Load()
	}
	s.Sum = time.Duration(r.creditDelay.sumNS.Load())
	return s
}

// writeCreditDelay writes the histogram in the Prometheus text format, with
// cumulative buckets.
func writeCreditDelay(w io.Writer, s CreditDelaySnapshot) {
	var cumulative uint64
	for i, value := range s.Buckets {
		cumulative += value
		le := "+Inf"
		if i < len(CreditDelayBounds) {
			le = fmt.Sprintf("%g", CreditDelayBounds[i].Seconds())
		}
		fmt.Fprintf(w, "queqiao_credit_delay_seconds_bucket{le=\"%s\"} %d\n", le, cumulative)
	}
	fmt.Fprintf(w, "queqiao_credit_delay_seconds_sum %.9f\n", s.Sum.Seconds())
	fmt.Fprintf(w, "queqiao_credit_delay_seconds_count %d\n", cumulative)
}
