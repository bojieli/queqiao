package congestion

import (
	"math"
	"sync/atomic"
	"time"

	"github.com/apernet/quic-go/monotime"
)

// rateCorrelation decides whether the loss a path authority sees rises with
// the authority's own sending rate (docs/DESIGN-EGRESS-JOINT-LIMIT.md §2).
//
// The erasure law compensates for loss, which is right only for loss that is
// independent of the rate: a channel that erases at random. Loss at a
// limiter or an overflowing buffer is a function of the offered rate, and
// compensating it is sending more into the overflow. Which is which is a
// measurement the sender can make, because ProbeBW already varies the rate:
// the pacing gain cycles through 1.25, 0.75 and 1 (tuicPacingGains). The loss
// of the packets sent at 1.25 is compared with the loss of those sent at the
// lower gains.
//
// The unit of evidence is the epoch, a run of consecutive sample packets in
// the path's send order that share a rate class. Fates reach the estimator in
// send order, so an epoch is complete when the first record of a later one is
// observed.
type rateCorrelation struct {
	// The epoch being accumulated, in observation order.
	curID      uint64
	curClass   lossClass
	curStart   monotime.Time
	curN, curL int
	open       bool

	// ring holds the last corrWindow closed, usable epochs.
	ring  [corrWindow]corrEpoch
	next  int
	count int

	flagged bool
	delta   float64
	floor   float64
	floorOK bool

	// published for telemetry and for readers outside the law's callbacks.
	flaggedBit atomic.Bool
	deltaPPM   atomic.Int64
	flips      atomic.Uint64
}

// lossClass is the rate class a packet was sent in.
type lossClass uint8

const (
	// classNone is every mode in which the rate moves for other reasons:
	// startup, drain, ProbeRTT. Its epochs are not used.
	classNone lossClass = iota
	classUp
	classCruise
	classDown
)

type corrEpoch struct {
	class lossClass
	loss  float64
	n     int
	start monotime.Time
}

const (
	// corrWindow is how many closed epochs the statistic looks at.
	corrWindow = 96
	// corrMinEpochPackets is the fewest decided packets an epoch needs.
	corrMinEpochPackets = 20
	// corrMinUp and corrMinBase are the fewest up epochs and cruise-or-down
	// epochs the verdict needs; corrMinFloor the fewest of one class its
	// floor needs.
	corrMinUp, corrMinBase, corrMinFloor = 6, 12, 6
	// corrEnterDelta and corrEnterSigmas flag; corrLeaveDelta and
	// corrLeaveSigmas withdraw the flag.
	corrEnterDelta, corrEnterSigmas = 0.02, 3.0
	corrLeaveDelta, corrLeaveSigmas = 0.01, 1.0
	// corrSmallSample widens the entry threshold by corrSmallSample/|U|
	// standard errors: 4 at six up epochs, 3.5 at twelve.
	corrSmallSample = 6.0
	// corrEpochFloor is the shortest epoch span.
	corrEpochFloor = 5 * time.Millisecond
	// corrOutageLookback is how many smoothed round trips before an
	// outage's first unanswered packet an epoch may still hold packets that
	// were in flight into it.
	corrOutageLookback = 2
)

// observe accounts one decided packet of epoch id, sent at the given time in
// the given class.
func (c *rateCorrelation) observe(id uint64, class lossClass, sent monotime.Time, lost bool) {
	if c == nil {
		return
	}
	if c.open && id != c.curID {
		c.close()
	}
	if !c.open {
		c.open, c.curID, c.curClass, c.curStart, c.curN, c.curL = true, id, class, sent, 0, 0
	}
	c.curN++
	if lost {
		c.curL++
	}
}

// close ends the epoch being accumulated. An epoch is not evidence if it is
// in no rate class, is too short to give a rate, or is a round with no
// acknowledgements at all: a path that was away loses everything whatever the
// rate (§2.2).
func (c *rateCorrelation) close() {
	n, l, class, start := c.curN, c.curL, c.curClass, c.curStart
	c.open = false
	if class == classNone || n < corrMinEpochPackets || l == n {
		return
	}
	c.ring[c.next] = corrEpoch{class: class, loss: float64(l) / float64(n), n: n, start: start}
	c.next = (c.next + 1) % corrWindow
	if c.count < corrWindow {
		c.count++
	}
	c.evaluate()
}

// discardSince drops the epochs sent at or after since, the one being
// accumulated included: they overlap a detected outage (§2.2). Epochs of
// packets that were still in flight when the silence began are dropped by the
// caller's choice of since.
func (c *rateCorrelation) discardSince(since monotime.Time) {
	if c.open && !c.curStart.Before(since) {
		c.open = false
	}
	kept := make([]corrEpoch, 0, c.count)
	for i := range c.count {
		e := c.ring[(c.next-c.count+i+corrWindow)%corrWindow]
		if e.start.Before(since) {
			kept = append(kept, e)
		}
	}
	c.next, c.count = 0, 0
	for _, e := range kept {
		c.ring[c.next] = e
		c.next = (c.next + 1) % corrWindow
		c.count++
	}
	c.evaluate()
}

type moments struct {
	n       int
	sum, sq float64
	packets int
}

func (m *moments) add(e corrEpoch) {
	m.n++
	m.sum += e.loss
	m.sq += e.loss * e.loss
	m.packets += e.n
}

func (m *moments) mean() float64 {
	if m.n == 0 {
		return 0
	}
	return m.sum / float64(m.n)
}

// variance is the sample variance of an epoch's loss, floored at what one
// epoch of that many packets would show from sampling alone, so that a few
// identical epochs do not claim certainty.
func (m *moments) variance() float64 {
	if m.n == 0 {
		return 0
	}
	p := m.mean()
	binomial := p * (1 - p) / (float64(m.packets) / float64(m.n))
	if m.n < 2 {
		return binomial
	}
	v := (m.sq - float64(m.n)*p*p) / float64(m.n-1)
	if v < binomial {
		v = binomial
	}
	return v
}

func (c *rateCorrelation) evaluate() {
	var up, base, cruise, down moments
	for i := range c.count {
		e := c.ring[(c.next-c.count+i+corrWindow)%corrWindow]
		switch e.class {
		case classUp:
			up.add(e)
		case classCruise:
			cruise.add(e)
			base.add(e)
		case classDown:
			down.add(e)
			base.add(e)
		}
	}
	c.floorOK = false
	if base.n >= corrMinBase {
		c.floor, c.floorOK = base.mean(), true
		if cruise.n >= corrMinFloor {
			c.floor = cruise.mean()
		}
		if down.n >= corrMinFloor && (cruise.n < corrMinFloor || down.mean() < c.floor) {
			c.floor = down.mean()
		}
	}
	if up.n < corrMinUp || base.n < corrMinBase {
		c.delta = 0
		c.set(false)
		return
	}
	delta := up.mean() - base.mean()
	se := math.Sqrt(up.variance()/float64(up.n) + base.variance()/float64(base.n))
	c.delta = delta
	if c.flagged {
		c.set(delta >= corrLeaveDelta && delta >= corrLeaveSigmas*se)
	} else {
		// With few up epochs the variance is itself an estimate and the
		// statistic's tail is heavier than a normal's: three standard
		// errors from six epochs is not one chance in a thousand.
		c.set(delta >= corrEnterDelta && delta >= (corrEnterSigmas+corrSmallSample/float64(up.n))*se)
	}
}

func (c *rateCorrelation) set(on bool) {
	if on != c.flagged {
		c.flips.Add(1)
	}
	c.flagged = on
	c.flaggedBit.Store(on)
	c.deltaPPM.Store(int64(c.delta * partsPerMillion))
}

// compensable is the loss the law may make up for while the loss is
// rate-correlated: the loss at the lowest rates the sender offers. ok is
// false while the loss is not flagged or the floor has too little behind it.
func (c *rateCorrelation) compensable() (float64, bool) {
	if !c.flagged || !c.floorOK {
		return 0, false
	}
	return c.floor, true
}
