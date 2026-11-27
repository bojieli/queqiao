package congestion

import (
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/apernet/quic-go/monotime"
)

// A feeder that drives rateCorrelation with the gain cycle ProbeBW runs: one
// up epoch, one down epoch and six cruise epochs per cycle, each of n
// packets, in send order.
type corrFeed struct {
	c     *rateCorrelation
	id    uint64
	now   monotime.Time
	n     int
	flags int
	looks int
	// effective accumulates what compensation would use, as a fraction of
	// the true channel erasure.
	eff, effN float64
}

var corrCycle = [...]lossClass{classUp, classDown, classCruise, classCruise, classCruise, classCruise, classCruise, classCruise}

func newCorrFeed(n int) *corrFeed {
	return &corrFeed{c: &rateCorrelation{}, n: n, now: monotime.Time(time.Second)}
}

// epoch feeds one epoch whose packets are lost as lost(i) says.
func (f *corrFeed) epoch(class lossClass, lost func(i int) bool) {
	f.id++
	start := f.now
	for i := range f.n {
		f.c.observe(f.id, class, start, lost(i))
	}
	f.now = f.now.Add(50 * time.Millisecond)
}

// look records the verdict after the epochs fed so far (the last epoch is
// closed by the next one, so it is called once per epoch after the first).
func (f *corrFeed) look(erasure float64) {
	f.looks++
	if f.c.flagged {
		f.flags++
	}
	use := erasure
	if floor, ok := f.c.compensable(); ok && floor < use {
		use = floor
	}
	f.eff += use / erasure
	f.effN++
}

func TestRateCorrelationIndependentLossIsCompensated(t *testing.T) {
	for _, tc := range []struct {
		name  string
		p     float64
		burst float64
	}{
		{"bernoulli 3%", 0.03, 1},
		{"bernoulli 20%", 0.20, 1},
		{"bernoulli 42%", 0.42, 1},
		{"gilbert 20% runs of 5.7", 0.20, 5.7},
		{"gilbert 42% runs of 5.7", 0.42, 5.7},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rng := rand.New(rand.NewSource(7))
			f := newCorrFeed(300)
			inBurst := false
			lost := func(int) bool {
				if tc.burst <= 1 {
					return rng.Float64() < tc.p
				}
				recover := 1 / tc.burst
				enter := recover * tc.p / (1 - tc.p)
				if inBurst {
					if rng.Float64() < recover {
						inBurst = false
					}
					return true
				}
				if rng.Float64() < enter {
					inBurst = true
					return true
				}
				return false
			}
			for cycle := range 600 {
				for _, class := range corrCycle {
					f.epoch(class, lost)
					if cycle > 1 {
						f.look(tc.p)
					}
				}
			}
			if share := float64(f.flags) / float64(f.looks); share > 0.02 {
				t.Errorf("flagged in %.1f%% of looks on independent loss, want under 2%%", 100*share)
			}
			// What compensation uses, against what the channel erases.
			if r := f.eff / f.effN; r < 0.97 {
				t.Errorf("compensation used %.3f of the channel's erasure, want at least 0.97", r)
			}
		})
	}
}

func TestRateCorrelationRateDependentLossIsFlagged(t *testing.T) {
	// A limiter of capacity C offered S x gain loses max(0, 1 - C/(S x gain)).
	for _, over := range []float64{0.9, 1.0, 1.5, 3} {
		rng := rand.New(rand.NewSource(11))
		f := newCorrFeed(300)
		gain := map[lossClass]float64{classUp: 1.25, classDown: 0.75, classCruise: 1}
		var flaggedAt int
		for cycle := range 12 {
			for _, class := range corrCycle {
				p := math.Max(0, 1-1/(over*gain[class]))
				f.epoch(class, func(int) bool { return rng.Float64() < p })
			}
			if f.c.flagged && flaggedAt == 0 {
				flaggedAt = cycle + 1
			}
		}
		if over < 0.9/1.25+0.001 {
			continue
		}
		wantFlag := over >= 1/1.25+0.05
		if wantFlag && (flaggedAt == 0 || flaggedAt > 8) {
			t.Errorf("offered %.1fx capacity: flagged after %d cycles, want within 8", over, flaggedAt)
		}
		if !wantFlag && flaggedAt != 0 {
			t.Errorf("offered %.1fx capacity: flagged though no phase overflows", over)
		}
		if !wantFlag {
			continue
		}
		floor, ok := f.c.compensable()
		if !ok {
			t.Fatalf("offered %.1fx capacity: flagged without a floor", over)
		}
		cruise := math.Max(0, 1-1/over)
		if cruise > 0.02 && floor > 0.9*cruise {
			t.Errorf("offered %.1fx capacity: floor %.3f against cruise loss %.3f; compensation would still cover the overflow", over, floor, cruise)
		}
	}
}

func TestRateCorrelationNeedsEnoughEvidence(t *testing.T) {
	rng := rand.New(rand.NewSource(3))
	f := newCorrFeed(300)
	// Strongly correlated loss, but only five cycles: five up epochs.
	for range corrMinUp - 1 {
		for _, class := range corrCycle {
			p := 0.0
			if class == classUp {
				p = 0.3
			}
			f.epoch(class, func(int) bool { return rng.Float64() < p })
		}
	}
	f.epoch(classCruise, func(int) bool { return false })
	if f.c.flagged {
		t.Fatalf("flagged on %d up epochs, want at least %d", corrMinUp-1, corrMinUp)
	}
	// Epochs below the packet minimum never count.
	g := newCorrFeed(corrMinEpochPackets - 1)
	for range 40 {
		for _, class := range corrCycle {
			p := 0.0
			if class == classUp {
				p = 0.5
			}
			g.epoch(class, func(int) bool { return rng.Float64() < p })
		}
	}
	if g.c.count != 0 || g.c.flagged {
		t.Fatalf("short epochs counted: %d in the window, flagged=%v", g.c.count, g.c.flagged)
	}
	// Classless epochs (startup, drain, ProbeRTT) never count either.
	h := newCorrFeed(300)
	for range 200 {
		h.epoch(classNone, func(int) bool { return rng.Float64() < 0.3 })
	}
	if h.c.count != 0 {
		t.Fatalf("classless epochs counted: %d", h.c.count)
	}
}

func TestRateCorrelationHysteresisDoesNotFlap(t *testing.T) {
	rng := rand.New(rand.NewSource(5))
	f := newCorrFeed(300)
	flow := func(upLoss, baseLoss float64, cycles int) {
		for range cycles {
			for _, class := range corrCycle {
				p := baseLoss
				if class == classUp {
					p = upLoss
				}
				f.epoch(class, func(int) bool { return rng.Float64() < p })
			}
		}
	}
	flow(0.15, 0.02, 14)
	if !f.c.flagged {
		t.Fatal("not flagged on a 13-point gap")
	}
	before := f.c.flips.Load()
	// The gap shrinks to the 0.02 above the leaving threshold: still
	// flagged, no flip in either direction.
	flow(0.04, 0.02, 40)
	if !f.c.flagged || f.c.flips.Load() != before {
		t.Fatalf("flagged=%v, flips %d->%d on a gap above the leaving threshold", f.c.flagged, before, f.c.flips.Load())
	}
	flow(0.02, 0.02, 40)
	if f.c.flagged {
		t.Fatal("still flagged with no gap")
	}
}

// An outage that starts during an up epoch loses every packet from then on,
// whatever its rate class. Without the exclusion the up epochs it lands in
// read as rate-correlated loss.
func TestRateCorrelationOutageDuringUpPhaseDoesNotFlag(t *testing.T) {
	run := func(exclude bool) (flagged bool, discarded int) {
		rng := rand.New(rand.NewSource(13))
		f := newCorrFeed(300)
		// A clean 2% channel with an outage every other cycle, beginning
		// half way through the up epoch and lasting through the next
		// epochs: the up epoch loses half its packets and the rest, a
		// whole run of epochs, lose everything.
		for cycle := range 40 {
			for i, class := range corrCycle {
				outage := cycle%2 == 1 && i <= 2
				start := f.now
				f.epoch(class, func(k int) bool {
					switch {
					case outage && i == 0:
						return k >= f.n/2 || rng.Float64() < 0.02
					case outage:
						return true
					}
					return rng.Float64() < 0.02
				})
				if outage && i == 0 && exclude {
					// noteAnswered's call when the silence ends: since is
					// the first packet sent into it, half way through the
					// up epoch, less two round trips of flight.
					f.c.discardSince(start.Add(-2 * 50 * time.Millisecond))
					discarded++
				}
			}
			if f.c.flagged {
				flagged = true
			}
		}
		return flagged, discarded
	}
	if flagged, _ := run(false); !flagged {
		t.Skip("the unprotected statistic does not flag this outage pattern, so the test would prove nothing")
	}
	if flagged, n := run(true); flagged || n == 0 {
		t.Fatalf("outages during up phases flagged the loss (flagged=%v, %d discards)", flagged, n)
	}
}

// The law end: noteAnswered, on judging a silence an outage, discards the
// epochs that overlap it.
func TestPathLawOutageDiscardsCorrelationEpochs(t *testing.T) {
	e := newPathLaw(1200, nil)
	rtt := &pcSimRTT{min: 50 * time.Millisecond, smoothed: 50 * time.Millisecond, latest: 50 * time.Millisecond}
	e.SetRTTStatsProvider(rtt)
	f := &corrFeed{c: e.corr, n: 300, now: monotime.Time(time.Second)}
	for range 4 {
		for _, class := range corrCycle {
			f.epoch(class, func(int) bool { return false })
		}
	}
	f.epoch(classCruise, func(int) bool { return false }) // closes the last
	kept := e.corr.count
	if kept == 0 {
		t.Fatal("no epochs to discard")
	}
	since := f.now.Add(-3 * 50 * time.Millisecond)
	e.unansweredSince = since
	e.noteAnswered(f.now.Add(5 * time.Second))
	if e.corr.count >= kept {
		t.Fatalf("outage left %d of %d epochs", e.corr.count, kept)
	}
	for i := range e.corr.count {
		ep := e.corr.ring[(e.corr.next-e.corr.count+i+corrWindow)%corrWindow]
		if !ep.start.Before(since) {
			t.Fatalf("an epoch sent at %v, after the silence began at %v, survived", ep.start, since)
		}
	}
}
