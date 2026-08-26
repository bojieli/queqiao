package classifier

import (
	"testing"
	"time"
)

func TestNewFlowGetsInteractiveClassAfterBudget(t *testing.T) {
	c := New(DefaultConfig())
	got := c.Observe(Observation{
		Age:                      4 * time.Second,
		SinceLastPayload:         300 * time.Millisecond,
		BytesUp:                  1024,
		BytesDown:                2048,
		Bidirectional:            true,
		SmallBidirectionalBursts: true,
	})
	if got != ClassInteractive {
		t.Fatalf("class = %s, want interactive", got)
	}
}

func TestSustainedOneWayFlowBecomesBulk(t *testing.T) {
	c := New(DefaultConfig())
	got := c.Observe(Observation{
		Age:              3 * time.Second,
		BytesDown:        4 * 1024 * 1024,
		DownRate:         8 * 1024 * 1024,
		SinceLastPayload: 5 * time.Millisecond,
	})
	if got != ClassBulk {
		t.Fatalf("class = %s, want bulk", got)
	}
}

func TestBulkPromotionStartsAtEarlyLaneIsolationBoundary(t *testing.T) {
	c := New(DefaultConfig())
	got := c.Observe(Observation{
		Age: 1 * time.Second, BytesDown: 128 * 1024,
		DownRate: 128 * 1024, SinceLastPayload: 5 * time.Millisecond,
	})
	if got != ClassBulk {
		t.Fatalf("class = %s, want bulk at the configured promotion boundary", got)
	}
}

func TestConstrainedOneWayFlowStillBecomesBulk(t *testing.T) {
	c := New(DefaultConfig())
	got := c.Observe(Observation{
		Age: 4 * time.Second, BytesDown: 4 * 1024 * 1024,
		DownRate: 512 * 1024, SinceLastPayload: 5 * time.Millisecond,
	})
	if got != ClassBulk {
		t.Fatalf("class = %s, want bulk on a constrained path", got)
	}
}

func TestInteractiveBurstIsNotBulk(t *testing.T) {
	c := New(DefaultConfig())
	got := c.Observe(Observation{
		Age:                      30 * time.Second,
		BytesUp:                  8 * 1024 * 1024,
		BytesDown:                8 * 1024 * 1024,
		UpRate:                   32 * 1024,
		DownRate:                 32 * 1024,
		Bidirectional:            true,
		SmallBidirectionalBursts: true,
	})
	if got != ClassInteractive {
		t.Fatalf("class = %s, want interactive", got)
	}
}

func TestBulkClassDoesNotFlap(t *testing.T) {
	c := New(DefaultConfig())
	c.Observe(Observation{Age: 4 * time.Second, BytesDown: 4 * 1024 * 1024, DownRate: 8 * 1024 * 1024})
	got := c.Observe(Observation{Age: 12 * time.Second, BytesDown: 4 * 1024 * 1024, SinceLastPayload: 10 * time.Second})
	if got != ClassBulk {
		t.Fatalf("class = %s after idle gap, want bulk", got)
	}
}

// The measured regression these settings exist for: a connection carrying
// repeated megabyte requests accumulates enough bytes to cross the access-link
// bulk threshold, is demoted permanently, and is then isolated to protect
// interactive traffic that does not exist on it. On the China-US datacenter
// path that cost a factor of two while every other case gained five to
// seventeen times.
//
// The thresholds carry most of this: one request must not look like a
// transfer. The veto carries the rest: many requests must not add up to one.
func dcClassifier() *Classifier {
	cfg := DefaultConfig()
	cfg.BulkBytes = 32 << 20
	cfg.BulkMinimumAge = 10 * time.Second
	cfg.BulkIdleGapVeto = time.Second
	return New(cfg)
}

func TestOneLargeRequestIsNotBulkOnADatacenterLeg(t *testing.T) {
	c := dcClassifier()
	busy := Observation{BytesUp: 5 << 20, Age: 2 * time.Second,
		SinceLastPayload: 10 * time.Millisecond, UpRate: 20 << 20}
	if got := c.Observe(busy); got == ClassBulk {
		t.Fatalf("a single 5MB request classified %v", got)
	}
}

func TestManyRequestsDoNotAddUpToBulk(t *testing.T) {
	c := dcClassifier()
	busy := Observation{BytesUp: 1 << 20, Age: 2 * time.Second,
		SinceLastPayload: 10 * time.Millisecond, UpRate: 20 << 20}
	c.Observe(busy)
	// The caller waits for a response, which is what a request does.
	idle := busy
	idle.SinceLastPayload = 2 * time.Second
	c.Observe(idle)
	// Enough further requests to pass even the datacenter byte threshold.
	for i := 0; i < 60; i++ {
		busy.BytesUp += 1 << 20
		busy.Age += 2 * time.Second
		if got := c.Observe(busy); got == ClassBulk {
			t.Fatalf("request %d classified bulk at %d bytes after an observed idle",
				i, busy.BytesUp)
		}
	}
}

// The veto must not change the profile whose published results depend on the
// old behaviour: there, a slow bulk transfer is slow because the path is bad,
// and it still has to unlock lanes.
func TestIdleGapVetoDisabledLeavesBulkReachable(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.BulkIdleGapVeto != 0 {
		t.Fatalf("default profile enabled the veto: %v", cfg.BulkIdleGapVeto)
	}
	c := New(cfg)
	o := Observation{BytesUp: 300 * 1024, Age: 2 * time.Second,
		SinceLastPayload: 5 * time.Second}
	if got := c.Observe(o); got != ClassBulk {
		t.Fatalf("with the veto off, a long-idle large flow classified %v, want bulk", got)
	}
}

// A genuine bulk transfer never stops asking, so it must still be demoted on
// the datacenter profile. Otherwise that profile would have no bulk class at
// all rather than a correctly narrow one.
func TestSustainedTransferStillBecomesBulkOnADatacenterLeg(t *testing.T) {
	c := dcClassifier()
	o := Observation{Age: 11 * time.Second, SinceLastPayload: 5 * time.Millisecond,
		UpRate: 100 << 20}
	for i := 0; i < 60; i++ {
		o.BytesUp += 4 << 20
		o.Age += time.Second
		if c.Observe(o) == ClassBulk {
			return
		}
	}
	t.Fatalf("a continuously busy multi-hundred-megabyte transfer never became bulk")
}

// The veto is sticky. A flow seen idle must not become bulk later merely
// because a subsequent observation lands mid-burst -- which is the common case,
// since the scheduler ticks during bursts too.
func TestIdleVetoSurvivesLaterBusyObservations(t *testing.T) {
	c := dcClassifier()
	c.Observe(Observation{Age: 2 * time.Second, SinceLastPayload: 3 * time.Second})
	busy := Observation{BytesUp: 500 << 20, Age: 300 * time.Second,
		SinceLastPayload: time.Millisecond, UpRate: 100 << 20}
	for i := 0; i < 10; i++ {
		if got := c.Observe(busy); got == ClassBulk {
			t.Fatalf("observation %d after an idle re-enabled bulk", i)
		}
	}
}

// The datacenter profile's two classifier changes are checked here for whether
// they still change any outcome, because a knob that no longer does is a knob
// to delete rather than to document.
//
// Since whether a flow is made of small exchanges became a rate over a recent
// window rather than a lifetime total, a long conversation stays interactive on
// every profile and neither of these is what saves it. What they still do is
// narrower and worth keeping: the thresholds stop one multi-megabyte request
// being read as a transfer, and the veto stops many requests adding up to one.
func TestTheDatacenterClassifierChangesStillChangeSomething(t *testing.T) {
	dc := dcClassifierConfig()

	// A single large request, busy while observed, is bulk on the access-link
	// thresholds and not on the datacenter ones.
	single := Observation{BytesUp: 5 << 20, Age: 2 * time.Second,
		SinceLastPayload: 5 * time.Millisecond, UpRate: 20 << 20}
	if New(DefaultConfig()).Observe(single) != ClassBulk {
		t.Error("a 5MB burst is no longer bulk on the access-link profile; the thresholds have stopped differing")
	}
	if got := New(dc).Observe(single); got == ClassBulk {
		t.Errorf("a 5MB request classified %v on the datacenter profile", got)
	}

	// Many requests on one connection, past even the datacenter byte
	// threshold. Only the veto keeps these out of bulk.
	many := func(cfg Config) Class {
		c := New(cfg)
		busy := Observation{Age: 12 * time.Second, SinceLastPayload: 5 * time.Millisecond,
			UpRate: 40 << 20}
		idle := busy
		idle.SinceLastPayload = 2 * time.Second
		var last Class
		for i := 0; i < 40; i++ {
			busy.BytesUp += 1 << 20
			busy.Age += time.Second
			last = c.Observe(busy)
			idle.BytesUp, idle.Age = busy.BytesUp, busy.Age
			c.Observe(idle)
		}
		return last
	}
	noVeto := dc
	noVeto.BulkIdleGapVeto = 0
	if many(dc) == ClassBulk {
		t.Error("forty requests past the threshold went bulk with the veto on")
	}
	if many(noVeto) != ClassBulk {
		t.Error("the veto no longer changes anything at these thresholds; delete it rather than document it")
	}
}

func dcClassifierConfig() Config {
	c := DefaultConfig()
	c.BulkBytes = 32 << 20
	c.BulkMinimumAge = 10 * time.Second
	c.BulkIdleGapVeto = time.Second
	return c
}
