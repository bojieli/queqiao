package classifier

import (
	"testing"
	"time"
)

// bulkObservation is one observation of a transfer in progress: old enough,
// large enough, and a recent window over the small-exchange rate.
func bulkObservation(age time.Duration) Observation {
	return Observation{
		BytesUp: 64, BytesDown: 4 << 20, Age: age, Bidirectional: true,
		SmallBidirectionalBursts: false, SendBacklog: 1 << 20, CarriedPayload: true,
	}
}

// quietObservation is a small exchange after the transfer: payload, a small
// window, no backlog.
func quietObservation(age time.Duration) Observation {
	return Observation{
		BytesUp: 1 << 20, BytesDown: 4 << 20, Age: age, Bidirectional: true,
		SmallBidirectionalBursts: true, CarriedPayload: true,
	}
}

func TestLegacyCurrentIsTheStickyClass(t *testing.T) {
	c := New(DefaultConfig())
	c.Observe(bulkObservation(2 * time.Second))
	for i := 0; i < 20; i++ {
		if got := c.Observe(quietObservation(3*time.Second + time.Duration(i)*time.Second)); got != ClassBulk {
			t.Fatalf("legacy current class left BULK: %v", got)
		}
	}
	if c.Current() != c.Class() {
		t.Fatalf("legacy current %v differs from class %v", c.Current(), c.Class())
	}
}

func phasedConfig() Config {
	cfg := DefaultConfig()
	cfg.Phased = true
	return cfg
}

// The first entry into BULK is the legacy test, at the same instant.
func TestPhasedEntersBulkWhenLegacyDoes(t *testing.T) {
	legacy, phased := New(DefaultConfig()), New(phasedConfig())
	for age := 100 * time.Millisecond; age < 3*time.Second; age += 100 * time.Millisecond {
		o := bulkObservation(age)
		if legacy.Observe(o) != phased.Observe(o) {
			t.Fatalf("at age %v legacy %v and phased %v disagree", age, legacy.Current(), phased.Current())
		}
	}
	if phased.Current() != ClassBulk {
		t.Fatalf("phased flow is %v after a transfer, want bulk", phased.Current())
	}
}

// Leaving BULK takes payload-carrying windows with no evidence; the sticky
// class does not change.
func TestPhasedLeavesBulkAfterQuietPayloadWindows(t *testing.T) {
	c := New(phasedConfig())
	c.Observe(bulkObservation(2 * time.Second))
	age := 2 * time.Second
	for ; age < 3*time.Second; age += 150 * time.Millisecond {
		if got := c.Observe(quietObservation(age)); got != ClassBulk {
			t.Fatalf("left BULK after %v of quiet, before two windows", age-2*time.Second)
		}
	}
	for ; age < 6*time.Second && c.Current() == ClassBulk; age += 150 * time.Millisecond {
		c.Observe(quietObservation(age))
	}
	if c.Current() != ClassInteractive {
		t.Fatalf("still %v after %v of quiet payload windows", c.Current(), age-2*time.Second)
	}
	if c.Class() != ClassBulk {
		t.Fatalf("the sticky class changed to %v", c.Class())
	}
	if exits, _ := c.PhaseTransitions(); exits != 1 {
		t.Fatalf("exits %d, want 1", exits)
	}
}

// An idle gap carries no payload and is no evidence either way: a transfer
// whose source stalls is still BULK when it resumes.
func TestPhasedIdleGapDoesNotLeaveBulk(t *testing.T) {
	c := New(phasedConfig())
	c.Observe(bulkObservation(2 * time.Second))
	for age := 2 * time.Second; age < 30*time.Second; age += 250 * time.Millisecond {
		o := quietObservation(age)
		o.CarriedPayload = false
		if got := c.Observe(o); got != ClassBulk {
			t.Fatalf("an idle re-examination at %v made the flow %v", age, got)
		}
	}
}

// A backlogged transfer never leaves, however slowly its path drains: the
// recent window is small but the backlog is evidence.
func TestPhasedBackloggedSlowTransferStaysBulk(t *testing.T) {
	c := New(phasedConfig())
	c.Observe(bulkObservation(2 * time.Second))
	for age := 2 * time.Second; age < 60*time.Second; age += 100 * time.Millisecond {
		o := quietObservation(age)
		o.SendBacklog = 512 << 10
		if got := c.Observe(o); got != ClassBulk {
			t.Fatalf("a backlogged transfer became %v at %v", got, age)
		}
	}
}

// Re-entry needs a sustained backlog: a prompt that drains within a second
// stays an exchange; a transfer that does not is BULK again.
func TestPhasedReentersBulkOnlyOnASustainedBacklog(t *testing.T) {
	c := New(phasedConfig())
	c.Observe(bulkObservation(2 * time.Second))
	age := 2 * time.Second
	for ; c.Current() == ClassBulk; age += 100 * time.Millisecond {
		c.Observe(quietObservation(age))
	}
	// A 900 ms backlog, then drained.
	for end := age + 900*time.Millisecond; age < end; age += 100 * time.Millisecond {
		o := quietObservation(age)
		o.SendBacklog = 400 << 10
		if got := c.Observe(o); got != ClassInteractive {
			t.Fatalf("a %v backlog made the flow %v", age, got)
		}
	}
	c.Observe(quietObservation(age))
	// A long one.
	start := age
	for ; age < start+3*time.Second && c.Current() != ClassBulk; age += 100 * time.Millisecond {
		o := quietObservation(age)
		o.SendBacklog = 4 << 20
		c.Observe(o)
	}
	if c.Current() != ClassBulk {
		t.Fatal("a sustained backlog did not re-enter BULK")
	}
	if held := age - start; held < time.Second || held > 1200*time.Millisecond {
		t.Fatalf("re-entered after %v of backlog, want about one second", held)
	}
	// On a long path four round trips is the floor.
	c2 := New(phasedConfig())
	c2.Observe(bulkObservation(2 * time.Second))
	age = 2 * time.Second
	for ; c2.Current() == ClassBulk; age += 100 * time.Millisecond {
		c2.Observe(quietObservation(age))
	}
	start = age
	for ; age < start+5*time.Second && c2.Current() != ClassBulk; age += 100 * time.Millisecond {
		o := quietObservation(age)
		o.SendBacklog = 4 << 20
		o.SmoothedRTT = 600 * time.Millisecond
		c2.Observe(o)
	}
	if held := age - start; held < 2400*time.Millisecond {
		t.Fatalf("re-entered after %v on a 600 ms path, want at least four round trips", held)
	}
}

// A backlog that dipped below the threshold between two observations is two
// stretches, not one.
func TestPhasedBacklogThatDippedIsNotContinuous(t *testing.T) {
	c := New(phasedConfig())
	c.Observe(bulkObservation(2 * time.Second))
	age := 2 * time.Second
	for ; c.Current() == ClassBulk; age += 100 * time.Millisecond {
		c.Observe(quietObservation(age))
	}
	for end := age + 3*time.Second; age < end; age += 100 * time.Millisecond {
		o := quietObservation(age)
		o.SendBacklog = 80 << 10
		o.BacklogDrained = true
		if got := c.Observe(o); got != ClassInteractive {
			t.Fatalf("a backlog that drained between every observation re-entered BULK at %v", age)
		}
	}
}

// Two quiet writes either side of a window boundary are two windows but not
// two windows' worth of quiet.
func TestPhasedExitNeedsTheWholeQuietStretch(t *testing.T) {
	c := New(phasedConfig())
	c.Observe(bulkObservation(2 * time.Second))
	c.Observe(quietObservation(2999 * time.Millisecond))
	c.Observe(quietObservation(3001 * time.Millisecond))
	if got := c.Observe(quietObservation(4001 * time.Millisecond)); got != ClassBulk {
		t.Fatalf("left BULK after %v of quiet", 4001*time.Millisecond-2999*time.Millisecond)
	}
	if got := c.Observe(quietObservation(5 * time.Second)); got != ClassInteractive {
		t.Fatalf("still %v after two seconds of quiet payload windows", got)
	}
}
