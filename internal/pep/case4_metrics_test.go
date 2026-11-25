package pep

import (
	"testing"
	"time"

	"github.com/bojieli/queqiao/internal/metrics"
)

// policerQueueDelay needs a measured baseline. A newly installed controller
// has no minimum yet, even when QUIC already reports a handshake RTT (or its
// initial 100 ms estimate). Subtracting that unknown zero manufactures an
// entire RTT of queue. Match the controller's validity check without hiding
// real delay once both measurements exist.
func policerQueueDelay(smoothed, minimum time.Duration) (time.Duration, bool) {
	if smoothed <= 0 || minimum <= 0 {
		return 0, false
	}
	return max(0, smoothed-minimum), true
}

func TestPolicerQueueDelayRequiresMeasuredBaseline(t *testing.T) {
	for _, tc := range []struct {
		name              string
		smoothed, minimum time.Duration
		want              time.Duration
		valid             bool
	}{
		{name: "no observations"},
		{name: "initial RTT estimate", smoothed: 100 * time.Millisecond},
		{name: "handshake RTT before controller baseline", smoothed: 300425 * time.Microsecond},
		{name: "missing smoothed RTT", minimum: 300 * time.Millisecond},
		{name: "negative minimum", smoothed: 300 * time.Millisecond, minimum: -time.Millisecond},
		{name: "no queue", smoothed: 300 * time.Millisecond, minimum: 300 * time.Millisecond, valid: true},
		{name: "smoothed below minimum", smoothed: 299 * time.Millisecond, minimum: 300 * time.Millisecond, valid: true},
		{name: "at existing bound", smoothed: 350 * time.Millisecond, minimum: 300 * time.Millisecond, want: 50 * time.Millisecond, valid: true},
		{name: "over existing bound", smoothed: 351 * time.Millisecond, minimum: 300 * time.Millisecond, want: 51 * time.Millisecond, valid: true},
		{name: "full RTT of real delay", smoothed: 600 * time.Millisecond, minimum: 300 * time.Millisecond, want: 300 * time.Millisecond, valid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got, valid := policerQueueDelay(tc.smoothed, tc.minimum); got != tc.want || valid != tc.valid {
				t.Fatalf("queue delay (%v, %v) = (%v, %v), want (%v, %v)",
					tc.smoothed, tc.minimum, got, valid, tc.want, tc.valid)
			}
		})
	}
}

// sustained reports whether count of total samples is more than a quarter:
// what TestCase4APolicedPathIsStillUnbraked requires before a queue or a
// brake counts, so that one host stall is not read as the path (issue #187).
func sustained(count, total int) bool { return count > total/4 }

// brakeTally counts delay-brake readings from polls that had a live
// controller. QUICControllerSamples is cumulative, so it stays nonzero after
// the flow ends; a poll without a live controller reports a default zero
// brake, which is not a reading that the brake was off.
type brakeTally struct{ readings, braked int }

func (b *brakeTally) observe(s metrics.Snapshot) {
	if s.QUICControllerKind == "" {
		return
	}
	b.readings++
	if s.QUICDelayBrake > 0 {
		b.braked++
	}
}

func (b brakeTally) sustained() bool { return sustained(b.braked, b.readings) }

func TestBrakeTallyCountsOnlyLiveControllerReadings(t *testing.T) {
	live := func(brake float64) metrics.Snapshot {
		return metrics.Snapshot{QUICControllerKind: "test", QUICDelayBrake: brake, QUICControllerSamples: 10}
	}
	// After the flow: the cumulative sample count persists, the controller
	// does not.
	gone := metrics.Snapshot{QUICControllerSamples: 10}

	var braked brakeTally
	for range 3 {
		braked.observe(live(0.5))
	}
	for range 9 {
		braked.observe(gone)
	}
	if braked.readings != 3 || !braked.sustained() {
		t.Fatalf("three braked live readings among nine dead polls: %+v, sustained=%v; want 3 readings, sustained", braked, braked.sustained())
	}

	var spike brakeTally
	spike.observe(live(0.07))
	for range 11 {
		spike.observe(live(0))
	}
	if spike.sustained() {
		t.Fatalf("one braked reading in twelve counted as sustained: %+v", spike)
	}

	for _, tc := range []struct {
		count, total int
		want         bool
	}{{0, 12, false}, {1, 12, false}, {3, 12, false}, {4, 12, true}, {1, 3, true}, {0, 0, false}} {
		if got := sustained(tc.count, tc.total); got != tc.want {
			t.Errorf("sustained(%d, %d) = %v, want %v", tc.count, tc.total, got, tc.want)
		}
	}
}
