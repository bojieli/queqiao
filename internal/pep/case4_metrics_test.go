package pep

import (
	"testing"
	"time"
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
