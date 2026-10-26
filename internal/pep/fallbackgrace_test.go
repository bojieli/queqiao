package pep

import (
	"testing"
	"time"
)

type measuredLane struct {
	bufferConn
	rtt time.Duration
}

func (*measuredLane) SetDeadline(time.Time) error { return nil }
func (l *measuredLane) transportStats() laneTransportStats {
	return laneTransportStats{smoothedRTT: l.rtt}
}

type unmeasuredLane struct{ bufferConn }

func (*unmeasuredLane) SetDeadline(time.Time) error { return nil }

// The adaptive grace scales a round trip. Feeding it how long a dial took --
// a handshake and an authentication, several round trips -- put it at its
// thirty second ceiling after any cold dial, which is how long a request then
// waited for QUIC with a TCP lane already up.
func TestTheFallbackGraceScalesTheMeasuredRoundTrip(t *testing.T) {
	c := &Client{cfg: ClientConfig{
		FallbackGrace: 2 * time.Second, FallbackGraceRTTFactor: 16, FallbackGraceMax: 30 * time.Second,
	}}
	c.observeLaneRTT(&measuredLane{rtt: 200 * time.Millisecond})
	if got := c.fallbackGrace(); got != 3200*time.Millisecond {
		t.Fatalf("a 200ms path got a %v grace, want 16 round trips (3.2s)", got)
	}
	// A lane with no measurement leaves the last one standing rather than
	// writing a duration that is not a round trip.
	c.observeLaneRTT(&unmeasuredLane{})
	if got := c.fallbackGrace(); got != 3200*time.Millisecond {
		t.Fatalf("an unmeasured lane moved the grace to %v", got)
	}
	// A fast path keeps the floor.
	c.observeLaneRTT(&measuredLane{rtt: 20 * time.Millisecond})
	if got := c.fallbackGrace(); got != 2*time.Second {
		t.Fatalf("a 20ms path got a %v grace, want the 2s floor", got)
	}
}
