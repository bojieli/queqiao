package congestion

import (
	"testing"
	"time"

	"github.com/apernet/quic-go/monotime"
)

// The clock the path law measures in resolves well under a millisecond on
// every system. On Windows that takes the performance counter (clock.go):
// Go's own clock there advances once per clock interrupt, and at that
// granularity the path controller starved on a short path.
func TestTheQUICClockResolvesUnderAMillisecond(t *testing.T) {
	smallest := time.Duration(1<<63 - 1)
	last := monotime.Now()
	// The wall clock ends the loop whatever granularity it has.
	for end := time.Now().Add(100 * time.Millisecond); time.Now().Before(end); {
		now := monotime.Now()
		if d := now.Sub(last); d > 0 {
			smallest = min(smallest, d)
			last = now
		}
	}
	if smallest > 100*time.Microsecond {
		t.Fatalf("the QUIC clock advances in steps of %v; the path law needs it finer than a short path's round trip", smallest)
	}
}
