package pathsim

import (
	"fmt"
	"testing"
	"time"
)

func TestOfferPacerExpiresCatchupCredit(t *testing.T) {
	// At 4 Mbit/s, a 330 ms pause earns about 138 packet allowances.
	// Allow only eight, including on retries at the same clock reading.
	var p offerPacer
	const sent = 833
	if got := p.allowed(971, sent); got != sent+8 || p.expired != 130 {
		t.Fatalf("allowed=%d expired=%d, want %d and 130", got, p.expired, sent+8)
	}
	if got := p.allowed(971, sent+7); got != sent+8 || p.expired != 130 {
		t.Fatalf("partial-write retry allowed=%d expired=%d, want %d and 130", got, p.expired, sent+8)
	}
	if got := p.allowed(971, sent+8); got != sent+8 {
		t.Fatalf("same-time loop repaid expired credit: allowed=%d, sent=%d", got, sent+8)
	}
	if got := p.allowed(972, sent+8); got != sent+9 {
		t.Fatalf("new credit allowed=%d, want %d", got, sent+9)
	}
}

func TestOfferPacerPreservesRegularRate(t *testing.T) {
	for _, rate := range []float64{1, 4, 12, 50} {
		t.Run(fmt.Sprintf("offered%.0f", rate), func(t *testing.T) {
			var p offerPacer
			var sent uint64
			perSecond := rate * 1e6 / 8 / 1200
			for elapsed := time.Duration(0); elapsed < 4*time.Second; elapsed += time.Millisecond {
				earned := uint64(elapsed.Seconds() * perSecond)
				sent = p.allowed(earned, sent)
				if sent != earned || p.expired != 0 {
					t.Fatalf("at %v sent=%d earned=%d expired=%d", elapsed, sent, earned, p.expired)
				}
			}
		})
	}
}
