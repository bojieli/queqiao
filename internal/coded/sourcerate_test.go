package coded

import (
	"testing"
	"time"

	"github.com/bojieli/queqiao/internal/pathmodel"
)

// Under congestion=path a coded window is sized from the encoder's own
// source bytes per round trip (§9): a sparse coded lane beside a busy bulk
// connection must not size its window from the bulk's throughput. Until a round trip has been measured the cold-start prior
// stands, and repairs do not count.
func TestASparseCodedLaneSizesItsWindowFromItsOwnSourceRate(t *testing.T) {
	model := pathmodel.NewPathModel()
	model.Report(1, pathmodel.Observation{Erasure: 0.1, BurstFactor: 1, ObservedSamples: 5000, RoundTrip: 20 * time.Millisecond})
	for _, sourceRate := range []bool{false, true} {
		pa, pb := newPipes(1, 0)
		p := New(pa, Config{SymbolBytes: 1100, RoundTrip: 20 * time.Millisecond, Path: model, SourceRate: sourceRate})
		peer := New(pb, Config{SymbolBytes: 1100, RoundTrip: 20 * time.Millisecond})
		prior := float64(p.symbolBytes()) * 64 / p.roundTrip().Seconds()
		if got := p.rate(); got != prior {
			t.Fatalf("before any sending the rate is %.0f, want the cold-start prior %.0f", got, prior)
		}
		// A sparse lane: one small frame every few round trips.
		for range 6 {
			if err := p.Send([]byte("ping")); err != nil {
				t.Fatal(err)
			}
			time.Sleep(50 * time.Millisecond)
		}
		if sourceRate {
			got := p.rate()
			t.Logf("sparse lane sizes from %.0f B/s; the cold-start prior is %.0f B/s", got, prior)
			if got <= 0 || got > prior/10 {
				t.Fatalf("a sparse coded lane sizes its window from %.0f B/s against a cold-start prior of %.0f", got, prior)
			}
		}
		p.Close()
		peer.Close()
	}
}

// A burst followed by an idle gap of several round trips ends the
// measurement: the next decision sizes from the cold-start prior, not from
// the stale burst spread over the gap.
func TestTheSourceRateRestartsAfterAnIdleGap(t *testing.T) {
	var m sourceMeter
	rtt := 20 * time.Millisecond
	t0 := time.Now()
	_, _ = m.rate(t0, rtt)
	for i := range 20 {
		m.add(1000, t0.Add(time.Duration(i)*time.Millisecond))
	}
	if r, ok := m.rate(t0.Add(25*time.Millisecond), rtt); !ok || r <= 0 {
		t.Fatalf("no rate measured over a busy round trip: %.0f", r)
	}
	if r, ok := m.rate(t0.Add(time.Second), rtt); ok {
		t.Fatalf("after an idle second the rate is %.0f B/s; want none, so the prior stands", r)
	}
}

// A window without a source symbol, short of the idle reset, is a sparse
// measurement, not a cold meter: the lane keeps a sparse window.
func TestAnEmptyWindowIsASparseMeasurementNotAColdMeter(t *testing.T) {
	var m sourceMeter
	rtt := 20 * time.Millisecond
	t0 := time.Now()
	_, _ = m.rate(t0, rtt)
	m.add(100, t0)
	_, _ = m.rate(t0.Add(25*time.Millisecond), rtt) // rolls the window holding the symbol
	m.add(100, t0.Add(50*time.Millisecond))         // rolls an empty window first
	if _, ok := m.rate(t0.Add(51*time.Millisecond), rtt); !ok {
		t.Fatal("an empty window read as no measurement: the lane would size like a cold burst")
	}
}

// A burst followed by a pause of a few round trips -- short of the idle
// reset -- measures the empty round trip before the next symbol, not the
// burst spread over the pause, whether or not anything polled meanwhile.
func TestThePauseAfterABurstIsMeasuredAsEmpty(t *testing.T) {
	var m sourceMeter
	rtt := 20 * time.Millisecond
	t0 := time.Now()
	_, _ = m.rate(t0, rtt)
	for i := range 10 {
		m.add(10_000, t0.Add(time.Duration(i)*time.Millisecond))
	}
	m.add(100, t0.Add(55*time.Millisecond)) // after a pause of over two round trips
	if r, ok := m.rate(t0.Add(56*time.Millisecond), rtt); !ok || r != 0 {
		t.Fatalf("after a pause the rate is %.0f B/s (%v); want the empty round trip's", r, ok)
	}
}
