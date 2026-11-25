package coded

import (
	"testing"
	"time"

	"github.com/bojieli/queqiao/internal/pathmodel"
)

// Under congestion=path a coded window is sized from the encoder's own
// source bytes per round trip (§9), not from the path model's share, which
// there describes a hypothetical joining member: a sparse coded lane beside
// a busy bulk connection must not size its window from the bulk's
// throughput. Until a round trip has been measured the cold-start prior
// stands, and repairs do not count.
func TestASparseCodedLaneSizesItsWindowFromItsOwnSourceRate(t *testing.T) {
	model := pathmodel.NewPathModel()
	model.Report(1, pathmodel.Observation{Erasure: 0.1, BurstFactor: 1, ObservedSamples: 5000, Delivered: 20e6, RoundTrip: 20 * time.Millisecond})
	share := model.Current().Share
	if share <= 0 {
		t.Fatal("the model offers no share to compare against")
	}
	for _, sourceRate := range []bool{false, true} {
		pa, pb := newPipes(1, 0)
		p := New(pa, Config{SymbolBytes: 1100, RoundTrip: 20 * time.Millisecond, Path: model, SourceRate: sourceRate})
		peer := New(pb, Config{SymbolBytes: 1100, RoundTrip: 20 * time.Millisecond})
		prior := float64(p.symbolBytes()) * 64 / p.roundTrip().Seconds()
		if !sourceRate {
			if got := p.rate(); got != share {
				t.Fatalf("connection scope sizes from %.0f, want the share %.0f", got, share)
			}
		} else if got := p.rate(); got != prior {
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
			t.Logf("sparse lane sizes from %.0f B/s; the share is %.0f B/s", got, share)
			if got <= 0 || got > share/100 {
				t.Fatalf("a sparse coded lane sizes its window from %.0f B/s against a share of %.0f", got, share)
			}
		}
		p.Close()
		peer.Close()
	}
}
