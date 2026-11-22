package congestion

import (
	quiccongestion "github.com/apernet/quic-go/congestion"
	"github.com/apernet/quic-go/monotime"
	"github.com/bojieli/queqiao/internal/pathmodel"
	"testing"
	"time"
)

// Hold the sibling observation fixed and capture exactly what the joining
// sender publishes. No network or subprocess is involved in this experiment.
type seedObservationProbe struct {
	pathmodel.State
	observation pathmodel.Observation
}

func (p *seedObservationProbe) Current() pathmodel.State { return p.State }
func (p *seedObservationProbe) Report(_ pathmodel.Member, o pathmodel.Observation) pathmodel.State {
	p.observation = o
	return p.State
}

func BenchmarkJoiningLaneSeed(b *testing.B) {
	for _, tc := range []struct {
		name string
		loss float64
	}{{"clean", 0}, {"loss42", 0.42}, {"arrivalFloor", 0.85}} {
		for _, cap := range []struct {
			name  string
			share float64
		}{{"uncapped", 0}, {"shared", 1.25e6}} {
			b.Run(tc.name+"/"+cap.name, func(b *testing.B) {
				probe := &seedObservationProbe{State: pathmodel.State{Seed: 1e6, Share: cap.share, Erasure: tc.loss, RoundTrip: 200 * time.Millisecond}}
				var pacing, estimate, reported float64
				for b.Loop() {
					sender := NewErasureSenderOn(1200, probe)
					pacing = float64(sender.bandwidth())
					estimate = float64(sender.inner.estimator.estimate())
					sender.OnCongestionEventEx(0, monotime.Now(), nil, []quiccongestion.LostPacketInfo{{PacketNumber: 1, BytesLost: 1200}})
					reported = probe.observation.Delivered
				}
				b.ReportMetric(estimate/probe.Seed, "filter/seed")
				b.ReportMetric(pacing/probe.Seed, "wire/seed")
				b.ReportMetric(reported/probe.Seed, "report/seed")
			})
		}
	}
}
