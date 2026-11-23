package congestion

import (
	"math"
	"testing"
	"time"

	quiccongestion "github.com/apernet/quic-go/congestion"
	"github.com/apernet/quic-go/monotime"
	"github.com/bojieli/queqiao/internal/pathmodel"
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

const joiningRoundTrip = 200 * time.Millisecond

// lossEvent hands the sender one lost packet, which is enough to make it
// report to its model and, once a minimum round trip exists, run the
// erasure compensation gate.
func lossEvent(e *ErasureSender) {
	e.OnCongestionEventEx(0, monotime.Now(), nil, []quiccongestion.LostPacketInfo{{PacketNumber: 1, BytesLost: 1200}})
}

// passGate gives the sender its own minimum round trip and then one loss
// event. Compensation waits for that minimum (see OnCongestionEventEx), so a
// measurement taken before it describes a sender the first loss is about to
// change.
func passGate(e *ErasureSender) {
	e.inner.minRTT = joiningRoundTrip
	lossEvent(e)
}

// ackEvent makes the sender report without adding a loss to the pooled
// erasure, for a test that wants the path to stay clean.
func ackEvent(e *ErasureSender) {
	e.OnCongestionEventEx(0, monotime.Now(), []quiccongestion.AckedPacketInfo{{PacketNumber: 1, BytesAcked: 1200}}, nil)
}

func BenchmarkJoiningLaneSeed(b *testing.B) {
	// earned is the compensation the siblings were allowed: on an erasure
	// channel the arrival rate, on a policer none at all.
	for _, tc := range []struct {
		name         string
		loss, earned float64
	}{{"clean", 0, 1}, {"loss42", 0.42, 0.58}, {"arrivalFloor", 0.85, erasureMinArrival}, {"policed42", 0.42, 1}} {
		for _, cap := range []struct {
			name  string
			share float64
		}{{"uncapped", 0}, {"shared", 1.25e6}} {
			b.Run(tc.name+"/"+cap.name, func(b *testing.B) {
				probe := &seedObservationProbe{State: pathmodel.State{Seed: 1e6, Share: cap.share, Erasure: tc.loss, Arrival: tc.earned, RoundTrip: joiningRoundTrip}}
				var pacing, estimate, reported, gated float64
				for b.Loop() {
					sender := NewErasureSenderOn(1200, probe)
					pacing = float64(sender.bandwidth())
					estimate = float64(sender.inner.estimator.estimate())
					lossEvent(sender)
					reported = probe.observation.Delivered
					passGate(sender)
					gated = float64(sender.bandwidth())
				}
				b.ReportMetric(estimate/probe.Seed, "filter/seed")
				b.ReportMetric(pacing/probe.Seed, "wire/seed")
				b.ReportMetric(gated/probe.Seed, "gated-wire/seed")
				b.ReportMetric(reported/probe.Seed, "report/seed")
			})
		}
	}
}

// The pooled seed is a delivered rate, the inner filter is a delivered rate,
// and the outer pacer divides by the arrival rate once. So the filter takes
// the seed as it is, and the wire carries gain/arrival times it -- before the
// compensation gate and after it. A joining lane that loses its inherited
// compensation at the first gated loss paces at its share times the gain,
// delivers arrival times that, and on the floor gets a fraction of its share.
func TestJoiningSeedIsCompensatedOnceAndOnlyMeasuredDeliveryIsReported(t *testing.T) {
	for _, loss := range []float64{0, 0.42, 0.85, 0.99} {
		arrival := math.Max(1-loss, erasureMinArrival)
		// An erasure channel: the siblings earned the whole compensation.
		probe := &seedObservationProbe{State: pathmodel.State{Seed: 1e6, Erasure: loss, Arrival: arrival, RoundTrip: joiningRoundTrip}}
		sender := NewErasureSenderOn(1200, probe)
		if got := float64(sender.inner.estimator.estimate()); math.Abs(got-probe.Seed) > 1 {
			t.Errorf("loss %.2f: seeded delivery %.0f, want the seed %.0f", loss, got, probe.Seed)
		}
		wantPacing := probe.Seed * sender.inner.highGain / arrival
		if got := float64(sender.bandwidth()); math.Abs(got-wantPacing) > wantPacing*1e-4 {
			t.Errorf("loss %.2f: initial pacing %.0f, want %.0f", loss, got, wantPacing)
		}
		lossEvent(sender)
		if got := probe.observation.Delivered; got != 0 {
			t.Errorf("loss %.2f: a lane that has delivered nothing reported %.0f; that is its seed echoed back", loss, got)
		}
		passGate(sender)
		if got := sender.arrivalRate(); math.Abs(got-arrival) > 1e-6 {
			t.Errorf("loss %.2f: the first gated loss moved the inherited arrival %.3f to %.3f", loss, arrival, got)
		}
		// The floor this guards: a joining lane on an erasure path must put at
		// least its delivered share over the arrival rate on the wire.
		if got := float64(sender.bandwidth()); got < probe.Seed/arrival {
			t.Errorf("loss %.2f: after the gate the joining lane paces %.0f (%.3fx seed), below its share over arrival %.0f",
				loss, got, got/probe.Seed, probe.Seed/arrival)
		}
		if got := float64(sender.bandwidth()); math.Abs(got-wantPacing) > wantPacing*1e-4 {
			t.Errorf("loss %.2f: post-gate pacing %.0f, want %.0f", loss, got, wantPacing)
		}
		if got := probe.observation.Arrival; math.Abs(got-arrival) > 1e-6 {
			t.Errorf("loss %.2f: reported earned arrival %.3f, want %.3f", loss, got, arrival)
		}
	}
}

// The same loss on a policer is not a compensation to inherit. Its siblings
// were refused it because sending more delivered nothing, and a joiner that
// read 1/arrival off the raw loss would overdrive the path its siblings are
// holding back on.
func TestAJoiningLaneDoesNotInheritCompensationItsSiblingsWereRefused(t *testing.T) {
	probe := &seedObservationProbe{State: pathmodel.State{Seed: 1e6, Erasure: 0.42, Arrival: 1, RoundTrip: joiningRoundTrip}}
	sender := NewErasureSenderOn(1200, probe)
	want := probe.Seed * sender.inner.highGain
	if got := float64(sender.bandwidth()); math.Abs(got-want) > want*1e-4 {
		t.Errorf("before the gate the joiner paces %.0f, want the uncompensated %.0f", got, want)
	}
	passGate(sender)
	if got := float64(sender.bandwidth()); math.Abs(got-want) > want*1e-4 {
		t.Errorf("after the gate the joiner paces %.0f, want the uncompensated %.0f", got, want)
	}
}

// End to end through a real model: a sibling on a 42% erasure channel that has
// earned its compensation, and a lane joining it after the gate has run.
func TestAJoiningLaneOnAnErasurePathPacesItsShareOverArrival(t *testing.T) {
	const sibling = pathmodel.Member(1 << 62)
	model := pathmodel.NewPathModel()
	model.Report(sibling, pathmodel.Observation{Erasure: 0.42, BurstFactor: 1, ObservedSamples: 5000, Delivered: 2e6, RoundTrip: joiningRoundTrip, Arrival: 0.58})
	seed := model.Current().Seed
	sender := NewErasureSenderOn(1200, model)
	passGate(sender)
	if got, floor := float64(sender.bandwidth()), seed/0.58; got < floor {
		t.Fatalf("after the gate the joiner paces %.0f (%.3fx its %.0f seed), below its share over arrival %.0f",
			got, got/seed, seed, floor)
	}
}

// A lane that has measured nothing must not report its seed as delivery. The
// seed came from the pool, so reporting it adds the pool to itself: with one
// lane established at B, every join inside the bottleneck window would add
// another seed to the summed delivery, and four joins took a clean path's
// pooled bottleneck from 1.5B to 3.0B.
func TestJoiningLanesDoNotEchoTheirSeedIntoThePool(t *testing.T) {
	const established = 2e6
	// Far from the identities NewErasureSenderOn allocates.
	const sibling = pathmodel.Member(1 << 62)
	model := pathmodel.NewPathModel()
	report := func() {
		model.Report(sibling, pathmodel.Observation{BurstFactor: 1, ObservedSamples: 5000, Delivered: established, RoundTrip: joiningRoundTrip})
	}
	report()
	bottleneck := func() float64 {
		state := model.Current()
		return state.Seed * float64(model.Members()+1)
	}
	for join := 1; join <= 4; join++ {
		sender := NewErasureSenderOn(1200, model)
		if seed := float64(sender.inner.estimator.estimate()); seed <= 0 {
			t.Fatalf("join %d was not seeded", join)
		}
		ackEvent(sender)
		sender.inner.minRTT = joiningRoundTrip
		ackEvent(sender)
		report()
		if got := bottleneck(); math.Abs(got-established) > established*0.01 {
			t.Errorf("after join %d the pooled bottleneck is %.0f (%.2fx) on a path delivering %.0f",
				join, got, got/established, established)
		}
	}
}

// Once a joining lane has its own delivery samples, those are what it
// reports, whatever its seed was.
func TestAJoiningLaneReportsItsOwnDeliveryOnceMeasured(t *testing.T) {
	probe := &seedObservationProbe{State: pathmodel.State{Seed: 1e6, RoundTrip: joiningRoundTrip}}
	sender := NewErasureSenderOn(1200, probe)
	const measured = 300_000
	sender.inner.estimator.measured.updateMax(1, monotime.Now(), measured)
	lossEvent(sender)
	if got := probe.observation.Delivered; got != measured {
		t.Fatalf("reported %.0f, want the measured %d rather than the seed", got, measured)
	}
}
