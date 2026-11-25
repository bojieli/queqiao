package congestion

import (
	"math/rand/v2"
	"net"
	"testing"
	"time"

	quiccongestion "github.com/apernet/quic-go/congestion"
	"github.com/apernet/quic-go/monotime"
)

// drainBudget spends the initial burst with ack-eliciting packets and waits
// for one and a half datagrams' worth of budget at the sender's rate.
func drainBudget(t *testing.T, sender *ErasureSender, sent func(now monotime.Time, number quiccongestion.PacketNumber)) (monotime.Time, quiccongestion.PacketNumber) {
	t.Helper()
	now := base()
	var number quiccongestion.PacketNumber
	for sender.HasPacingBudget(now) {
		number++
		sent(now, number)
	}
	rate := float64(sender.bandwidth())
	now = now.Add(time.Duration(1.5 * 1200 / rate * float64(time.Second)))
	if !sender.HasPacingBudget(now) {
		t.Fatalf("no budget for a datagram after waiting 1.5 datagrams at %.0f B/s", rate)
	}
	return now, number
}

// The ACK-throttle mechanism. A connection that mostly receives sends many
// ACK-only packets. Through the per-packet callbacks each is charged to the
// pacer, although the delivery sampler credits it nothing, so they spend the
// budget the connection's next stream frame -- a flow acknowledgement --
// needs. Through cc-api v2 they are registered and not debited.
func TestReverseAckOnlyTrafficDoesNotDrainThePacerUnderCCAPIV2(t *testing.T) {
	const ackOnly, ackOnlySize = 60, 60
	rtt := &fixedRTT{min: 200 * time.Millisecond, smoothed: 200 * time.Millisecond}

	t.Run("v1 (main): ACK-only packets drain the budget", func(t *testing.T) {
		sender := NewErasureSender(1200)
		sender.SetRTTStatsProvider(rtt)
		now, number := drainBudget(t, sender, func(now monotime.Time, n quiccongestion.PacketNumber) {
			sender.OnPacketSent(now, 1200, n, 1200, true)
		})
		for i := 0; i < ackOnly; i++ {
			number++
			sender.OnPacketSent(now, 1200, number, ackOnlySize, false)
		}
		if sender.HasPacingBudget(now) {
			t.Fatal("the per-packet callbacks no longer charge ACK-only packets; v1 must stay as main")
		}
		tel := sender.Telemetry()
		if tel.NonElicitingBytes != ackOnly*ackOnlySize || tel.NonElicitingDebited != tel.NonElicitingBytes {
			t.Fatalf("non-eliciting %d, debited %d; want both %d", tel.NonElicitingBytes, tel.NonElicitingDebited, ackOnly*ackOnlySize)
		}
	})

	t.Run("v2: ACK-only packets are not debited", func(t *testing.T) {
		sender := NewErasureSender(1200)
		attached := NewErasurePathSender(sender, nil)
		attached.SetRTTStatsProvider(rtt)
		attached.OnAttach(base(), 0, 1200, func() {})
		now, number := drainBudget(t, sender, func(now monotime.Time, n quiccongestion.PacketNumber) {
			attached.OnSent(quiccongestion.SentPacket{Space: quiccongestion.SpaceApplication, Number: n, Bytes: 1200,
				AckEliciting: true, InFlightCounted: true, Kind: quiccongestion.KindOrdinary, InFlight: 1200, Time: now})
		})
		for i := 0; i < ackOnly; i++ {
			number++
			attached.OnSent(quiccongestion.SentPacket{Space: quiccongestion.SpaceApplication, Number: number, Bytes: ackOnlySize,
				Kind: quiccongestion.KindAckOnly, Exempt: true, InFlight: 1200, Time: now})
		}
		if !attached.Admit(now, 1200).OK {
			t.Fatal("ACK-only packets spent the pacing budget the next stream frame needs")
		}
		tel := attached.Telemetry()
		if tel.NonElicitingBytes != ackOnly*ackOnlySize || tel.NonElicitingDebited != 0 {
			t.Fatalf("non-eliciting %d, debited %d; want %d and 0", tel.NonElicitingBytes, tel.NonElicitingDebited, ackOnly*ackOnlySize)
		}
	})
}

// Everything else reaches the control law exactly as the per-packet callbacks
// delivered it: drive two senders with one trace of ack-eliciting sends,
// acknowledgements, losses and discards, one through each API, and they agree
// at every step on window, pacing and state.
func TestErasurePathSenderDrivesTheSenderLikeTheCallbacks(t *testing.T) {
	rtt := &fixedRTT{min: 80 * time.Millisecond, smoothed: 90 * time.Millisecond}
	legacy := NewErasureSender(1200)
	legacy.SetRTTStatsProvider(rtt)
	attachedSender := NewErasureSender(1200)
	attached := NewErasurePathSender(attachedSender, nil)
	attached.SetRTTStatsProvider(rtt)
	attached.OnAttach(base(), 0, 1200, func() {})

	rng := rand.New(rand.NewPCG(1, 2))
	type outstanding struct {
		number quiccongestion.PacketNumber
		bytes  quiccongestion.ByteCount
	}
	var flight []outstanding
	var inFlight quiccongestion.ByteCount
	var number quiccongestion.PacketNumber
	now := base()
	compare := func(step int) {
		t.Helper()
		lw, aw := legacy.CanSend(inFlight), attached.Admit(now, 1200)
		if lw != (aw.OK || !aw.RetryAt.IsZero()) {
			t.Fatalf("step %d: window disagrees: legacy %v, attached %+v", step, lw, aw)
		}
		if lw {
			lp := legacy.HasPacingBudget(now)
			if lp != aw.OK {
				t.Fatalf("step %d: pacing disagrees: legacy %v, attached %+v", step, lp, aw)
			}
			if !lp && legacy.TimeUntilSend(inFlight) != aw.RetryAt {
				t.Fatalf("step %d: retry time disagrees", step)
			}
		}
		lt, at := legacy.Telemetry(), attached.Telemetry()
		if lt.PacingRate != at.PacingRate || lt.CongestionWindow != at.CongestionWindow || lt.MaxBandwidth != at.MaxBandwidth || lt.Mode != at.Mode || lt.PacketsLost != at.PacketsLost {
			t.Fatalf("step %d: state disagrees:\nlegacy   %+v\nattached %+v", step, lt, at)
		}
	}
	for step := 0; step < 4000; step++ {
		now = now.Add(time.Duration(rng.IntN(3000)) * time.Microsecond)
		switch r := rng.IntN(10); {
		case r < 6 && legacy.CanSend(inFlight) && legacy.HasPacingBudget(now):
			number++
			size := quiccongestion.ByteCount(200 + rng.IntN(1000))
			inFlight += size
			flight = append(flight, outstanding{number, size})
			legacy.OnPacketSent(now, inFlight, number, size, true)
			attached.OnSent(quiccongestion.SentPacket{Space: quiccongestion.SpaceApplication, Number: number, Bytes: size,
				AckEliciting: true, InFlightCounted: true, Kind: quiccongestion.KindOrdinary, InFlight: inFlight, Time: now})
		case r < 9 && len(flight) > 0:
			prior := inFlight
			var acked []quiccongestion.AckedPacketInfo
			var lost []quiccongestion.LostPacketInfo
			var ackedRefs, lostRefs []quiccongestion.PacketRef
			n := 1 + rng.IntN(min(len(flight), 8))
			for _, p := range flight[:n] {
				inFlight -= p.bytes
				if rng.IntN(10) < 2 {
					lost = append(lost, quiccongestion.LostPacketInfo{PacketNumber: p.number, BytesLost: p.bytes})
					lostRefs = append(lostRefs, quiccongestion.PacketRef{Space: quiccongestion.SpaceApplication, Number: p.number, Bytes: p.bytes})
				} else {
					acked = append(acked, quiccongestion.AckedPacketInfo{PacketNumber: p.number, BytesAcked: p.bytes})
					ackedRefs = append(ackedRefs, quiccongestion.PacketRef{Space: quiccongestion.SpaceApplication, Number: p.number, Bytes: p.bytes})
				}
			}
			flight = flight[n:]
			legacy.OnCongestionEventEx(prior, now, acked, lost)
			attached.OnAckEvent(quiccongestion.AckEvent{Time: now, InFlight: inFlight, Acked: ackedRefs, Lost: lostRefs})
		case len(flight) > 0:
			// A PTO probe declaration: the flight shrinks silently for the
			// callbacks, and is reported as a discard to the PathSender.
			inFlight -= flight[0].bytes
			attached.OnDiscarded(now, inFlight, []quiccongestion.PacketRef{{Space: quiccongestion.SpaceApplication, Number: flight[0].number, Bytes: flight[0].bytes}}, false, quiccongestion.DiscardPTOProbe)
			flight = flight[1:]
		}
		compare(step)
	}
	if legacy.Telemetry().Samples == 0 {
		t.Fatal("the trace produced no delivery samples")
	}
}

// A migration keeps the controller and everything it measured, unless the
// path itself changed.
func TestErasurePathSenderIsKeptAcrossMigration(t *testing.T) {
	rtt := &fixedRTT{min: 80 * time.Millisecond, smoothed: 90 * time.Millisecond}
	first := NewErasureSender(1200)
	var asked int
	var next *ErasureSender
	attached := NewErasurePathSender(first, func(oldLocal, newLocal, oldRemote, newRemote net.Addr) *ErasureSender {
		asked++
		return next
	})
	attached.SetRTTStatsProvider(rtt)
	attached.SetMaxDatagramSize(1350)
	a := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1000}
	b := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 2000}
	attached.OnPathMigrated(base(), a, a, a, b)
	if attached.Sender() != first || asked != 1 {
		t.Fatal("a migration on the same path replaced the controller")
	}
	next = NewErasureSender(1200)
	attached.OnPathMigrated(base(), a, a, a, &net.UDPAddr{IP: net.IPv4(198, 51, 100, 1), Port: 443})
	if attached.Sender() != next {
		t.Fatal("a migration to another path kept the old path's controller")
	}
	if next.inner.maxDatagramSize != 1350 || next.rttStats == nil {
		t.Fatal("the replacement was not configured like the controller it replaced")
	}
}
