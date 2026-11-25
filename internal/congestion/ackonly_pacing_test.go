package congestion

import (
	"testing"
	"time"

	quiccongestion "github.com/apernet/quic-go/congestion"
)

// QUIC's ACK-only packets are sent past the pacer and are not in flight, so
// they must not spend its budget. A lane that mostly receives sends many of
// them, and charged for them it has nothing left for the stream frame -- a
// flow acknowledgement -- that it does have to pace.
func TestAckOnlyPacketsDoNotSpendThePacingBudget(t *testing.T) {
	sender := NewErasureSender(1200)
	sender.SetRTTStatsProvider(&fixedRTT{min: 200 * time.Millisecond, smoothed: 200 * time.Millisecond})
	now := base()
	// Drain the initial burst with ack-eliciting packets, then wait for one
	// datagram's budget at the current rate.
	var number quiccongestion.PacketNumber
	for sender.HasPacingBudget(now) {
		number++
		sender.OnPacketSent(now, 1200, number, 1200, true)
	}
	rate := float64(sender.bandwidth())
	now = now.Add(time.Duration(1.5 * 1200 / rate * float64(time.Second)))
	if !sender.HasPacingBudget(now) {
		t.Fatalf("no budget for a datagram after waiting 1.5 datagrams at %.0f B/s", rate)
	}
	// A receive-heavy connection acknowledges what it receives in the
	// meantime: several ACK-only packets, none of them in flight.
	for i := 0; i < 60; i++ {
		number++
		sender.OnPacketSent(now, 1200, number, 60, false)
	}
	if !sender.HasPacingBudget(now) {
		t.Fatal("ACK-only packets spent the pacing budget the next stream frame needs")
	}
	if got := sender.Telemetry().AckOnlyBytes; got != 60*60 {
		t.Fatalf("AckOnlyBytes = %d, want %d", got, 60*60)
	}
}
