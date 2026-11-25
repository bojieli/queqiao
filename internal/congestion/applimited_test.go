package congestion

import (
	"testing"
	"time"

	quiccongestion "github.com/apernet/quic-go/congestion"
	"github.com/apernet/quic-go/monotime"

	"github.com/bojieli/queqiao/internal/pathmodel"
)

// closeLimitRound plays one round of sends at inFlight, with or without a
// refused poll ahead of them, and closes it.
func closeLimitRound(e *ErasureSender, inFlight quiccongestion.ByteCount, held bool) {
	now := monotime.Now()
	if held {
		// Refused, and released now: the send below is the one it held.
		e.denied, e.releaseAt = true, now
	}
	e.OnPacketSent(now, inFlight, quiccongestion.PacketNumber(e.inner.round+1), 1200, true)
	e.inner.round++
	e.judgeAppLimited()
}

// A lane whose demand is a trickle is application-limited after
// appLimitedRounds rounds, even when the trickle is held by the pacer -- a
// share below its demand makes it so -- and the pool is told.
func TestATrickleIsApplicationLimitedEvenWhenHeld(t *testing.T) {
	e := NewErasureSenderOn(1200, pathmodel.NewPathModel())
	e.inner.minRTT = 200 * time.Millisecond
	for i := 0; i < appLimitedRounds; i++ {
		if e.appLimited {
			t.Fatalf("application-limited after %d rounds, want %d", i, appLimitedRounds)
		}
		closeLimitRound(e, 1200, true)
	}
	if !e.appLimited {
		t.Fatalf("a lane with 1200 bytes in flight against an uncapped allowance of %.0f is not application-limited", e.allowanceForTest())
	}
	if !e.Telemetry().AppLimited {
		t.Fatal("the verdict is not in the telemetry")
	}
}

// A lane that fills its allowance and is held is send-limited, and one such
// round ends the verdict; filling it without a refused poll is not enough.
func TestAFullHeldRoundIsSendLimited(t *testing.T) {
	e := NewErasureSenderOn(1200, pathmodel.NewPathModel())
	e.inner.minRTT = 200 * time.Millisecond
	for i := 0; i < appLimitedRounds; i++ {
		closeLimitRound(e, 1200, false)
	}
	if !e.appLimited {
		t.Fatal("setup: the lane should be application-limited")
	}
	full := quiccongestion.ByteCount(e.allowanceForTest())
	closeLimitRound(e, full, false)
	closeLimitRound(e, full, false)
	closeLimitRound(e, full, false)
	if !e.appLimited {
		t.Fatal("rounds no poll held back ended the verdict")
	}
	closeLimitRound(e, full, true)
	if e.appLimited {
		t.Fatal("a held round with its whole allowance in flight left the lane application-limited")
	}
}

func (e *ErasureSender) allowanceForTest() float64 {
	allowed := float64(e.GetCongestionWindow())
	if rate := float64(e.inner.bandwidth()) / e.arrivalRate() * e.inner.minRoundTrip().Seconds(); rate < allowed {
		allowed = rate
	}
	return allowed
}

// A refusal after the last packet of a request is not evidence against the
// next request, which finds the budget waiting for it.
func TestARefusalBeforeAnIdleGapHoldsNothingBack(t *testing.T) {
	e := NewErasureSender(1200)
	e.SetRTTStatsProvider(&fixedRTT{min: 200 * time.Millisecond, smoothed: 200 * time.Millisecond})
	now := base()
	var number quiccongestion.PacketNumber
	for e.HasPacingBudget(now) {
		number++
		e.OnPacketSent(now, 1200, number, 1200, true)
	}
	// Refused; the application then has nothing to send for a second.
	if e.HasPacingBudget(now) {
		t.Fatal("setup: the budget should be spent")
	}
	number++
	e.OnPacketSent(now.Add(time.Second), 1200, number, 1200, true)
	if got := e.Telemetry().HeldSends; got != 0 {
		t.Fatalf("a send a second after the refusal counted as held (%d)", got)
	}
	// Refused again, and this time the send waits for the pacer's release.
	later := now.Add(time.Second)
	for e.HasPacingBudget(later) {
		number++
		e.OnPacketSent(later, 1200, number, 1200, true)
	}
	release := e.pacer.timeUntilSend()
	number++
	e.OnPacketSent(release, 1200, number, 1200, true)
	if got := e.Telemetry().HeldSends; got != 1 {
		t.Fatalf("the send at the pacer's release counted %d held, want 1", got)
	}
}
