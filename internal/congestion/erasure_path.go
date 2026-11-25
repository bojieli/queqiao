package congestion

import (
	"net"
	"sync/atomic"

	quiccongestion "github.com/apernet/quic-go/congestion"
	"github.com/apernet/quic-go/monotime"
)

// ErasurePathSender drives one connection's ErasureSender through the fork's
// PathSender API (cc-api v2). It is stage 1 of the path-authority migration:
// the control law, the pacer and the window are today's, one per connection,
// and the events that reach them are the same ones the per-packet callbacks
// delivered. Three things differ, and only these:
//
//   - A packet that elicits no acknowledgement is not debited from the pacer.
//     The delivery sampler never credits such a packet, so a rate limit
//     charged for it limits traffic by an estimate that was not measured on
//     it. On a connection that mostly receives, the reverse ACK-only packets
//     drained the budget its own stream frames (flow acknowledgements) needed.
//   - The controller is kept across a connection migration. Without a
//     PathSender quic-go replaces it with Reno. It is replaced with a fresh
//     ErasureSender only when the migration changed the path itself, which is
//     what rekey decides.
//   - Exempt obligations (ACK-only packets, PTO probes, liveness PINGs, path
//     validation) have a build path while Admit refuses. That is the fork's
//     doing; this type only admits.
//
// All PathSender calls arrive on the connection's run loop. Telemetry may be
// called from any goroutine.
type ErasurePathSender struct {
	sender atomic.Pointer[ErasureSender]

	// gauge is the connection's bytes in flight as the last event reported it.
	// Every change of the connection's flight is reported, so it is also the
	// flight before the next event: the prior flight the per-packet callbacks
	// carried.
	gauge quiccongestion.ByteCount

	rekey   func(oldLocal, newLocal, oldRemote, newRemote net.Addr) *ErasureSender
	rtt     quiccongestion.RTTStatsProvider
	maxSize quiccongestion.ByteCount
	wake    func()
	demand  quiccongestion.Demand

	acked    []quiccongestion.AckedPacketInfo
	lost     []quiccongestion.LostPacketInfo
	attached bool
}

var (
	_ quiccongestion.PathSender = (*ErasurePathSender)(nil)
	_ TelemetryProvider         = (*ErasurePathSender)(nil)
)

// NewErasurePathSender wraps sender. rekey, if not nil, is asked after a
// migration whether the path changed; it returns the sender for the new path,
// or nil to keep the current one.
func NewErasurePathSender(sender *ErasureSender, rekey func(oldLocal, newLocal, oldRemote, newRemote net.Addr) *ErasureSender) *ErasurePathSender {
	p := &ErasurePathSender{rekey: rekey}
	p.sender.Store(sender)
	return p
}

// Sender returns the ErasureSender currently driven.
func (p *ErasurePathSender) Sender() *ErasureSender { return p.sender.Load() }

func (p *ErasurePathSender) Telemetry() ControllerTelemetry { return p.Sender().Telemetry() }

// OnAttach is the first call.
func (p *ErasurePathSender) OnAttach(_ monotime.Time, inFlight, maxPacket quiccongestion.ByteCount, wake func()) {
	p.attached = true
	p.gauge = inFlight
	p.wake = wake
	if maxPacket > 0 && p.maxSize == 0 {
		p.maxSize = maxPacket
	}
}

// OnDemand records the connection's demand. One controller per connection
// has no class to arbitrate and derives nothing from it yet.
func (p *ErasurePathSender) OnDemand(d quiccongestion.Demand) { p.demand = d }

// Admit asks the same two questions, in the same order, as the connection did
// of a CongestionControl: the window, then the pacer.
func (p *ErasurePathSender) Admit(now monotime.Time, maxBytes quiccongestion.ByteCount) quiccongestion.Permit {
	e := p.Sender()
	if !e.CanSend(p.gauge) {
		return quiccongestion.Permit{}
	}
	if !e.HasPacingBudget(now) {
		retry := e.TimeUntilSend(p.gauge)
		if retry.IsZero() {
			// A pacing refusal is never read as a window refusal.
			retry = now
		}
		return quiccongestion.Permit{RetryAt: retry}
	}
	return quiccongestion.Permit{OK: true, Turn: quiccongestion.ClassBulk, MaxBytes: maxBytes}
}

// OnSent registers a packet as it was built.
func (p *ErasurePathSender) OnSent(s quiccongestion.SentPacket) {
	p.gauge = s.InFlight
	if s.Kind == quiccongestion.KindPathProbe {
		// Path probes are outside the flight and never reached the controller.
		return
	}
	e := p.Sender()
	if s.AckEliciting {
		e.OnPacketSent(s.Time, s.InFlight, s.Number, s.Bytes, true)
		return
	}
	e.onNonElicitingSent(s.Time, s.InFlight, s.Number, s.Bytes)
}

// OnAckEvent hands the event's outcomes to the control law in one batch, with
// the flight before the event, exactly as the extended callback did.
func (p *ErasurePathSender) OnAckEvent(ev quiccongestion.AckEvent) {
	prior := p.gauge
	p.gauge = ev.InFlight
	if len(ev.Acked) == 0 && len(ev.Lost) == 0 {
		return
	}
	p.acked = p.acked[:0]
	for _, r := range ev.Acked {
		p.acked = append(p.acked, quiccongestion.AckedPacketInfo{PacketNumber: r.Number, BytesAcked: r.Bytes})
	}
	p.lost = p.lost[:0]
	for _, r := range ev.Lost {
		p.lost = append(p.lost, quiccongestion.LostPacketInfo{PacketNumber: r.Number, BytesLost: r.Bytes})
	}
	p.Sender().OnCongestionEventEx(prior, ev.Time, p.acked, p.lost)
}

// OnDiscarded updates the flight. The per-packet callbacks never told the
// controller about discards, and neither does this.
func (p *ErasurePathSender) OnDiscarded(_ monotime.Time, inFlight quiccongestion.ByteCount, _ []quiccongestion.PacketRef, _ bool, _ quiccongestion.DiscardReason) {
	p.gauge = inFlight
}

// OnPathMigrated keeps the controller unless the path itself changed.
func (p *ErasurePathSender) OnPathMigrated(_ monotime.Time, oldLocal, newLocal, oldRemote, newRemote net.Addr) {
	if p.rekey == nil {
		return
	}
	next := p.rekey(oldLocal, newLocal, oldRemote, newRemote)
	if next == nil {
		return
	}
	if p.rtt != nil {
		next.SetRTTStatsProvider(p.rtt)
	}
	if p.maxSize > 0 {
		next.SetMaxDatagramSize(p.maxSize)
	}
	p.sender.Store(next)
}

func (p *ErasurePathSender) OnDetach(monotime.Time) { p.attached = false }

// The CongestionControl methods below are the ones a connection still calls
// on a PathSender: configuration and state queries.

func (p *ErasurePathSender) SetRTTStatsProvider(provider quiccongestion.RTTStatsProvider) {
	p.rtt = provider
	p.Sender().SetRTTStatsProvider(provider)
}

func (p *ErasurePathSender) SetMaxDatagramSize(size quiccongestion.ByteCount) {
	p.maxSize = size
	p.Sender().SetMaxDatagramSize(size)
}

func (p *ErasurePathSender) GetCongestionWindow() quiccongestion.ByteCount {
	return p.Sender().GetCongestionWindow()
}
func (p *ErasurePathSender) InSlowStart() bool { return p.Sender().InSlowStart() }
func (p *ErasurePathSender) InRecovery() bool  { return p.Sender().InRecovery() }
func (p *ErasurePathSender) OnRetransmissionTimeout(retransmitted bool) {
	p.Sender().OnRetransmissionTimeout(retransmitted)
}

// The per-packet methods are not called on a PathSender. They forward, so the
// type still behaves as its sender if a caller drives it the old way.

func (p *ErasurePathSender) TimeUntilSend(bytesInFlight quiccongestion.ByteCount) monotime.Time {
	return p.Sender().TimeUntilSend(bytesInFlight)
}
func (p *ErasurePathSender) HasPacingBudget(now monotime.Time) bool {
	return p.Sender().HasPacingBudget(now)
}
func (p *ErasurePathSender) OnPacketSent(sentTime monotime.Time, bytesInFlight quiccongestion.ByteCount, number quiccongestion.PacketNumber, bytes quiccongestion.ByteCount, retransmittable bool) {
	p.Sender().OnPacketSent(sentTime, bytesInFlight, number, bytes, retransmittable)
}
func (p *ErasurePathSender) CanSend(bytesInFlight quiccongestion.ByteCount) bool {
	return p.Sender().CanSend(bytesInFlight)
}
func (p *ErasurePathSender) MaybeExitSlowStart() { p.Sender().MaybeExitSlowStart() }
func (p *ErasurePathSender) OnPacketAcked(number quiccongestion.PacketNumber, ackedBytes, priorInFlight quiccongestion.ByteCount, eventTime monotime.Time) {
	p.Sender().OnPacketAcked(number, ackedBytes, priorInFlight, eventTime)
}
func (p *ErasurePathSender) OnCongestionEvent(number quiccongestion.PacketNumber, lostBytes, priorInFlight quiccongestion.ByteCount) {
	p.Sender().OnCongestionEvent(number, lostBytes, priorInFlight)
}
