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
	// state is the current sender and the cumulative counters of the
	// senders it replaced after a path change, published together so the
	// connection's counters never go backwards or count twice.
	state atomic.Pointer[erasurePathState]

	// gauge is the connection's bytes in flight as the last event reported it.
	// Every change of the connection's flight is reported, so it is also the
	// flight before the next event: the prior flight the per-packet callbacks
	// carried.
	gauge quiccongestion.ByteCount

	rekey func(oldLocal, newLocal, oldRemote, newRemote net.Addr) *ErasureSender
	// addrs, if set, reports the connection's current addresses. It is read
	// at attachment, on the run loop, so a migration between choosing the
	// sender and installing it is not missed.
	addrs   func() (local, remote net.Addr)
	rtt     quiccongestion.RTTStatsProvider
	maxSize quiccongestion.ByteCount
	wake    func()
	demand  quiccongestion.Demand

	acked    []quiccongestion.AckedPacketInfo
	lost     []quiccongestion.LostPacketInfo
	attached bool

	// arb, if set, arbitrates between send classes (sched=class): Admit
	// answers with its turn and Fill, and the connection's builder executes
	// them. It is set before installation and never changes.
	arb *ClassArbiter
}

type erasurePathState struct {
	sender  *ErasureSender
	retired ControllerTelemetry
}

var (
	_ quiccongestion.PathSender     = (*ErasurePathSender)(nil)
	_ quiccongestion.ClassScheduler     = (*ErasurePathSender)(nil)
	_ quiccongestion.EmptyBuildObserver = (*ErasurePathSender)(nil)
	_ TelemetryProvider             = (*ErasurePathSender)(nil)
)

// EnableClassScheduling makes the sender arbitrate between send classes with
// the §4.3 deficit round robin (stage 2 of the path-authority migration,
// sched=class): the window and the pacer still decide whether a packet may
// go, and the arbiter decides which class it serves. It must be called before
// the sender is installed on a connection.
func (p *ErasurePathSender) EnableClassScheduling(w ClassWeights) {
	p.arb = NewClassArbiter(w, p.Sender().inner.maxDatagramSize)
}

// OnEmptyBuild is told that a packet admitted under a permit could not be
// built although demand is unchanged: the turn's work does not fit. The
// arbiter passes that class over until something changes, and the
// connection is woken to try another decision. Once every class with
// demand has been passed over the turn is EXEMPT, which does not wake.
func (p *ErasurePathSender) OnEmptyBuild(_ monotime.Time, permit quiccongestion.Permit) {
	if p.arb != nil && p.arb.OnEmptyBuild(permit.Turn) && p.wake != nil {
		p.wake()
	}
}

// SchedulesClasses says whether the connection's builder executes this
// sender's class decisions.
func (p *ErasurePathSender) SchedulesClasses() bool { return p.arb != nil }

// ClassStats returns the arbiter's state, or false without class scheduling.
func (p *ErasurePathSender) ClassStats() (ClassArbiterStats, bool) {
	if p.arb == nil {
		return ClassArbiterStats{}, false
	}
	return p.arb.Stats(), true
}

// NewErasurePathSender wraps sender. rekey, if not nil, is asked after a
// migration whether the path changed; it returns the sender for the new path,
// or nil to keep the current one.
func NewErasurePathSender(sender *ErasureSender, rekey func(oldLocal, newLocal, oldRemote, newRemote net.Addr) *ErasureSender) *ErasurePathSender {
	p := &ErasurePathSender{rekey: rekey}
	p.state.Store(&erasurePathState{sender: sender})
	return p
}

// SetAddresses gives the sender a way to read the connection's current
// addresses at attachment; see OnPathMigrated.
func (p *ErasurePathSender) SetAddresses(addrs func() (local, remote net.Addr)) { p.addrs = addrs }

// Sender returns the ErasureSender currently driven.
func (p *ErasurePathSender) Sender() *ErasureSender { return p.state.Load().sender }

// Telemetry is the current sender's state. Its counters include those of the
// senders it replaced: they count this connection's events, while the gauges
// and estimates describe the current path.
func (p *ErasurePathSender) Telemetry() ControllerTelemetry {
	st := p.state.Load()
	t := st.sender.Telemetry()
	addCounters(&t, &st.retired)
	return t
}

// addCounters adds the cumulative counters of from to t.
func addCounters(t *ControllerTelemetry, from *ControllerTelemetry) {
	t.Samples += from.Samples
	t.NonAppSamples += from.NonAppSamples
	t.AppSamples += from.AppSamples
	t.StateMisses += from.StateMisses
	t.ZeroSamples += from.ZeroSamples
	t.BytesLost += from.BytesLost
	t.PacketsLost += from.PacketsLost
	t.PacketsLostObserved += from.PacketsLostObserved
	t.NonElicitingBytes += from.NonElicitingBytes
	t.NonElicitingDebited += from.NonElicitingDebited
}

// OnAttach is the first call.
func (p *ErasurePathSender) OnAttach(_ monotime.Time, inFlight, maxPacket quiccongestion.ByteCount, wake func()) {
	p.attached = true
	p.gauge = inFlight
	p.wake = wake
	// The connection may have migrated after the sender was chosen and
	// before this installation on the run loop: revalidate the path.
	if p.addrs != nil && p.rekey != nil {
		local, remote := p.addrs()
		if next := p.rekey(local, local, remote, remote); next != nil {
			p.replace(next)
		}
	}
	// Path MTU discovery may have raised the packet size before the
	// controller was installed on the run loop. Only a change is applied:
	// telling the sender the size it was built with would reset its pacer's
	// packet size, which the per-packet callbacks never did.
	if maxPacket > 0 && maxPacket != p.Sender().inner.maxDatagramSize {
		p.SetMaxDatagramSize(maxPacket)
	}
}

// OnDemand records the connection's demand. Without class scheduling one
// controller per connection has no class to arbitrate and derives nothing
// from it; with it, the arbiter normalises its deficits on the transition.
func (p *ErasurePathSender) OnDemand(d quiccongestion.Demand) {
	p.demand = d
	if p.arb != nil {
		p.arb.OnDemand(d)
	}
}

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
	if p.arb != nil {
		return p.arb.Permit(maxBytes)
	}
	return quiccongestion.Permit{OK: true, Turn: quiccongestion.ClassBulk, MaxBytes: maxBytes}
}

// OnSent registers a packet as it was built.
func (p *ErasurePathSender) OnSent(s quiccongestion.SentPacket) {
	p.gauge = s.InFlight
	if p.arb != nil {
		// Every class byte is settled, whatever built the packet.
		p.arb.OnSent(s.ClassBytes)
	}
	if s.Kind == quiccongestion.KindPathProbe && !s.InFlightCounted {
		// Probes sent outside the flight never reached the controller. A
		// path-validation packet on the current path is in flight and is
		// accounted like any other.
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
	if p.arb != nil {
		// The flight and the window moved: work that did not fit may now.
		p.arb.Unstick()
	}
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
	if next := p.rekey(oldLocal, newLocal, oldRemote, newRemote); next != nil {
		p.replace(next)
	}
}

// replace makes next the sender, keeping the connection's counters.
func (p *ErasurePathSender) replace(next *ErasureSender) {
	if p.rtt != nil {
		next.SetRTTStatsProvider(p.rtt)
	}
	if p.maxSize > 0 {
		next.SetMaxDatagramSize(p.maxSize)
	}
	// The old sender gets no more events, so its counters are final.
	p.state.Store(&erasurePathState{sender: next, retired: p.Telemetry()})
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
	if p.arb != nil {
		p.arb.SetQuantum(size)
	}
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
