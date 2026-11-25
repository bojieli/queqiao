package congestion

import (
	"sync"

	quiccongestion "github.com/apernet/quic-go/congestion"
)

// ClassWeights are the deficit round robin weights of the three scheduled
// classes (docs/DESIGN-PATH-AUTHORITY.md §4.3). Only their ratios matter.
type ClassWeights struct {
	Control, Interactive, Bulk int64
}

// DefaultClassWeights serve CONTROL ahead of everything while its share is
// unused and still leave every class a bounded fraction under contention:
// with all three saturated, CONTROL gets 4/7 of the bytes, INTERACTIVE 2/7 and
// BULK 1/7. A class alone gets everything. They are initial values, for the
// class-scheduler arm of the live comparison to settle.
var DefaultClassWeights = ClassWeights{Control: 4, Interactive: 2, Bulk: 1}

// scheduledClasses are the classes the arbiter serves, in priority order.
var scheduledClasses = [...]quiccongestion.SendClass{
	quiccongestion.ClassControl, quiccongestion.ClassInteractive, quiccongestion.ClassBulk,
}

// ClassArbiter is the §4.3 arbiter: a deficit round robin over CONTROL,
// INTERACTIVE and BULK, in which every class's service is bounded relative to
// the bytes actually served, at any rate.
//
// Each class has a byte deficit. The turn goes to the highest-priority class
// with eligible demand and a positive deficit; the other classes with demand
// and a positive deficit may fill the rest of the packet up to their deficit.
// Every byte a packet carries is charged to the class it was served at, and a
// deficit may go negative: the shortfall is carried. When no class with
// demand has a positive deficit, every class with demand gets quanta of its
// weight times the quantum until one has, so an indebted class regains the
// turn in a bounded number of rounds.
//
// Deficits are normalised on every demand transition and again under the
// lock immediately before a turn is chosen, so a class with demand can always
// be selected, whatever happened to the other classes' demand since the last
// packet; nothing has to be sent, and no timer has to fire, to restore that.
// A class that loses its demand keeps a debt but drops any credit.
//
// It is safe for concurrent use. One arbiter serves one controller.
type ClassArbiter struct {
	mu      sync.Mutex
	weights [quiccongestion.NumSendClasses]int64
	quantum int64
	deficit [quiccongestion.NumSendClasses]int64
	demand  [quiccongestion.NumSendClasses]bool
	served  [quiccongestion.NumSendClasses]uint64
	turns   [quiccongestion.NumSendClasses]uint64
	// round counts replenishments; credited is the round in which each
	// class last got the quantum it is owed on becoming eligible.
	round    uint64
	credited [quiccongestion.NumSendClasses]uint64
}

// NewClassArbiter returns an arbiter with the given weights; a weight below
// one is one. quantum is the byte quantum a weight of one adds per round,
// normally the maximum packet size.
func NewClassArbiter(w ClassWeights, quantum quiccongestion.ByteCount) *ClassArbiter {
	a := &ClassArbiter{quantum: max(int64(quantum), 1)}
	a.weights[quiccongestion.ClassControl] = max(w.Control, 1)
	a.weights[quiccongestion.ClassInteractive] = max(w.Interactive, 1)
	a.weights[quiccongestion.ClassBulk] = max(w.Bulk, 1)
	return a
}

// SetQuantum changes the quantum, for a new maximum packet size.
func (a *ClassArbiter) SetQuantum(q quiccongestion.ByteCount) {
	a.mu.Lock()
	a.quantum = max(int64(q), 1)
	a.mu.Unlock()
}

// OnDemand records the eligible demand per class.
func (a *ClassArbiter) OnDemand(d quiccongestion.Demand) {
	a.mu.Lock()
	for _, c := range scheduledClasses {
		if d.Eligible[c] && !a.demand[c] && a.deficit[c] >= 0 && a.credited[c] != a.round+1 {
			// A class that becomes eligible joins the current round with
			// its quantum, as in deficit round robin, so a sparse ACK does
			// not wait out another class's whole allocation. Debt is kept,
			// and the credit is given once per round however often the
			// class's demand toggles.
			a.deficit[c] = max(a.deficit[c], a.weights[c]*a.quantum)
			a.credited[c] = a.round + 1
		}
		a.demand[c] = d.Eligible[c]
	}
	a.normalize()
	a.mu.Unlock()
}

// Permit chooses the turn for one packet of at most maxBytes and the Fill
// allowances of the other classes. It reserves nothing: the packet is
// settled by OnSent with what it actually carried.
func (a *ClassArbiter) Permit(maxBytes quiccongestion.ByteCount) quiccongestion.Permit {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.normalize()
	p := quiccongestion.Permit{OK: true, MaxBytes: maxBytes, Turn: quiccongestion.ClassControl}
	turn := quiccongestion.ClassExempt
	for _, c := range scheduledClasses {
		if a.demand[c] && a.deficit[c] > 0 {
			turn = c
			break
		}
	}
	if turn == quiccongestion.ClassExempt {
		// No class has demand. Whatever the builder finds is control work
		// the demand query missed; its empty build refreshes demand.
		return p
	}
	p.Turn = turn
	a.turns[turn]++
	for _, c := range scheduledClasses {
		if c != turn && a.demand[c] && a.deficit[c] > 0 {
			p.Fill[c] = quiccongestion.ByteCount(a.deficit[c])
		}
	}
	return p
}

// Select normalises the deficits and returns the turn the path's demand
// gives: the highest-priority class with eligible demand and a positive
// deficit, or ClassExempt when no class has demand. It issues nothing.
func (a *ClassArbiter) Select() quiccongestion.SendClass {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.normalize()
	for _, c := range scheduledClasses {
		if a.demand[c] && a.deficit[c] > 0 {
			return c
		}
	}
	return quiccongestion.ClassExempt
}

// SelectAmong returns the highest-priority class in mask that has demand and
// a positive deficit, or, failing that, the highest-priority class in mask
// that has demand at all, which then overdraws its deficit; ClassExempt when
// no class in mask has demand. A path controller uses it when another
// attachment's claim on the path's turn has lapsed: the attachment asking is
// served from its own demand, and an overdraft is carried like any other.
func (a *ClassArbiter) SelectAmong(mask [quiccongestion.NumSendClasses]bool) quiccongestion.SendClass {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.normalize()
	fallback := quiccongestion.ClassExempt
	for _, c := range scheduledClasses {
		if !mask[c] || !a.demand[c] {
			continue
		}
		if a.deficit[c] > 0 {
			return c
		}
		if fallback == quiccongestion.ClassExempt {
			fallback = c
		}
	}
	return fallback
}

// PermitTurn issues a permit for a turn already chosen. With fill, the other
// classes with demand and a positive deficit may fill the rest of the packet
// up to their deficit; without it the packet carries the turn's class only,
// which is what a permit that relies on borrowed budget must do.
func (a *ClassArbiter) PermitTurn(turn quiccongestion.SendClass, maxBytes quiccongestion.ByteCount, fill bool) quiccongestion.Permit {
	a.mu.Lock()
	defer a.mu.Unlock()
	p := quiccongestion.Permit{OK: true, MaxBytes: maxBytes, Turn: turn}
	if turn == quiccongestion.ClassExempt {
		p.Turn = quiccongestion.ClassControl
		return p
	}
	a.turns[turn]++
	if !fill {
		return p
	}
	for _, c := range scheduledClasses {
		if c != turn && a.demand[c] && a.deficit[c] > 0 {
			p.Fill[c] = quiccongestion.ByteCount(a.deficit[c])
		}
	}
	return p
}

// OnSent charges each class for the bytes a packet carried of it.
func (a *ClassArbiter) OnSent(classBytes [quiccongestion.NumSendClasses]quiccongestion.ByteCount) {
	a.mu.Lock()
	for _, c := range scheduledClasses {
		if b := int64(classBytes[c]); b > 0 {
			a.deficit[c] -= b
			a.served[c] += uint64(b)
		}
	}
	a.normalize()
	a.mu.Unlock()
}

// normalize drops the credit of classes without demand and, if no class
// with demand has a positive deficit, adds whole rounds of quanta to every
// class with demand until one has. It must be called with the lock held.
func (a *ClassArbiter) normalize() {
	anyDemand, anyPositive := false, false
	for _, c := range scheduledClasses {
		if !a.demand[c] {
			a.deficit[c] = min(a.deficit[c], 0)
			continue
		}
		anyDemand = true
		anyPositive = anyPositive || a.deficit[c] > 0
	}
	if !anyDemand || anyPositive {
		return
	}
	// The fewest whole rounds after which some class with demand is
	// positive.
	rounds := int64(-1)
	for _, c := range scheduledClasses {
		if !a.demand[c] {
			continue
		}
		step := a.weights[c] * a.quantum
		need := (1 - a.deficit[c] + step - 1) / step
		if rounds < 0 || need < rounds {
			rounds = need
		}
	}
	a.round++
	for _, c := range scheduledClasses {
		if a.demand[c] {
			a.deficit[c] += rounds * a.weights[c] * a.quantum
			// This round's quantum is given: a demand toggle within the
			// round earns no second one.
			a.credited[c] = a.round + 1
		}
	}
}

// ClassArbiterStats is what an arbiter has done.
type ClassArbiterStats struct {
	// Served is the bytes charged to each class.
	Served [quiccongestion.NumSendClasses]uint64
	// Turns is the permits issued with each class as the turn.
	Turns [quiccongestion.NumSendClasses]uint64
	// Deficit is each class's current deficit.
	Deficit [quiccongestion.NumSendClasses]int64
}

// Stats returns a snapshot.
func (a *ClassArbiter) Stats() ClassArbiterStats {
	a.mu.Lock()
	defer a.mu.Unlock()
	return ClassArbiterStats{Served: a.served, Turns: a.turns, Deficit: a.deficit}
}
