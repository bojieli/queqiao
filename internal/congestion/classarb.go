package congestion

import (
	"sync"
	"time"

	quiccongestion "github.com/apernet/quic-go/congestion"
	"github.com/apernet/quic-go/monotime"
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
	// held is the credit a class had when it lost demand, kept for the rest
	// of the round in which it went idle (heldRound) and restored if it is
	// eligible again within it; it never carries into a later round.
	held      [quiccongestion.NumSendClasses]int64
	heldRound [quiccongestion.NumSendClasses]uint64
	// stuckUntil passes over a class whose turn built nothing although it
	// had demand -- its work did not fit the permit -- until then, or until
	// its demand changes, a packet is sent or an ACK event moves the
	// flight. Another class can use the path meanwhile; if every class with
	// demand is passed over, admission is refused until the earliest mark
	// expires, so the connection neither spins nor waits for an unrelated
	// event: a write that arrives in the class is tried again then.
	stuckUntil [quiccongestion.NumSendClasses]monotime.Time
}

// emptyBuildHold is how long a class whose turn built nothing is passed over.
const emptyBuildHold = 5 * time.Millisecond

// OnEmptyBuild records that a turn of class built nothing at now, and
// reports whether another decision is possible at once.
func (a *ClassArbiter) OnEmptyBuild(now monotime.Time, class quiccongestion.SendClass) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if class == quiccongestion.ClassExempt || class >= quiccongestion.NumSendClasses || a.stuckAt(now, class) {
		return false
	}
	a.stuckUntil[class] = now.Add(emptyBuildHold)
	return true
}

// stuckAt says whether class is passed over at now. A zero now (unknown)
// honours any mark. It must be called with the lock held.
func (a *ClassArbiter) stuckAt(now monotime.Time, class quiccongestion.SendClass) bool {
	until := a.stuckUntil[class]
	return !until.IsZero() && (now.IsZero() || now.Before(until))
}

// Unstick clears every mark: something changed that may let the work fit (a
// packet was sent, flight changed).
func (a *ClassArbiter) Unstick() {
	a.mu.Lock()
	a.stuckUntil = [quiccongestion.NumSendClasses]monotime.Time{}
	a.mu.Unlock()
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
		if d.Eligible[c] != a.demand[c] {
			a.stuckUntil[c] = 0
		}
		if d.Eligible[c] && !a.demand[c] && a.held[c] > 0 && a.heldRound[c] == a.round {
			// Back within the round it went idle in: it keeps what it had
			// not spent of this round's allocation, and nothing more.
			// What was charged to it while idle (a PTO probe's
			// retransmission) has already been taken off what it held.
			a.deficit[c] += a.held[c]
			a.held[c] = 0
		} else if d.Eligible[c] && !a.demand[c] && a.deficit[c] >= 0 && a.credited[c] != a.round+1 {
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
	return a.PermitAt(0, maxBytes)
}

// PermitAt is Permit at a known time, at which passed-over classes may have
// become eligible again.
func (a *ClassArbiter) PermitAt(now monotime.Time, maxBytes quiccongestion.ByteCount) quiccongestion.Permit {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.normalize()
	p := quiccongestion.Permit{OK: true, MaxBytes: maxBytes, Turn: quiccongestion.ClassExempt}
	turn := quiccongestion.ClassExempt
	for _, c := range scheduledClasses {
		if a.demand[c] && !a.stuckAt(now, c) && a.deficit[c] > 0 {
			turn = c
			break
		}
	}
	if turn == quiccongestion.ClassExempt {
		// Every class with credit is passed over. The others are
		// arbitrated by weight among themselves: replenished as if the
		// passed-over classes had no demand, so no sendable class is left
		// without a turn or a fill while another's work does not fit.
		var eligible [quiccongestion.NumSendClasses]bool
		var retry monotime.Time
		for _, c := range scheduledClasses {
			if !a.demand[c] {
				continue
			}
			if !a.stuckAt(now, c) {
				eligible[c] = true
				continue
			}
			if u := a.stuckUntil[c]; retry.IsZero() || u.Before(retry) {
				retry = u
			}
		}
		a.replenish(eligible)
		for _, c := range scheduledClasses {
			if eligible[c] && a.deficit[c] > 0 {
				turn = c
				break
			}
		}
		if turn == quiccongestion.ClassExempt && !retry.IsZero() {
			// Work is waiting, none of it fits now: refuse until a mark
			// expires, which also schedules the retry.
			return quiccongestion.Permit{RetryAt: retry}
		}
		if turn == quiccongestion.ClassExempt {
			return p
		}
	}
	p.Turn = turn
	a.turns[turn]++
	for _, c := range scheduledClasses {
		if c != turn && a.demand[c] && !a.stuckAt(now, c) && a.deficit[c] > 0 {
			p.Fill[c] = quiccongestion.ByteCount(a.deficit[c])
		}
	}
	return p
}

// OnSent charges each class for the bytes a packet carried of it.
func (a *ClassArbiter) OnSent(classBytes [quiccongestion.NumSendClasses]quiccongestion.ByteCount) {
	a.mu.Lock()
	a.stuckUntil = [quiccongestion.NumSendClasses]monotime.Time{}
	for _, c := range scheduledClasses {
		if b := int64(classBytes[c]); b > 0 {
			a.served[c] += uint64(b)
			if !a.demand[c] && a.heldRound[c] == a.round && a.held[c] > 0 {
				// Charged while idle (a PTO probe's retransmission): the
				// credit it held for this round pays first, so only an
				// excess becomes debt that outlives the round.
				use := min(a.held[c], b)
				a.held[c] -= use
				b -= use
			}
			a.deficit[c] -= b
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
			if a.deficit[c] > 0 {
				a.held[c], a.heldRound[c] = a.deficit[c], a.round
			}
			a.deficit[c] = min(a.deficit[c], 0)
			continue
		}
		anyDemand = true
		anyPositive = anyPositive || a.deficit[c] > 0
	}
	if !anyDemand || anyPositive {
		return
	}
	a.replenish(a.demand)
}

// replenish adds the fewest whole rounds of quanta to the classes marked
// eligible that makes one of them positive, if none is. It must be called
// with the lock held.
func (a *ClassArbiter) replenish(eligible [quiccongestion.NumSendClasses]bool) {
	found := false
	for _, c := range scheduledClasses {
		if eligible[c] {
			if a.deficit[c] > 0 {
				return
			}
			found = true
		}
	}
	if !found {
		return
	}
	// The fewest whole rounds after which some class with demand is
	// positive.
	rounds := int64(-1)
	for _, c := range scheduledClasses {
		if !eligible[c] {
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
		if eligible[c] {
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
