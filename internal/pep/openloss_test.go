package pep

import (
	"testing"
	"time"
)

// At most six optional copies can leave at once, even with many outstanding
// OPEN and pre-borrow handshakes. Idle periods cannot bank a larger burst.
func TestInitialPrefixRepairConnectionBudget(t *testing.T) {
	var budget prefixRepairBudget
	now := time.Unix(1, 0)
	total := 0
	for range 64 {
		total += budget.take(now)
	}
	if total != 6 {
		t.Fatalf("64 simultaneous opens reserved %d copies", total)
	}
	for i := 1; i <= 40; i++ {
		n := budget.take(now.Add(time.Duration(i) * time.Millisecond))
		if i < 40 && n != 0 || i == 40 && n != 1 {
			t.Fatalf("at %d ms reserved %d copies", i, n)
		}
	}
	if first, second, third := budget.take(now.Add(time.Hour)), budget.take(now.Add(time.Hour)), budget.take(now.Add(time.Hour)); first != 3 || second != 3 || third != 0 {
		t.Fatalf("after idle got %d/%d/%d copies", first, second, third)
	}
}

func TestPoolRotationGraceEndsOnProofCloseOrDeadline(t *testing.T) {
	deadline := time.Unix(100, 0)
	g := &controlQUICGeneration{borrowers: 2, pendingOpens: make(map[*controlPoolStreamConn]pendingOpen)}
	c := &Client{}
	attachTestCarriers(c)
	stream := &proofReadQUICStream{read: func(buf []byte) (int, error) { return copy(buf, "x"), nil }}
	proved := &controlPoolStreamConn{owner: c, generation: g, quicStreamConn: &quicStreamConn{stream: stream}}
	closing := &controlPoolStreamConn{owner: c, generation: g, quicStreamConn: &quicStreamConn{stream: &closeTrackingQUICStream{}}}
	stopped := 0
	for _, lane := range []*controlPoolStreamConn{proved, closing} {
		g.pendingOpens[lane] = pendingOpen{deadline: deadline, stop: func() { stopped++ }}
	}
	if !g.awaitingOpen(deadline.Add(-time.Nanosecond)) || g.awaitingOpen(deadline) || g.awaitingOpen(deadline.Add(time.Nanosecond)) {
		t.Fatal("grace must end exactly at the existing deadline")
	}
	if _, err := proved.Read(make([]byte, 1)); err != nil {
		t.Fatal(err)
	}
	if len(g.pendingOpens) != 1 || stopped != 1 {
		t.Fatal("first remote response did not release only its own waiting OPEN")
	}
	if err := proved.Close(); err != nil {
		t.Fatal(err)
	}
	if stopped != 1 {
		t.Fatal("close counted the already confirmed stream twice")
	}
	if err := closing.Close(); err != nil {
		t.Fatal(err)
	}
	if len(g.pendingOpens) != 0 || stopped != 2 || g.awaitingOpen(deadline.Add(-time.Second)) {
		t.Fatal("closing last unconfirmed stream retained recovery work")
	}
}
