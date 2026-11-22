package pep

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestFailedPoolProbeRebuildsWithoutClosingSibling(t *testing.T) {
	rig := newJoinTestRig(t, TransportQUIC, TransportQUIC, 1)
	c := rig.client
	t.Cleanup(c.closeQUICPool)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cc := congestionConfig{kind: defaultCongestion()}
	first, err := c.dialPooledQUICLane(ctx, cc)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	old := first.(*controlPoolStreamConn).generation
	old.probeMu.Lock()
	old.verified = time.Time{}
	old.probeMu.Unlock()
	rejected := errors.New("probe cannot progress")
	c.probeControlGenerationForTest = func(context.Context, *controlQUICGeneration, time.Duration) error { return rejected }
	if _, err = c.dialPooledQUICLane(ctx, cc); !errors.Is(err, rejected) {
		t.Fatalf("failed probe: %v", err)
	}
	next, err := c.dialPooledQUICLane(ctx, cc)
	if err != nil {
		t.Fatalf("new borrow did not recover: %v", err)
	}
	defer next.Close()
	fresh := next.(*controlPoolStreamConn).generation
	if fresh == old {
		t.Fatal("failed probe generation was reused")
	}
	if old.conn.Context().Err() != nil {
		t.Fatal("healthy sibling was closed during rotation")
	}
	// The retained transport still handles a real server round trip.
	if err = probeControlGeneration(ctx, old, time.Second); err != nil {
		t.Fatalf("sibling transport unusable: %v", err)
	}
	first.Close()
	select {
	case <-old.conn.Context().Done():
	case <-time.After(time.Second):
		t.Fatal("drained transport leaked after last borrower closed")
	}
	if fresh.conn.Context().Err() != nil {
		t.Fatal("closing old generation killed replacement")
	}
}
