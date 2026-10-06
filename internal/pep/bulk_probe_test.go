package pep

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestBulkExpiredProofRetiresFailedEntryWithoutClosingControl(t *testing.T) {
	rig := newJoinTestRig(t, TransportQUIC, TransportQUIC, 1)
	c := rig.client
	t.Cleanup(c.closeQUICPool)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	control, err := c.dialPooledQUICLane(ctx, congestionConfig{kind: defaultCongestion()})
	if err != nil {
		t.Fatal(err)
	}
	defer control.Close()
	stream, err := c.openBulkPoolStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	old := stream.(*bulkPoolStreamConn).entry
	stream.Close()
	c.bulkMu.Lock()
	old.verified = time.Time{}
	c.bulkMu.Unlock()
	rejected := errors.New("stale path cannot exchange authenticated probe")
	probes := 0
	c.probeBulkConnectionForTest = func(context.Context, *bulkConn, time.Duration) error { probes++; return rejected }
	if _, err = c.openBulkPoolStream(ctx); !errors.Is(err, rejected) {
		t.Fatalf("probe error: %v", err)
	}
	if probes != 1 || c.bulkConnCount() != 0 {
		t.Fatalf("failed entry retained: probes=%d count=%d", probes, c.bulkConnCount())
	}
	if old.conn.Context().Err() == nil {
		t.Fatal("failed exclusive transport remains open")
	}
	if err = probeControlGeneration(ctx, control.(*controlPoolStreamConn).generation, time.Second); err != nil {
		t.Fatalf("control sibling: %v", err)
	}
	fresh, err := c.openBulkPoolStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	if fresh.(*bulkPoolStreamConn).entry == old || probes != 1 {
		t.Fatal("new handshake reused failed entry or unnecessarily probed")
	}
}

func TestBulkExpiredProofUsesRealProbeAndHotReuseSkipsProbe(t *testing.T) {
	rig := newJoinTestRig(t, TransportQUIC, TransportQUIC, 1)
	c := rig.client
	t.Cleanup(c.closeQUICPool)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := c.openBulkPoolStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	entry := stream.(*bulkPoolStreamConn).entry
	stream.Close()
	c.bulkMu.Lock()
	entry.verified = time.Time{}
	c.bulkMu.Unlock()
	verified, err := c.openBulkPoolStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if verified.(*bulkPoolStreamConn).entry != entry {
		t.Fatal("healthy stale entry replaced")
	}
	verified.Close()
	c.probeBulkConnectionForTest = func(context.Context, *bulkConn, time.Duration) error {
		t.Error("fresh proof triggered another probe")
		return errors.New("unnecessary probe")
	}
	hot, err := c.openBulkPoolStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	hot.Close()
}
