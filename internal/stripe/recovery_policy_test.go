package stripe

import (
	"bytes"
	"context"
	"testing"
	"time"
)

func TestByteScopedRecoveryKeepsFreshChunksUnreliable(t *testing.T) {
	now := time.Now()
	var recoveryEnd uint64
	sched := New(bytes.NewReader([]byte("old-datafresh!!!")), Config{
		ChunkSize:       8,
		Reliable:        func(uint64) bool { return false },
		ForceReliable:   func(offset uint64) bool { return offset < recoveryEnd },
		RetransmitAfter: func() time.Duration { return time.Second },
		Now:             func() time.Time { return now },
	})
	defer sched.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	first, err := sched.Next(ctx, 1, 0)
	if err != nil || first == nil {
		t.Fatalf("first dispatch = %+v, %v", first, err)
	}
	if first.Reliable {
		t.Fatal("normal datagram dispatch is reliable")
	}
	sched.Wrote(1, first)
	recoveryEnd = first.End()
	now = now.Add(2 * time.Second)
	if got := sched.ReissueExpired(); got != 1 {
		t.Fatalf("reissued %d chunks, want the missing datagram", got)
	}
	retry, err := sched.Next(ctx, 1, 0)
	if err != nil || retry == nil || retry.Offset != first.Offset || !retry.Reliable {
		t.Fatalf("stalled chunk retry = %+v, %v; want reliable offset 0", retry, err)
	}
	fresh, err := sched.Next(ctx, 1, 0)
	if err != nil || fresh == nil || fresh.Offset != recoveryEnd || fresh.Reliable {
		t.Fatalf("fresh dispatch = %+v, %v; want unreliable offset %d", fresh, err, recoveryEnd)
	}
	// Clearing the policy before the writer runs cannot change the retry's
	// lifetime or routing. Its old unreliable predecessor is already retired.
	recoveryEnd = 0
	if !retry.Reliable {
		t.Fatal("clearing recovery changed a dispatched reliable retry")
	}
	sched.Wrote(1, retry)
	sched.Complete(1, fresh)
	now = now.Add(2 * time.Second)
	if got := sched.ReissueExpired(); got != 1 {
		t.Fatalf("reliable retry offered %d speculative copies, want one", got)
	}
	sched.mu.Lock()
	defer sched.mu.Unlock()
	out := sched.live[retry.Offset]
	if len(out.attempts) != 1 || !out.attempts[0].reliable {
		t.Fatalf("clearing recovery retired or changed reliable attempt: %+v", out.attempts)
	}
}
