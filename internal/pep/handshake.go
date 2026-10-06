package pep

import (
	"context"
	"sync"
	"time"
)

// bindHandshake interrupts only this lane and joins its cancellation callback
// before ownership can pass to the flow. One budget covers both read and write.
func bindHandshake(ctx context.Context, conn streamConn, budget time.Duration) (time.Time, func() error) {
	deadline := time.Now().Add(budget)
	if parent, ok := ctx.Deadline(); ok && parent.Before(deadline) {
		deadline = parent
	}
	_ = conn.SetDeadline(deadline)
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { _ = conn.SetDeadline(time.Now()); close(done) })
	var once sync.Once
	finish := func() error {
		once.Do(func() {
			if !stop() {
				<-done
			}
			_ = conn.SetDeadline(time.Time{})
		})
		return ctx.Err()
	}
	return deadline, finish
}
