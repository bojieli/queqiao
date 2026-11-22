package pep

import (
	"context"
	"sync"
)

// contextMutex makes a frame's deadline include waiting for the previous
// writer, not just its own socket write. The zero value is ready for use.
type contextMutex struct {
	once  sync.Once
	token chan struct{}
}

func (m *contextMutex) LockContext(ctx context.Context) error {
	m.once.Do(func() { m.token = make(chan struct{}, 1) })
	select {
	case m.token <- struct{}{}:
		if err := ctx.Err(); err != nil {
			m.Unlock()
			return err
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (m *contextMutex) Lock()   { _ = m.LockContext(context.Background()) }
func (m *contextMutex) Unlock() { <-m.token }
