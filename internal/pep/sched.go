package pep

import (
	"context"
	"io"
	"sync"

	"github.com/apernet/quic-go"
	quiccongestion "github.com/apernet/quic-go/congestion"

	"github.com/bojieli/queqiao/internal/protocol"
)

// classScheduledConns holds the connections whose controller schedules
// classes (sched=class). A lane's framing tags its writes and boosts its
// stream only on these.
var classScheduledConns sync.Map // *quic.Conn -> struct{}

func markClassScheduled(conn *quic.Conn) {
	if _, loaded := classScheduledConns.LoadOrStore(conn, struct{}{}); !loaded {
		context.AfterFunc(conn.Context(), func() { classScheduledConns.Delete(conn) })
	}
}

func isClassScheduled(conn *quic.Conn) bool {
	_, ok := classScheduledConns.Load(conn)
	return ok
}

// frameSendClass is the class of work a frame is, for the sending endpoint's
// scheduler (§4.3, §6). The frame header's class byte is the flow's class and
// a hint for the peer; it decides only DATA's class here.
func frameSendClass(f protocol.Frame) quiccongestion.SendClass {
	switch f.Header.Type {
	case protocol.TypeData:
		if f.Header.Class == protocol.ClassBulk {
			return quiccongestion.ClassBulk
		}
		return quiccongestion.ClassInteractive
	case protocol.TypePacket:
		// A SOCKS UDP packet carried on the stream is INTERACTIVE, as on
		// the datagram substrate (§9).
		return quiccongestion.ClassInteractive
	default:
		// OPEN, OPEN_OK, JOIN, ACK, CLOSE, RESET, PROBE: a flow's control.
		return quiccongestion.ClassControl
	}
}

// classWriter is a lane transport that can tag its writes with a class and
// boost its stream. quicStreamConn is one; on a connection that does not
// schedule classes it writes as usual and setBoost reports false.
type classWriter interface {
	writeClass(p []byte, class quiccongestion.SendClass) (int, error)
	setBoost(on bool) bool
	// classed says the connection schedules classes.
	classed() bool
}

// writeFullClass writes all of p as bytes of class.
func writeFullClass(w classWriter, p []byte, class quiccongestion.SendClass) error {
	for len(p) > 0 {
		n, err := w.writeClass(p, class)
		if n > 0 {
			p = p[n:]
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

// controlBoost is a lane's count of control frames waiting for its writeMu,
// and the stream boost that count drives (§6).
//
// A flow's DATA writer holds writeMu for the whole blocking stream write, so
// a control frame -- the ACK for the reverse direction, above all -- can wait
// outside the stream behind a chunk the builder serves at the flow's class.
// While any control frame waits, the stream is boosted: everything it holds
// and everything it accepts is served at CONTROL, within CONTROL's share. The
// waiter that raises the count from zero turns the boost on; the boost is
// turned off only when the last waiter has had its own frame accepted by the
// stream, or has given up waiting. Both transitions are made under mu
// together with the count, so a waiter that arrives as the last one leaves
// can never see the boost off while it waits, and one that is cancelled
// cannot leave it on.
type controlBoost struct {
	mu      sync.Mutex
	waiters int
	// pending counts control frames that have announced they are waiting
	// for writeMu, from before the boost call until they hold it or give
	// up; clear is closed when it returns to zero. A DATA writer that
	// arrives meanwhile waits for clear, so it cannot take writeMu ahead of
	// an ACK that is already waiting.
	pending int
	clear   chan struct{}
}

// announce registers a control frame's place ahead of later DATA writers.
func (b *controlBoost) announce() {
	b.mu.Lock()
	if b.pending == 0 {
		b.clear = make(chan struct{})
	}
	b.pending++
	b.mu.Unlock()
}

// settle ends an announcement: the control frame holds writeMu, or gave up.
func (b *controlBoost) settle() {
	b.mu.Lock()
	b.pending--
	if b.pending == 0 {
		close(b.clear)
	}
	b.mu.Unlock()
}

// yield waits until no announced control frame is waiting.
func (b *controlBoost) yield(ctx context.Context) error {
	b.mu.Lock()
	if b.pending == 0 {
		b.mu.Unlock()
		return nil
	}
	clear := b.clear
	b.mu.Unlock()
	select {
	case <-clear:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// enter registers a waiting control frame. It reports false, and registers
// nothing, when the transport cannot boost.
func (b *controlBoost) enter(w classWriter) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.waiters == 0 {
		if !w.setBoost(true) {
			return false
		}
	}
	b.waiters++
	return true
}

// leave unregisters a waiting control frame: its frame was accepted by the
// stream, or it stopped waiting.
func (b *controlBoost) leave(w classWriter) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.waiters--
	if b.waiters == 0 {
		w.setBoost(false)
	}
}

// count reports the waiters, for tests.
func (b *controlBoost) count() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.waiters
}

// lockWrite acquires writeMu for f. A control frame that finds it held boosts
// the lane's stream while it waits; boosted says so, and the caller leaves
// once the frame has been written. A cancelled wait leaves at once.
//
// A control frame first announces itself, then tries writeMu, and if it is
// held boosts and waits: a DATA writer that arrives after the announcement
// waits for it, so it cannot take writeMu ahead of the control frame. A control
// write whose context is already done never boosts, since boosting would
// promote buffered DATA that a write which cannot proceed has no claim on.
func (c *frameConn) lockWrite(ctx context.Context, f protocol.Frame) (boosted bool, err error) {
	lock := func() error { return c.writeMu.LockContext(ctx) }
	cw, ok := c.control.(classWriter)
	if !ok || !cw.classed() {
		return false, lock()
	}
	if frameSendClass(f) != quiccongestion.ClassControl {
		if err := c.boost.yield(ctx); err != nil {
			return false, err
		}
		return false, lock()
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	// Announce before looking at writeMu: a DATA writer checks for
	// announced control frames before it locks, so once this frame finds
	// writeMu held, no DATA writer that arrives after it -- including the
	// holder's next chunk -- can take writeMu first.
	c.boost.announce()
	if c.writeMu.TryLock() {
		c.boost.settle()
		return false, nil
	}
	boosted = c.boost.enter(cw)
	err = lock()
	c.boost.settle()
	if err != nil {
		if boosted {
			c.boost.leave(cw)
		}
		return false, err
	}
	return boosted, nil
}

// endBoost ends a boosted wait after the frame was written.
func (c *frameConn) endBoost(boosted bool) {
	if boosted {
		c.boost.leave(c.control.(classWriter))
	}
}
