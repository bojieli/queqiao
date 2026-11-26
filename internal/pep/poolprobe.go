package pep

import (
	"context"
	"errors"
	"time"

	"github.com/apernet/quic-go"
	"github.com/bojieli/queqiao/internal/protocol"
	"github.com/bojieli/queqiao/internal/session"
)

// Remote proof freshness is independent of the local interface polling cadence.
const pooledPathProofMaxAge = 2 * time.Second

// Read records the first authenticated response on a newly opened stream.
// Repeated short exchanges already prove the pooled connection works; ignoring
// them would charge a redundant probe round trip every two seconds. Use the
// stream's creation time, not the read time: old buffered bytes must not make
// an idle connection appear freshly verified. Writes alone prove nothing.
func (s *controlPoolStreamConn) Read(p []byte) (int, error) {
	n, err := s.quicStreamConn.Read(p)
	if n > 0 {
		s.proofOnce.Do(func() {
			s.finishOpen()
			g := s.generation
			g.probeMu.Lock()
			// A failure after this stream opened invalidates its evidence.
			// A late response cannot undo the next borrow's required probe.
			if s.proofEpoch == g.proofEpoch && s.proofStarted.After(g.verified) {
				g.verified = s.proofStarted
			}
			g.probeMu.Unlock()
		})
	}
	return n, err
}

func (g *controlQUICGeneration) invalidatePathProof() {
	g.probeMu.Lock()
	g.verified = time.Time{}
	g.proofEpoch++
	g.probeMu.Unlock()
}

// Borrowers share one round-trip check when the last real proof is stale. A
// failure fails the borrows that waited on it and nothing else.
type controlPathProbe struct {
	done chan struct{}
	err  error
}

func (c *Client) verifyControlPath(ctx context.Context, g *controlQUICGeneration) error {
	g.probeMu.Lock()
	if time.Since(g.verified) < pooledPathProofMaxAge {
		g.probeMu.Unlock()
		return nil
	}
	pending := g.probe
	if pending == nil {
		pending = &controlPathProbe{done: make(chan struct{})}
		g.probe = pending
		go func() {
			probeCtx, cancel := context.WithTimeout(g.conn.Context(), c.cfg.HandshakeTimeout)
			defer cancel()
			probe := probeControlGeneration
			if c.probeControlGenerationForTest != nil {
				probe = c.probeControlGenerationForTest
			}
			pending.err = probe(probeCtx, g, c.cfg.HandshakeTimeout)
			// A failed proof fails only that borrow: replacing the carrier
			// is the carrier manager's decision, on transport evidence
			// (§10.2).
			g.probeMu.Lock()
			if pending.err == nil {
				g.verified = time.Now()
			}
			g.probe = nil
			close(pending.done)
			g.probeMu.Unlock()
		}()
	}
	g.probeMu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-g.borrowing.Done():
		return g.borrowing.Err()
	case <-pending.done:
		return pending.err
	}
}

func probeControlGeneration(ctx context.Context, g *controlQUICGeneration, budget time.Duration) error {
	return probePooledQUIC(ctx, g.conn, budget, g)
}

func probePooledQUIC(ctx context.Context, conn *quic.Conn, budget time.Duration, g *controlQUICGeneration) error {
	return probePooledQUICGranted(ctx, conn, budget, g, false)
}

// probePooledQUICGranted is probePooledQUIC with, when grant is set, a
// verification grant bound to the probe's bytes, so the exchange is
// admitted while the path's window and balance are held by a silent sibling
// (§4.3, §10.2).
func probePooledQUICGranted(ctx context.Context, conn *quic.Conn, budget time.Duration, g *controlQUICGeneration, grant bool) error {
	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return err
	}
	if grant {
		bindProbeGrant(conn, stream, 0, budget)
	}
	if g.highErasure() {
		if stop := g.protectPrefix(stream, protocol.HeaderSize+1); stop != nil {
			defer stop()
		}
	}
	outer := &quicStreamConn{stream: stream, conn: conn, closeConn: false}
	defer outer.Close()
	_, finish := bindHandshake(ctx, outer, budget)
	defer finish()
	id, err := session.NewSessionID()
	if err != nil {
		return err
	}
	frame := protocol.Frame{Header: protocol.Header{Version: protocol.Version, Type: protocol.TypeProbe, SessionID: id, Class: protocol.ClassNew}, Payload: []byte{1}}
	fc := newFrameConn(outer)
	if err = fc.Write(frame); err != nil {
		return err
	}
	response, err := fc.Read()
	if err != nil {
		return err
	}
	if response.Header.Type != frame.Header.Type || response.Header.SessionID != id || response.Header.FlowID != 0 || response.Header.Sequence != 0 || response.Header.Flags != 0 || len(response.Payload) != 1 || response.Payload[0] != 1 {
		return errors.New("invalid pooled path probe echo")
	}
	return finish()
}

// Detach before marking the generation draining, under the same pool lock
// used to reserve borrowers. No future acquire can revive it.
func (c *Client) drainControlQUICGeneration(g *controlQUICGeneration) {
	c.quicMu.Lock()
	if c.quicGeneration == g {
		c.quicGeneration = nil
	}
	g.borrowMu.Lock()
	g.draining = true
	closeNow := g.borrowers == 0
	g.borrowMu.Unlock()
	c.quicMu.Unlock()
	if closeNow {
		g.close("queqiao failed path probe drained")
	}
}

func (g *controlQUICGeneration) releaseBorrow() {
	g.borrowMu.Lock()
	g.borrowers--
	closeNow := g.draining && g.borrowers == 0
	g.borrowMu.Unlock()
	if closeNow {
		g.close("queqiao drained generation released")
	}
}

func (g *controlQUICGeneration) highErasure() bool {
	if g == nil || g.lossPath == nil {
		return false
	}
	state := g.lossPath.Current()
	return state.Erasure >= .20 && state.ObservedSamples >= 50
}

// A generation starts with six copies and earns one every 40 ms. Even
// simultaneous opens share that burst and rate. Account in copies before
// scheduling: the QUIC packetizer separately charges wire bytes to congestion.
type prefixRepairBudget struct {
	last    time.Time
	credits int
}

func (b *prefixRepairBudget) take(now time.Time) int {
	const refill = 40 * time.Millisecond
	if b.last.IsZero() {
		b.last = now
		b.credits = 6
	} else if elapsed := now.Sub(b.last); elapsed >= refill {
		earned := int(elapsed / refill)
		b.credits = min(6, b.credits+earned)
		b.last = b.last.Add(time.Duration(earned) * refill)
	}
	copies := min(3, b.credits)
	b.credits -= copies
	return copies
}

// Caller has already observed severe outbound loss on this route.
func (g *controlQUICGeneration) protectPrefix(stream quicBidiStream, size int) func() {
	lane, ok := stream.(*quic.Stream)
	if !ok {
		return nil
	}
	g.probeMu.Lock()
	copies := g.prefixBudget.take(time.Now())
	g.probeMu.Unlock()
	return lane.ProtectInitialPrefix(size, copies)
}

// New borrowers leave a silent generation immediately. Its original OPEN
// cannot be retried on another stream without risking a second destination
// connection; allow it to finish only within its existing handshake deadline.
type pendingOpen struct {
	deadline time.Time
	stop     func()
}

func (s *controlPoolStreamConn) trackOpen(deadline time.Time, size int) bool {
	g := s.generation
	// Rotation detaches under this same lock. Before admission the caller
	// can safely discard its unused stream. After admission the existing
	// borrow owns exactly one OPEN and, on a lossy route, its drain deadline.
	s.owner.quicMu.Lock()
	defer s.owner.quicMu.Unlock()
	if s.owner.quicGeneration != g || g.conn.Context().Err() != nil {
		return false
	}
	if m := s.owner.carriers; m != nil && (!m.accepting(g) || m.openGated(g)) {
		// recovery=carrier: SUSPECT, or flows are waiting to rebind onto it.
		return false
	}
	if g.highErasure() {
		stop := g.protectPrefix(s.stream, size)
		g.probeMu.Lock()
		if g.pendingOpens == nil {
			g.pendingOpens = make(map[*controlPoolStreamConn]pendingOpen)
		}
		g.pendingOpens[s] = pendingOpen{deadline: deadline, stop: stop}
		g.probeMu.Unlock()
	}
	return true
}

func (s *controlPoolStreamConn) finishOpen() {
	g := s.generation
	if g == nil {
		return
	}
	g.probeMu.Lock()
	pending := g.pendingOpens[s]
	delete(g.pendingOpens, s)
	g.probeMu.Unlock()
	if pending.stop != nil {
		pending.stop()
	}
}

// awaitingOpen reports whether an OPEN on this generation still waits for
// its acknowledgement.
func (g *controlQUICGeneration) awaitingOpen(now time.Time) bool {
	g.probeMu.Lock()
	defer g.probeMu.Unlock()
	for _, pending := range g.pendingOpens {
		if now.Before(pending.deadline) {
			return true
		}
	}
	return false
}
