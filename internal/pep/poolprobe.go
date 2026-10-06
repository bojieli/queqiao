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

// Borrowers share one round-trip check when the last real proof is stale.
// Failure drains this generation: new borrowers get a fresh connection,
// while existing siblings keep their transport until they finish.
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
			if pending.err != nil {
				c.drainControlQUICGeneration(g)
			}
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
	case <-pending.done:
		return pending.err
	}
}

func probeControlGeneration(ctx context.Context, g *controlQUICGeneration, budget time.Duration) error {
	return probePooledQUIC(ctx, g.conn, budget)
}

func probePooledQUIC(ctx context.Context, conn *quic.Conn, budget time.Duration) error {
	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return err
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

// Bulk entries are exclusively reserved before validation. A failed check can
// retire that entry without closing any control-pool sibling. No background
// keepalive is introduced: idle entries are checked only when borrowed again.
func (c *Client) verifyBulkPath(ctx context.Context, entry *bulkConn) error {
	c.bulkMu.Lock()
	fresh := time.Since(entry.verified) < pooledPathProofMaxAge
	c.bulkMu.Unlock()
	if fresh {
		return nil
	}
	probeCtx, cancel := context.WithTimeout(ctx, c.cfg.HandshakeTimeout)
	defer cancel()
	var err error
	if c.probeBulkConnectionForTest != nil {
		err = c.probeBulkConnectionForTest(probeCtx, entry, c.cfg.HandshakeTimeout)
	} else {
		err = probePooledQUIC(probeCtx, entry.conn, c.cfg.HandshakeTimeout)
	}
	if err == nil {
		c.bulkMu.Lock()
		entry.verified = time.Now()
		c.bulkMu.Unlock()
	}
	return err
}
