package pep

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apernet/quic-go"
	"github.com/bojieli/queqiao/internal/protocol"
)

// Age the previous proof explicitly rather than depending on a particular
// machine taking two seconds to carry several short exchanges. The first
// response on each new stream must leave the next borrower a warm path.
func TestNewPooledStreamResponseRenewsPathProof(t *testing.T) {
	rig := newJoinTestRig(t, TransportQUIC, TransportQUIC, 1)
	c := rig.client
	t.Cleanup(c.closeQUICPool)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cc := congestionConfig{kind: defaultCongestion()}
	outer, err := c.dialPooledQUICLane(ctx, cc)
	if err != nil {
		t.Fatal(err)
	}
	g := outer.(*controlPoolStreamConn).generation
	c.probeControlGenerationForTest = func(context.Context, *controlQUICGeneration, time.Duration) error {
		return errors.New("a completed exchange was ignored and an unnecessary probe started")
	}
	for i := 0; i < 3; i++ {
		lane := outer.(*controlPoolStreamConn)
		g.probeMu.Lock()
		g.verified = time.Time{}
		g.probeMu.Unlock()
		fc := newFrameConn(outer)
		frame := protocol.Frame{Header: protocol.Header{
			Version: protocol.Version, Type: protocol.TypeProbe,
			SessionID: [16]byte{byte(i + 1)}, Class: protocol.ClassNew,
		}, Payload: []byte{1}}
		if err := fc.Write(frame); err != nil {
			t.Fatal(err)
		}
		g.probeMu.Lock()
		verified := g.verified
		g.probeMu.Unlock()
		if !verified.IsZero() {
			t.Fatal("a local write was mistaken for remote proof")
		}
		response, err := fc.Read()
		if err != nil || response.Header.SessionID != frame.Header.SessionID {
			t.Fatalf("response = %+v, error = %v", response.Header, err)
		}
		g.probeMu.Lock()
		verified = g.verified
		g.probeMu.Unlock()
		if !verified.Equal(lane.proofStarted) {
			t.Fatalf("remote proof = %v, want stream creation %v", verified, lane.proofStarted)
		}
		if err := fc.Close(); err != nil {
			t.Fatal(err)
		}
		outer, err = c.dialPooledQUICLane(ctx, cc)
		if err != nil {
			t.Fatalf("warm borrow %d: %v", i, err)
		}
		if outer.(*controlPoolStreamConn).generation != g {
			t.Fatal("a successful short exchange replaced the pooled generation")
		}
	}
	outer.Close()
}

type proofReadQUICStream struct {
	closeTrackingQUICStream
	read func([]byte) (int, error)
}

func (s *proofReadQUICStream) Read(p []byte) (int, error) { return s.read(p) }

func TestPooledStreamProofCannotInventFreshness(t *testing.T) {
	now := time.Now()
	for _, test := range []struct {
		name     string
		started  time.Time
		verified time.Time
		failed   bool
		want     time.Time
	}{
		{name: "new response", started: now, want: now},
		{name: "delayed buffered response", started: now.Add(-2 * pooledPathProofMaxAge), want: now.Add(-2 * pooledPathProofMaxAge)},
		{name: "older response", started: now.Add(-time.Second), verified: now, want: now},
		{name: "response after invalidation", started: now, failed: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			g := &controlQUICGeneration{verified: test.verified}
			stream := &proofReadQUICStream{read: func(p []byte) (int, error) { return copy(p, "x"), nil }}
			lane := &controlPoolStreamConn{
				quicStreamConn: &quicStreamConn{stream: stream}, generation: g,
				proofStarted: test.started, proofEpoch: g.proofEpoch,
			}
			if test.failed {
				g.invalidatePathProof()
			}
			if n, err := lane.Read(make([]byte, 1)); n != 1 || err != nil {
				t.Fatalf("read = %d, %v", n, err)
			}
			if !g.verified.Equal(test.want) {
				t.Fatalf("proof = %v, want %v", g.verified, test.want)
			}
			// Later reads of that same stream never extend the evidence or
			// revive it after a subsequent transport failure.
			g.invalidatePathProof()
			lane.Read(make([]byte, 1))
			if !g.verified.IsZero() {
				t.Fatal("another read on an old stream revived invalidated proof")
			}
		})
	}
}

func TestPooledStreamEmptyReadDoesNotConsumeRemoteProof(t *testing.T) {
	for _, emptyErr := range []error{nil, io.EOF} {
		g := &controlQUICGeneration{}
		stream := &proofReadQUICStream{read: func([]byte) (int, error) { return 0, emptyErr }}
		lane := &controlPoolStreamConn{
			quicStreamConn: &quicStreamConn{stream: stream}, generation: g,
			proofStarted: time.Now(),
		}
		lane.Read(make([]byte, 1))
		if !g.verified.IsZero() {
			t.Fatal("an empty read was mistaken for remote proof")
		}
		// A final positive read can return data and EOF together.
		stream.read = func(p []byte) (int, error) { return copy(p, "x"), io.EOF }
		lane.Read(make([]byte, 1))
		if !g.verified.Equal(lane.proofStarted) {
			t.Fatal("an empty read consumed the first positive response")
		}
	}
}

// Teardown from an older stream can arrive after a sibling has proved the
// path. Only known stream lifecycle outcomes may retain that proof; actual
// timeouts and unclassified errors must still make the next borrower probe.
func TestPooledStreamFailurePathProof(t *testing.T) {
	for _, test := range []struct {
		name              string
		failure           func(*testing.T, *controlPoolStreamConn) error
		err               error
		invalidate        bool
		equalProofStarted bool
	}{
		{name: "remote EOF", failure: pooledStreamRemoteEOF},
		{name: "wrapped remote EOF", failure: func(t *testing.T, stream *controlPoolStreamConn) error {
			return fmt.Errorf("lane read: %w", pooledStreamRemoteEOF(t, stream))
		}},
		{name: "local zero-code cancellation", failure: func(t *testing.T, stream *controlPoolStreamConn) error {
			stream.stream.CancelRead(0)
			_, err := stream.Read(make([]byte, 1))
			var streamErr *quic.StreamError
			if !errors.As(err, &streamErr) || streamErr.Remote || streamErr.ErrorCode != 0 {
				t.Fatalf("local close error = %v, want local code-0 stream cancellation", err)
			}
			return err
		}},
		{name: "remote zero-code cancellation", err: &quic.StreamError{Remote: true}},
		{name: "wrapped remote zero-code cancellation", err: fmt.Errorf("lane write: %w", &quic.StreamError{Remote: true})},
		{name: "remote nonzero cancellation", err: &quic.StreamError{Remote: true, ErrorCode: 1}, invalidate: true},
		{name: "local nonzero cancellation", err: &quic.StreamError{ErrorCode: 1}, invalidate: true},
		{name: "unknown failure", err: errors.New("lane writer stopped"), invalidate: true},
		{name: "truncated frame", err: io.ErrUnexpectedEOF, invalidate: true},
		{name: "parser missing payload", invalidate: true, failure: func(t *testing.T, _ *controlPoolStreamConn) error {
			encoded, err := protocol.AppendFrame(nil, protocol.Frame{
				Header: protocol.Header{Version: protocol.Version, Type: protocol.TypeData}, Payload: []byte("payload"),
			})
			if err != nil {
				t.Fatal(err)
			}
			_, err = protocol.ReadFrame(bytes.NewReader(encoded[:protocol.HeaderSize]))
			if err == nil {
				t.Fatal("parser accepted a frame with its entire payload missing")
			}
			return fmt.Errorf("lane read: %w", err)
		}},
		{name: "EOF with unknown failure", err: errors.Join(io.EOF, errors.New("lane writer stopped")), invalidate: true},
		{name: "zero-code cancellation with unknown failure", err: errors.Join(&quic.StreamError{Remote: true}, errors.New("lane writer stopped")), invalidate: true},
		{name: "wrapped timeout", err: fmt.Errorf("lane read: %w", context.DeadlineExceeded), invalidate: true},
		{name: "timeout with EOF", err: errors.Join(io.EOF, context.DeadlineExceeded), invalidate: true},
		{name: "stream read timeout", invalidate: true, failure: func(t *testing.T, stream *controlPoolStreamConn) error {
			if err := stream.SetDeadline(time.Now().Add(-time.Second)); err != nil {
				t.Fatal(err)
			}
			_, err := stream.Read(make([]byte, 1))
			if !pooledTransportTimedOut(err) {
				t.Fatalf("expired stream read = %v, want timeout", err)
			}
			return err
		}},
		{name: "equal timestamps remote EOF", failure: pooledStreamRemoteEOF, equalProofStarted: true},
		{name: "equal timestamps unknown failure", err: errors.New("lane writer stopped"), invalidate: true, equalProofStarted: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			rig := newJoinTestRig(t, TransportQUIC, TransportQUIC, 1)
			c := rig.client
			t.Cleanup(c.closeQUICPool)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			cc := congestionConfig{kind: defaultCongestion()}
			old, err := c.dialPooledQUICLane(ctx, cc)
			if err != nil {
				t.Fatal(err)
			}
			defer old.Close()
			oldStream := old.(*controlPoolStreamConn)
			pooledStreamProofRoundTrip(t, oldStream)
			sibling, err := c.dialPooledQUICLane(ctx, cc)
			if err != nil {
				t.Fatal(err)
			}
			defer sibling.Close()
			siblingStream := sibling.(*controlPoolStreamConn)
			if test.equalProofStarted {
				// Sequential stream creation can share a timestamp on coarse
				// clocks. Exercise that case deterministically before the real
				// authenticated response records the sibling's proof.
				siblingStream.proofStarted = oldStream.proofStarted
			}
			pooledStreamProofRoundTrip(t, siblingStream)
			g := oldStream.generation
			g.probeMu.Lock()
			verified, epoch := g.verified, g.proofEpoch
			g.probeMu.Unlock()
			if siblingStream.generation != g {
				t.Fatal("sibling unexpectedly used a different pooled generation")
			}
			if !verified.Equal(siblingStream.proofStarted) || verified.Before(oldStream.proofStarted) {
				t.Fatalf("sibling proof = %v, sibling creation = %v, old creation = %v; want exact nondecreasing proof",
					verified, siblingStream.proofStarted, oldStream.proofStarted)
			}
			if oldStream.proofEpoch != epoch || siblingStream.proofEpoch != epoch {
				t.Fatalf("proof epochs: old=%d sibling=%d generation=%d; want one unchanged epoch",
					oldStream.proofEpoch, siblingStream.proofEpoch, epoch)
			}

			failure := test.err
			if test.failure != nil {
				failure = test.failure(t, oldStream)
			}
			oldStream.transportFailed(failure)
			g.probeMu.Lock()
			after, afterEpoch := g.verified, g.proofEpoch
			g.probeMu.Unlock()
			if test.invalidate {
				if !after.IsZero() || afterEpoch != epoch+1 {
					t.Fatalf("failure retained path proof: verified=%v, epoch=%d (was %d)", after, afterEpoch, epoch)
				}
			} else if !after.Equal(verified) || afterEpoch != epoch {
				t.Fatalf("stream teardown discarded newer sibling proof: verified=%v (was %v), epoch=%d (was %d)", after, verified, afterEpoch, epoch)
			}
			if g.conn.Context().Err() != nil {
				t.Fatal("stream-local outcome closed sibling connection")
			}

			var probes atomic.Int32
			var bounded atomic.Bool
			c.probeControlGenerationForTest = func(probeCtx context.Context, generation *controlQUICGeneration, budget time.Duration) error {
				probes.Add(1)
				deadline, ok := probeCtx.Deadline()
				bounded.Store(ok && time.Until(deadline) > 0 && time.Until(deadline) <= c.cfg.HandshakeTimeout && budget == c.cfg.HandshakeTimeout)
				return probeControlGeneration(probeCtx, generation, budget)
			}
			next, err := c.dialPooledQUICLane(ctx, cc)
			if err != nil {
				t.Fatalf("borrow after stream-local outcome: %v", err)
			}
			defer next.Close()
			if next.(*controlPoolStreamConn).generation != g {
				t.Fatal("stream-local outcome replaced healthy generation")
			}
			if test.invalidate {
				if probes.Load() != 1 || !bounded.Load() {
					t.Fatalf("next borrow used %d probes, bounded=%v; want one bounded probe", probes.Load(), bounded.Load())
				}
			} else if got := probes.Load(); got != 0 {
				t.Fatalf("ordinary stream teardown charged next borrower %d unnecessary probes", got)
			}
		})
	}
}

func pooledStreamProofRoundTrip(t *testing.T, stream *controlPoolStreamConn) {
	t.Helper()
	if err := stream.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	fc := newFrameConn(stream)
	frame := protocol.Frame{Header: protocol.Header{
		Version: protocol.Version, Type: protocol.TypeProbe,
		SessionID: [16]byte{1}, Class: protocol.ClassNew,
	}, Payload: []byte{1}}
	if err := fc.Write(frame); err != nil {
		t.Fatal(err)
	}
	response, err := fc.Read()
	if err != nil || response.Header.SessionID != frame.Header.SessionID || response.Header.Type != frame.Header.Type {
		t.Fatalf("proof response = %+v, error = %v", response.Header, err)
	}
}

func pooledStreamRemoteEOF(t *testing.T, stream *controlPoolStreamConn) error {
	t.Helper()
	if err := stream.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	_, err := stream.Read(make([]byte, 1))
	if !errors.Is(err, io.EOF) {
		t.Fatalf("finished remote stream = %v, want EOF", err)
	}
	return err
}

func TestPooledConnectionFailureRetiresEvenAfterStreamEOF(t *testing.T) {
	rig := newJoinTestRig(t, TransportQUIC, TransportQUIC, 1)
	c := rig.client
	t.Cleanup(c.closeQUICPool)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cc := congestionConfig{kind: defaultCongestion()}
	outer, err := c.dialPooledQUICLane(ctx, cc)
	if err != nil {
		t.Fatal(err)
	}
	defer outer.Close()
	old := outer.(*controlPoolStreamConn)
	g := old.generation
	if err := g.conn.CloseWithError(0, "test connection failure"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-g.conn.Context().Done():
	case <-ctx.Done():
		t.Fatal("failed connection context was not canceled")
	}
	old.transportFailed(fmt.Errorf("late stream EOF: %w", io.EOF))
	c.quicMu.Lock()
	current := c.quicGeneration
	c.quicMu.Unlock()
	if current != nil {
		t.Fatal("stream EOF hid a failed pooled connection")
	}
	next, err := c.dialPooledQUICLane(ctx, cc)
	if err != nil {
		t.Fatal(err)
	}
	defer next.Close()
	fresh := next.(*controlPoolStreamConn).generation
	old.transportFailed(&quic.StreamError{Remote: true})
	c.quicMu.Lock()
	current = c.quicGeneration
	c.quicMu.Unlock()
	if fresh == g || current != fresh || fresh.conn.Context().Err() != nil {
		t.Fatal("late stream failure affected replacement generation")
	}
}
