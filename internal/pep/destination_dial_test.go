package pep

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestDestinationDialDoesNotLetFirstAddressExhaustDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	winner, peer := net.Pipe()
	defer winner.Close()
	defer peer.Close()
	started := time.Now()
	conn, err := dialDestinationCandidates(ctx, []string{"first", "second"}, func(ctx context.Context, address string) (net.Conn, error) {
		if address == "first" {
			<-ctx.Done() // A route which silently drops SYNs, rather than refusing them.
			return nil, ctx.Err()
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return winner, nil
	})
	if err != nil || conn != winner {
		t.Fatalf("reachable second address starved behind failed first address: %v", err)
	}
	if elapsed := time.Since(started); elapsed > 800*time.Millisecond {
		t.Fatalf("fallback took %v of a one-second total budget", elapsed)
	}
	t.Logf("reachable second address connected in %v", time.Since(started))
}

func TestDestinationDialBoundsConcurrencyAndReachesLaterAddresses(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	winner, peer := net.Pipe()
	defer winner.Close()
	defer peer.Close()
	var active, peak atomic.Int32
	conn, err := dialDestinationCandidates(ctx, []string{"first", "second", "third", "fourth"}, func(ctx context.Context, address string) (net.Conn, error) {
		n := active.Add(1)
		defer active.Add(-1)
		for old := peak.Load(); n > old && !peak.CompareAndSwap(old, n); old = peak.Load() {
		}
		if address == "third" {
			return winner, nil
		}
		<-ctx.Done()
		return nil, ctx.Err()
	})
	if err != nil || conn != winner || peak.Load() > 2 {
		t.Fatalf("later candidate unreachable or socket bound exceeded: conn=%v err=%v peak=%d", conn, err, peak.Load())
	}
}

type destinationObservedConn struct {
	net.Conn
	closed chan struct{}
	once   sync.Once
}

func (c *destinationObservedConn) Close() error {
	c.once.Do(func() { close(c.closed) })
	return c.Conn.Close()
}

func TestDestinationDialClosesLateSuccessfulLoser(t *testing.T) {
	winner, peer := net.Pipe()
	defer winner.Close()
	defer peer.Close()
	l, lp := net.Pipe()
	defer lp.Close()
	loser := &destinationObservedConn{Conn: l, closed: make(chan struct{})}
	defer loser.Close()
	release := make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := dialDestinationCandidates(ctx, []string{"first", "second"}, func(_ context.Context, address string) (net.Conn, error) {
		if address == "first" {
			<-release // Completion racing with cancellation must close this socket.
			return loser, nil
		}
		return winner, nil
	})
	close(release)
	if err != nil || conn != winner {
		t.Fatalf("fallback failed: %v", err)
	}
	select {
	case <-loser.closed:
	case <-time.After(time.Second):
		t.Fatal("successful losing connection leaked")
	}
}

func TestDestinationDialDoesNotSpeculateAfterImmediateSuccess(t *testing.T) {
	winner, peer := net.Pipe()
	defer winner.Close()
	defer peer.Close()
	var calls atomic.Int32
	conn, err := dialDestinationCandidates(context.Background(), []string{"first", "second"}, func(context.Context, string) (net.Conn, error) {
		calls.Add(1)
		return winner, nil
	})
	if err != nil || conn != winner || calls.Load() != 1 {
		t.Fatalf("healthy first address dialed %d times: %v", calls.Load(), err)
	}
}

func TestDestinationDialHonorsCancellationAndPrivatePolicy(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := dialDestinationCandidates(ctx, []string{"first", "second"}, func(context.Context, string) (net.Conn, error) {
		t.Error("dial attempted after cancellation")
		return nil, errors.New("unexpected dial")
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled dial: %v", err)
	}
	if conn, err := (DestinationPolicy{}).DialContext(context.Background(), "127.0.0.1:443"); err == nil || conn != nil {
		t.Fatal("private TCP destination accepted")
	}
}
