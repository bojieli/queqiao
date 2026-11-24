package pep

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"
)

// A JOIN interrupted because its flow's manager ended is cancellation, not a
// handshake timeout, even though bindHandshake interrupts it by moving the
// socket deadline to now (issue #157). It must be classified `canceled` and,
// with the manager's context gone, not logged at all on either transport's
// recovery line.
func TestCancelledLaneJoinIsCanceledAndNotLogged(t *testing.T) {
	for _, readJoin := range []bool{false, true} {
		t.Run(map[bool]string{false: "blocked-write", true: "blocked-read"}[readJoin], func(t *testing.T) {
			local, remote := net.Pipe()
			t.Cleanup(func() { _ = local.Close(); _ = remote.Close() })
			manageCtx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if readJoin {
				// The peer takes the JOIN and never answers it.
				go func() { _, _ = newFrameConn(remote).Read(); cancel() }()
			} else {
				// The peer never reads, so the JOIN write itself blocks.
				time.AfterFunc(20*time.Millisecond, cancel)
			}
			var logs bytes.Buffer
			client := &Client{cfg: ClientConfig{
				HandshakeTimeout: 10 * time.Second,
				Logger:           slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
			}}
			done := make(chan error, 1)
			go func() {
				_, err := client.completeLaneJoin(manageCtx, &authenticatedLane{
					fc: newFrameConn(local), outer: local, sessionID: [16]byte{1}, kind: TransportTCP, laneID: 1,
				}, 7, 0)
				done <- err
			}()
			var err error
			select {
			case err = <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("JOIN ignored cancellation")
			}
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("err = %v, want context.Canceled", err)
			}
			var netErr net.Error
			if !errors.As(err, &netErr) || !netErr.Timeout() {
				t.Fatalf("err = %v, want the interrupted I/O error kept as its cause", err)
			}
			if got := replacementFailureKind(err); got != "canceled" {
				t.Fatalf("failure_kind = %q, want canceled", got)
			}
			client.logTCPJoinFailure(manageCtx, 7, laneJoinResult{id: 1, err: err, replacement: true, attempt: 1}, -1)
			client.logLaneRecoveryFailure(manageCtx, 7, 1, err)
			if logs.Len() != 0 {
				t.Fatalf("logged after the manager's context ended:\n%s", logs.String())
			}
		})
	}
}

// A handshake that runs out its own budget while the flow is still live is
// the case `timeout` exists for, and stays one.
func TestExpiredLaneJoinOnLiveContextStaysTimeout(t *testing.T) {
	local, remote := net.Pipe()
	t.Cleanup(func() { _ = local.Close(); _ = remote.Close() })
	go func() { _, _ = newFrameConn(remote).Read() }()
	var logs bytes.Buffer
	client := &Client{cfg: ClientConfig{
		HandshakeTimeout: 50 * time.Millisecond,
		Logger:           slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
	}}
	manageCtx := context.Background()
	_, err := client.completeLaneJoin(manageCtx, &authenticatedLane{
		fc: newFrameConn(local), outer: local, sessionID: [16]byte{1}, kind: TransportTCP, laneID: 1,
	}, 7, 0)
	if err == nil || errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want a handshake timeout", err)
	}
	if got := replacementFailureKind(err); got != "timeout" {
		t.Fatalf("failure_kind = %q, want timeout", got)
	}
	client.logTCPJoinFailure(manageCtx, 7, laneJoinResult{id: 1, err: err, replacement: true, attempt: 1}, -1)
	if line := logs.String(); !strings.Contains(line, "level=INFO") || !strings.Contains(line, "failure_kind=timeout") {
		t.Fatalf("log = %q, want an INFO failure_kind=timeout line", line)
	}
}
