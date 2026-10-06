package pep

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// A delivered request can legitimately wait for application work or a push
// message. Delivery acknowledgements, not application response timing, are
// the evidence available to a transport watchdog.
func TestAcknowledgedRequestWaitingForResponseDoesNotRescue(t *testing.T) {
	flow := newStallTestFlow(t, nil)
	flow.observe(64, true)
	flow.bytesUp.Add(64)
	flow.noteSent(0, 64)
	if err := flow.acknowledgeReplay(64, false); err != nil {
		t.Fatal(err)
	}
	if flow.pendingOutbound() {
		t.Fatal("request was not acknowledged")
	}
	flow.stallScan = 5 * time.Millisecond
	flow.stallGrace = 20 * time.Millisecond
	stop := make(chan struct{})
	defer close(stop)
	go flow.stallWatchdog(stop)
	select {
	case <-flow.stallSignals():
		t.Fatal("an acknowledged request waiting on its application triggered lane rescue")
	case <-time.After(120 * time.Millisecond):
	}
}

// A TCP JOIN changes serverFlow.tcpMode and retires its QUIC lanes before
// OPEN_OK reaches the client. No QUIC contender may be crowned beside that
// irreversible handoff, even when the TCP acknowledgement is slower.
func TestAutoRescueDoesNotRaceTCPHandoffWithQUIC(t *testing.T) {
	_, credentials := testCertificate(t)
	tcpStarted := make(chan struct{})
	releaseTCP := make(chan struct{})
	quicStarted := make(chan struct{}, 8)
	var started, release sync.Once
	t.Cleanup(func() { release.Do(func() { close(releaseTCP) }) })
	dialErr := errors.New("test stops before sending packets")
	client, err := NewClient(ClientConfig{
		ListenAddr: "127.0.0.1:0", RemoteAddr: "127.0.0.1:9",
		LocalAddress: "127.0.0.1", Credentials: credentials,
		Transport: TransportAuto, DialTimeout: time.Second,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		SocketControl: func(network, _ string, _ syscall.RawConn) error {
			if strings.HasPrefix(network, "tcp") {
				started.Do(func() { close(tcpStarted) })
				<-releaseTCP
			} else {
				quicStarted <- struct{}{}
			}
			return dialErr
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	flow := newGraceTestFlow(t) // no shared generation: AUTO commits to TCP
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- client.openParallelRescue(ctx, flow, flow.sessionID, flow.flowID) }()
	select {
	case <-tcpStarted:
	case <-ctx.Done():
		t.Fatal("TCP recovery did not start")
	}
	raced := false
	select {
	case <-quicStarted:
		raced = true
	case <-time.After(100 * time.Millisecond):
	}
	release.Do(func() { close(releaseTCP) })
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("injected dial failure unexpectedly succeeded")
		}
	case <-ctx.Done():
		t.Fatal("recovery did not return")
	}
	if raced {
		t.Fatal("AUTO launched a QUIC contender alongside the irreversible TCP handoff")
	}
}
