package pep

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/apernet/quic-go"
	"github.com/bojieli/queqiao/internal/identity"
	"github.com/bojieli/queqiao/internal/pathmodel"
	"github.com/bojieli/queqiao/internal/protocol"
)

// Count actual DATAGRAMs at the peer. Concurrent OPENs must share a single
// allowance, and no payload or reliable stream may reach the application.
func TestPooledLossPulsesShareOneConnectionBudget(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	serverCred, clientCred := testCertificate(t)
	serverTLS, err := identity.ServerTLSConfig(serverCred, protocol.DataALPN, false)
	if err != nil {
		t.Fatal(err)
	}
	clientTLS, err := identity.ClientTLSConfig(clientCred, protocol.DataALPN)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := quic.ListenAddr("127.0.0.1:0", serverTLS, quicConfig(flowWindows{}))
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	client, err := quic.DialAddr(ctx, listener.Addr().String(), clientTLS, quicConfig(flowWindows{}))
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseWithError(0, "done")
	server, err := listener.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	model := pathmodel.NewPathModel()
	g := &controlQUICGeneration{conn: client, lossPath: model}
	if stop := g.startLossPulse(ctx); stop != nil {
		t.Fatal("unmeasured path sent a pulse")
	}
	model.Report(1, pathmodel.Observation{Erasure: .01, ObservedSamples: 500})
	if stop := g.startLossPulse(ctx); stop != nil {
		t.Fatal("clean path sent a pulse")
	}
	model.Report(1, pathmodel.Observation{Erasure: .45, ObservedSamples: 500})
	now := time.Now()
	var lanes [32]controlPoolStreamConn
	for i := range lanes {
		lanes[i].generation = g
		lanes[i].trackOpen(now.Add(time.Minute))
	}
	if !g.awaitingOpen(now) || g.awaitingOpen(now.Add(time.Minute)) {
		t.Fatal("OPEN grace outlived the original deadline")
	}
	for i := range len(lanes) - 1 {
		lanes[i].finishOpen()
	}
	if !g.awaitingOpen(now) {
		t.Fatal("one stream cancelled its sibling's grace")
	}
	deadline, stop := context.WithTimeout(ctx, lossPulseDuration+150*time.Millisecond)
	defer stop()
	count := 0
	for {
		msg, err := server.ReceiveDatagram(deadline)
		if errors.Is(err, context.DeadlineExceeded) {
			break
		}
		if err != nil || len(msg) != 0 {
			t.Fatalf("unexpected payload %x or error %v", msg, err)
		}
		count++
	}
	if count == 0 || count > lossPulseMaxDatagrams {
		t.Fatalf("received %d datagrams for %d concurrent opens", count, len(lanes))
	}
	lanes[len(lanes)-1].finishOpen()
	if g.awaitingOpen(now) || len(g.pendingOpens) != 0 {
		t.Fatal("finished OPEN retained its grace")
	}
	quiet, end := context.WithTimeout(ctx, 80*time.Millisecond)
	defer end()
	if _, err := server.AcceptStream(quiet); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unexpected application stream: %v", err)
	}
}

func TestPoolRotationGraceEndsOnProofCloseOrDeadline(t *testing.T) {
	deadline := time.Unix(100, 0)
	g := &controlQUICGeneration{borrowers: 2, pendingOpens: make(map[*controlPoolStreamConn]pendingOpen)}
	c := &Client{}
	stream := &proofReadQUICStream{read: func(buf []byte) (int, error) { return copy(buf, "x"), nil }}
	proved := &controlPoolStreamConn{owner: c, generation: g, quicStreamConn: &quicStreamConn{stream: stream}}
	closing := &controlPoolStreamConn{owner: c, generation: g, quicStreamConn: &quicStreamConn{stream: &closeTrackingQUICStream{}}}
	pulseCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pulse := &controlLossPulse{waiters: 2, cancel: cancel}
	g.lossPulse = pulse
	for _, lane := range []*controlPoolStreamConn{proved, closing} {
		g.pendingOpens[lane] = pendingOpen{deadline: deadline, stop: func() { g.releaseLossPulse(pulse) }}
	}
	if !g.awaitingOpen(deadline.Add(-time.Nanosecond)) || g.awaitingOpen(deadline) || g.awaitingOpen(deadline.Add(time.Nanosecond)) {
		t.Fatal("grace must end exactly at the existing deadline")
	}
	if _, err := proved.Read(make([]byte, 1)); err != nil {
		t.Fatal(err)
	}
	if len(g.pendingOpens) != 1 || pulse.waiters != 1 || pulseCtx.Err() != nil {
		t.Fatal("first remote response did not release only its own waiting OPEN")
	}
	if err := proved.Close(); err != nil {
		t.Fatal(err)
	}
	if pulse.waiters != 1 {
		t.Fatal("close counted the already confirmed stream twice")
	}
	if err := closing.Close(); err != nil {
		t.Fatal(err)
	}
	if len(g.pendingOpens) != 0 || pulseCtx.Err() != context.Canceled || g.awaitingOpen(deadline.Add(-time.Second)) {
		t.Fatal("closing last unconfirmed stream retained recovery work")
	}
}
