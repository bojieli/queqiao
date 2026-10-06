package pep

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"net"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apernet/quic-go"
	"github.com/bojieli/queqiao/internal/coded"
	"github.com/bojieli/queqiao/internal/fec"
	"github.com/bojieli/queqiao/internal/identity"
	"github.com/bojieli/queqiao/internal/metrics"
	"github.com/bojieli/queqiao/internal/pathmodel"
	"github.com/bojieli/queqiao/internal/protocol"
)

// codedMetricsCarrier removes specified datagrams before the real decoder.
// Its bounded, reliable in-memory queue introduces no additional random loss.
type codedMetricsCarrier struct {
	in, out chan []byte
	done    chan struct{}
	once    sync.Once
	drop    func([]byte) bool
}

func (c *codedMetricsCarrier) Send(datagram []byte) error {
	if c.drop != nil && c.drop(datagram) {
		return nil
	}
	select {
	case c.out <- bytes.Clone(datagram):
		return nil
	case <-c.done:
		return io.EOF
	}
}

func (c *codedMetricsCarrier) Receive() ([]byte, error) {
	select {
	case datagram := <-c.in:
		return datagram, nil
	case <-c.done:
		return nil, io.EOF
	}
}

func (c *codedMetricsCarrier) Close() error {
	c.once.Do(func() { close(c.done) })
	return nil
}

// The end-to-end degrading-channel test may finish with erased symbols still
// inside the decoder window. This test establishes both completed outcomes:
// one source is repaired, another remains missing until the window evicts it.
// Neither outcome is injected into metrics: the real coded path, QUIC lane
// statistics, pooled connection deltas and registry have to carry it through.
func TestCodedReceiveOutcomesReachConnectionMetrics(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Only the connection identity and transport statistics need a live QUIC
	// connection; deterministic erasure happens in the coded carrier below.
	serverCredentials, clientCredentials := testCertificate(t)
	serverTLS, err := identity.ServerTLSConfig(serverCredentials, protocol.DataALPN, false)
	if err != nil {
		t.Fatal(err)
	}
	clientTLS, err := identity.ClientTLSConfig(clientCredentials, protocol.DataALPN)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := quic.ListenAddr("127.0.0.1:0", serverTLS, quicConfig(flowWindows{}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	conn, err := quic.DialAddr(ctx, listener.Addr().String(), clientTLS, quicConfig(flowWindows{}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.CloseWithError(0, "test complete") })

	forward, reverse := make(chan []byte, 1024), make(chan []byte, 1)
	var blockRepairs atomic.Bool
	sourceCount := 0
	sendCarrier := &codedMetricsCarrier{
		in: reverse, out: forward, done: make(chan struct{}),
		drop: func(datagram []byte) bool {
			// Protocol 2's kind follows the four-byte transmission sequence.
			if datagram[4] == 1 {
				return blockRepairs.Load()
			}
			sourceCount++
			return sourceCount <= 2
		},
	}
	receiveCarrier := &codedMetricsCarrier{in: forward, out: reverse, done: make(chan struct{})}
	model := pathmodel.NewPathModel()
	model.Report(1, pathmodel.Observation{Erasure: 0.4, BurstFactor: 1, ObservedSamples: 5000})
	sender := coded.New(sendCarrier, coded.Config{SymbolBytes: 1100, Path: model})
	receiver := coded.New(receiveCarrier, coded.Config{})
	t.Cleanup(func() { _ = sender.Close(); _ = receiver.Close() })

	frames := make(chan []byte, fec.MinDecoderWidth+2)
	go func() {
		defer close(frames)
		for {
			frame, err := receiver.Receive()
			if err != nil {
				return
			}
			select {
			case frames <- frame:
			case <-ctx.Done():
				return
			}
		}
	}()
	payload := func(index uint32) []byte {
		// Two frames cannot fit in one 1100-byte symbol, so every source
		// identifier corresponds to exactly one frame in this test.
		frame := bytes.Repeat([]byte{0x5a}, 900)
		binary.BigEndian.PutUint32(frame, index)
		return frame
	}
	send := func(index uint32) {
		t.Helper()
		if err := sender.SendContext(ctx, payload(index)); err != nil {
			t.Fatalf("send frame %d: %v", index, err)
		}
	}
	receive := func(index uint32) {
		t.Helper()
		select {
		case frame, ok := <-frames:
			if !ok || !bytes.Equal(frame, payload(index)) {
				t.Fatalf("frame %d was not delivered intact", index)
			}
		case <-ctx.Done():
			t.Fatalf("receive frame %d: %v", index, ctx.Err())
		}
	}

	registry := metrics.New()
	local, remote := net.Pipe()
	t.Cleanup(func() { _ = local.Close(); _ = remote.Close() })
	flow := newMultipathFlow(ctx, local, [16]byte{1}, 7, 0, 0, 0, nil, registry)
	for id := uint64(0); id < 3; id++ {
		// These lanes are observed but never started or used for stream I/O.
		transport := &quicStreamConn{conn: conn, bulk: receiver}
		flow.lanes[id] = &mpLane{id: id, kind: TransportQUIC, fc: newFrameConn(transport)}
	}
	check := func(sources, recovered, lost uint64) {
		t.Helper()
		raw := receiver.Stats()
		if raw.Sources != sources || raw.Recovered != recovered || raw.Lost != lost {
			t.Fatalf("decoder outcomes = %d/%d/%d, want %d/%d/%d",
				raw.Sources, raw.Recovered, raw.Lost, sources, recovered, lost)
		}
		// Three lanes on one connection and repeated observations must bank
		// each outcome once, including outcomes finalized after an earlier poll.
		flow.snapshot()
		flow.snapshot()
		got := registry.Snapshot()
		if got.QUICCodedSources != sources || got.QUICCodedRecovered != recovered || got.QUICCodedLost != lost {
			t.Fatalf("published outcomes = %d/%d/%d, want %d/%d/%d",
				got.QUICCodedSources, got.QUICCodedRecovered, got.QUICCodedLost, sources, recovered, lost)
		}
		total := float64(sources + recovered + lost)
		if math.Abs(got.ReceiveErasure()-float64(recovered+lost)/total) > 1e-12 ||
			math.Abs(got.ReceiveResidual()-float64(lost)/total) > 1e-12 {
			t.Fatalf("published receive ratios = %v/%v", got.ReceiveErasure(), got.ReceiveResidual())
		}
		recorder := httptest.NewRecorder()
		registry.ServeHTTP(recorder, httptest.NewRequest("GET", "/metrics", nil))
		for _, want := range []string{
			fmt.Sprintf(`queqiao_coded_symbols_total{outcome="arrived"} %d`, sources),
			fmt.Sprintf(`queqiao_coded_symbols_total{outcome="recovered"} %d`, recovered),
			fmt.Sprintf(`queqiao_coded_symbols_total{outcome="lost"} %d`, lost),
			fmt.Sprintf(`queqiao_erasure_ratio{direction="receive"} %.9f`, float64(recovered+lost)/total),
			fmt.Sprintf(`queqiao_erasure_residual_ratio{direction="receive"} %.9f`, float64(lost)/total),
		} {
			if !strings.Contains(recorder.Body.String(), want+"\n") {
				t.Errorf("metrics exposition is missing %q", want)
			}
		}
	}

	// The first source is erased, but its tail repair must reconstruct it.
	send(0)
	receive(0)
	check(0, 1, 0)

	// The next source is also erased and now every repair is erased. A newer
	// source proves the gap exists, but it is still pending, not finalized.
	blockRepairs.Store(true)
	send(1)
	send(2)
	receive(2)
	check(1, 1, 0)

	// Advance just far enough to evict source 1 from the real decoder window.
	for index := uint32(3); index < fec.MinDecoderWidth+2; index++ {
		send(index)
	}
	for index := uint32(3); index < fec.MinDecoderWidth+2; index++ {
		receive(index)
	}
	check(fec.MinDecoderWidth, 1, 1)
}
