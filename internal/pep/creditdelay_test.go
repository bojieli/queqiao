package pep

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"net/http/httptest"
	"testing"
	"time"

	wancongestion "github.com/bojieli/queqiao/internal/congestion"
	"github.com/bojieli/queqiao/internal/metrics"
)

// One sample: from the carrier taking a chunk to the acknowledgement that
// covers it, minus the path's minimum round trip as the transport measures
// it. An acknowledgement that does not cover the chunk leaves the sample
// open, a second take while it is open does not restart it, and neither a
// TCP lane, a coded write, a re-issue nor a chunk already acknowledged
// starts one.
func TestCreditDelayIsTimedFromTheCarriersTakeToTheCoveringACK(t *testing.T) {
	registry := metrics.New()
	local, remote := net.Pipe()
	t.Cleanup(func() { _ = local.Close(); _ = remote.Close() })
	flow := newMultipathFlow(context.Background(), local, [16]byte{1}, 7, 0, 0, 0, nil, registry)
	const minRTT = 40 * time.Millisecond
	// The controller reports a much larger (smoothed or fallback) figure,
	// and Reno none at all: the baseline is the transport's own minimum.
	quic := &mpLane{id: 0, kind: TransportQUIC, fc: newFrameConn(&statsConn{
		stats: laneTransportStats{minRTT: minRTT, controller: wancongestion.ControllerTelemetry{MinRTT: 200 * time.Millisecond}},
	})}
	tcp := &mpLane{id: 1, kind: TransportTCP, fc: newFrameConn(&statsConn{
		stats: laneTransportStats{minRTT: minRTT},
	})}

	flow.noteSent(0, 8000)
	flow.noteCarrierTook(tcp, 0, 1000, true)
	if flow.credit.pending.Load() {
		t.Fatal("a TCP lane started a sample")
	}
	flow.noteCarrierTook(quic, 1000, 1000, false)
	if flow.credit.pending.Load() {
		t.Fatal("a coded write started a sample")
	}
	flow.noteCarrierTook(quic, 2000, 1000, true)
	time.Sleep(20 * time.Millisecond)
	flow.noteCarrierTook(quic, 3000, 1000, true) // not a restart
	if _, err := flow.acknowledgeCoverage(2500, false, nil); err != nil {
		t.Fatal(err)
	}
	if n := registry.Snapshot().CreditDelay.Count(); n != 0 {
		t.Fatalf("an acknowledgement short of the chunk recorded %d samples", n)
	}
	time.Sleep(100 * time.Millisecond)
	if _, err := flow.acknowledgeCoverage(3000, false, nil); err != nil {
		t.Fatal(err)
	}
	got := registry.Snapshot().CreditDelay
	if got.Count() != 1 {
		t.Fatalf("recorded %d samples, want 1", got.Count())
	}
	// About 120 ms since the take, less the transport's 40 ms minimum:
	// about 80 ms. The controller's 200 ms would have made it zero.
	if got.Sum < 60*time.Millisecond || got.Sum > 400*time.Millisecond {
		t.Fatalf("credit delay %v, want about 80ms (take to ACK less the minimum round trip)", got.Sum)
	}
	if flow.credit.pending.Load() {
		t.Fatal("the completed sample is still pending")
	}
	// A re-issue of bytes a carrier already took (here the chunk at 3000,
	// still unacknowledged) starts nothing: it would time the repair.
	flow.noteCarrierTook(quic, 3000, 1000, true)
	if flow.credit.pending.Load() {
		t.Fatal("a re-issue started a sample")
	}
	// Selective coverage completes a sample too.
	flow.noteCarrierTook(quic, 5000, 1000, true)
	if _, err := flow.acknowledgeCoverage(3000, false, [][2]uint64{{5000, 6000}}); err != nil {
		t.Fatal(err)
	}
	if n := registry.Snapshot().CreditDelay.Count(); n != 2 {
		t.Fatalf("a selective acknowledgement of the chunk left %d samples, want 2", n)
	}
	// A chunk already acknowledged arms nothing, even as new bytes.
	if _, err := flow.acknowledgeCoverage(3000, false, [][2]uint64{{5000, 7000}}); err != nil {
		t.Fatal(err)
	}
	flow.noteCarrierTook(quic, 6000, 1000, true)
	if flow.credit.pending.Load() {
		t.Fatal("a chunk already covered armed a sample")
	}
	// A re-issue may complete a sample armed by the original.
	flow.noteCarrierTook(quic, 7000, 1000, true)
	flow.noteCarrierTook(quic, 7000, 1000, true)
	if _, err := flow.acknowledgeCoverage(8000, false, nil); err != nil {
		t.Fatal(err)
	}
	if n := registry.Snapshot().CreditDelay.Count(); n != 3 {
		t.Fatalf("the original's sample was not completed (%d samples, want 3)", n)
	}
}

// End to end, through a real client and gateway: an upload is timed by the
// client and a download by the gateway, each on its own clock, and both
// export the histogram.
func TestBothEndpointsExportCreditDelay(t *testing.T) {
	// Under every controller: Reno installs none of its own telemetry, so
	// the baseline must come from the transport.
	for _, congestion := range []CongestionControlKind{CongestionErasure, CongestionReno} {
		t.Run(string(congestion), func(t *testing.T) { exportsCreditDelay(t, congestion) })
	}
}

func exportsCreditDelay(t *testing.T, congestion CongestionControlKind) {
	destinationListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer destinationListener.Close()
	go echoDestination(destinationListener)

	certificate, roots := testCertificate(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	packetConn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serverMetrics, clientMetrics := metrics.New(), metrics.New()
	server, err := NewServer(ServerConfig{
		ListenAddr: "127.0.0.1:0", Credentials: certificate,
		DestinationPolicy: DestinationPolicy{AllowPrivate: true}, EnableQUIC: true, Logger: logger,
		Metrics: serverMetrics, Congestion: congestion,
	})
	if err != nil {
		t.Fatal(err)
	}
	clientListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewClient(ClientConfig{
		ListenAddr: clientListener.Addr().String(), RemoteAddr: packetConn.LocalAddr().String(), Credentials: roots,
		Transport: TransportQUIC, Logger: logger, Metrics: clientMetrics, Congestion: congestion,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = server.ServePacketConn(ctx, packetConn) }()
	go func() { _ = client.ServeListener(ctx, clientListener) }()

	conn := dialTestSOCKS(t, clientListener.Addr().String(), destinationListener.Addr().String())
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
	payload := bytes.Repeat([]byte("credit-delay...."), 1<<16) // 1 MiB each way
	go func() { _, _ = conn.Write(payload) }()
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		c, s := clientMetrics.Snapshot().CreditDelay.Count(), serverMetrics.Snapshot().CreditDelay.Count()
		if c > 0 && s > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("credit delay samples: client %d, gateway %d; want both", c, s)
		}
		time.Sleep(20 * time.Millisecond)
	}
	recorder := httptest.NewRecorder()
	serverMetrics.ServeHTTP(recorder, httptest.NewRequest("GET", "/metrics", nil))
	if !bytes.Contains(recorder.Body.Bytes(), []byte("queqiao_credit_delay_seconds_bucket{le=\"+Inf\"}")) {
		t.Fatal("/metrics has no credit delay histogram")
	}
}
