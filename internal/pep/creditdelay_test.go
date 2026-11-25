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
// covers it, minus the path's minimum round trip. An acknowledgement that
// does not cover the chunk leaves the sample open, a second take while it is
// open does not restart it, and a lane with no minimum round trip (TCP, or
// none measured yet) takes no sample at all.
func TestCreditDelayIsTimedFromTheCarriersTakeToTheCoveringACK(t *testing.T) {
	registry := metrics.New()
	local, remote := net.Pipe()
	t.Cleanup(func() { _ = local.Close(); _ = remote.Close() })
	flow := newMultipathFlow(context.Background(), local, [16]byte{1}, 7, 0, 0, 0, nil, registry)
	const minRTT = 40 * time.Millisecond
	quic := &mpLane{id: 0, kind: TransportQUIC, fc: newFrameConn(&statsConn{
		stats: laneTransportStats{controller: wancongestion.ControllerTelemetry{MinRTT: minRTT}},
	})}
	tcp := &mpLane{id: 1, kind: TransportTCP, fc: newFrameConn(&statsConn{})}

	flow.noteSent(0, 3000)
	flow.noteCarrierTook(tcp, 0, 1000)
	if flow.credit.pending.Load() {
		t.Fatal("a lane without a minimum round trip started a sample")
	}
	flow.noteCarrierTook(quic, 1000, 1000)
	time.Sleep(20 * time.Millisecond)
	flow.noteCarrierTook(quic, 2000, 1000) // not a restart
	if _, err := flow.acknowledgeCoverage(1500, false, nil); err != nil {
		t.Fatal(err)
	}
	if n := registry.Snapshot().CreditDelay.Count(); n != 0 {
		t.Fatalf("an acknowledgement short of the chunk recorded %d samples", n)
	}
	time.Sleep(100 * time.Millisecond)
	if _, err := flow.acknowledgeCoverage(2000, false, nil); err != nil {
		t.Fatal(err)
	}
	got := registry.Snapshot().CreditDelay
	if got.Count() != 1 {
		t.Fatalf("recorded %d samples, want 1", got.Count())
	}
	// About 120 ms since the take, less the 40 ms minimum: about 80 ms, in
	// the bucket up to 100 ms (or a slower runner's next one).
	if got.Sum < 60*time.Millisecond || got.Sum > 400*time.Millisecond {
		t.Fatalf("credit delay %v, want about 80ms (take to ACK less the minimum round trip)", got.Sum)
	}
	if flow.credit.pending.Load() {
		t.Fatal("the completed sample is still pending")
	}
	// Selective coverage completes a sample too.
	flow.noteSent(3000, 1000)
	flow.noteCarrierTook(quic, 3000, 1000)
	if _, err := flow.acknowledgeCoverage(2000, false, [][2]uint64{{3000, 4000}}); err != nil {
		t.Fatal(err)
	}
	if n := registry.Snapshot().CreditDelay.Count(); n != 2 {
		t.Fatalf("a selective acknowledgement of the chunk left %d samples, want 2", n)
	}
}

// End to end, through a real client and gateway: an upload is timed by the
// client and a download by the gateway, each on its own clock, and both
// export the histogram.
func TestBothEndpointsExportCreditDelay(t *testing.T) {
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
		Metrics: serverMetrics,
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
		Transport: TransportQUIC, Logger: logger, Metrics: clientMetrics,
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
