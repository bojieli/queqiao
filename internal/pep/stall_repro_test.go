package pep

import (
	"context"
	"encoding/binary"
	"io"
	"log/slog"
	"net"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/bojieli/queqiao/internal/metrics"
	"github.com/bojieli/queqiao/internal/pathmodel"
	"github.com/bojieli/queqiao/internal/pathsim"
)

// TestFourDownloadsOnAFastLossyPath is an opt-in measurement, not a gate:
// QUEQIAO_STALL_REPRO=1 runs it. It brings up the real client and server
// across an emulated path shaped like the fast lossy one the #161 A/B ran on
// (REPRO_RATE bytes/s, REPRO_QUEUE bytes of buffer, REPRO_LOSS erasure in
// bursts of REPRO_BURST, REPRO_OWD_MS each way with REPRO_WANDER_MS of
// correlated delay wander), runs REPRO_FLOWS concurrent downloads of REPRO_MB
// MiB after a warm-up exchange and an idle REPRO_IDLE_S, and reports what the
// server, the sender, counted: goodput, flows completed, stalls, wire bytes
// per useful byte, loss, and the excess wire that loss does not explain.
// REPRO_LOG=dir writes both ends' runtime logs there (with
// QUEQIAO_LANE_TRACE=1, per-lane traces).
//
// It is how the stall and wire-overhead excess on that path was reproduced and
// how the fixes for it were compared; single runs vary by tens of percent, so
// compare several seeds (REPRO_SEED) of each build, run side by side. See
// docs/CONTROL-REDESIGN.md for the settings and numbers.
func TestFourDownloadsOnAFastLossyPath(t *testing.T) {
	if os.Getenv("QUEQIAO_STALL_REPRO") == "" {
		t.Skip("opt-in measurement; set QUEQIAO_STALL_REPRO=1")
	}
	env := func(k string, d float64) float64 {
		if v, err := strconv.ParseFloat(os.Getenv(k), 64); err == nil {
			return v
		}
		return d
	}
	size := int(env("REPRO_MB", 16)) << 20
	flows := int(env("REPRO_FLOWS", 4))
	path := pathsim.Config{
		OneWayDelay:       time.Duration(env("REPRO_OWD_MS", 110)) * time.Millisecond,
		LossRate:          env("REPRO_LOSS", 0.12),
		RateBytesPerSec:   uint64(env("REPRO_RATE", 6e6)),
		QueueBytes:        int(env("REPRO_QUEUE", 300e3)),
		Seed:              int64(env("REPRO_SEED", 7)),
		DelayWander:       time.Duration(env("REPRO_WANDER_MS", 0)) * time.Millisecond,
		DelayWanderPeriod: time.Duration(env("REPRO_WANDER_PERIOD_MS", 100)) * time.Millisecond,
		LossBurstPackets:  env("REPRO_BURST", 0),
	}
	pathmodel.Reset()
	t.Cleanup(pathmodel.Reset)
	dest, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer dest.Close()
	payload := make([]byte, 1<<20)
	go func() {
		for {
			c, err := dest.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				var hdr [8]byte
				if _, err := io.ReadFull(c, hdr[:]); err != nil {
					return
				}
				n := int(binary.BigEndian.Uint64(hdr[:]))
				for n > 0 {
					k := min(n, len(payload))
					if _, err := c.Write(payload[:k]); err != nil {
						return
					}
					n -= k
				}
			}(c)
		}
	}()
	certificate, roots := testCertificate(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	clientLogger := logger
	if dir := os.Getenv("REPRO_LOG"); dir != "" {
		sf, _ := os.Create(dir + "/server.jsonl")
		cf, _ := os.Create(dir + "/client.jsonl")
		defer sf.Close()
		defer cf.Close()
		logger = slog.New(slog.NewJSONHandler(sf, nil))
		clientLogger = slog.New(slog.NewJSONHandler(cf, nil))
	}
	packetConn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serverMetrics, clientMetrics := metrics.New(), metrics.New()
	server, err := NewServer(ServerConfig{
		ListenAddr: "127.0.0.1:0", Credentials: certificate,
		DestinationPolicy: DestinationPolicy{AllowPrivate: true}, EnableQUIC: true, Logger: logger,
		Metrics: serverMetrics, HandshakeTimeout: 30 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	relay, err := pathsim.New("127.0.0.1:0", packetConn.LocalAddr().String(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer relay.Close()
	clientListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewClient(ClientConfig{
		ListenAddr: clientListener.Addr().String(), RemoteAddr: relay.LocalAddr(), Credentials: roots, Transport: TransportQUIC,
		EnableQUICPool: true, Logger: clientLogger, Metrics: clientMetrics,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = server.ServePacketConn(ctx, packetConn) }()
	go func() { _ = client.ServeListener(ctx, clientListener) }()
	socks := clientListener.Addr().String()

	// Warm the pool with one small exchange, as the control connection does,
	// then let it idle the way the live pool did between trials.
	download := func(n int) (int64, error) {
		conn, err := trySocksDial(socks, dest, 120*time.Second)
		if err != nil {
			return 0, err
		}
		defer conn.Close()
		var hdr [8]byte
		binary.BigEndian.PutUint64(hdr[:], uint64(n))
		if _, err := conn.Write(hdr[:]); err != nil {
			return 0, err
		}
		return io.CopyN(io.Discard, conn, int64(n))
	}
	if _, err := download(64 << 10); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Duration(env("REPRO_IDLE_S", 3)) * time.Second)
	before, cbefore := serverMetrics.Snapshot(), clientMetrics.Snapshot()
	start := time.Now()
	t.Logf("REPRO start %s", start.UTC().Format(time.RFC3339Nano))
	var wg sync.WaitGroup
	errs := make([]error, flows)
	got := make([]int64, flows)
	for i := 0; i < flows; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			got[i], errs[i] = download(size)
		}(i)
	}
	wg.Wait()
	wall := time.Since(start)
	after, cafter := serverMetrics.Snapshot(), clientMetrics.Snapshot()
	ok := 0
	for _, e := range errs {
		if e == nil {
			ok++
		}
	}
	// What arrived, not what was asked for: a failed transfer's bytes are
	// all it delivered.
	useful := 0.0
	for _, n := range got {
		useful += float64(n)
	}
	sent := float64(after.QUICBytesSent - before.QUICBytesSent)
	pk := float64(after.QUICPacketsSent - before.QUICPacketsSent)
	lost := float64(after.QUICLossObservedPackets - before.QUICLossObservedPackets)
	t.Logf("REPRO goodput %.1f Mbit/s ok %d/%d stalls %d reinjections %d wire/useful %.2f loss %.3f excess %.2f",
		useful*8/wall.Seconds()/1e6, ok, flows, after.FlowStallsDetected-before.FlowStallsDetected,
		after.Reinjections-before.Reinjections, sent/useful, lost/pk, sent/useful*(1-lost/pk))
	src := float64(cafter.QUICCodedSources - cbefore.QUICCodedSources)
	rec := float64(cafter.QUICCodedRecovered - cbefore.QUICCodedRecovered)
	lst := float64(cafter.QUICCodedLost - cbefore.QUICCodedLost)
	t.Logf("REPRO coded: source symbols %.0f (vs useful/1200 %.0f) arrived %.0f recovered %.0f unrecovered %.0f residual %.3f",
		src+rec+lst, useful/1200, src, rec, lst, lst/(src+rec+lst+1))
}
