package pep

import (
	"context"
	"encoding/binary"
	"io"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apernet/quic-go"
	quiccongestion "github.com/apernet/quic-go/congestion"

	wancongestion "github.com/bojieli/queqiao/internal/congestion"
	"github.com/bojieli/queqiao/internal/identity"
	"github.com/bojieli/queqiao/internal/pathmodel"
	"github.com/bojieli/queqiao/internal/pathsim"
	"github.com/bojieli/queqiao/internal/protocol"
)

// schedPair is one connection across an emulated bottleneck. The client's
// controller is the cc-api=v2 erasure sender, with the given scheduler; the
// client is the sender in these tests.
func schedPair(t *testing.T, sched SchedMode, cfg pathsim.Config, windows ...flowWindows) (client, server *quic.Conn, controller *wancongestion.ErasurePathSender) {
	t.Helper()
	serverCredentials, clientCredentials := testCertificate(t)
	serverTLS, err := identity.ServerTLSConfig(serverCredentials, defaultALPN, false)
	if err != nil {
		t.Fatal(err)
	}
	clientTLS, err := tlsClientConfig(clientCredentials, nil)
	if err != nil {
		t.Fatal(err)
	}
	var w flowWindows
	if len(windows) > 0 {
		w = windows[0]
	}
	listener, err := quic.ListenAddr("127.0.0.1:0", serverTLS, quicConfig(w))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	relay, err := pathsim.New("127.0.0.1:0", listener.Addr().String(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = relay.Close() })
	accepted := make(chan *quic.Conn, 1)
	go func() {
		if conn, err := listener.Accept(context.Background()); err == nil {
			accepted <- conn
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	client, err = quic.DialAddr(ctx, relay.LocalAddr(), clientTLS, quicConfig(w))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.CloseWithError(0, "") })
	select {
	case server = <-accepted:
	case <-ctx.Done():
		t.Fatal("not accepted")
	}
	t.Cleanup(func() { _ = server.CloseWithError(0, "") })
	pathmodel.Forget(peerKey(client))
	c := configureQUICController(client, congestionConfig{kind: CongestionErasure, ccAPI: CCAPIV2, sched: sched})
	controller, ok := c.(*wancongestion.ErasurePathSender)
	if !ok {
		t.Fatalf("installed %T", c)
	}
	if controller.SchedulesClasses() != (sched == SchedClass) {
		t.Fatalf("sched %q: SchedulesClasses() = %v", sched, controller.SchedulesClasses())
	}
	return client, server, controller
}

// schedWorkload is a set of flows on one connection, each on its own stream
// with its own framing, as lanes multiplexed on a pooled connection are.
type schedWorkload struct {
	// flows each write 32 KiB BULK DATA frames back to back; the first
	// ackFlows of them also write a timestamped ACK every ackEvery.
	flows, ackFlows int
	ackEvery        time.Duration
	duration        time.Duration
}

type schedOutcome struct {
	ackDelays []time.Duration
	// dataBytes is the DATA payload each flow delivered.
	dataBytes []int64
}

func (w schedWorkload) run(t *testing.T, client, server *quic.Conn) schedOutcome {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), w.duration)
	defer cancel()
	out := schedOutcome{dataBytes: make([]int64, w.flows)}
	var mu sync.Mutex
	var wg sync.WaitGroup

	// The receiving end reads every stream to its end.
	go func() {
		for {
			stream, err := server.AcceptStream(context.Background())
			if err != nil {
				return
			}
			fc := newFrameConn(&quicStreamConn{stream: stream, conn: server})
			go func() {
				for {
					f, err := fc.Read()
					if err != nil {
						return
					}
					mu.Lock()
					switch f.Header.Type {
					case protocol.TypeAck:
						sent := time.Unix(0, int64(binary.BigEndian.Uint64(f.Payload)))
						out.ackDelays = append(out.ackDelays, time.Since(sent))
					case protocol.TypeData:
						if ctx.Err() == nil {
							out.dataBytes[f.Header.FlowID] += int64(len(f.Payload))
						}
					}
					mu.Unlock()
				}
			}()
		}
	}()

	for i := range w.flows {
		stream, err := client.OpenStreamSync(ctx)
		if err != nil {
			t.Fatal(err)
		}
		fc := newFrameConn(&quicStreamConn{stream: stream, conn: client})
		chunk := protocol.Frame{Header: protocol.Header{Version: protocol.Version, Type: protocol.TypeData, FlowID: uint64(i), Class: protocol.ClassBulk}, Payload: make([]byte, 32<<10)}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				if err := fc.WriteContext(ctx, chunk); err != nil {
					return
				}
			}
		}()
		if i >= w.ackFlows {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			ticker := time.NewTicker(w.ackEvery)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
				}
				payload := make([]byte, 8)
				binary.BigEndian.PutUint64(payload, uint64(time.Now().UnixNano()))
				ack := protocol.Frame{Header: protocol.Header{Version: protocol.Version, Type: protocol.TypeAck, FlowID: uint64(i)}, Payload: payload}
				if err := fc.WriteContext(ctx, ack); err != nil {
					return
				}
			}
		}()
	}
	wg.Wait()
	// Let ACKs already sent arrive.
	time.Sleep(300 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	return schedOutcome{ackDelays: append([]time.Duration(nil), out.ackDelays...), dataBytes: append([]int64(nil), out.dataBytes...)}
}

func percentile(d []time.Duration, p float64) time.Duration {
	if len(d) == 0 {
		return 0
	}
	s := append([]time.Duration(nil), d...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	return s[int(p*float64(len(s)-1))]
}

// A paced connection carrying eight bulk flows, one of which also sends its
// peer flow acknowledgements: the ACK waits behind its own flow's 32 KiB DATA
// write. Under sched=class the waiting ACK boosts that flow's stream to
// CONTROL, so the bytes ahead of it are served ahead of the queued BULK of
// the other seven flows and the ACK goes out within about one chunk at the
// path rate; under the legacy builder the same chunk drains at an eighth of
// it. BULK is not starved for it: every bulk flow keeps moving and BULK is
// served at least its weight.
func TestControlFeedbackIsServedAheadOfQueuedBulkWhilePaced(t *testing.T) {
	if testing.Short() {
		t.Skip("emulated-path timing test")
	}
	path := pathsim.Config{OneWayDelay: 10 * time.Millisecond, RateBytesPerSec: 1_500_000, Seed: 7}
	w := schedWorkload{flows: 8, ackFlows: 1, ackEvery: 25 * time.Millisecond, duration: 5 * time.Second}

	results := map[SchedMode]schedOutcome{}
	var stats wancongestion.ClassArbiterStats
	for _, sched := range []SchedMode{SchedLegacy, SchedClass} {
		client, server, controller := schedPair(t, sched, path)
		results[sched] = w.run(t, client, server)
		if sched == SchedClass {
			stats, _ = controller.ClassStats()
		}
	}
	legacy, class := results[SchedLegacy], results[SchedClass]
	if len(class.ackDelays) < 50 || len(legacy.ackDelays) < 10 {
		t.Fatalf("too few ACKs delivered: legacy %d, class %d", len(legacy.ackDelays), len(class.ackDelays))
	}
	lp50, lp90 := percentile(legacy.ackDelays, 0.5), percentile(legacy.ackDelays, 0.9)
	cp50, cp90 := percentile(class.ackDelays, 0.5), percentile(class.ackDelays, 0.9)
	t.Logf("ACK delay p50/p90: legacy %v/%v (%d ACKs), class %v/%v (%d ACKs)", lp50, lp90, len(legacy.ackDelays), cp50, cp90, len(class.ackDelays))
	t.Logf("class arbiter: served %v turns %v", stats.Served, stats.Turns)
	if cp50*2 > lp50 || cp90*2 > lp90 {
		t.Fatalf("CONTROL feedback was not served ahead of queued BULK: class p50/p90 %v/%v against legacy %v/%v", cp50, cp90, lp50, lp90)
	}
	if stats.Served[quiccongestion.ClassControl] == 0 {
		t.Fatal("nothing was served at CONTROL")
	}

	// BULK is never starved: every bulk flow keeps moving, and BULK is
	// served at least its weight. The boosted flow's DATA is charged to
	// CONTROL, so it is CONTROL's weight, not BULK's, that bounds it.
	for i, b := range class.dataBytes[1:] {
		if b < 64<<10 {
			t.Fatalf("bulk flow %d delivered only %d bytes: starved", i+1, b)
		}
	}
	served := stats.Served
	if bulk := float64(served[quiccongestion.ClassBulk]) / float64(served[quiccongestion.ClassControl]+served[quiccongestion.ClassInteractive]+served[quiccongestion.ClassBulk]); bulk < 0.15 {
		t.Fatalf("BULK served %.2f of the bytes, below its weight", bulk)
	}
}

// Test 6 of §15.1, last case: many flows boosted at once stay within
// CONTROL's share. Every flow but one has an ACK waiting nearly all the time,
// so their streams are boosted and their DATA is served at CONTROL; the one
// unboosted flow is BULK, and BULK still gets its weight.
func TestManyBoostedFlowsStayWithinControlsShare(t *testing.T) {
	if testing.Short() {
		t.Skip("emulated-path timing test")
	}
	path := pathsim.Config{OneWayDelay: 10 * time.Millisecond, RateBytesPerSec: 1_500_000, Seed: 11}
	client, server, controller := schedPair(t, SchedClass, path)
	w := schedWorkload{flows: 7, ackFlows: 6, ackEvery: 2 * time.Millisecond, duration: 5 * time.Second}
	out := w.run(t, client, server)
	stats, _ := controller.ClassStats()
	served := stats.Served
	sum := float64(served[quiccongestion.ClassControl] + served[quiccongestion.ClassInteractive] + served[quiccongestion.ClassBulk])
	control, bulk := float64(served[quiccongestion.ClassControl])/sum, float64(served[quiccongestion.ClassBulk])/sum
	t.Logf("served CONTROL %.2f BULK %.2f of %0.f bytes; flows %v", control, bulk, sum, out.dataBytes)
	if control < 0.3 {
		t.Fatalf("CONTROL served %.2f: the flows were not boosted", control)
	}
	// With CONTROL saturated BULK's weight is 1/5.
	if bulk < 0.15 {
		t.Fatalf("BULK served %.2f, below its weight against saturated CONTROL", bulk)
	}
	unboosted := out.dataBytes[6]
	if unboosted < 256<<10 {
		t.Fatalf("the unboosted bulk flow delivered %d bytes", unboosted)
	}
}

// Test 7 of §15.1, on one connection: a due window update is CONTROL demand
// and is served against a busy BULK sibling. The client uploads BULK without
// pause while it downloads through small receive windows, so the download
// moves only as fast as the client's MAX_STREAM_DATA and MAX_DATA frames get
// out between its own BULK packets.
func TestDueWindowUpdatesAreServedAgainstBusyBulk(t *testing.T) {
	if testing.Short() {
		t.Skip("emulated-path timing test")
	}
	path := pathsim.Config{OneWayDelay: 10 * time.Millisecond, RateBytesPerSec: 1_500_000, Seed: 13}
	client, server, controller := schedPair(t, SchedClass, path, flowWindows{stream: 64 << 10, connection: 128 << 10, maxStream: 64 << 10, maxConnection: 128 << 10})
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()

	upload, err := client.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	fc := newFrameConn(&quicStreamConn{stream: upload, conn: client})
	chunk := protocol.Frame{Header: protocol.Header{Version: protocol.Version, Type: protocol.TypeData, FlowID: 1, Class: protocol.ClassBulk}, Payload: make([]byte, 32<<10)}
	uploadErr := make(chan error, 1)
	go func() {
		for ctx.Err() == nil {
			if err := fc.WriteContext(ctx, chunk); err != nil {
				// A write cut off at the test's own deadline is the end of
				// the test, not a failure.
				if deadline, _ := ctx.Deadline(); time.Until(deadline) > 200*time.Millisecond {
					uploadErr <- err
				}
				return
			}
		}
	}()
	var uploaded atomic.Int64
	go func() {
		stream, err := server.AcceptStream(context.Background())
		if err != nil {
			return
		}
		go func() {
			n, _ := io.Copy(io.Discard, countingReader{stream, &uploaded})
			_ = n
		}()
		// The download, on a stream of the server's own.
		down, err := server.OpenStreamSync(context.Background())
		if err != nil {
			return
		}
		buf := make([]byte, 32<<10)
		for ctx.Err() == nil {
			if _, err := down.Write(buf); err != nil {
				return
			}
		}
	}()
	down, err := client.AcceptStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var got int64
	buf := make([]byte, 64<<10)
	for ctx.Err() == nil {
		_ = down.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		n, _ := down.Read(buf)
		got += int64(n)
	}
	stats, _ := controller.ClassStats()
	t.Logf("downloaded %d bytes through 64 KiB windows, uploaded %d; client served %v", got, uploaded.Load(), stats.Served)
	select {
	case err := <-uploadErr:
		t.Fatalf("the BULK upload failed: %v", err)
	default:
	}
	// The upload kept the path busy with BULK the whole time.
	if uploaded.Load() < 1<<20 {
		t.Fatalf("uploaded only %d bytes: the window updates did not compete with busy BULK", uploaded.Load())
	}
	// A stalled window update would hold the download at one window.
	if got < 1<<20 {
		t.Fatalf("downloaded %d bytes: window updates were held behind BULK", got)
	}
	if stats.Turns[quiccongestion.ClassControl] == 0 {
		t.Fatal("no CONTROL turn was taken for the window updates")
	}
}

type countingReader struct {
	r io.Reader
	n *atomic.Int64
}

func (c countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n.Add(int64(n))
	return n, err
}
