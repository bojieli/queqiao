package pep

import (
	"context"
	"encoding/binary"
	"io"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apernet/quic-go"
	quiccongestion "github.com/apernet/quic-go/congestion"

	wancongestion "github.com/bojieli/queqiao/internal/congestion"
	"github.com/bojieli/queqiao/internal/identity"
	"github.com/bojieli/queqiao/internal/pathsim"
)

// Stage 7 (topology=single) on real fork send loops over an emulated path:
// what isolation bought with extra connections -- throughput for several bulk
// flows, and service for small flows beside them -- one connection under the
// path controller's class arbitration must deliver too (design §7, §4.3).

// streamBed is a gateway-side listener behind one emulated bottleneck whose
// streams start with a header: a kind byte, the stream's index, and the
// sender's clock at the stream's start.
type streamBed struct {
	t        *testing.T
	relay    *pathsim.Relay
	dialConn func() *quic.Conn
	// principal is this bed's client identity, so each bed starts cold.
	principal string

	mu sync.Mutex
	// received counts bulk bytes per stream index.
	received map[uint32]*atomic.Int64
	// done records when each burst stream's FIN arrived, after its start.
	done map[uint32]time.Duration
	// lateness collects, per message stream, each message's one-way delay.
	lateness map[uint32][]time.Duration
}

const (
	streamKindBulk    = 1
	streamKindBurst   = 2
	streamKindMessage = 3
	// messageSize is one W5 delta: a server-sent event of about 180 bytes
	// (docs/WORKLOAD-MODEL.md W5).
	messageSize = 180
)

func newStreamBed(t *testing.T, cfg pathsim.Config, windows flowWindows) *streamBed {
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
	listener, err := quic.ListenAddr("127.0.0.1:0", serverTLS, quicConfig(windows))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	relay, err := pathsim.New("127.0.0.1:0", listener.Addr().String(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = relay.Close() })
	b := &streamBed{
		t: t, relay: relay, principal: newClientPathPrincipal(),
		received: map[uint32]*atomic.Int64{}, done: map[uint32]time.Duration{}, lateness: map[uint32][]time.Duration{},
	}
	go func() {
		for {
			conn, err := listener.Accept(context.Background())
			if err != nil {
				return
			}
			t.Cleanup(func() { _ = conn.CloseWithError(0, "") })
			go func() {
				for {
					stream, err := conn.AcceptStream(context.Background())
					if err != nil {
						return
					}
					go b.serve(stream)
				}
			}()
		}
	}()
	b.dialConn = func() *quic.Conn {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		conn, err := quic.DialAddr(ctx, relay.LocalAddr(), clientTLS, quicConfig(windows))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.CloseWithError(0, "") })
		return conn
	}
	return b
}

func (b *streamBed) serve(stream *quic.Stream) {
	var header [13]byte
	if _, err := io.ReadFull(stream, header[:]); err != nil {
		return
	}
	index := binary.BigEndian.Uint32(header[1:5])
	started := time.Unix(0, int64(binary.BigEndian.Uint64(header[5:13])))
	switch header[0] {
	case streamKindBulk:
		counter := b.bulkCounter(index)
		buf := make([]byte, 64<<10)
		for {
			n, err := stream.Read(buf)
			counter.Add(int64(n))
			if err != nil {
				return
			}
		}
	case streamKindBurst:
		if _, err := io.Copy(io.Discard, stream); err != nil {
			return
		}
		b.mu.Lock()
		b.done[index] = time.Since(started)
		b.mu.Unlock()
	case streamKindMessage:
		var msg [messageSize]byte
		for {
			if _, err := io.ReadFull(stream, msg[:]); err != nil {
				return
			}
			sent := time.Unix(0, int64(binary.BigEndian.Uint64(msg[:8])))
			late := time.Since(sent)
			b.mu.Lock()
			b.lateness[index] = append(b.lateness[index], late)
			b.mu.Unlock()
		}
	}
}

func (b *streamBed) bulkCounter(index uint32) *atomic.Int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	c, ok := b.received[index]
	if !ok {
		c = &atomic.Int64{}
		b.received[index] = c
	}
	return c
}

// dial opens a client connection through the bottleneck, attached to the
// bed's path controller.
func (b *streamBed) dial() (*quic.Conn, *wancongestion.PathController) {
	conn := b.dialConn()
	cfg := pathScopeConfig
	cfg.principal = func() string { return b.principal }
	att, ok := configureQUICController(conn, cfg).(*wancongestion.PathAttachment)
	if !ok {
		b.t.Fatal("congestion=path did not install a path attachment")
	}
	return conn, att.Controller()
}

func streamHeader(kind byte, index uint32) []byte {
	h := make([]byte, 13)
	h[0] = kind
	binary.BigEndian.PutUint32(h[1:5], index)
	binary.BigEndian.PutUint64(h[5:13], uint64(time.Now().UnixNano()))
	return h
}

// bulkStream saturates one stream of conn, tagged BULK, until ctx ends.
func bulkStream(ctx context.Context, conn *quic.Conn, index uint32) {
	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return
	}
	stop := context.AfterFunc(ctx, func() { stream.CancelWrite(0) })
	defer stop()
	if _, err := stream.WriteClass(streamHeader(streamKindBulk, index), quiccongestion.ClassControl); err != nil {
		return
	}
	chunk := make([]byte, 32<<10)
	for ctx.Err() == nil {
		if _, err := stream.WriteClass(chunk, quiccongestion.ClassBulk); err != nil {
			return
		}
	}
}

// burstStream is one object of a page: a new flow's whole request written at
// once and closed, as a fan-out of NEW flows is (W3).
func burstStream(ctx context.Context, conn *quic.Conn, index uint32, size int, class quiccongestion.SendClass) {
	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return
	}
	payload := append(streamHeader(streamKindBurst, index), make([]byte, size)...)
	if _, err := stream.WriteClass(payload, class); err != nil {
		return
	}
	_ = stream.Close()
}

// messageDeltas is how many deltas a message stream sends: three seconds
// worth, so in a four-second run every one is due at least a second before
// the run ends.
const messageDeltas = 120

// messageStream is an LLM response: messageDeltas 180-byte deltas, one every
// 25 ms (W5),
// each stamped with the sender's clock, at the given class: INTERACTIVE
// under classifier=phased, BULK where the legacy classifier latched the flow
// (docs/AUDIT-CLASSIFIER-WORKLOADS.md F1).
func messageStream(ctx context.Context, conn *quic.Conn, index uint32, class quiccongestion.SendClass) {
	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return
	}
	// Cancelling discards unsent bytes: only an abandoned run cancels; a
	// finished one closes, so its last delta is delivered.
	stop := context.AfterFunc(ctx, func() { stream.CancelWrite(0) })
	defer stop()
	if _, err := stream.WriteClass(streamHeader(streamKindMessage, index), quiccongestion.ClassControl); err != nil {
		return
	}
	// A fixed emission schedule: delta i is due at start + i×25 ms and is
	// stamped with that due time, not with when the writer got to it, so a
	// write that blocks shows up as lateness of every delta behind it.
	start := time.Now()
	var msg [messageSize]byte
	for i := 1; i <= messageDeltas; i++ {
		due := start.Add(time.Duration(i) * 25 * time.Millisecond)
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Until(due)):
		}
		binary.BigEndian.PutUint64(msg[:8], uint64(due.UnixNano()))
		if _, err := stream.WriteClass(msg[:], class); err != nil {
			return
		}
	}
	_ = stream.Close()
}

func (b *streamBed) bulkTotals(n int) (total float64, per []float64) {
	for i := range n {
		got := float64(b.bulkCounter(uint32(i)).Load())
		per = append(per, got)
		total += got
	}
	return total, per
}

// The stage-7 throughput test: N bulk flows as N streams on one connection
// deliver what the same N flows deliver as N isolated connections on the
// same path and controller, and every stream gets its share: none starves
// and none takes a multiple of another's. Desktop receive windows; the
// mobile windows are TestSingleConnectionWindowsKeepIsolatedCapacity.
func TestBulkStreamsOnOneConnectionMatchIsolatedConnections(t *testing.T) {
	if testing.Short() {
		t.Skip("emulated-path timing test")
	}
	path := pathsim.Config{OneWayDelay: 10 * time.Millisecond, RateBytesPerSec: 1_500_000, Seed: 17}
	const run = 4 * time.Second
	for _, n := range []int{4, 8} {
		var isolated, single float64
		for _, oneConnection := range []bool{false, true} {
			pathAuthorities.forget()
			bed := newStreamBed(t, path, flowWindows{})
			conns := make([]*quic.Conn, n)
			var ctl *wancongestion.PathController
			for i := range conns {
				if oneConnection && i > 0 {
					conns[i] = conns[0]
					continue
				}
				var c *wancongestion.PathController
				conns[i], c = bed.dial()
				if ctl == nil {
					ctl = c
				} else if c != ctl {
					t.Fatalf("connection %d attached to another controller", i)
				}
			}
			want := n
			if oneConnection {
				want = 1
			}
			waitAttached(t, ctl, want)
			ctx, cancel := context.WithTimeout(context.Background(), run)
			for i, c := range conns {
				go bulkStream(ctx, c, uint32(i))
			}
			for ctx.Err() == nil {
				time.Sleep(50 * time.Millisecond)
				checkFlight(t, ctl.Stats())
			}
			cancel()
			total, per := bed.bulkTotals(n)
			rate := total / run.Seconds()
			if rate < 0.5*float64(path.RateBytesPerSec) {
				t.Fatalf("n=%d one connection=%v: %.2f MB/s on a %.2f MB/s path; no working arm to compare", n, oneConnection, rate/1e6, float64(path.RateBytesPerSec)/1e6)
			}
			t.Logf("n=%d one connection=%v: aggregate %.2f MB/s, per stream %v", n, oneConnection, rate/1e6, per)
			for i, got := range per {
				if got < 0.5*total/float64(n) || got > 2*total/float64(n) {
					t.Errorf("n=%d one connection=%v: stream %d delivered %.0f of %.0f bytes", n, oneConnection, i, got, total)
				}
			}
			if oneConnection {
				single = rate
			} else {
				isolated = rate
			}
		}
		if single < 0.9*isolated {
			t.Errorf("n=%d: one connection delivered %.2f MB/s against %.2f MB/s isolated (%.2fx)", n, single/1e6, isolated/1e6, single/isolated)
		}
	}
}

// No stream starves on one connection. Four bulk streams saturate it while,
// on the same connection,
//
//   - a page's fan-out opens 32 new flows of 24 KiB at once (W3, the
//     HTTP/1.1 and HTTP/2 browsing shapes as NEW flows), and
//   - eight LLM responses stream a 180-byte delta every 25 ms (W5-swarm).
//
// The deltas are run both INTERACTIVE, as classifier=phased labels a
// response's trickle, and BULK, as the legacy classifier's latch mislabels
// it: under the class scheduler a mislabelled delta stream is one more
// stream in BULK's round robin, not a stream queued behind the others'
// backlog, so it is late by a few packets, not by a bulk window.
func TestNoStreamStarvesOnOneConnection(t *testing.T) {
	if testing.Short() {
		t.Skip("emulated-path timing test")
	}
	path := pathsim.Config{OneWayDelay: 10 * time.Millisecond, RateBytesPerSec: 1_500_000, Seed: 23}
	const (
		bulkStreams    = 4
		burstStreams   = 32
		burstSize      = 24 << 10
		messageStreams = 8
		run            = 4 * time.Second
	)
	for _, deltaClass := range []quiccongestion.SendClass{quiccongestion.ClassInteractive, quiccongestion.ClassBulk} {
		name := "interactive deltas"
		if deltaClass == quiccongestion.ClassBulk {
			name = "bulk-labelled deltas"
		}
		t.Run(name, func(t *testing.T) {
			pathAuthorities.forget()
			bed := newStreamBed(t, path, flowWindows{})
			conn, ctl := bed.dial()
			waitAttached(t, ctl, 1)
			ctx, cancel := context.WithTimeout(context.Background(), run)
			defer cancel()
			for i := range bulkStreams {
				go bulkStream(ctx, conn, uint32(i))
			}
			for i := range messageStreams {
				go messageStream(ctx, conn, uint32(i), deltaClass)
			}
			// The page arrives once the bulk streams own the path.
			time.Sleep(time.Second)
			for i := range burstStreams {
				go burstStream(ctx, conn, uint32(i), burstSize, quiccongestion.ClassInteractive)
			}
			<-ctx.Done()

			total, per := bed.bulkTotals(bulkStreams)
			// The bulk class keeps its weight beside the page and the
			// deltas: well over a third of the path.
			if total/run.Seconds() < 0.4*float64(path.RateBytesPerSec) {
				t.Errorf("bulk delivered %.2f MB/s beside interactive load on a %.2f MB/s path", total/run.Seconds()/1e6, float64(path.RateBytesPerSec)/1e6)
			}
			for i, got := range per {
				if got < 0.5*total/bulkStreams {
					t.Errorf("bulk stream %d delivered %.0f of %.0f bytes", i, got, total)
				}
			}
			bed.mu.Lock()
			defer bed.mu.Unlock()
			if len(bed.done) != burstStreams {
				t.Fatalf("%d of %d page objects completed while the bulk streams ran", len(bed.done), burstStreams)
			}
			var page []time.Duration
			for _, d := range bed.done {
				page = append(page, d)
			}
			// Together the page is 768 KiB on a 1.5 MB/s path: about
			// 0.5 s at the whole rate. Interactive weight beside bulk
			// must finish it well inside the run.
			slowest := slices.Max(page)
			var all []time.Duration
			for i := range messageStreams {
				samples := bed.lateness[uint32(i)]
				// Every delta is due a second before the run ends; one that
				// has not arrived by then is a stalled tail.
				if len(samples) != messageDeltas {
					t.Errorf("message stream %d delivered %d of %d deltas within a second of their due time; starved", i, len(samples), messageDeltas)
				}
				all = append(all, samples...)
			}
			p50, p99 := percentile(all, 0.5), percentile(all, 0.99)
			t.Logf("bulk %.2f MB/s %v; page objects median %v slowest %v; deltas p50 %v p99 %v",
				total/run.Seconds()/1e6, per, percentile(page, 0.5), slowest, p50, p99)
			if slowest > 2500*time.Millisecond {
				t.Errorf("slowest page object took %v", slowest)
			}
			// One-way delay is 10 ms; a bulk window queued ahead of a
			// delta would add hundreds of milliseconds.
			limit := 150 * time.Millisecond
			if deltaClass == quiccongestion.ClassBulk {
				limit = 400 * time.Millisecond
			}
			if p99 > limit {
				t.Errorf("delta lateness p99 %v, want under %v", p99, limit)
			}
		})
	}
}

// Many bulk flows on one connection: 32 streams all progress, within a
// factor of their fair share, so connection-level flow control and the
// framer's round robin within BULK serve every one.
func TestManyBulkStreamsOnOneConnectionAllProgress(t *testing.T) {
	if testing.Short() {
		t.Skip("emulated-path timing test")
	}
	path := pathsim.Config{OneWayDelay: 10 * time.Millisecond, RateBytesPerSec: 1_500_000, Seed: 29}
	const n, run = 32, 4 * time.Second
	pathAuthorities.forget()
	bed := newStreamBed(t, path, flowWindows{})
	conn, ctl := bed.dial()
	waitAttached(t, ctl, 1)
	ctx, cancel := context.WithTimeout(context.Background(), run)
	defer cancel()
	for i := range n {
		go bulkStream(ctx, conn, uint32(i))
	}
	<-ctx.Done()
	total, per := bed.bulkTotals(n)
	t.Logf("aggregate %.2f MB/s; per stream min %.0f max %.0f", total/run.Seconds()/1e6, slices.Min(per), slices.Max(per))
	if total/run.Seconds() < 0.6*float64(path.RateBytesPerSec) {
		t.Errorf("32 streams delivered %.2f MB/s on a %.2f MB/s path", total/run.Seconds()/1e6, float64(path.RateBytesPerSec)/1e6)
	}
	for i, got := range per {
		if got < 0.33*total/n {
			t.Errorf("stream %d delivered %.0f of %.0f bytes", i, got, total)
		}
	}
}
