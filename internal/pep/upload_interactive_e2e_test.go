package pep

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bojieli/queqiao/internal/metrics"
	"github.com/bojieli/queqiao/internal/pathmodel"
	"github.com/bojieli/queqiao/internal/pathsim"
	"github.com/bojieli/queqiao/internal/protocol"
	"github.com/bojieli/queqiao/internal/stripe"
)

// An LLM prompt uploaded beside a bulk upload (the impaired campaign's
// blackhole trial, between drops): a 20 Mbit/s path with +150 ms of round
// trip, one flow uploading without pause, and an agent loop on one
// persistent connection that uploads a ~100-130 KB prompt per turn and then
// receives a stream of small deltas. R is what the campaign's workload server
// measures: the time from the first to the last byte of a prompt at the
// destination.

type uploadArm string

const (
	armMain uploadArm = "main" // classifier=phased only, as the campaign's control
	armS7   uploadArm = "s7"   // the stage 1-7 bundle
	armS8   uploadArm = "s8"   // s7 plus recovery=carrier
	// armS7MemLegacy is s7 without mem=owned: separates the memory bundle
	// from the path controller and topology.
	armS7MemLegacy uploadArm = "s7-mem-legacy"
)

type uploadPair struct {
	socks       string
	destination net.Listener
	client      *Client
	server      *Server
	clientLogs  *lockedBuffer
}

func newUploadPair(t *testing.T, arm uploadArm, path pathsim.Config, serve func(net.Listener)) *uploadPair {
	t.Helper()
	pathmodel.Reset()
	t.Cleanup(pathmodel.Reset)
	p := &uploadPair{clientLogs: &lockedBuffer{}}
	destination, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = destination.Close() })
	go serve(destination)
	p.destination = destination
	certificate, roots := testCertificate(t)
	packetConn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	scfg := ServerConfig{
		ListenAddr: "127.0.0.1:0", Credentials: certificate,
		DestinationPolicy: DestinationPolicy{AllowPrivate: true}, EnableQUIC: true,
		Logger: slog.New(slog.DiscardHandler), Metrics: metrics.New(), HandshakeTimeout: 30 * time.Second,
		Classifier: ClassifierPhased,
	}
	ccfg := ClientConfig{
		Credentials: roots, Transport: TransportQUIC, EnableQUICPool: true, Metrics: metrics.New(),
		Logger:     slog.New(slog.NewJSONHandler(p.clientLogs, &slog.HandlerOptions{Level: slog.LevelDebug})),
		Classifier: ClassifierPhased,
	}
	if arm != armMain {
		scfg.CCAPI, scfg.Sched, scfg.CongestionScope = CCAPIV2, SchedClass, CongestionScopePath
		scfg.CodedRetry, scfg.Memory, scfg.GatewayAdmission = CodedRetrySticky, MemoryOwned, GatewayAdmissionOwned
		ccfg.CCAPI, ccfg.Sched, ccfg.CongestionScope, ccfg.Topology = CCAPIV2, SchedClass, CongestionScopePath, TopologySingle
		ccfg.CodedRetry, ccfg.Memory = CodedRetrySticky, MemoryOwned
	}
	if arm == armS7MemLegacy {
		scfg.Memory, ccfg.Memory = MemoryLegacy, MemoryLegacy
	}
	if arm == armS8 {
		scfg.Recovery, ccfg.Recovery = RecoveryCarrier, RecoveryCarrier
	}
	server, err := NewServer(scfg)
	if err != nil {
		t.Fatal(err)
	}
	relay, err := pathsim.New("127.0.0.1:0", packetConn.LocalAddr().String(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = relay.Close() })
	clientListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ccfg.ListenAddr, ccfg.RemoteAddr = clientListener.Addr().String(), relay.LocalAddr()
	client, err := NewClient(ccfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = server.ServePacketConn(ctx, packetConn) }()
	go func() { _ = client.ServeListener(ctx, clientListener) }()
	p.socks, p.client, p.server = clientListener.Addr().String(), client, server
	return p
}

// promptDestination serves two kinds of connection by their first byte: a
// bulk upload it counts and discards, and an agent loop whose every turn is
// a 4-byte length, the prompt, then a stream of deltas. It reports each
// prompt's R and the bulk bytes received.
type promptDestination struct {
	mu       sync.Mutex
	rs       []time.Duration
	bulkIn   atomic.Int64
	deltas   int
	cadence  time.Duration
	thinking time.Duration
}

func (d *promptDestination) serve(l net.Listener) {
	for {
		conn, err := l.Accept()
		if err != nil {
			return
		}
		go func() {
			defer conn.Close()
			var first [1]byte
			if _, err := io.ReadFull(conn, first[:]); err != nil {
				return
			}
			if first[0] == uploadMarker {
				buf := make([]byte, 64<<10)
				for {
					n, err := conn.Read(buf)
					d.bulkIn.Add(int64(n))
					if err != nil {
						return
					}
				}
			}
			var hdr [4]byte
			buf := make([]byte, 64<<10)
			delta := make([]byte, 180)
			for {
				if _, err := io.ReadFull(conn, hdr[:]); err != nil {
					return
				}
				left := int(binary.BigEndian.Uint32(hdr[:]))
				var start time.Time
				for left > 0 {
					n, err := conn.Read(buf[:min(left, len(buf))])
					if n > 0 && start.IsZero() {
						start = time.Now()
					}
					left -= n
					if err != nil {
						return
					}
				}
				d.mu.Lock()
				d.rs = append(d.rs, time.Since(start))
				d.mu.Unlock()
				time.Sleep(d.thinking)
				for range d.deltas {
					if _, err := conn.Write(delta); err != nil {
						return
					}
					time.Sleep(d.cadence)
				}
			}
		}()
	}
}

type promptRun struct {
	arm uploadArm
	rs  []time.Duration
	// classes and coded record, per prompt, the LLM flow's DATA class when
	// the prompt was written and how many of its bytes went coded.
	classes    []string
	coded      []uint64
	bulkMbps   float64
	withUpload bool
}

func (r promptRun) String() string {
	rs := append([]time.Duration(nil), r.rs...)
	sort.Slice(rs, func(i, j int) bool { return rs[i] < rs[j] })
	s := fmt.Sprintf("%-4s upload=%-5v R per prompt:", r.arm, r.withUpload)
	for i, d := range r.rs {
		s += fmt.Sprintf(" %.3fs", d.Seconds())
		if i < len(r.classes) {
			s += fmt.Sprintf("[%s,coded %dK]", r.classes[i], r.coded[i]/1000)
		}
	}
	return s + fmt.Sprintf(" | median %.3fs max %.3fs | bulk upload %.2f Mbit/s", rs[len(rs)/2].Seconds(), rs[len(rs)-1].Seconds(), r.bulkMbps)
}

// runPromptLoop runs one arm: optionally a bulk upload, then an agent loop of
// turns prompts.
func runPromptLoop(t *testing.T, arm uploadArm, withUpload bool, turns int) promptRun {
	t.Helper()
	d := &promptDestination{deltas: 40, cadence: 25 * time.Millisecond, thinking: 500 * time.Millisecond}
	path := pathsim.Config{OneWayDelay: 75 * time.Millisecond, RateBytesPerSec: 20_000_000 / 8, QueueBytes: 512 << 10, Seed: 61}
	var flows flowTracker
	p := newUploadPair(t, arm, path, d.serve)
	p.client.flowStartedForTest = flows.add
	open := func() net.Conn {
		conn, err := trySocksDial(p.socks, p.destination, 30*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		_ = conn.SetDeadline(time.Time{})
		t.Cleanup(func() { _ = conn.Close() })
		return conn
	}
	if withUpload {
		up := open()
		_, _ = up.Write([]byte{uploadMarker})
		go func() {
			buf := make([]byte, 32<<10)
			for {
				if _, err := up.Write(buf); err != nil {
					return
				}
			}
		}()
		time.Sleep(5 * time.Second)
	}
	llm := open()
	_, _ = llm.Write([]byte{0x01})
	index := 0
	if withUpload {
		index = 1
	}
	flow := flows.at(t, index)
	var classes []string
	var codedBytes []uint64
	bulkStart, start := d.bulkIn.Load(), time.Now()
	reply := make([]byte, 180*d.deltas)
	for turn := range turns {
		size := 100_000 + 8_000*turn
		prompt := make([]byte, 4+size)
		binary.BigEndian.PutUint32(prompt, uint32(size))
		_ = llm.SetDeadline(time.Now().Add(60 * time.Second))
		codedBefore, _ := flow.dataSubstrates()
		if _, err := llm.Write(prompt); err != nil {
			t.Fatalf("%s: prompt %d: %v", arm, turn, err)
		}
		if _, err := io.ReadFull(llm, reply[:1]); err != nil {
			t.Fatalf("%s: reply %d: %v", arm, turn, err)
		}
		codedAfter, _ := flow.dataSubstrates()
		classes = append(classes, fmt.Sprint(f2class(flow)))
		codedBytes = append(codedBytes, codedAfter-codedBefore)
		if _, err := io.ReadFull(llm, reply[1:]); err != nil {
			t.Fatalf("%s: reply %d: %v", arm, turn, err)
		}
		time.Sleep(time.Second) // tool execution between turns
	}
	elapsed := time.Since(start)
	d.mu.Lock()
	defer d.mu.Unlock()
	return promptRun{arm: arm, withUpload: withUpload, rs: append([]time.Duration(nil), d.rs...), classes: classes, coded: codedBytes,
		bulkMbps: float64(d.bulkIn.Load()-bulkStart) * 8 / elapsed.Seconds() / 1e6}
}

func TestPromptUploadBesideABulkUpload(t *testing.T) {
	if testing.Short() {
		t.Skip("wall-clock emulated path")
	}
	for _, arm := range []uploadArm{armMain, armS7, armS7MemLegacy, armS8} {
		for _, withUpload := range []bool{false, true} {
			r := runPromptLoop(t, arm, withUpload, 6)
			t.Log(r)
			if withUpload || arm == armMain {
				continue
			}
			// Alone on the path, a prompt over the four-chunk startup
			// allowance (turns 4 and 5: 132 and 140 KB) must not wait a
			// credit round for its last bytes: the path's round trip is
			// 150 ms. (A single early prompt occasionally takes about one
			// round trip with or without mem=owned; that is not this bound.)
			for turn, d := range r.rs {
				if size := 100_000 + 8_000*turn; size <= stripe.DefaultAutotuneStartupChunks*32<<10 {
					continue
				}
				if d > 100*time.Millisecond {
					t.Errorf("%s: prompt %d (%d bytes) took R = %v alone on the path; a credit round trip was paid", arm, turn, 100_000+8_000*turn, d)
				}
			}
		}
	}
}

func f2class(f *multipathFlow) string {
	switch f.sendClass(false) {
	case protocol.ClassBulk:
		return "B"
	case protocol.ClassInteractive:
		return "I"
	}
	return "N"
}
