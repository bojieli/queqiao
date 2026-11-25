package pep

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/bojieli/queqiao/internal/metrics"
	"github.com/bojieli/queqiao/internal/stripe"
)

// iosMemoryLimits mirrors the ios-fixed-28m profile (mobile/core/resources.go):
// 3 MiB shared send and receive budgets and 16 KiB chunks, so 192 chunks fill
// the send budget.
func iosMemoryLimits() *MemoryLimits {
	return &MemoryLimits{
		SendBudgetBytes: 3 * 1024 * 1024, ReceiveBudgetBytes: 3 * 1024 * 1024,
		MaxFlowSendBytes: 1024 * 1024, MaxFlowReceiveBytes: 1024 * 1024,
		MaxFlowOutstanding: 64, MaxFlowReceiveFrames: 128,
		EventQueueFrames: 2, LaneWriteQueueFrames: 4, LaneInteractiveReserve: 1,
		FrameReadBufferBytes: 8 * 1024, MaxUDPPacketBytes: 4 * 1024, MaxBulkConnections: 1,
	}
}

const iosChunkSize = 16 * 1024

// TestIdleReadersCannotStarveAnActiveRequestUnderIOSLimits is issue #169. On
// main a flow's producer reserved a whole chunk of the endpoint budget before
// blocking in Read on its application socket, so budget/chunk idle keep-alive
// sockets held all of it and nothing released it: no holder had a chunk in
// flight, owed an ACK, or met the no-progress predicate. A request behind
// them was never read, and neither was an EOF on a later socket.
func TestIdleReadersCannotStarveAnActiveRequestUnderIOSLimits(t *testing.T) {
	for _, mode := range []MemoryMode{MemoryLegacy, MemoryOwned} {
		t.Run(string(mode), func(t *testing.T) {
			limits := iosMemoryLimits()
			idle := int(limits.SendBudgetBytes/iosChunkSize) + 8

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
			server, err := NewServer(ServerConfig{
				ListenAddr: "127.0.0.1:0", Credentials: certificate, Memory: mode,
				DestinationPolicy: DestinationPolicy{AllowPrivate: true}, EnableQUIC: true, Logger: logger,
				Metrics: metrics.New(), MaxSessions: 2048,
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
				Transport: TransportQUIC, EnableQUICPool: true, Logger: logger, Metrics: metrics.New(),
				ChunkSize: iosChunkSize, MaxSessions: 1024, MaxPendingOpens: 128,
				StreamReceiveWindow: 64 * 1024, MaxStreamReceiveWindow: 64 * 1024,
				ConnectionReceiveWindow: 2 * 1024 * 1024, MaxConnectionReceiveWindow: 2 * 1024 * 1024,
				MaxIncomingStreams: 32, MemoryLimits: limits, Memory: mode,
			})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go func() { _ = server.ServePacketConn(ctx, packetConn) }()
			go func() { _ = client.ServeListener(ctx, clientListener) }()

			idleConns := make([]net.Conn, 0, idle)
			defer func() {
				for _, conn := range idleConns {
					_ = conn.Close()
				}
			}()
			for range idle {
				idleConns = append(idleConns, dialTestSOCKS(t, clientListener.Addr().String(), destinationListener.Addr().String()))
			}
			waitFor(t, 10*time.Second, "idle flows to start", func() bool {
				return client.Metrics().Snapshot().ActiveFlows >= int64(idle)
			})
			// Give every idle producer time to reach its blocking read. On
			// main this is when they would have reserved the whole budget.
			time.Sleep(200 * time.Millisecond)
			if used := client.MemoryStats().Send.Used; used != 0 {
				t.Fatalf("%d idle readers hold %d bytes of shared send memory; a producer waiting on the application must hold none", idle, used)
			}

			// An active request behind them is read, sent, and answered.
			active := dialTestSOCKS(t, clientListener.Addr().String(), destinationListener.Addr().String())
			defer active.Close()
			_ = active.SetDeadline(time.Now().Add(10 * time.Second))
			request := bytes.Repeat([]byte("GET /behind-idle-readers HTTP/1.1\r\n"), 2048)
			if err := writeFull(active, request); err != nil {
				t.Fatal(err)
			}
			if err := active.(*net.TCPConn).CloseWrite(); err != nil {
				t.Fatal(err)
			}
			got, err := io.ReadAll(active)
			if err != nil {
				t.Fatalf("active request behind %d idle readers: %v", idle, err)
			}
			if !bytes.Equal(got, request) {
				t.Fatalf("active request echo: got %d bytes, want %d", len(got), len(request))
			}

			// EOF discovery on a socket opened behind the idle ones: its
			// half-close must reach the destination, whose echo then ends.
			late := dialTestSOCKS(t, clientListener.Addr().String(), destinationListener.Addr().String())
			defer late.Close()
			_ = late.SetDeadline(time.Now().Add(10 * time.Second))
			if err := late.(*net.TCPConn).CloseWrite(); err != nil {
				t.Fatal(err)
			}
			if rest, err := io.ReadAll(late); err != nil || len(rest) != 0 {
				t.Fatalf("EOF behind idle readers was not discovered: %d bytes, %v", len(rest), err)
			}

			// And an EOF on one of the idle sockets themselves.
			first := idleConns[0]
			_ = first.SetDeadline(time.Now().Add(10 * time.Second))
			if err := first.(*net.TCPConn).CloseWrite(); err != nil {
				t.Fatal(err)
			}
			if rest, err := io.ReadAll(first); err != nil || len(rest) != 0 {
				t.Fatalf("EOF on an idle reader was not discovered: %d bytes, %v", len(rest), err)
			}
			if snap := client.MemoryStats().Send; snap.Peak > snap.Capacity {
				t.Fatalf("send budget exceeded: %+v", snap)
			}
		})
	}
}

func waitFor(t *testing.T, limit time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestFlowSourceReadsOnlyWhatIsAvailable pins the contract the scheduler
// relies on while it holds a reservation: filling a chunk returns what the
// application already wrote and never waits for more.
func TestFlowSourceReadsOnlyWhatIsAvailable(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, _ := listener.Accept()
		accepted <- conn
	}()
	app, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()
	inner := <-accepted
	defer inner.Close()
	flow := newGraceTestFlow(t)
	flow.inner = inner
	source := &flowSource{flow: flow}

	if err := writeFull(app, bytes.Repeat([]byte{'x'}, 3000)); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 16*1024)
	waitFor(t, time.Second, "bytes to arrive", func() bool {
		n, err := source.ReadAvailable(buf)
		if err != nil {
			t.Fatal(err)
		}
		buf = buf[n:]
		return len(buf) == 16*1024-3000
	})
	started := time.Now()
	n, err := source.ReadAvailable(buf)
	if n != 0 || err != nil {
		t.Fatalf("idle source returned %d, %v", n, err)
	}
	if waited := time.Since(started); waited > 100*time.Millisecond {
		t.Fatalf("ReadAvailable waited %v on an idle application", waited)
	}
	// The deadline is cleared: an ordinary read still blocks and then works.
	go func() {
		time.Sleep(20 * time.Millisecond)
		_, _ = app.Write([]byte("later"))
	}()
	small := make([]byte, 5)
	if _, err := io.ReadFull(source, small); err != nil || string(small) != "later" {
		t.Fatalf("ordinary read after ReadAvailable: %q, %v", small, err)
	}
	// EOF reaches the caller as EOF.
	_ = app.(*net.TCPConn).CloseWrite()
	waitFor(t, time.Second, "EOF", func() bool {
		_, err := source.ReadAvailable(buf)
		return errors.Is(err, io.EOF)
	})
}

// TestReadyBytesArePendingWorkOnlyUnderOwnedMemory: bytes read while a flow
// has no binding hold a shared reservation, so under the owned bundle they
// count toward the no-progress budget that ends the flow (design §8.3).
func TestReadyBytesArePendingWorkOnlyUnderOwnedMemory(t *testing.T) {
	for _, owned := range []bool{false, true} {
		t.Run(fmt.Sprintf("owned=%v", owned), func(t *testing.T) {
			flow := newGraceTestFlow(t)
			flow.memOwned = owned
			sched := stripe.New(bytes.NewReader([]byte("ready but unbound")), stripe.Config{ChunkSize: 1024})
			defer sched.Close()
			flow.scheduler.Store(sched)
			waitFor(t, time.Second, "READY bytes", func() bool { return sched.ReadyBytes() > 0 })
			if got := flow.pendingOutbound(); got != owned {
				t.Fatalf("pendingOutbound = %v with only READY bytes", got)
			}
		})
	}
}

func TestReadyBytesWithNoBindingEndTheOwnedFlow(t *testing.T) {
	flow := newGraceTestFlow(t)
	flow.memOwned = true
	flow.progressGrace = 150 * time.Millisecond
	flow.stallScan = time.Millisecond
	flow.stallGrace = 2 * time.Millisecond
	sched := stripe.New(bytes.NewReader([]byte("ready but unbound")), stripe.Config{ChunkSize: 1024})
	defer sched.Close()
	flow.scheduler.Store(sched)
	waitFor(t, time.Second, "READY bytes", func() bool { return sched.ReadyBytes() > 0 })
	stop := make(chan struct{})
	defer close(stop)
	go flow.stallWatchdog(stop)
	select {
	case err := <-flow.ackErr:
		if !errors.Is(err, errFlowNoProgress) {
			t.Fatalf("flow ended with %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("READY bytes with no binding never ended the flow")
	}
}

func TestMemoryBundleValidator(t *testing.T) {
	for _, value := range []string{"", "legacy", "owned"} {
		if _, err := ParseMemoryMode(value); err != nil {
			t.Fatalf("%q rejected: %v", value, err)
		}
	}
	for _, value := range []string{"on", "OWNED", "owned,legacy"} {
		if _, err := ParseMemoryMode(value); err == nil {
			t.Fatalf("%q accepted", value)
		}
	}
	_, sendMemory, _, err := resolveMemoryLimits(iosMemoryLimits(), iosChunkSize, MemoryOwned)
	if err != nil || sendMemory == nil {
		t.Fatalf("owned iOS limits: %v", err)
	}
}

// Replay safety (design §8.4) does not depend on the memory bundle: retained
// bytes are released only by validated flow ACKs, so a flow whose lane dies,
// or which is rescued from QUIC onto TCP, replays everything the peer had not
// acknowledged. These run the existing end-to-end replay tests with a shared
// send budget -- so every chunk goes through staging -- in both bundles.
func TestReplaySurvivesLaneDeathWithSharedMemory(t *testing.T) {
	for _, mode := range []MemoryMode{MemoryLegacy, MemoryOwned} {
		t.Run(string(mode), func(t *testing.T) { runQUICFlowSurvivesOneLaneFailure(t, mode, iosMemoryLimits()) })
	}
}

func TestReplaySurvivesQUICToTCPRescueWithSharedMemory(t *testing.T) {
	for _, mode := range []MemoryMode{MemoryLegacy, MemoryOwned} {
		t.Run(string(mode), func(t *testing.T) { runAutoFlowInstallsTCPRescueAfterAllQUICLanesFail(t, mode, iosMemoryLimits()) })
	}
}
