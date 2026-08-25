package pep

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	crand "crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/bojieli/queqiao/internal/baseline"
	"github.com/bojieli/queqiao/internal/pathsim"
)

// The datacenter workload is not one flow. It is hundreds of concurrent
// requests, every one of them latency-critical, and the question this file
// exists to answer is what happens to the slowest of them.
//
// That question cannot be answered by a throughput number or by a single
// flow's completion time, and it is the one the published comparison in
// docs/COMPARISON.md currently loses: SSH p99 under load at 940ms against
// TUIC's 662ms. On an access link that is a caveat, because the bulk transfer
// it competes with is also the thing being won. On a datacenter leg there is
// no bulk transfer -- every flow is a request -- so the tail is not a caveat,
// it is the whole result.
//
// The comparison runs against internal/baseline, which is TUIC's data-path
// shape on the same QUIC fork and the same controllers in the same process, so
// a gap is the design rather than the library.

// requestSize is one inference request: the payload size the datacenter path
// characterisation measured, chosen because it is what an audio upload is.
const requestSize = 300 * 1024

// concurrentFlows is the load. It is large enough that flows contend and small
// enough that a laptop can run it; the shape of the tail, not its absolute
// value, is what transfers to a real gateway.
const concurrentFlows = 48

type latencies []time.Duration

func (l latencies) quantile(p float64) time.Duration {
	if len(l) == 0 {
		return 0
	}
	s := append(latencies(nil), l...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	i := int(p * float64(len(s)-1))
	return s[i]
}

func (l latencies) report(t *testing.T, name string) {
	t.Helper()
	if len(l) == 0 {
		t.Fatalf("%s: no flow completed", name)
	}
	p50, p99 := l.quantile(0.50), l.quantile(0.99)
	ratio := float64(p99) / float64(p50)
	t.Logf("%-22s n=%3d p50=%7.1fms p90=%7.1fms p99=%7.1fms  p99/p50=%.2f",
		name, len(l),
		float64(p50.Microseconds())/1000,
		float64(l.quantile(0.90).Microseconds())/1000,
		float64(p99.Microseconds())/1000, ratio)
}

// echoServer answers each request with a single byte once it has the whole
// thing, which is the request/response shape and makes completion something
// the peer confirms rather than something the sender's socket reports.
func echoServer(t *testing.T) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				if _, err := io.CopyN(io.Discard, c, requestSize); err != nil {
					return
				}
				_, _ = c.Write([]byte{1})
			}(c)
		}
	}()
	return ln
}

// driveConcurrent opens every flow at once through the SOCKS listener and
// returns how long each took from dial to acknowledgement.
//
// They start together on purpose. Staggering them would measure a queue that
// never forms, and the case the datacenter profile has to survive is a gateway
// whose sessions all speak at once because their users did.
func driveConcurrent(t *testing.T, socksAddr string, destination net.Listener, n int) latencies {
	t.Helper()
	payload := make([]byte, requestSize)
	if _, err := rand.Read(payload[:1024]); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var out latencies
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			began := time.Now()
			conn, err := trySocksDial(socksAddr, destination, 120*time.Second)
			if err != nil {
				return
			}
			defer conn.Close()
			if _, err := conn.Write(payload); err != nil {
				return
			}
			ack := make([]byte, 1)
			_ = conn.SetReadDeadline(time.Now().Add(120 * time.Second))
			if _, err := io.ReadFull(conn, ack); err != nil {
				return
			}
			mu.Lock()
			out = append(out, time.Since(began))
			mu.Unlock()
		}()
	}
	close(start)
	wg.Wait()
	return out
}

// TestTailUnderConcurrentRequests is the Phase 6 gate of the datacenter plan.
//
// It is skipped in short mode because it runs a hundred flows across an
// emulated 200ms path, and it reports rather than asserts a threshold: the
// number that matters is the gap to the reference, and pinning an absolute
// millisecond figure to a shared CI machine would produce a test that fails
// for reasons having nothing to do with the transport.
// TestConcurrencyHarnessWorksAtOne verifies both arms before either is
// believed at load. An arm that completes nothing under concurrency and an arm
// that was never wired correctly are indistinguishable in the result, and only
// one of them is a finding.
func TestConcurrencyHarnessWorksAtOne(t *testing.T) {
	if testing.Short() {
		t.Skip("brings up two transports across an emulated path")
	}
	clean := pathsim.DCLongHaulClean()
	if got := measureBaselineTailN(t, clean, 1); len(got) != 1 {
		t.Fatalf("the reference arm completed %d of 1 flow on a clean path: the harness is wrong, not the transport", len(got))
	} else {
		got.report(t, "reference @1 clean")
	}
	// Separating the two variables matters. An arm that fails at concurrency
	// on a lossy path has told us nothing until we know whether it fails at
	// one flow on the same path.
	lossy := pathsim.DCLongHaul()
	got := measureBaselineTailN(t, lossy, 1)
	if len(got) == 0 {
		t.Log("reference @1 lossy: no flow completed -- the 14% erasure alone defeats it, before any concurrency")
	} else {
		got.report(t, "reference @1 lossy")
	}
	qq := measureQueqiaoTailN(t, lossy, 1)
	if len(qq) == 0 {
		t.Log("queqiao @1 lossy: no flow completed")
	} else {
		qq.report(t, "queqiao   @1 lossy")
	}
}

func TestTailUnderConcurrentRequests(t *testing.T) {
	if testing.Short() {
		t.Skip("concurrent tail measurement is slow by construction")
	}
	path := pathsim.DCLongHaul()

	// A sweep rather than a single load. One point tells you whether an arm
	// survived a number somebody picked; a curve tells you where it stops
	// surviving, and the shape between those points is the scheduling
	// behaviour this gate exists to examine.
	for _, n := range []int{1, 4, 16, 48} {
		qq := measureQueqiaoTailN(t, path, n)
		ref := measureBaselineTailN(t, path, n)
		t.Logf("--- %d concurrent 300KB requests ---", n)
		reportOrSilence(t, fmt.Sprintf("queqiao   @%d", n), qq, n)
		reportOrSilence(t, fmt.Sprintf("reference @%d", n), ref, n)
	}
}

// reportOrSilence prints the distribution, or says plainly that flows did not
// finish. Completion rate is part of the result: an arm that abandons flows has
// a better tail for the worst possible reason, and averaging over only the
// survivors would hide it.
func reportOrSilence(t *testing.T, name string, l latencies, want int) {
	t.Helper()
	if len(l) == 0 {
		t.Logf("%-22s completed 0/%d", name, want)
		return
	}
	p50, p99 := l.quantile(0.50), l.quantile(0.99)
	t.Logf("%-22s %d/%d  p50=%7.1fms p90=%7.1fms p99=%7.1fms  p99/p50=%.2f",
		name, len(l), want,
		float64(p50.Microseconds())/1000,
		float64(l.quantile(0.90).Microseconds())/1000,
		float64(p99.Microseconds())/1000, float64(p99)/float64(p50))
}

func measureQueqiaoTail(t *testing.T, path pathsim.Config) latencies {
	return measureQueqiaoTailN(t, path, concurrentFlows)
}

func measureQueqiaoTailN(t *testing.T, path pathsim.Config, n int) latencies {
	t.Helper()
	socks, destination := codedPairWith(t, true, &path, func(ln net.Listener) {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				if _, err := io.CopyN(io.Discard, c, requestSize); err != nil {
					return
				}
				_, _ = c.Write([]byte{1})
			}(c)
		}
	})
	return driveConcurrent(t, socks, destination, n)
}

// measureBaselineTail runs the same load across the same emulated path through
// the TUIC-shaped reference.
func measureBaselineTail(t *testing.T, path pathsim.Config) latencies {
	return measureBaselineTailN(t, path, concurrentFlows)
}

func measureBaselineTailN(t *testing.T, path pathsim.Config, n int) latencies {
	t.Helper()
	destination := echoServer(t)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	token := make([]byte, 32)
	if _, err := rand.Read(token); err != nil {
		t.Fatal(err)
	}
	packet, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = packet.Close() })

	cert, pool := baselineIdentity(t)
	server, err := baseline.NewServer(baseline.ServerConfig{
		ListenAddr: "127.0.0.1:0", Certificate: cert,
		Token: token, Logger: logger,
	})
	if err != nil {
		t.Skipf("baseline server unavailable: %v", err)
	}
	relay, err := pathsim.New("127.0.0.1:0", packet.LocalAddr().String(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = relay.Close() })

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	client, err := baseline.NewClient(baseline.ClientConfig{
		ListenAddr: listener.Addr().String(), RemoteAddr: relay.LocalAddr(),
		ServerName: "queqiao.test", RootCAs: pool,
		Token: token, Logger: logger,
	})
	if err != nil {
		t.Skipf("baseline client unavailable: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = server.Serve(ctx, packet) }()
	go func() { _ = client.ServeListener(ctx, listener) }()

	return driveConcurrent(t, listener.Addr().String(), destination, n)
}

// The reference takes a tls.Certificate and an *x509.CertPool where queqiao
// takes its own credential types, so the harness issues one identity for the
// reference arm and reuses it. Both arms then present a certificate of the
// same shape, which matters because handshake size is not the variable under
// test.
var (
	baselineOnce sync.Once
	baselineCert tls.Certificate
	baselinePool *x509.CertPool
	baselineErr  error
)

func baselineIdentity(t *testing.T) (tls.Certificate, *x509.CertPool) {
	t.Helper()
	baselineOnce.Do(func() {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			baselineErr = err
			return
		}
		serial, err := crand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
		if err != nil {
			baselineErr = err
			return
		}
		template := x509.Certificate{
			SerialNumber:          serial,
			Subject:               pkix.Name{CommonName: "queqiao.test"},
			DNSNames:              []string{"queqiao.test"},
			NotBefore:             time.Now().Add(-time.Hour),
			NotAfter:              time.Now().Add(24 * time.Hour),
			KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
			ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
			IsCA:                  true,
			BasicConstraintsValid: true,
		}
		der, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
		if err != nil {
			baselineErr = err
			return
		}
		leaf, err := x509.ParseCertificate(der)
		if err != nil {
			baselineErr = err
			return
		}
		baselineCert = tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}
		baselinePool = x509.NewCertPool()
		baselinePool.AddCert(leaf)
	})
	if baselineErr != nil {
		t.Fatalf("baseline identity: %v", baselineErr)
	}
	return baselineCert, baselinePool
}

// The other traffic shape. A voice session sends a 20ms frame of audio fifty
// times a second, and a gateway carries hundreds of them at once. Nothing
// about that resembles the burst measured above: the payloads are tens of
// bytes rather than hundreds of kilobytes, the flow never ends, and what
// matters is not when it completes but whether any single frame arrives late.
//
// The mechanisms it stresses are different too. A burst is a rate problem; a
// frame stream is a scheduling and head-of-line problem, where one lost packet
// can hold up a session that had nothing to do with it.
const (
	frameBytes    = 80
	frameInterval = 20 * time.Millisecond
	framesPerFlow = 100 // two seconds of speech
	voiceSessions = 24
	// pathRoundTrip is what pathsim.DCLongHaul emulates, and the floor no
	// frame can beat.
	pathRoundTrip = 200 * time.Millisecond
)

// TestFrameLatencyUnderConcurrentSessions measures the other half of §2.3.
//
// It reports the distribution of per-frame round trips rather than a
// completion time, because a session that finishes on schedule while a tenth
// of its frames arrived 400ms late is a session the listener heard break up.
func TestFrameLatencyUnderConcurrentSessions(t *testing.T) {
	if testing.Short() {
		t.Skip("brings up a transport across an emulated path")
	}
	path := pathsim.DCLongHaul()
	socks, destination := codedPairWith(t, true, &path, echoFrames)

	var mu sync.Mutex
	var all latencies
	var late int
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < voiceSessions; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			conn, err := trySocksDial(socks, destination, 120*time.Second)
			if err != nil {
				return
			}
			defer conn.Close()
			frame := make([]byte, frameBytes)
			reply := make([]byte, frameBytes)
			ticker := time.NewTicker(frameInterval)
			defer ticker.Stop()
			for f := 0; f < framesPerFlow; f++ {
				<-ticker.C
				sent := time.Now()
				if _, err := conn.Write(frame); err != nil {
					return
				}
				_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
				if _, err := io.ReadFull(conn, reply); err != nil {
					return
				}
				rtt := time.Since(sent)
				mu.Lock()
				all = append(all, rtt)
				// Lateness has to be measured against the path, not against
				// the frame interval. Every frame on a 200ms path takes at
				// least 200ms, so counting anything above one interval counts
				// all of them and distinguishes nothing. What a jitter buffer
				// actually has to absorb is the spread above the path's own
				// minimum, and a frame more than one interval above it has
				// displaced the frame behind it.
				if rtt > pathRoundTrip+frameInterval {
					late++
				}
				mu.Unlock()
			}
		}()
	}
	close(start)
	wg.Wait()

	if len(all) == 0 {
		t.Fatal("no frame completed a round trip")
	}
	p50, p99 := all.quantile(0.50), all.quantile(0.99)
	t.Logf("%d sessions x %d frames: %d delivered, p50=%.1fms p90=%.1fms p99=%.1fms p99.9=%.1fms",
		voiceSessions, framesPerFlow, len(all),
		float64(p50.Microseconds())/1000,
		float64(all.quantile(0.90).Microseconds())/1000,
		float64(p99.Microseconds())/1000,
		float64(all.quantile(0.999).Microseconds())/1000)
	t.Logf("frames more than one interval above the path's own round trip: %d of %d (%.2f%%)",
		late, len(all), 100*float64(late)/float64(len(all)))
	// The path's own round trip is 200ms, so a frame cannot beat that and the
	// interesting quantity is how far above it the tail sits.
	t.Logf("p99 above the path's round trip: %.1fms", float64((p99-200*time.Millisecond).Microseconds())/1000)
}

// echoFrames returns each frame as it arrives, which is what a decoder feeding
// a response stream does.
func echoFrames(ln net.Listener) {
	for {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		go func(c net.Conn) {
			defer c.Close()
			buf := make([]byte, frameBytes)
			for {
				if _, err := io.ReadFull(c, buf); err != nil {
					return
				}
				if _, err := c.Write(buf); err != nil {
					return
				}
			}
		}(c)
	}
}
