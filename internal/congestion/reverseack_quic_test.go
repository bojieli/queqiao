package congestion

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"io"
	"math/big"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/apernet/quic-go"

	"github.com/bojieli/queqiao/internal/pathmodel"
	"github.com/bojieli/queqiao/internal/pathsim"
)

func reverseAckTLS(t *testing.T) (*tls.Config, *tls.Config) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "reverse-ack.test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		DNSNames:     []string{"reverse-ack.test"},
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IsCA:         true,
	}
	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(leaf)
	server := &tls.Config{
		Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}},
		NextProtos:   []string{"reverse-ack-test"},
	}
	client := &tls.Config{RootCAs: roots, ServerName: "reverse-ack.test", NextProtos: []string{"reverse-ack-test"}}
	return server, client
}

// reverseAckPair is one download connection: the server end sends bulk, the
// client end's controller is a member of the client's shared upstream pool.
func reverseAckPair(t *testing.T, cfg pathsim.Config, upstream pathmodel.Model, downstream pathmodel.Model) (client, server *quic.Conn) {
	t.Helper()
	serverTLS, clientTLS := reverseAckTLS(t)
	quicCfg := &quic.Config{
		MaxIdleTimeout:                 60 * time.Second,
		KeepAlivePeriod:                5 * time.Second,
		InitialStreamReceiveWindow:     8 << 20,
		InitialConnectionReceiveWindow: 16 << 20,
		MaxStreamReceiveWindow:         32 << 20,
		MaxConnectionReceiveWindow:     64 << 20,
	}
	listener, err := quic.ListenAddr("127.0.0.1:0", serverTLS, quicCfg)
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
		conn, err := listener.Accept(context.Background())
		if err == nil {
			accepted <- conn
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	client, err = quic.DialAddr(ctx, relay.LocalAddr(), clientTLS, quicCfg)
	if err != nil {
		t.Fatalf("dial across the emulated path: %v", err)
	}
	t.Cleanup(func() { _ = client.CloseWithError(0, "") })
	select {
	case server = <-accepted:
	case <-time.After(20 * time.Second):
		t.Fatal("server did not accept")
	}
	t.Cleanup(func() { _ = server.CloseWithError(0, "") })
	client.SetCongestionControl(NewErasureSenderOn(client.InitialPacketSize(), upstream))
	server.SetCongestionControl(NewErasureSenderOn(server.InitialPacketSize(), downstream))
	return client, server
}

// A download's reverse direction, over real QUIC connections: several
// downloads at once, each client connection a member of the client's pooled
// upstream model, each carrying the heavy stream of QUIC ACK-only packets its
// download elicits and, on a stream, sparse small messages -- the flow
// acknowledgements the gateway's sender is clocked by. Those messages are a
// trickle on a path with room for thousands of times as much, so each should
// arrive about one way's delay after it is written.
//
// On the branch before this change they waited seconds: the pool capped each
// upstream connection at 1.25x the trickle it had measured delivering, the
// ACK-only packets were charged to that pacer though they bypass it, and a
// message waits for a full datagram's budget.
func TestSparseStreamWritesAmidReverseAcksArriveOnTime(t *testing.T) {
	if testing.Short() {
		t.Skip("runs real QUIC transfers for several seconds")
	}
	const (
		downloads  = 4
		oneWay     = 40 * time.Millisecond
		duration   = 8 * time.Second
		warmup     = 3 * time.Second
		interval   = 100 * time.Millisecond
		messageLen = 64
	)
	cfg := pathsim.Config{OneWayDelay: oneWay, RateBytesPerSec: 3e6, LossRate: 0.01, Seed: 5}
	upstream, downstream := pathmodel.NewPathModel(), pathmodel.NewPathModel()
	start := time.Now()
	var mu sync.Mutex
	var latencies []time.Duration
	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), duration+10*time.Second)
	defer cancel()
	for i := 0; i < downloads; i++ {
		cfg.Seed = int64(5 + i)
		client, server := reverseAckPair(t, cfg, upstream, downstream)
		// Stream reads and writes do not watch ctx. Closing the connections
		// when it expires turns a transfer that stopped making progress --
		// the regression this test is for -- into a bounded failure.
		stop := context.AfterFunc(ctx, func() {
			_ = client.CloseWithError(0, "test deadline")
			_ = server.CloseWithError(0, "test deadline")
		})
		defer stop()
		bulk, err := server.OpenStreamSync(ctx)
		if err != nil {
			t.Fatal(err)
		}
		wg.Add(4)
		// The download.
		go func() {
			defer wg.Done()
			buf := make([]byte, 32<<10)
			for time.Since(start) < duration {
				if _, err := bulk.Write(buf); err != nil {
					return
				}
			}
			_ = bulk.Close()
		}()
		go func() {
			defer wg.Done()
			stream, err := client.AcceptStream(ctx)
			if err != nil {
				return
			}
			_, _ = io.Copy(io.Discard, stream)
		}()
		// The acknowledgements: timestamped messages up a client stream.
		up, err := client.OpenStreamSync(ctx)
		if err != nil {
			t.Fatal(err)
		}
		go func() {
			defer wg.Done()
			msg := make([]byte, messageLen)
			for time.Since(start) < duration {
				binary.BigEndian.PutUint64(msg, uint64(time.Now().UnixNano()))
				if _, err := up.Write(msg); err != nil {
					return
				}
				time.Sleep(interval)
			}
			_ = up.Close()
		}()
		go func() {
			defer wg.Done()
			stream, err := server.AcceptStream(ctx)
			if err != nil {
				return
			}
			msg := make([]byte, messageLen)
			for {
				if _, err := io.ReadFull(stream, msg); err != nil {
					return
				}
				sent := time.Unix(0, int64(binary.BigEndian.Uint64(msg)))
				if sent.Sub(start) >= warmup {
					mu.Lock()
					latencies = append(latencies, time.Since(sent))
					mu.Unlock()
				}
			}
		}()
	}
	wg.Wait()
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	if len(latencies) == 0 {
		t.Fatal("no acknowledgement arrived after the warm-up")
	}
	expected := int((duration - warmup) / interval * downloads)
	p50, p90 := latencies[len(latencies)/2], latencies[len(latencies)*9/10]
	t.Logf("%d of about %d messages after the warm-up: transit p50 %v p90 %v max %v",
		len(latencies), expected, p50.Round(time.Millisecond), p90.Round(time.Millisecond), latencies[len(latencies)-1].Round(time.Millisecond))
	if p90 > 6*oneWay {
		t.Errorf("sparse upstream messages took %v (p90) to cross a %v one-way path with room to spare", p90, oneWay)
	}
	if len(latencies) < expected*8/10 {
		t.Errorf("only %d of about %d messages arrived", len(latencies), expected)
	}
}
