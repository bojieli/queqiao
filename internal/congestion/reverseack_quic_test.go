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
func reverseAckPair(t *testing.T, cfg pathsim.Config, upstream pathmodel.Model, downstream pathmodel.Model, v2 bool) (client, server *quic.Conn, clientController TelemetryProvider) {
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
	clientSender := NewErasureSenderOn(client.InitialPacketSize(), upstream)
	serverSender := NewErasureSenderOn(server.InitialPacketSize(), downstream)
	if v2 {
		attached := NewErasurePathSender(clientSender, nil)
		client.SetCongestionControl(attached)
		server.SetCongestionControl(NewErasurePathSender(serverSender, nil))
		return client, server, attached
	}
	client.SetCongestionControl(clientSender)
	server.SetCongestionControl(serverSender)
	return client, server, clientSender
}

// A download's reverse direction, over real QUIC connections and the fork's
// real send loops: several downloads at once, each client connection a member
// of the client's pooled upstream model, each carrying the heavy stream of QUIC
// ACK-only packets its download elicits and, on a stream, sparse small
// messages -- the flow acknowledgements the gateway's sender is clocked by.
//
// Through the per-packet callbacks (cc-api v1, main) every ACK-only packet is
// charged to the client's pacer. Through cc-api v2 none is: the ACK-only bytes
// are registered and the pacer's budget is left for the messages.
func TestReverseAckOnlyTrafficOverRealQUIC(t *testing.T) {
	if testing.Short() {
		t.Skip("runs real QUIC transfers for several seconds")
	}
	for _, v2 := range []bool{false, true} {
		name := "v1"
		if v2 {
			name = "v2"
		}
		t.Run(name, func(t *testing.T) {
			p50, p90, tel := runReverseAckDownloads(t, v2)
			t.Logf("cc-api %s: upstream message transit p50 %v p90 %v; client ACK-only bytes %d, debited %d",
				name, p50.Round(time.Millisecond), p90.Round(time.Millisecond), tel.NonElicitingBytes, tel.NonElicitingDebited)
			if tel.NonElicitingBytes == 0 {
				t.Fatal("the download elicited no ACK-only packets from the client")
			}
			if !v2 && tel.NonElicitingDebited != tel.NonElicitingBytes {
				t.Fatalf("v1 debited %d of %d ACK-only bytes; it must stay as main and debit all of them", tel.NonElicitingDebited, tel.NonElicitingBytes)
			}
			if v2 && tel.NonElicitingDebited != 0 {
				t.Fatalf("v2 debited %d ACK-only bytes from the pacer", tel.NonElicitingDebited)
			}
		})
	}
}

func runReverseAckDownloads(t *testing.T, v2 bool) (p50, p90 time.Duration, clientTelemetry ControllerTelemetry) {
	t.Helper()
	const (
		downloads  = 4
		oneWay     = 40 * time.Millisecond
		duration   = 6 * time.Second
		warmup     = 2 * time.Second
		interval   = 100 * time.Millisecond
		messageLen = 64
	)
	cfg := pathsim.Config{OneWayDelay: oneWay, RateBytesPerSec: 3e6, LossRate: 0.01, Seed: 5}
	upstream, downstream := pathmodel.NewPathModel(), pathmodel.NewPathModel()
	start := time.Now()
	var mu sync.Mutex
	var latencies []time.Duration
	var controllers []TelemetryProvider
	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), duration+10*time.Second)
	defer cancel()
	for i := 0; i < downloads; i++ {
		cfg.Seed = int64(5 + i)
		client, server, controller := reverseAckPair(t, cfg, upstream, downstream, v2)
		controllers = append(controllers, controller)
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
		t.Fatal("no message arrived after the warm-up")
	}
	for _, c := range controllers {
		tel := c.Telemetry()
		clientTelemetry.NonElicitingBytes += tel.NonElicitingBytes
		clientTelemetry.NonElicitingDebited += tel.NonElicitingDebited
	}
	return latencies[len(latencies)/2], latencies[len(latencies)*9/10], clientTelemetry
}
