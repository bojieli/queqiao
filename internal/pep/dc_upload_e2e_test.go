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
	"testing"
	"time"

	"github.com/bojieli/queqiao/internal/metrics"
	"github.com/bojieli/queqiao/internal/pathmodel"
	"github.com/bojieli/queqiao/internal/pathsim"
	"github.com/bojieli/queqiao/internal/profile"
)

// The live DC gate's upload and light-request workloads on a 53 ms,
// 300 Mbit/s emulated leg, with the dc-long-haul profile: the legacy bundle
// against the full stage 1-8 bundle with classifier=phased. Each upload is a
// 4 MiB object on a fresh flow, as the gate's upload jobs send them.

type gateArm string

const (
	gateLegacy   gateArm = "legacy"
	gatePathAuth gateArm = "pathauth"
)

// gateDestination serves objects framed as a 4-byte length and that many
// bytes, answering each with a 4-byte reply length and that many bytes.
// A request whose first byte is 'D' asks for its reply size in the next
// four bytes and sends nothing else: a download.
func gateDestination(l net.Listener) {
	for {
		conn, err := l.Accept()
		if err != nil {
			return
		}
		go func() {
			defer conn.Close()
			var hdr [8]byte
			buf := make([]byte, 256<<10)
			for {
				if _, err := io.ReadFull(conn, hdr[:]); err != nil {
					return
				}
				up := int64(binary.BigEndian.Uint32(hdr[:4]))
				down := int(binary.BigEndian.Uint32(hdr[4:]))
				if _, err := io.CopyN(io.Discard, conn, up); err != nil {
					return
				}
				for down > 0 {
					n, err := conn.Write(buf[:min(down, len(buf))])
					down -= n
					if err != nil {
						return
					}
				}
			}
		}()
	}
}

type gatePair struct {
	socks       string
	destination net.Listener
	client      *Client
}

func newGatePair(t *testing.T, arm gateArm, path pathsim.Config) *gatePair {
	t.Helper()
	pathmodel.Reset()
	t.Cleanup(pathmodel.Reset)
	dc, err := profile.ByName("dc-long-haul")
	if err != nil {
		t.Fatal(err)
	}
	destination, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = destination.Close() })
	go gateDestination(destination)
	certificate, roots := testCertificate(t)
	packetConn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	scfg := ServerConfig{
		ListenAddr: "127.0.0.1:0", Credentials: certificate,
		DestinationPolicy: DestinationPolicy{AllowPrivate: true}, EnableQUIC: true,
		Logger: slog.New(slog.DiscardHandler), Metrics: metrics.New(), HandshakeTimeout: 30 * time.Second,
		Profile: dc,
	}
	ccfg := ClientConfig{
		Credentials: roots, Transport: TransportQUIC, EnableQUICPool: true, Metrics: metrics.New(),
		Logger: slog.New(slog.DiscardHandler), Profile: dc,
	}
	if arm == gatePathAuth {
		applyFlipBundleToServer(&scfg)
		applyFlipBundleToClient(&ccfg)
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
	return &gatePair{socks: clientListener.Addr().String(), destination: destination, client: client}
}

// exchange opens a fresh flow, sends up bytes and reads down bytes back, and
// reports how long it took from the first byte written to the last read.
func (p *gatePair) exchange(t *testing.T, up, down int) time.Duration {
	t.Helper()
	conn, err := trySocksDial(p.socks, p.destination, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(120 * time.Second))
	msg := make([]byte, 8+up)
	binary.BigEndian.PutUint32(msg[:4], uint32(up))
	binary.BigEndian.PutUint32(msg[4:], uint32(max(down, 1)))
	start := time.Now()
	if _, err := conn.Write(msg); err != nil {
		t.Fatal(err)
	}
	if _, err := io.CopyN(io.Discard, conn, int64(max(down, 1))); err != nil {
		t.Fatal(err)
	}
	return time.Since(start)
}

// dcLeg is the gate's leg: 53 ms of round trip, 300 Mbit/s, a queue of
// about one bandwidth-delay product.
func gateLeg(seed int64) pathsim.Config {
	return pathsim.Config{OneWayDelay: 26500 * time.Microsecond, RateBytesPerSec: 300_000_000 / 8, QueueBytes: 2 << 20, Seed: seed}
}

func gateMbps(bytes int, d time.Duration) float64 { return float64(bytes) * 8 / d.Seconds() / 1e6 }

// A cold client uploads 4 MiB objects one after another, each on a fresh
// flow. Before the retention floor, every new flow under mem=owned started at
// the four-chunk allowance and doubled once per credit round, so a 4 MiB
// object spent most of its life opening its own bound.
func TestDCUploadObjectsOnFreshFlows(t *testing.T) {
	if testing.Short() {
		t.Skip("wall-clock emulated path")
	}
	const object = 4 << 20
	results := map[gateArm][]float64{}
	for _, arm := range []gateArm{gateLegacy, gatePathAuth} {
		p := newGatePair(t, arm, gateLeg(71))
		for range 8 {
			results[arm] = append(results[arm], gateMbps(object, p.exchange(t, object, 1)))
		}
		r := results[arm]
		sorted := append([]float64(nil), r...)
		sort.Float64s(sorted)
		t.Logf("%-8s 4 MiB uploads, Mbit/s in order: %s | first %.0f, median of the rest %.0f", arm, fmtGateRates(r), r[0], medianOf(r[1:]))
	}
	if legacy, cand := medianOf(results[gateLegacy][1:]), medianOf(results[gatePathAuth][1:]); cand < 0.9*legacy {
		t.Errorf("pathauth uploads 4 MiB objects at %.0f Mbit/s against legacy's %.0f", cand, legacy)
	}
}

// A light request beside load, as the gate's control leaf and upload sibling:
// each a fresh flow carrying a request up -- 300 KB next to two flows
// downloading without pause, 355 KB next to two flows uploading without pause
// -- answered with 200 B, once a second. Before the retention floor, each such
// flow under mem=owned started at the four-chunk allowance and paid one or two
// extra credit rounds to send its request: on the live gate, the control
// leaf's p50 was 0.236 s against legacy's 0.110 s, two rounds of 53 ms.
func TestDCLightRequestBesideLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("wall-clock emulated path")
	}
	for _, load := range []string{"download", "upload"} {
		for _, arm := range []gateArm{gateLegacy, gatePathAuth} {
			p := newGatePair(t, arm, gateLeg(72))
			ctx, cancel := context.WithCancel(context.Background())
			var wg sync.WaitGroup
			for range 2 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					conn, err := trySocksDial(p.socks, p.destination, 30*time.Second)
					if err != nil {
						return
					}
					defer conn.Close()
					context.AfterFunc(ctx, func() { _ = conn.Close() })
					const chunk = 16 << 20
					msg := make([]byte, 8)
					if load == "download" {
						binary.BigEndian.PutUint32(msg[4:], chunk)
						for ctx.Err() == nil {
							if _, err := conn.Write(msg); err != nil {
								return
							}
							if _, err := io.CopyN(io.Discard, conn, chunk); err != nil {
								return
							}
						}
						return
					}
					body := make([]byte, 8+chunk)
					binary.BigEndian.PutUint32(body[:4], chunk)
					binary.BigEndian.PutUint32(body[4:], 1)
					for ctx.Err() == nil {
						if _, err := conn.Write(body); err != nil {
							return
						}
						if _, err := io.CopyN(io.Discard, conn, 1); err != nil {
							return
						}
					}
				}()
			}
			time.Sleep(4 * time.Second)
			up := 300 << 10
			if load == "upload" {
				up = 355_000
			}
			var lat []float64
			for range 12 {
				lat = append(lat, p.exchange(t, up, 200).Seconds())
				time.Sleep(time.Second)
			}
			cancel()
			wg.Wait()
			sort.Float64s(lat)
			t.Logf("beside a loaded %-8s %-8s light request: p50 %.3fs p90 %.3fs max %.3fs", load, arm, lat[len(lat)/2], lat[len(lat)*9/10], lat[len(lat)-1])
			// A request of a few hundred kilobytes on this leg is one round
			// trip (53 ms), the request's own serialization and the loaded
			// queue's delay. More than that plus two rounds is a request
			// waiting on its own flow's credit.
			if arm == gatePathAuth && lat[len(lat)/2] > 0.25 {
				t.Errorf("pathauth: a light request beside a loaded %s took p50 %.3fs", load, lat[len(lat)/2])
			}
		}
	}
}

func medianOf(v []float64) float64 {
	s := append([]float64(nil), v...)
	sort.Float64s(s)
	return s[len(s)/2]
}

func fmtGateRates(r []float64) string {
	s := ""
	for _, v := range r {
		s += fmt.Sprintf("%.0f ", v)
	}
	return s
}
