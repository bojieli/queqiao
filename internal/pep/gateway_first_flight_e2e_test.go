package pep

import (
	"context"
	"encoding/binary"
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
	"github.com/bojieli/queqiao/internal/profile"
)

// The DC gate's control leaf on a gateway that is busy sending to other
// leaves (§11.3, stage 9): ONE gateway, two loaded clients each with a
// backlogged download, and a light client on a path of its own that fetches
// 300 KB on a fresh flow every half second. On a 53 ms round trip the answer
// is one flight when the response's own window and the lane allow it: the
// light client's path is not shared and its window at flow start is far
// larger than the response. A response that goes out as a part of a flight and
// then waits for the first acknowledgement pays one more round trip, which is
// what the live gate's control leaf paid on most probes (p50 0.131 s against
// legacy's 0.113 s, the bound being 1.10x).

const (
	ffFetchBytes = 300 << 10
	ffEvery      = 500 * time.Millisecond
	ffWarmup     = 4 * time.Second
	ffProbes     = 24
	// ffRoundGap is the silence that marks a second flight: a round trip is
	// 53 ms, and one flight's own serialization on this leg is under 10 ms.
	ffRoundGap = 35 * time.Millisecond
	// ffFirstFlight is the first flight's window at the gateway: under the
	// 53 ms round trip, so nothing that leaves in it was released by an
	// acknowledgement of the response.
	ffFirstFlight = 45 * time.Millisecond
)

// ffBed is one gateway and its clients.
type ffBed struct {
	t           *testing.T
	ctx         context.Context
	cancel      context.CancelFunc
	destination *dcDestination
	server      *Server
	socks       []string
	loadedLink  *pathsim.Bottleneck
	relays      []*pathsim.Relay
	clients     int
}

func newFFBed(t *testing.T, arm dcArm, clients int, sharedLight bool) *ffBed {
	t.Helper()
	pathmodel.Reset()
	pathmodel.ResetTree()
	pathAuthorities.forget()
	ctx, cancel := context.WithCancel(context.Background())
	b := &ffBed{t: t, ctx: ctx, cancel: cancel, destination: newDCDestination(t), clients: clients}
	t.Cleanup(func() {
		cancel()
		pathmodel.Reset()
		pathmodel.ResetTree()
		pathAuthorities.forget()
	})
	dc, err := profile.ByName(dcLongHaulProfile)
	if err != nil {
		t.Fatal(err)
	}
	serverCredentials, clientCredentials := testDeviceCertificates(t, clients)
	gatewayConn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	discard := slog.New(slog.NewTextHandler(io.Discard, nil))
	scfg := ServerConfig{
		ListenAddr: "127.0.0.1:0", Credentials: serverCredentials, Profile: dc,
		DestinationPolicy: DestinationPolicy{AllowPrivate: true}, EnableQUIC: true,
		Logger: discard, Metrics: metrics.New(), HandshakeTimeout: 30 * time.Second,
	}
	scfg.Congestion = arm.congestion
	if !arm.legacy {
		applyFlipBundleToServer(&scfg)
	}
	b.server, err = NewServer(scfg)
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = b.server.ServePacketConn(ctx, gatewayConn) }()
	context.AfterFunc(ctx, func() { _ = gatewayConn.Close() })
	leg := dcCtlLeg()
	b.loadedLink = pathsim.NewBottleneck(leg)
	for i := range clients {
		var relay *pathsim.Relay
		listen := dcHost(t, 20, i) + ":0"
		if i < clients-1 || sharedLight {
			relay, err = b.loadedLink.Attach(listen, gatewayConn.LocalAddr().String(), leg)
		} else {
			// The last client, the light one, has a path of its own.
			relay, err = pathsim.New(listen, gatewayConn.LocalAddr().String(), leg)
		}
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = relay.Close() })
		b.relays = append(b.relays, relay)
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		ccfg := ClientConfig{
			ListenAddr: listener.Addr().String(), RemoteAddr: relay.LocalAddr(), Credentials: clientCredentials[i],
			Transport: TransportQUIC, EnableQUICPool: true, Profile: dc, Metrics: metrics.New(), Logger: discard,
		}
		ccfg.Congestion = arm.congestion
		if !arm.legacy {
			applyFlipBundleToClient(&ccfg)
		}
		client, err := NewClient(ccfg)
		if err != nil {
			t.Fatal(err)
		}
		go func() { _ = client.ServeListener(ctx, listener) }()
		context.AfterFunc(ctx, func() { _ = listener.Close() })
		b.socks = append(b.socks, listener.Addr().String())
	}
	return b
}

func (b *ffBed) dial(client int, up, down uint32) (net.Conn, error) {
	conn, err := trySocksDial(b.socks[client], b.destination.listener, 30*time.Second)
	if err != nil {
		return nil, err
	}
	var header [8]byte
	binary.BigEndian.PutUint32(header[:4], up)
	binary.BigEndian.PutUint32(header[4:], down)
	if _, err := conn.Write(header[:]); err != nil {
		conn.Close()
		return nil, err
	}
	return conn, nil
}

// bulkDownload keeps one backlogged download running on client i until the
// bed ends, counting what arrives.
func (b *ffBed) bulkDownload(i int, got *atomic.Int64, wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		conn, err := b.dial(i, 0, dcUnbounded)
		if err != nil {
			return
		}
		defer conn.Close()
		context.AfterFunc(b.ctx, func() { _ = conn.Close() })
		buf := make([]byte, 64<<10)
		for {
			n, err := conn.Read(buf)
			got.Add(int64(n))
			if err != nil {
				return
			}
		}
	}()
}

// ffProbe is one light fetch: how long it took, how many flights the
// response arrived in, and how many bytes the first flight carried.
type ffProbe struct {
	total       time.Duration
	first       int // bytes in the first arrival flight, at the client
	flights     int // arrival flights at the client
	firstArrive time.Duration
	// sent is what the gateway had put on the path within ffFirstFlight of
	// its first response byte, before any acknowledgement could be back.
	sent int
	// dropped is the response packets the path lost or tail-dropped.
	dropped uint64
}

type ffSample struct {
	at    time.Time
	bytes uint64
}

func (b *ffBed) probe(client int) (ffProbe, error) {
	// Sample the bytes the gateway puts on the client's path, every
	// millisecond, from before the request to the end of the fetch.
	var (
		samples  []ffSample
		samplesM sync.Mutex
		stop     = make(chan struct{})
		sampled  sync.WaitGroup
	)
	_, before := b.relays[client].Stats()
	sampled.Add(1)
	go func() {
		defer sampled.Done()
		for tick := time.NewTicker(time.Millisecond); ; {
			select {
			case <-stop:
				tick.Stop()
				return
			case now := <-tick.C:
				_, down := b.relays[client].Stats()
				samplesM.Lock()
				samples = append(samples, ffSample{now, down.BytesIn})
				samplesM.Unlock()
			}
		}
	}()
	p, err := b.fetch(client)
	close(stop)
	sampled.Wait()
	_, after := b.relays[client].Stats()
	p.dropped = (after.PacketsLost + after.PacketsDropped) - (before.PacketsLost + before.PacketsDropped)
	// The first response byte is the first sample 4 KiB above the baseline
	// (a response's control frames are far smaller).
	for i, s := range samples {
		if s.bytes < before.BytesIn+4096 {
			continue
		}
		deadline := s.at.Add(ffFirstFlight)
		last := s.bytes
		for _, later := range samples[i:] {
			if later.at.After(deadline) {
				break
			}
			last = later.bytes
		}
		p.sent = int(last - before.BytesIn)
		break
	}
	return p, err
}

func (b *ffBed) fetch(client int) (ffProbe, error) {
	began := time.Now()
	conn, err := b.dial(client, 0, ffFetchBytes)
	if err != nil {
		return ffProbe{}, err
	}
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	var p ffProbe
	buf := make([]byte, 64<<10)
	var last time.Time
	got := 0
	for got < ffFetchBytes {
		n, err := conn.Read(buf[:min(len(buf), ffFetchBytes-got)])
		now := time.Now()
		if n > 0 {
			if last.IsZero() {
				p.flights, p.firstArrive = 1, now.Sub(began)
			} else if now.Sub(last) > ffRoundGap {
				p.flights++
			}
			if p.flights == 1 {
				p.first += n
			}
			last = now
			got += n
		}
		if err != nil {
			return p, err
		}
	}
	p.total = time.Since(began)
	return p, nil
}

type ffResult struct {
	probes   []ffProbe
	loadMbps float64
}

// wholeFlight counts the probes whose response was entirely on the path
// within the first flight's window.
func (r ffResult) wholeFlight() int {
	n := 0
	for _, p := range r.probes {
		if p.sent >= ffFetchBytes {
			n++
		}
	}
	return n
}

func (r ffResult) median(f func(ffProbe) float64) float64 { return r.quantile(f, 0.5) }

func (r ffResult) quantile(f func(ffProbe) float64, q float64) float64 {
	v := make([]float64, 0, len(r.probes))
	for _, p := range r.probes {
		v = append(v, f(p))
	}
	sort.Float64s(v)
	return v[min(len(v)-1, int(q*float64(len(v))))]
}

func runFirstFlight(t *testing.T, arm dcArm, loaded int, sharedLight bool) ffResult {
	t.Helper()
	b := newFFBed(t, arm, loaded+1, sharedLight)
	light := loaded
	// Warm the light client's carrier, then load the gateway.
	if _, err := b.probe(light); err != nil {
		t.Fatalf("%s: warm-up: %v", arm.name, err)
	}
	var got atomic.Int64
	var wg sync.WaitGroup
	for i := range loaded {
		for range 2 {
			b.bulkDownload(i, &got, &wg)
		}
	}
	time.Sleep(ffWarmup)
	start, began := got.Load(), time.Now()
	var res ffResult
	for range ffProbes {
		at := time.Now()
		p, err := b.probe(light)
		if err != nil {
			t.Fatalf("%s: probe: %v", arm.name, err)
		}
		res.probes = append(res.probes, p)
		time.Sleep(time.Until(at.Add(ffEvery)))
	}
	res.loadMbps = float64(got.Load()-start) * 8 / time.Since(began).Seconds() / 1e6
	b.cancel()
	wg.Wait()
	return res
}

// The light client's response goes out whole in its first flight, beside two
// loaded clients' downloads on the same gateway. This is a regression guard
// for the gateway's send path, not a performance claim: the live DC gate's
// control leaf (stage 9) was first thought to be capped to about 172 KB in its
// first flight, but the gateway's own lane snapshots at 50 ms show every chunk
// issued and the whole response in flight in every flow. The slow probes are
// the ones that lost a packet and paid one round trip to repair it.
//
// On a path of its own the light client's fetch is asserted. On the loaded
// clients' bottleneck it is only reported: the joint queue's delay and drops
// are a separate matter (no joint limit across independent path authorities,
// design section 11.3 Q2), and the relay's sent-byte count there includes the
// loaded clients' traffic, so only the fetch time and drops are meaningful.
func TestGatewayFirstFlightBesideBulkDownloads(t *testing.T) {
	requireDCBed(t)
	const loaded = 2
	for _, shared := range []bool{false, true} {
		where := "own path"
		if shared {
			where = "shared bottleneck (report only)"
		}
		for _, arm := range []dcArm{dcLegacyArm, dcNewArm} {
			r := runFirstFlight(t, arm, loaded, shared)
			ms := func(p ffProbe) float64 { return float64(p.total.Microseconds()) / 1000 }
			sent := func(p ffProbe) float64 { return float64(p.sent) / 1024 }
			flights := func(p ffProbe) float64 { return float64(p.flights) }
			dropped := func(p ffProbe) float64 { return float64(p.dropped) }
			if shared {
				t.Logf("%-31s %-14s light 300 KB fetch: p50 %.0f ms p90 %.0f ms | arrival flights p50 %.0f | packets dropped on the link p50 %.0f | loaded clients %.0f Mbit/s",
					where, arm.name, r.median(ms), r.quantile(ms, 0.9), r.median(flights), r.median(dropped), r.loadMbps)
				continue
			}
			t.Logf("%-31s %-14s light 300 KB fetch: p50 %.0f ms | sent in the first %v: p50 %.0f KiB, whole on %d/%d probes | arrival flights p50 %.0f | packets dropped p50 %.0f | loaded clients %.0f Mbit/s",
				where, arm.name, r.median(ms), ffFirstFlight, r.median(sent), r.wholeFlight(), len(r.probes), r.median(flights), r.median(dropped), r.loadMbps)
			// The light client's window is several times the response, and so
			// is its lane's: the response is whole on the path before any
			// acknowledgement of it can return.
			if need := len(r.probes) * 9 / 10; r.wholeFlight() < need {
				t.Errorf("%s, %s: the gateway had the whole 300 KB response on the path within %v on %d of %d probes, want at least %d (p50 %.0f KiB)",
					where, arm.name, ffFirstFlight, r.wholeFlight(), len(r.probes), need, r.median(sent))
			}
		}
	}
}
