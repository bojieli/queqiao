package pep

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bojieli/queqiao/internal/pathsim"
)

// The live impaired campaign's blackhole trial, deterministically: a
// 20 Mbit/s path with +150 ms of round trip, dropped completely for 8 s and,
// 16 s later, for 25 s, from every source. Under it run what the campaign
// runs: an LLM agent loop on one persistent connection (a ~130 KB prompt,
// 0.5 s of server think time, then a stream of small deltas), a terminal
// echo, a bulk upload, and a new connection every 0.5 s. Until stage 11 a
// second arm ran legacy recovery (recovery=flow) for comparison; it went with
// the legacy code.
//
// Unlike the live loop, whose turns drift by a second or two from trial to
// trial, every LLM call here starts at a fixed offset from an outage, so
// trials are measured on the same calls. QUIC only: the live blackhole drops
// TCP too, and pathsim's TCP relay cannot drop.

const (
	llmMarker    = 0xFD
	uploadMarker = 0xFE
	// llmPrompt is the campaign's prompt at its fourth turn.
	llmPrompt  = 132_000
	llmThink   = 500 * time.Millisecond
	llmDeltas  = 40
	llmCadence = 25 * time.Millisecond
)

// campaignDestination serves the workloads by their first byte.
func campaignDestination(l net.Listener) {
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
			switch first[0] {
			case uploadMarker:
				_, _ = io.Copy(io.Discard, conn)
			case llmMarker:
				var hdr [4]byte
				delta := make([]byte, 180)
				for {
					if _, err := io.ReadFull(conn, hdr[:]); err != nil {
						return
					}
					if _, err := io.CopyN(io.Discard, conn, int64(binary.BigEndian.Uint32(hdr[:]))); err != nil {
						return
					}
					time.Sleep(llmThink)
					for range llmDeltas {
						if _, err := conn.Write(delta); err != nil {
							return
						}
						time.Sleep(llmCadence)
					}
				}
			default:
				if _, err := conn.Write(first[:]); err != nil {
					return
				}
				_, _ = io.Copy(conn, conn)
			}
		}()
	}
}

type llmCall struct {
	label string
	ttft  time.Duration
	// afterRestore is, for a call whose first delta came after the path
	// returned, how long after the restore it came.
	afterRestore time.Duration
	failed       bool
}

type probeResult struct {
	start, end time.Time
	ok         bool
}

type blackholeCampaign struct {
	calls []llmCall
	// echoAfter and probeAfter are how long after each restore the terminal
	// echoed again and a new connection first completed.
	echoAfter, probeAfter [2]time.Duration
	probesFailed          int
	stalls                uint64
	failures              []string
}

func (c blackholeCampaign) String() string {
	s := "carrier"
	for _, call := range c.calls {
		if call.afterRestore > 0 {
			s += fmt.Sprintf(" | %s: ttft %.2fs (%.2fs after restore)", call.label, call.ttft.Seconds(), call.afterRestore.Seconds())
		} else {
			s += fmt.Sprintf(" | %s: ttft %.2fs", call.label, call.ttft.Seconds())
		}
	}
	return s + fmt.Sprintf(" | echo after restore %.2fs / %.2fs | first new connection after restore %.2fs / %.2fs | failed new connections %d | stalls %d",
		c.echoAfter[0].Seconds(), c.echoAfter[1].Seconds(), c.probeAfter[0].Seconds(), c.probeAfter[1].Seconds(), c.probesFailed, c.stalls)
}

// runBlackholeCampaign runs one trial. lead is how long before each drop the
// straddling LLM call starts, which also moves the drops relative to every
// timer the transport has running.
func runBlackholeCampaign(t *testing.T, lead time.Duration, seed int64) blackholeCampaign {
	t.Helper()
	p := newCarrierPair(t, carrierPairOptions{
		path:  pathsim.Config{OneWayDelay: 75 * time.Millisecond, RateBytesPerSec: 20_000_000 / 8, QueueBytes: 512 << 10, Seed: seed},
		serve: campaignDestination,
	})
	res := blackholeCampaign{}
	open := func(marker byte) net.Conn {
		conn, err := trySocksDial(p.socks, p.destination, 30*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		_ = conn.SetDeadline(time.Time{})
		t.Cleanup(func() { _ = conn.Close() })
		if _, err := conn.Write([]byte{marker}); err != nil {
			t.Fatal(err)
		}
		return conn
	}
	up := open(uploadMarker)
	go func() {
		buf := make([]byte, 32<<10)
		for {
			if _, err := up.Write(buf); err != nil {
				return
			}
		}
	}()
	term := startEchoStream(t, p, 64, 50*time.Millisecond)
	llm := open(llmMarker)
	prompt := make([]byte, 4+llmPrompt)
	binary.BigEndian.PutUint32(prompt, llmPrompt)
	call := func(label string) llmCall {
		c := llmCall{label: label}
		start := time.Now()
		_ = llm.SetDeadline(start.Add(120 * time.Second))
		buf := make([]byte, 180*llmDeltas)
		if _, err := llm.Write(prompt); err != nil {
			c.failed = true
		} else if _, err := io.ReadFull(llm, buf[:1]); err != nil {
			c.failed = true
		} else {
			c.ttft = time.Since(start)
			if _, err := io.ReadFull(llm, buf[1:]); err != nil {
				c.failed = true
			}
		}
		if c.failed {
			res.failures = append(res.failures, "LLM call "+label)
		}
		return c
	}

	var probesMu sync.Mutex
	var probes []probeResult
	var stopProbes atomic.Bool
	var probeWG sync.WaitGroup
	probeWG.Add(1)
	go func() {
		defer probeWG.Done()
		for !stopProbes.Load() {
			probeWG.Add(1)
			go func() {
				defer probeWG.Done()
				r := probeResult{start: time.Now()}
				conn, err := trySocksDial(p.socks, p.destination, 30*time.Second)
				if err == nil {
					var b [1]byte
					if _, err = conn.Write([]byte{7}); err == nil {
						_, err = io.ReadFull(conn, b[:])
					}
					conn.Close()
				}
				r.end, r.ok = time.Now(), err == nil
				probesMu.Lock()
				probes = append(probes, r)
				probesMu.Unlock()
			}()
			time.Sleep(500 * time.Millisecond)
		}
	}()

	time.Sleep(5 * time.Second)
	res.calls = append(res.calls, call("warm"))
	var restored [2]time.Time
	// outage drops everything for d; an LLM call starts lead before the drop,
	// too late for its first delta to make it.
	outage := func(i int, d, lead time.Duration, label string) {
		results := make(chan llmCall, 1)
		start := time.Now()
		go func() { results <- call(label) }()
		time.Sleep(lead)
		p.relay.SetLossRate(1, 1)
		time.Sleep(d)
		p.relay.SetLossRate(0, 0)
		restored[i] = time.Now()
		c := <-results
		if first := start.Add(c.ttft); first.After(restored[i]) {
			c.afterRestore = first.Sub(restored[i])
		}
		res.calls = append(res.calls, c)
		echoed := term.echoed.Load()
		for term.echoed.Load() == echoed && time.Since(restored[i]) < 60*time.Second && term.failure() == nil {
			time.Sleep(10 * time.Millisecond)
		}
		res.echoAfter[i] = time.Since(restored[i])
	}
	outage(0, 8*time.Second, lead, "8s drop")
	time.Sleep(6 * time.Second)
	res.calls = append(res.calls, call("between"))
	time.Sleep(6 * time.Second)
	outage(1, 25*time.Second, lead, "25s drop")
	time.Sleep(2 * time.Second)
	res.calls = append(res.calls, call("after"))
	time.Sleep(3 * time.Second)
	stopProbes.Store(true)
	probeWG.Wait()

	sort.Slice(probes, func(i, j int) bool { return probes[i].end.Before(probes[j].end) })
	for i, at := range restored {
		res.probeAfter[i] = -1
		for _, pr := range probes {
			if pr.ok && pr.end.After(at) {
				res.probeAfter[i] = pr.end.Sub(at)
				break
			}
		}
	}
	for _, pr := range probes {
		if !pr.ok {
			res.probesFailed++
		}
	}
	res.stalls = p.clientMetrics.Snapshot().FlowStallsDetected + p.gatewayMetrics.Snapshot().FlowStallsDetected
	if err := term.failure(); err != nil {
		res.failures = append(res.failures, "terminal: "+err.Error())
	}
	return res
}

// recoveryAfter collects, over trials, how long after each restore the
// terminal echoed again: the campaign's recover_8s_s and recover_25s_s.
func recoveryAfter(trials []blackholeCampaign, drop int) (mean, worst time.Duration) {
	for _, tr := range trials {
		mean += tr.echoAfter[drop]
		worst = max(worst, tr.echoAfter[drop])
	}
	return mean / time.Duration(len(trials)), worst
}

// The live blackhole trial, three times at different phases of the drops
// against the transport's timers. Hard assertions: no flow fails, and the
// carrier manager notices a returned path within its probe cadence: the
// terminal echoes again within 3 s of each restore in every trial.
func TestBlackholeCampaign(t *testing.T) {
	if testing.Short() {
		t.Skip("wall-clock impairment campaign")
	}
	leads := []time.Duration{300 * time.Millisecond, 1100 * time.Millisecond, 1900 * time.Millisecond}
	var trials []blackholeCampaign
	for i, lead := range leads {
		r := runBlackholeCampaign(t, lead, int64(41+i))
		t.Log(r)
		if len(r.failures) > 0 {
			t.Errorf("%v", r.failures)
		}
		trials = append(trials, r)
	}
	m8, w8 := recoveryAfter(trials, 0)
	m25, w25 := recoveryAfter(trials, 1)
	t.Logf("recovery after restore: 8 s drop mean %.2fs worst %.2fs; 25 s drop mean %.2fs worst %.2fs",
		m8.Seconds(), w8.Seconds(), m25.Seconds(), w25.Seconds())
	for _, r := range trials {
		for drop, after := range r.echoAfter {
			if after > 3*time.Second {
				t.Errorf("the terminal echoed %.2fs after restore %d", after.Seconds(), drop+1)
			}
		}
	}
}

// A receive-only carrier across an 8 s drop: one bulk download and nothing
// else, so the client has almost nothing ack-eliciting outstanding and its
// probe timeouts -- the SUSPECT evidence -- barely accrue, as the live
// campaign's logs showed for most trials. The download moves again within 3 s
// of the restore: before the silence probe it waited for the keep-alive PING
// sent early in the outage to come round again on its backed-off probe timer,
// about 5 s.
func TestBlackholeReceiveOnly(t *testing.T) {
	if testing.Short() {
		t.Skip("wall-clock impairment campaign")
	}
	var results []time.Duration
	for i, lead := range []time.Duration{0, 700 * time.Millisecond, 1400 * time.Millisecond} {
		p := impairmentPair(t, pathsim.Config{OneWayDelay: 75 * time.Millisecond, RateBytesPerSec: 20_000_000 / 8, QueueBytes: 512 << 10, Seed: int64(51 + i)}, CarrierRecoveryConfig{})
		d := startDownload(t, p)
		if !d.waitReceived(1<<20, 30*time.Second) {
			t.Fatal("the download never started")
		}
		time.Sleep(3*time.Second + lead)
		p.relay.SetLossRate(1, 1)
		time.Sleep(8 * time.Second)
		p.relay.SetLossRate(0, 0)
		restored := time.Now()
		mark := d.received.Load()
		if !d.waitReceived(mark+256<<10, 40*time.Second) {
			t.Fatalf("the download did not resume: %v", d.failure())
		}
		results = append(results, time.Since(restored))
	}
	t.Logf("download resumed after restore: %v", results)
	for _, r := range results {
		if r > 3*time.Second {
			t.Errorf("a receive-only download resumed %.2fs after the restore", r.Seconds())
		}
	}
}
