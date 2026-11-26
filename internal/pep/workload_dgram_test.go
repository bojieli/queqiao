package pep

import (
	"encoding/binary"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/bojieli/queqiao/internal/pathsim"
)

// udpReflector answers every datagram with reply bytes that start with the
// request's first four, so a client can match answers to questions. It is
// the far side of W11 and of M5's audio.
func udpReflector(t *testing.T, reply func(request []byte) []byte) *net.UDPAddr {
	t.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	go func() {
		buf := make([]byte, 64*1024)
		for {
			n, from, err := conn.ReadFromUDP(buf)
			if err != nil {
				return
			}
			_, _ = conn.WriteToUDP(reply(buf[:n]), from)
		}
	}()
	return conn.LocalAddr().(*net.UDPAddr)
}

// oneShotQuery is a DNS-shaped exchange: 60 B up, 300 B back.
func oneShotQuery(id uint32) []byte {
	q := make([]byte, 60)
	binary.BigEndian.PutUint32(q, id)
	return q
}

func oneShotAnswer(request []byte) []byte {
	a := make([]byte, 300)
	copy(a, request[:min(4, len(request))])
	return a
}

// W11. One-shot datagrams (docs/WORKLOAD-MODEL.md): one 60 B query up, one
// 300 B answer back, nothing else, half on a fresh UDP association per query
// and half on one reused association. Nothing follows a query, so a lost
// tail has no later arrival to reveal it and must be repaired by the coded
// path's own tail protection (design §15.1 test 15); a fresh association
// also pays its set-up. A stub resolver retries after 1 s, so the measure is
// the fraction of queries answered later than that or not at all, for fresh
// and reused associations separately.
//
// Bound (model W11): fraction over 1 s ≤ legacy + 0.5 pp, per kind, as a
// count. Legacy answered every query of each kind within 1 s on this path
// (0 of 100, measured at stage 10 before the legacy bundle was deleted), and
// 0.5 pp of 100 queries is less than one query, so the frozen bound is
// oneShotOverLimit: one query of a hundred, the smallest allowance a count
// can express above none. One query is several points at -short's size, so
// -short is a smoke test that each kind is answered.
//
// The path is a long-haul shape at a modest size: 120 ms round trip, 10 %
// random loss each way, a policed 20 Mbit/s. Queries run one at a time, the
// next starting when the last is answered or reaches the 1 s retry: fresh
// and reused associations share the tunnel's coded symbol stream, so a later
// query in flight could reveal a lost tail that the tunnel must repair on its
// own. Queries are one a second, the model's cadence. The full suite sends
// 200; -short sends 16 and reports.
func TestOneShotDatagramOnAnEmulatedPath(t *testing.T) {
	requireStableImpairmentClock(t)
	queries := 200
	if testing.Short() {
		queries = 16
	}
	path := pathsim.Config{
		OneWayDelay: 60 * time.Millisecond, LossRate: 0.10, Seed: 11,
		RateBytesPerSec: 20e6 / 8, PolicerRefillPeriod: 8 * time.Millisecond,
	}

	rig := newWorkloadRig(t, path, pathAuthorityArm, nil)
	r := runOneShotArm(rig, udpReflector(t, oneShotAnswer), queries)
	if r.err != nil {
		t.Fatal(r.err)
	}
	for kind, name := range []string{"reused", "fresh"} {
		t.Logf("%s: %d of %d over %v; answered p50 %v, p99 %v", name, r.over[kind], queries/2, oneShotRetry,
			durationQuantile(r.answered[kind], 0.50).Round(time.Millisecond),
			durationQuantile(r.answered[kind], 0.99).Round(time.Millisecond))
		if len(r.answered[kind]) == 0 {
			t.Fatalf("no query on a %s association was answered", name)
		}
	}
	if testing.Short() || !judgeBounds(t) {
		// At 8 queries a kind one query is several points; the bound needs
		// the full suite's count to mean anything.
		return
	}
	for kind, name := range []string{"reused", "fresh"} {
		if limit := queries / 2 * oneShotOverLimit / 100; r.over[kind] > limit {
			t.Errorf("%s associations: %d of %d queries over %v; the bound allows %d",
				name, r.over[kind], queries/2, oneShotRetry, limit)
		}
	}
}

// oneShotOverLimit is W11's frozen bound: the queries in a hundred, of each
// kind, that may take longer than the retry or go unanswered.
const oneShotOverLimit = 1

// oneShotRetry is a stub resolver's first retry.
const oneShotRetry = time.Second

// oneShotArm is W11's outcome, by kind: reused, fresh.
type oneShotArm struct {
	over     [2]int
	answered [2][]time.Duration
	err      error
}

// runOneShotArm sends one query a second, alternating fresh and reused
// associations, never more than one outstanding.
func runOneShotArm(rig *workloadRig, reflector *net.UDPAddr, queries int) (out oneShotArm) {
	// The reused association, warmed by one exchange first, as a
	// resolver's long-lived socket is.
	reused, err := rig.associate()
	if err != nil {
		out.err = err
		return out
	}
	defer reused.Close()
	var (
		waitersMu sync.Mutex
		waiters   = map[uint32]chan time.Time{}
	)
	go func() {
		buf := make([]byte, 4096)
		for {
			payload, err := reused.receive(buf, time.Now().Add(time.Minute))
			if err != nil {
				return
			}
			if len(payload) < 4 {
				continue
			}
			waitersMu.Lock()
			ch := waiters[binary.BigEndian.Uint32(payload)]
			waitersMu.Unlock()
			if ch != nil {
				select {
				case ch <- time.Now():
				default:
				}
			}
		}
	}()
	exchangeReused := func(id uint32) (time.Duration, bool) {
		ch := make(chan time.Time, 1)
		waitersMu.Lock()
		waiters[id] = ch
		waitersMu.Unlock()
		start := time.Now()
		if err := reused.send(reflector, oneShotQuery(id)); err != nil {
			return 0, false
		}
		select {
		case at := <-ch:
			return at.Sub(start), true
		case <-time.After(oneShotRetry):
			return 0, false
		}
	}
	if _, ok := exchangeReused(0); !ok {
		// The warm-up is retried once: the path loses 10 %.
		if _, ok := exchangeReused(1); !ok {
			out.err = errors.New("the reused association never answered")
			return out
		}
	}
	exchangeFresh := func(id uint32) (time.Duration, bool) {
		start := time.Now()
		association, err := rig.associate()
		if err != nil {
			return 0, false
		}
		defer association.Close()
		if err := association.send(reflector, oneShotQuery(id)); err != nil {
			return 0, false
		}
		buf := make([]byte, 4096)
		for {
			payload, err := association.receive(buf, start.Add(oneShotRetry))
			if err != nil {
				return 0, false
			}
			if len(payload) >= 4 && binary.BigEndian.Uint32(payload) == id {
				return time.Since(start), true
			}
		}
	}
	start := time.Now()
	for i := range queries {
		// One query a second, as the model's resolver sends them: an early
		// answer waits out its second, so both arms offer the same load.
		if wait := time.Until(start.Add(time.Duration(i) * time.Second)); wait > 0 {
			time.Sleep(wait)
		}
		kind, exchange := 0, exchangeReused
		if i%2 == 0 {
			kind, exchange = 1, exchangeFresh
		}
		elapsed, ok := exchange(uint32(100 + i))
		if !ok || elapsed > oneShotRetry {
			out.over[kind]++
		}
		if ok {
			out.answered[kind] = append(out.answered[kind], elapsed)
		}
	}
	return out
}
