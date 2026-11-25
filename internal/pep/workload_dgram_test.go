package pep

import (
	"encoding/binary"
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

// oneShotResult is what became of one W11 query.
type oneShotResult struct {
	fresh    bool
	answered bool
	elapsed  time.Duration
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
// Bound (model W11): fraction over 1 s ≤ legacy + 0.5 pp, per kind. The
// model's margin is for thousands of pooled live queries; at this size one
// query is several points, so the bound is applied as a count with an
// allowance of one query, stated in the failure.
//
// The path is a long-haul shape at a modest size: 120 ms round trip, 10 %
// random loss each way, a policed 20 Mbit/s. Queries run one at a time, the
// next starting when the last is answered or reaches the 1 s retry: fresh
// and reused associations share the tunnel's coded symbol stream, so a later
// query in flight could reveal a lost tail that the tunnel must repair on its
// own. That is the model's 1 s spacing without its idle remainder. -short
// halves the queries.
func TestOneShotDatagramOnAnEmulatedPath(t *testing.T) {
	requireStableImpairmentClock(t)
	queries := 60
	if testing.Short() {
		queries = 30
	}
	path := pathsim.Config{
		OneWayDelay: 60 * time.Millisecond, LossRate: 0.10, Seed: 11,
		RateBytesPerSec: 20e6 / 8, PolicerRefillPeriod: 8 * time.Millisecond,
	}
	const retry = time.Second
	const wait = retry

	over := map[string][2]int{}
	for _, arm := range workloadArms() {
		t.Run(arm.name, func(t *testing.T) {
			rig := newWorkloadRig(t, path, arm, nil)
			reflector := udpReflector(t, oneShotAnswer)

			// The reused association, warmed by one exchange first, as a
			// resolver's long-lived socket is.
			reused, err := rig.associate()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = reused.Close() })
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
				case <-time.After(wait):
					return 0, false
				}
			}
			if _, ok := exchangeReused(0); !ok {
				// The warm-up is retried once: the path loses 10 %.
				if _, ok := exchangeReused(1); !ok {
					t.Fatal("the reused association never answered")
				}
			}

			results := make([]oneShotResult, queries)
			for i := range queries {
				func() {
					id := uint32(100 + i)
					fresh := i%2 == 0
					r := oneShotResult{fresh: fresh}
					if !fresh {
						r.elapsed, r.answered = exchangeReused(id)
						results[i] = r
						return
					}
					start := time.Now()
					association, err := rig.associate()
					if err != nil {
						results[i] = r
						return
					}
					defer association.Close()
					if err := association.send(reflector, oneShotQuery(id)); err != nil {
						results[i] = r
						return
					}
					buf := make([]byte, 4096)
					deadline := start.Add(wait)
					for {
						payload, err := association.receive(buf, deadline)
						if err != nil {
							break
						}
						if len(payload) >= 4 && binary.BigEndian.Uint32(payload) == id {
							r.answered, r.elapsed = true, time.Since(start)
							break
						}
					}
					results[i] = r
				}()
			}

			var counts [2]int
			var answered [2][]time.Duration
			for _, r := range results {
				kind := 0
				if r.fresh {
					kind = 1
				}
				if !r.answered || r.elapsed > retry {
					counts[kind]++
				}
				if r.answered {
					answered[kind] = append(answered[kind], r.elapsed)
				}
			}
			for kind, name := range []string{"reused", "fresh"} {
				t.Logf("%s: %d of %d over %v; answered p50 %v, p99 %v", name, counts[kind], queries/2, retry,
					durationQuantile(answered[kind], 0.50).Round(time.Millisecond),
					durationQuantile(answered[kind], 0.99).Round(time.Millisecond))
			}
			over[arm.name] = counts
		})
	}
	if !judgeComparisons(t) {
		return
	}
	legacy, flip := over[legacyArm.name], over[flipArm.name]
	for kind, name := range []string{"reused", "fresh"} {
		if limit := oneShotLimit(legacy[kind], queries/2); flip[kind] > limit {
			t.Errorf("%s associations: %d of %d queries over %v under the flip bundle against %d under legacy; "+
				"the bound (legacy + 0.5 pp, plus one query at this size) allows %d",
				name, flip[kind], queries/2, retry, legacy[kind], limit)
		}
	}
}

// oneShotLimit is W11's bound as a count: legacy's count plus 0.5 pp of the
// queries in whole queries, plus one query of small-sample allowance.
func oneShotLimit(legacy, queries int) int {
	return legacy + queries*5/1000 + 1
}
