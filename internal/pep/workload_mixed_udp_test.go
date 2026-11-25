package pep

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/bojieli/queqiao/internal/pathsim"
)

// m5Outcome is one arm's M5 measures.
type m5Outcome struct {
	audioSent     int
	audioLate     int             // lost, or later than the playout budget
	audioLateness []time.Duration // of the packets that arrived
	keystrokes    []time.Duration // echo times
	heavyGoodput  float64
	pathDrops     uint64 // the emulated bottleneck's own drops, both directions
}

// M5. Heavy UDP beside thin UDP and coded stream chunks
// (docs/WORKLOAD-MODEL.md): a tunnelled QUIC download (W8) fills the path
// while a call's audio (W7, 160 B every 20 ms on its own association) and
// terminal keystrokes (W6, on a TCP flow small enough to be coded) run
// beside it. All three share the tunnel's one ordered symbol stream (design
// Q5), so a heavy association that the tunnel lets queue delays the others
// by its queue.
//
// The audio is reflected, and a packet's lateness is its round trip less the
// path's: L = RTT − 2 × one-way delay. A packet is late when L exceeds the
// 150 ms playout budget or it never returns. Bounds (models W7, W6): audio
// late loss ≤ legacy + 0.5 pp, with one packet of allowance at this size;
// audio lateness p99 ≤ legacy × 1.10 + 10 ms; keystroke echo p99 ≤ legacy ×
// 1.10 + 20 ms. The model also reports the coded FIFO's sojourn p99 and
// the UDP PACKET frames dropped at overflow; no build exports either yet,
// so the emulated bottleneck's own drops and the heavy flow's goodput are
// reported in their place.
//
// Both arms run at once, each on its own client, gateway and emulated path:
// 120 ms round trip, 20 Mbit/s behind a drop-tail queue, for 30 s. The
// measures follow the inner controller's sawtooth, and over 8 s the audio
// late count of either arm moved by half from run to run, so -short runs 8 s
// as a smoke test (every keystroke echoed, audio measured) and the bounds
// are applied in the full suite only, where they held with room in every
// run.
func TestHeavyUDPDoesNotDelayThinUDPOrCodedChunks(t *testing.T) {
	requireStableImpairmentClock(t)
	const oneWay = 60 * time.Millisecond
	const playout = 150 * time.Millisecond
	measure := 30 * time.Second
	if testing.Short() {
		measure = 8 * time.Second
	}
	path := pathsim.Config{OneWayDelay: oneWay, Seed: 5, RateBytesPerSec: 20e6 / 8}

	arms := workloadArms()
	rigs := make([]*workloadRig, len(arms))
	servers := make([]*innerQUICServer, len(arms))
	for i, arm := range arms {
		rigs[i] = newWorkloadRig(t, path, arm, nil)
		servers[i] = newInnerQUICServer(t)
	}
	reflector := udpReflector(t, func(request []byte) []byte { return append([]byte(nil), request...) })
	echoListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = echoListener.Close() })
	go echoDestination(echoListener)

	outcomes := make([]m5Outcome, len(arms))
	errs := make(chan error, 4*len(arms))
	var wg sync.WaitGroup
	for i, arm := range arms {
		wg.Add(1)
		go func() {
			defer wg.Done()
			o := &outcomes[i]
			rig := rigs[i]
			ctx, cancel := context.WithTimeout(context.Background(), measure+2*time.Minute)
			defer cancel()

			// The heavy association: a download that outlasts the
			// measurement, started first so it has filled the path.
			conn, err := dialInnerQUIC(ctx, t, rig, servers[i])
			if err != nil {
				errs <- fmt.Errorf("%s: inner handshake: %w", arm.name, err)
				return
			}
			defer conn.CloseWithError(0, "")
			heavyDone := make(chan struct{})
			go func() {
				defer close(heavyDone)
				n, elapsed, err := innerGetFor(ctx, conn, 1<<30, measure+3*time.Second)
				if err != nil {
					errs <- fmt.Errorf("%s: heavy download: %w", arm.name, err)
					return
				}
				o.heavyGoodput = float64(n) / elapsed.Seconds()
			}()

			keys := rig.dial(echoListener.Addr().String())
			audio, err := rig.associate()
			if err != nil {
				errs <- fmt.Errorf("%s: audio association: %w", arm.name, err)
				return
			}
			defer audio.Close()
			time.Sleep(3 * time.Second)
			upBefore, downBefore := rig.relay.Stats()

			var thin sync.WaitGroup
			thin.Add(2)
			go func() {
				defer thin.Done()
				o.audioSent, o.audioLate, o.audioLateness = runAudio(audio, reflector, measure, 2*oneWay, playout)
			}()
			go func() {
				defer thin.Done()
				echoes, err := runKeystrokes(keys, measure)
				if err != nil {
					errs <- fmt.Errorf("%s: keystrokes: %w", arm.name, err)
				}
				o.keystrokes = echoes
			}()
			thin.Wait()
			upAfter, downAfter := rig.relay.Stats()
			o.pathDrops = upAfter.PacketsDropped - upBefore.PacketsDropped + downAfter.PacketsDropped - downBefore.PacketsDropped
			<-heavyDone
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	for i, arm := range arms {
		o := outcomes[i]
		t.Logf("%s: audio %d of %d late or lost, lateness p99 %v; keystroke echo p99 %v over %d; heavy goodput %.2f Mbit/s; bottleneck drops %d",
			arm.name, o.audioLate, o.audioSent, durationQuantile(o.audioLateness, 0.99).Round(time.Millisecond),
			durationQuantile(o.keystrokes, 0.99).Round(time.Millisecond), len(o.keystrokes),
			o.heavyGoodput*8/1e6, o.pathDrops)
		if o.audioSent == 0 || len(o.audioLateness) == 0 || len(o.keystrokes) == 0 {
			t.Fatalf("%s: nothing was measured", arm.name)
		}
	}
	if testing.Short() {
		return
	}
	legacy, flip := outcomes[0], outcomes[1]
	legacyRate := float64(legacy.audioLate) / float64(legacy.audioSent)
	if allowed := legacyRate*float64(flip.audioSent) + 0.005*float64(flip.audioSent) + 1; float64(flip.audioLate) > allowed {
		t.Errorf("audio late loss %d of %d under the flip bundle against %d of %d under legacy; the bound (legacy + 0.5 pp, plus one packet) allows %.1f",
			flip.audioLate, flip.audioSent, legacy.audioLate, legacy.audioSent, allowed)
	}
	if got, limit := durationQuantile(flip.audioLateness, 0.99), durationQuantile(legacy.audioLateness, 0.99)*11/10+10*time.Millisecond; got > limit {
		t.Errorf("audio lateness p99 %v under the flip bundle; the bound is legacy × 1.10 + 10 ms = %v", got.Round(time.Millisecond), limit.Round(time.Millisecond))
	}
	if got, limit := durationQuantile(flip.keystrokes, 0.99), durationQuantile(legacy.keystrokes, 0.99)*11/10+20*time.Millisecond; got > limit {
		t.Errorf("keystroke echo p99 %v under the flip bundle; the bound is legacy × 1.10 + 20 ms = %v", got.Round(time.Millisecond), limit.Round(time.Millisecond))
	}
}

// runAudio sends a 160 B packet every 20 ms for d and collects the
// reflections. It returns the packets sent, how many were late or lost, and
// the lateness of each that returned.
func runAudio(a *socksAssociation, reflector *net.UDPAddr, d, pathRTT, playout time.Duration) (int, int, []time.Duration) {
	const interval = 20 * time.Millisecond
	count := int(d / interval)
	sent := make([]time.Time, count)
	arrived := make([]time.Duration, count)
	var mu sync.Mutex
	done := make(chan struct{})
	go func() {
		defer close(done)
		buf := make([]byte, 2048)
		for {
			payload, err := a.receive(buf, time.Now().Add(playout+pathRTT+time.Second))
			if err != nil {
				return
			}
			if len(payload) < 4 {
				continue
			}
			seq := int(binary.BigEndian.Uint32(payload))
			now := time.Now()
			mu.Lock()
			if seq < count && !sent[seq].IsZero() && arrived[seq] == 0 {
				arrived[seq] = now.Sub(sent[seq])
			}
			mu.Unlock()
		}
	}()
	start := time.Now()
	packet := make([]byte, 160)
	for seq := range count {
		if wait := time.Until(start.Add(time.Duration(seq) * interval)); wait > 0 {
			time.Sleep(wait)
		}
		binary.BigEndian.PutUint32(packet, uint32(seq))
		mu.Lock()
		sent[seq] = time.Now()
		mu.Unlock()
		_ = a.send(reflector, packet)
	}
	<-done
	late := 0
	var lateness []time.Duration
	for _, rtt := range arrived {
		if rtt == 0 {
			late++
			continue
		}
		l := max(0, rtt-pathRTT)
		lateness = append(lateness, l)
		if l > playout {
			late++
		}
	}
	return count, late, lateness
}

// runKeystrokes types a 36-byte keystroke every 50 ms for d on conn, which
// echoes, without waiting for each echo, as a person types; it returns each
// echo's time. The stream is ordered, so echoes are matched by position.
func runKeystrokes(conn net.Conn, d time.Duration) ([]time.Duration, error) {
	const interval = 50 * time.Millisecond
	count := int(d / interval)
	sent := make(chan time.Time, count)
	_ = conn.SetDeadline(time.Now().Add(d + 30*time.Second))
	writeErr := make(chan error, 1)
	go func() {
		key := make([]byte, 36)
		start := time.Now()
		for i := range count {
			if wait := time.Until(start.Add(time.Duration(i) * interval)); wait > 0 {
				time.Sleep(wait)
			}
			sent <- time.Now()
			if _, err := conn.Write(key); err != nil {
				writeErr <- err
				return
			}
		}
		writeErr <- nil
	}()
	echo := make([]byte, 36)
	echoes := make([]time.Duration, 0, count)
	for range count {
		if _, err := io.ReadFull(conn, echo); err != nil {
			return echoes, err
		}
		echoes = append(echoes, time.Since(<-sent))
	}
	return echoes, <-writeErr
}
