package pep

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bojieli/queqiao/internal/pathsim"
)

// audioInterval and audioBytes are W7's audio: 160 B every 20 ms.
const (
	audioInterval = 20 * time.Millisecond
	audioBytes    = 160
)

// audioDirection is one direction of a call's audio: each packet's one-way
// delay, zero for one that never arrived.
type audioDirection struct {
	delays []time.Duration
}

// judge returns how many packets were late (lost, or more than playout
// past the direction's best one-way delay) and the lateness of each that
// arrived. Both ends of the test share one clock, so one-way delays are
// exact.
func (a audioDirection) judge(playout time.Duration) (late int, lateness []time.Duration) {
	var best time.Duration
	for _, d := range a.delays {
		if d > 0 && (best == 0 || d < best) {
			best = d
		}
	}
	for _, d := range a.delays {
		if d == 0 {
			late++
			continue
		}
		l := d - best
		lateness = append(lateness, l)
		if l > playout {
			late++
		}
	}
	return late, lateness
}

// audioPacket stamps a packet with its sequence and send time.
func audioPacket(seq int) []byte {
	p := make([]byte, audioBytes)
	binary.BigEndian.PutUint32(p, uint32(seq))
	binary.BigEndian.PutUint64(p[4:], uint64(time.Now().UnixNano()))
	return p
}

// audioDelay reads a stamped packet's sequence and one-way delay.
func audioDelay(p []byte) (int, time.Duration, bool) {
	if len(p) < 12 {
		return 0, 0, false
	}
	sent := time.Unix(0, int64(binary.BigEndian.Uint64(p[4:])))
	return int(binary.BigEndian.Uint32(p)), time.Since(sent), true
}

// audioFarEnd is the far side of a call: it records the uplink's arrivals
// and, once the first uplink packet names the call's return address, sends
// its own open-loop downlink of count packets. Uplink arrivals never pace it.
type audioFarEnd struct {
	conn  *net.UDPConn
	count int
	mu    sync.Mutex
	up    audioDirection
}

func newAudioFarEnd(t *testing.T, count int) *audioFarEnd {
	t.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	f := &audioFarEnd{conn: conn, count: count, up: audioDirection{delays: make([]time.Duration, count)}}
	go func() {
		buf := make([]byte, 2048)
		started := false
		for {
			n, from, err := conn.ReadFromUDP(buf)
			if err != nil {
				return
			}
			seq, delay, ok := audioDelay(buf[:n])
			if !ok {
				continue
			}
			f.mu.Lock()
			if seq < count && f.up.delays[seq] == 0 {
				f.up.delays[seq] = delay
			}
			f.mu.Unlock()
			if !started {
				started = true
				go func() {
					start := time.Now()
					for seq := range count {
						if wait := time.Until(start.Add(time.Duration(seq) * audioInterval)); wait > 0 {
							time.Sleep(wait)
						}
						_, _ = conn.WriteToUDP(audioPacket(seq), from)
					}
				}()
			}
		}
	}()
	return f
}

func (f *audioFarEnd) uplink() audioDirection {
	f.mu.Lock()
	defer f.mu.Unlock()
	return audioDirection{delays: append([]time.Duration(nil), f.up.delays...)}
}

// runCall sends the uplink for count packets and records the downlink until
// the call and a drain period are over, reading on across any gap.
func runCall(a *socksAssociation, far *audioFarEnd, count int, drain time.Duration) (down audioDirection) {
	down.delays = make([]time.Duration, count)
	end := time.Now().Add(time.Duration(count)*audioInterval + drain)
	done := make(chan struct{})
	go func() {
		defer close(done)
		buf := make([]byte, 2048)
		for time.Now().Before(end) {
			payload, err := a.receive(buf, end)
			if err != nil {
				if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
					continue
				}
				return
			}
			if seq, delay, ok := audioDelay(payload); ok && seq < count && down.delays[seq] == 0 {
				down.delays[seq] = delay
			}
		}
	}()
	start := time.Now()
	target := far.conn.LocalAddr().(*net.UDPAddr)
	for seq := range count {
		if wait := time.Until(start.Add(time.Duration(seq) * audioInterval)); wait > 0 {
			time.Sleep(wait)
		}
		_ = a.send(target, audioPacket(seq))
	}
	<-done
	return down
}

// m5Direction is one audio direction's M5 measures.
type m5Direction struct {
	sent     int
	late     int
	lateness []time.Duration
}

// m5Outcome is one arm's M5 measures.
type m5Outcome struct {
	audio        [2]m5Direction // uplink, downlink
	keystrokes   []time.Duration
	heavyGoodput float64 // during the thin measurements
	pathDrops    uint64  // the emulated bottleneck's own drops, both directions
}

// M5. Heavy UDP beside thin UDP and coded stream chunks
// (docs/WORKLOAD-MODEL.md): a tunnelled QUIC download (W8) fills the path
// while a call's audio (W7, 160 B every 20 ms each way on its own
// association) and terminal keystrokes (W6, on a TCP flow small enough to be
// coded) run beside it. All three share the tunnel's one ordered symbol
// stream (design Q5), so a heavy association that the tunnel lets queue
// delays the others by its queue.
//
// The audio is two open-loop streams, one per direction, each on its own
// clock: the far end starts its downlink when the first uplink packet names
// the call. Both ends share the test's clock, so each packet's one-way delay
// is exact, and its lateness is that delay less the direction's best. A
// packet is late when its lateness exceeds the 150 ms playout budget or it
// never arrives. Bounds, per direction (models W7, W6): audio late loss ≤
// legacy + 0.5 pp, with one packet of allowance at this size; audio lateness
// p99 ≤ legacy × 1.10 + 10 ms; keystroke echo p99 ≤ legacy × 1.10 + 20 ms.
// The heavy download must move data throughout the measurements. The model
// also reports the coded FIFO's sojourn p99 and the UDP PACKET frames
// dropped at overflow; no build exports either yet, so the emulated
// bottleneck's own drops and the heavy flow's goodput are reported in their
// place.
//
// Both arms run at once, each on its own client, gateway and emulated path:
// 120 ms round trip, 20 Mbit/s behind a drop-tail queue, for 30 s. That is a
// block. The measures follow the inner controller's sawtooth, so one block's
// tail moves by a third from run to run in either arm and either direction,
// as the design expects of a tail (§15.4 judges tails over blocks). The full
// suite runs five blocks and holds each measure's median over them to its
// bound. -short runs one 8 s block as a smoke test: every keystroke echoed,
// audio measured both ways, the heavy flow moving.
func TestHeavyUDPDoesNotDelayThinUDPOrCodedChunks(t *testing.T) {
	requireStableImpairmentClock(t)
	const playout = 150 * time.Millisecond
	measure, blocks := 30*time.Second, 5
	if testing.Short() {
		measure, blocks = 8*time.Second, 1
	}
	echoListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = echoListener.Close() })
	go echoDestination(echoListener)

	arms := workloadArms()
	// measures[arm] holds, per block: late fraction up, late fraction down,
	// lateness p99 up, lateness p99 down, keystroke echo p99.
	measures := make([][5][]float64, len(arms))
	for block := range blocks {
		outcomes := runM5Block(t, echoListener.Addr().String(), measure, playout)
		for i, arm := range arms {
			o := outcomes[i]
			for d, name := range []string{"uplink", "downlink"} {
				a := o.audio[d]
				t.Logf("block %d, %s %s audio: %d of %d late or lost, lateness p99 %v", block, arm.name, name, a.late, a.sent,
					durationQuantile(a.lateness, 0.99).Round(time.Millisecond))
				if len(a.lateness) == 0 {
					t.Fatalf("%s: no %s audio arrived", arm.name, name)
				}
				measures[i][d] = append(measures[i][d], float64(a.late)/float64(a.sent))
				measures[i][2+d] = append(measures[i][2+d], durationQuantile(a.lateness, 0.99).Seconds())
			}
			t.Logf("block %d, %s: keystroke echo p99 %v over %d; heavy goodput %.2f Mbit/s; bottleneck drops %d",
				block, arm.name, durationQuantile(o.keystrokes, 0.99).Round(time.Millisecond), len(o.keystrokes),
				o.heavyGoodput*8/1e6, o.pathDrops)
			measures[i][4] = append(measures[i][4], durationQuantile(o.keystrokes, 0.99).Seconds())
			// A heavy flow that stalled would leave the thin flows nothing
			// to compete with: the measurement would not be M5.
			if o.heavyGoodput < 1e6/8 {
				t.Fatalf("%s: the heavy download moved %.2f Mbit/s during the measurements", arm.name, o.heavyGoodput*8/1e6)
			}
		}
	}
	if testing.Short() || !judgeComparisons(t) {
		return
	}
	median := func(arm, measure int) float64 { return floatQuantile(measures[arm][measure], 0.5) }
	packets := float64(measure / audioInterval)
	for d, name := range []string{"uplink", "downlink"} {
		if got, limit := median(1, d), median(0, d)+0.005+1/packets; got > limit {
			t.Errorf("%s audio late loss, median over %d blocks: %.2f %% under the flip bundle against %.2f %% under legacy; "+
				"the bound (legacy + 0.5 pp, plus one packet) allows %.2f %%", name, blocks, 100*got, 100*median(0, d), 100*limit)
		}
		if got, limit := median(1, 2+d), median(0, 2+d)*1.10+0.010; got > limit {
			t.Errorf("%s audio lateness p99, median over %d blocks: %.0f ms under the flip bundle; the bound is legacy × 1.10 + 10 ms = %.0f ms",
				name, blocks, 1000*got, 1000*limit)
		}
	}
	if got, limit := median(1, 4), median(0, 4)*1.10+0.020; got > limit {
		t.Errorf("keystroke echo p99, median over %d blocks: %.0f ms under the flip bundle; the bound is legacy × 1.10 + 20 ms = %.0f ms",
			blocks, 1000*got, 1000*limit)
	}
}

// runM5Block runs one M5 block: both arms at once, each on a fresh client,
// gateway, emulated path and far ends.
func runM5Block(t *testing.T, echo string, measure, playout time.Duration) []m5Outcome {
	t.Helper()
	path := pathsim.Config{OneWayDelay: 60 * time.Millisecond, Seed: 5, RateBytesPerSec: 20e6 / 8}
	arms := workloadArms()
	rigs := make([]*workloadRig, len(arms))
	servers := make([]*innerQUICServer, len(arms))
	fars := make([]*audioFarEnd, len(arms))
	for i, arm := range arms {
		rigs[i] = newWorkloadRig(t, path, arm, nil)
		servers[i] = newInnerQUICServer(t)
		fars[i] = newAudioFarEnd(t, int(measure/audioInterval))
	}
	outcomes := make([]m5Outcome, len(arms))
	errs := make(chan error, len(arms))
	var wg sync.WaitGroup
	for i, arm := range arms {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := runM5Arm(t, rigs[i], servers[i], fars[i], echo, measure, playout, &outcomes[i]); err != nil {
				errs <- fmt.Errorf("%s: %w", arm.name, err)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	return outcomes
}

// runM5Arm runs one arm's M5: the thin endpoints are set up first, then the
// heavy download starts and fills the path, then the call and the keystrokes
// run while the download's progress is counted. Every worker it starts has
// ended when it returns.
func runM5Arm(t *testing.T, rig *workloadRig, server *innerQUICServer, far *audioFarEnd, echo string, measure, playout time.Duration, o *m5Outcome) error {
	ctx, cancel := context.WithTimeout(context.Background(), measure+2*time.Minute)
	defer cancel()

	keys := rig.dial(echo)
	audio, err := rig.associate()
	if err != nil {
		return fmt.Errorf("audio association: %w", err)
	}
	defer audio.Close()
	conn, err := dialInnerQUIC(ctx, t, rig, server)
	if err != nil {
		return fmt.Errorf("inner handshake: %w", err)
	}
	defer conn.CloseWithError(0, "")
	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return err
	}
	var request [4]byte
	binary.BigEndian.PutUint32(request[:], 1<<31)
	if _, err := stream.Write(request[:]); err != nil {
		return err
	}
	_ = stream.Close()

	// The heavy download runs until the measurements end; heavyDone is
	// waited on whatever happens below.
	var heavy atomic.Int64
	heavyCtx, stopHeavy := context.WithCancel(ctx)
	heavyDone := make(chan struct{})
	go func() {
		defer close(heavyDone)
		go func() {
			<-heavyCtx.Done()
			stream.CancelRead(0)
		}()
		buf := make([]byte, 64*1024)
		for {
			n, err := stream.Read(buf)
			heavy.Add(int64(n))
			if err != nil {
				return
			}
		}
	}()
	defer func() {
		stopHeavy()
		<-heavyDone
	}()
	time.Sleep(3 * time.Second) // let it fill the path

	upBefore, downBefore := rig.relay.Stats()
	heavyBefore, start := heavy.Load(), time.Now()
	var thin sync.WaitGroup
	var keyErr error
	thin.Add(2)
	go func() {
		defer thin.Done()
		down := runCall(audio, far, int(measure/audioInterval), time.Second)
		o.audio[1] = summarizeAudio(down, playout)
	}()
	go func() {
		defer thin.Done()
		o.keystrokes, keyErr = runKeystrokes(keys, measure)
	}()
	thin.Wait()
	o.heavyGoodput = float64(heavy.Load()-heavyBefore) / time.Since(start).Seconds()
	upAfter, downAfter := rig.relay.Stats()
	o.pathDrops = upAfter.PacketsDropped - upBefore.PacketsDropped + downAfter.PacketsDropped - downBefore.PacketsDropped
	o.audio[0] = summarizeAudio(far.uplink(), playout)
	if keyErr != nil {
		return fmt.Errorf("keystrokes: %w", keyErr)
	}
	return nil
}

func summarizeAudio(a audioDirection, playout time.Duration) m5Direction {
	late, lateness := a.judge(playout)
	return m5Direction{sent: len(a.delays), late: late, lateness: lateness}
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
