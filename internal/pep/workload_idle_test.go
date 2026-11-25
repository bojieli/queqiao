package pep

import (
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

// W9b's messages (docs/WORKLOAD-MODEL.md, W9). Each starts with its tag and
// is fixed-size, so a reader knows where the next one starts.
const (
	idleSyncBytes  = 200 * 1024
	idlePing       = 'P' // client to destination, 40 B
	idlePong       = 'p' // destination to client, 40 B
	idleSend       = 'S' // the client-send wake, 300 B
	idleSendAck    = 'a' // the destination's acknowledgement of it, 60 B
	idlePush       = 'W' // the server-push wake, 300 B
	idleCellHeader = 4   // the cell number, client to destination, first
)

func idleMessageSize(tag byte) int {
	switch tag {
	case idlePing, idlePong:
		return 40
	case idleSend, idlePush:
		return 300
	case idleSendAck:
		return 60
	}
	return 0
}

func idleMessage(tag byte) []byte {
	m := make([]byte, idleMessageSize(tag))
	m[0] = tag
	return m
}

// idleEnd is how and when one application's side of a flow saw it end.
type idleEnd struct {
	at  time.Time
	err error
}

// idleSide reads one application's side of a cell's connection for as long as
// it lasts, so the moment it ends is seen when it happens, not when the test
// next looks.
type idleSide struct {
	conn     net.Conn
	messages chan byte
	ended    chan idleEnd
}

func newIdleSide(conn net.Conn, skip int) *idleSide {
	s := &idleSide{conn: conn, messages: make(chan byte, 16), ended: make(chan idleEnd, 1)}
	go func() {
		if skip > 0 {
			if _, err := io.CopyN(io.Discard, conn, int64(skip)); err != nil {
				s.ended <- idleEnd{at: time.Now(), err: err}
				return
			}
			s.messages <- 0 // the sync is complete
		}
		var tag [1]byte
		for {
			if _, err := io.ReadFull(conn, tag[:]); err != nil {
				s.ended <- idleEnd{at: time.Now(), err: err}
				return
			}
			if size := idleMessageSize(tag[0]); size > 1 {
				if _, err := io.CopyN(io.Discard, conn, int64(size-1)); err != nil {
					s.ended <- idleEnd{at: time.Now(), err: err}
					return
				}
			}
			s.messages <- tag[0]
		}
	}()
	return s
}

// expect waits for the next message, which must be tag.
func (s *idleSide) expect(tag byte, within time.Duration) error {
	select {
	case got := <-s.messages:
		if got != tag {
			return fmt.Errorf("got message %q, want %q", got, tag)
		}
		return nil
	case end := <-s.ended:
		s.ended <- end
		return fmt.Errorf("ended (%v) instead of receiving %q", end.err, tag)
	case <-time.After(within):
		return fmt.Errorf("no %q within %v", tag, within)
	}
}

// end is when the side ended, if it has.
func (s *idleSide) end() (idleEnd, bool) {
	select {
	case end := <-s.ended:
		s.ended <- end
		return end, true
	default:
		return idleEnd{}, false
	}
}

// idleCell is one cell of W9b: an idle target, a heartbeat mode and a wake
// direction, on a connection of its own.
type idleCell struct {
	target    time.Duration
	heartbeat time.Duration // zero: none
	push      bool
	client    *idleSide
	far       *idleSide
	woken     bool
	delivered bool
	// closed is whether the destination saw the client's close after the
	// wake within the bound.
	closed bool
	wake   time.Duration
}

func (c *idleCell) String() string {
	direction := "send"
	if c.push {
		direction = "push"
	}
	heartbeat := "none"
	if c.heartbeat > 0 {
		heartbeat = c.heartbeat.String()
	}
	return fmt.Sprintf("idle %v, heartbeat %s, %s", c.target, heartbeat, direction)
}

// survives is the documented outcome: a flow is delivered when its longest
// silence stays below the flow idle timeout, and otherwise ends at it.
func (c *idleCell) survives(idleTimeout time.Duration) bool {
	longest := c.target
	if c.heartbeat > 0 {
		longest = min(longest, c.heartbeat)
	}
	return longest < idleTimeout
}

// W9b. Every cell of idle target × heartbeat mode × wake direction, each on a
// connection of its own, all through one tunnel whose flows read an injected
// clock: an hour of idleness passes in seconds. A cell's flow first moves a
// 200 KB sync, then is silent but for its heartbeat until its wake.
//
// Below the flow idle timeout the wake must arrive within 5 s. Above it the
// documented outcome is that the tunnel ends the flow at the timeout, and
// then both applications must see EOF or RST within 5 s of the flow ending:
// a flow the tunnel ends silently, leaving an application holding a socket
// that will never deliver again, is the failure this exists to catch. The
// legacy bundle and the flip bundle must both give exactly these outcomes,
// so the flip delivers at least what legacy does in every cell.
//
// The clock drives the flow idle limit and classification on both ends. The
// path's own timers (QUIC idle and keepalive, carrier liveness) run on real
// time; the test takes seconds of it, far inside every one of them. The
// binding deadline and the adaptive keepalive schedule of design §10 are not
// built yet; they read this clock when they are.
func TestIdleFlowSurvivesEachIdleTargetOnAnInjectedClock(t *testing.T) {
	targets := []time.Duration{5, 14, 16, 29, 31, 45, 60}
	heartbeats := []time.Duration{0, time.Minute, 10 * time.Minute}
	idleTimeout := defaultFlowIdleTimeout
	// The flow's idle check runs on a real one-second ticker; once the
	// clock passes the timeout the flow ends within one tick, and its end
	// must then reach each application within the bound.
	const propagation = 5 * time.Second
	const tick = time.Second

	delivered := map[string]map[string]bool{}
	for _, arm := range workloadArms() {
		t.Run(arm.name, func(t *testing.T) {
			var offset atomic.Int64
			clock := func() time.Time { return time.Now().Add(time.Duration(offset.Load())) }
			advance := func(to time.Duration) { offset.Store(int64(to)) }

			rig := newWorkloadRig(t, pathsim.Config{OneWayDelay: 10 * time.Millisecond, Seed: 9}, arm,
				func(c *Client, s *Server) { c.flowClockForTest, s.flowClockForTest = clock, clock })

			var cells []*idleCell
			for _, target := range targets {
				for _, heartbeat := range heartbeats {
					for _, push := range []bool{true, false} {
						cells = append(cells, &idleCell{target: target * time.Minute, heartbeat: heartbeat, push: push})
					}
				}
			}

			// The destination: each connection names its cell, gets its
			// sync, then answers pings and sends.
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = listener.Close() })
			farSides := make([]chan *idleSide, len(cells))
			for i := range farSides {
				farSides[i] = make(chan *idleSide, 1)
			}
			go func() {
				for {
					conn, err := listener.Accept()
					if err != nil {
						return
					}
					go func() {
						var header [idleCellHeader]byte
						if _, err := io.ReadFull(conn, header[:]); err != nil {
							_ = conn.Close()
							return
						}
						index := int(binary.BigEndian.Uint32(header[:]))
						if index >= len(farSides) {
							_ = conn.Close()
							return
						}
						if _, err := conn.Write(make([]byte, idleSyncBytes)); err != nil {
							_ = conn.Close()
							return
						}
						farSides[index] <- newIdleSide(conn, 0)
					}()
				}
			}()

			// Open every cell and complete its sync at clock zero.
			var wg sync.WaitGroup
			errs := make(chan error, len(cells))
			for i, cell := range cells {
				wg.Add(1)
				go func() {
					defer wg.Done()
					conn := dialTestSOCKS(t, rig.socks, listener.Addr().String())
					var header [idleCellHeader]byte
					binary.BigEndian.PutUint32(header[:], uint32(i))
					if _, err := conn.Write(header[:]); err != nil {
						errs <- fmt.Errorf("%v: %w", cell, err)
						return
					}
					cell.client = newIdleSide(conn, idleSyncBytes)
					if err := cell.client.expect(0, 30*time.Second); err != nil {
						errs <- fmt.Errorf("%v: sync: %w", cell, err)
						return
					}
					select {
					case cell.far = <-farSides[i]:
					case <-time.After(10 * time.Second):
						errs <- fmt.Errorf("%v: the destination never saw the connection", cell)
					}
				}()
			}
			wg.Wait()
			close(errs)
			for err := range errs {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				for _, cell := range cells {
					_ = cell.client.conn.Close()
					_ = cell.far.conn.Close()
				}
			})

			// One minute at a time to the last target. Each minute's
			// heartbeats and wakes complete before the clock moves on, so
			// every flow's activity is stamped with the minute it happened.
			crossed := false
			last := targets[len(targets)-1] * time.Minute
			for now := time.Minute; now <= last; now += time.Minute {
				advance(now)
				if !crossed && now >= idleTimeout {
					crossed = true
					checkIdleEnds(t, cells, idleTimeout, time.Now(), tick+propagation)
				}
				if !crossed && now+time.Minute >= idleTimeout {
					// The last minute before the timeout: every flow is
					// still open, including those about to end at it.
					for _, cell := range cells {
						if cell.woken {
							continue
						}
						for name, side := range map[string]*idleSide{"client": cell.client, "destination": cell.far} {
							if end, ok := side.end(); ok {
								t.Errorf("%v: the %s application saw the flow end (%v) before the flow idle timeout", cell, name, end.err)
							}
						}
					}
				}
				var wg sync.WaitGroup
				for _, cell := range cells {
					if cell.woken || !cell.survives(idleTimeout) {
						continue
					}
					waking := now == cell.target
					if waking {
						// Decided here, not in the worker, so the loop
						// below reads it without a race.
						cell.woken = true
					}
					wg.Add(1)
					go func() {
						defer wg.Done()
						switch {
						case waking:
							runIdleWake(cell, propagation)
						case cell.heartbeat > 0 && now%cell.heartbeat == 0:
							if _, err := cell.client.conn.Write(idleMessage(idlePing)); err != nil {
								return // seen below as the cell ending early
							}
							_ = cell.client.expect(idlePong, propagation)
						}
					}()
				}
				// The destination answers what reaches it.
				for _, cell := range cells {
					if cell.woken || !cell.survives(idleTimeout) {
						continue
					}
					if cell.heartbeat > 0 && now%cell.heartbeat == 0 {
						if err := cell.far.expect(idlePing, propagation); err == nil {
							_, _ = cell.far.conn.Write(idleMessage(idlePong))
						}
					}
				}
				wg.Wait()
			}

			arm := map[string]bool{}
			for _, cell := range cells {
				if !cell.survives(idleTimeout) {
					continue
				}
				arm[cell.String()] = cell.delivered
				if cell.delivered && !cell.closed {
					t.Errorf("%v: the destination did not see the client close within %v", cell, propagation)
				}
				if !cell.delivered {
					client, clientEnded := cell.client.end()
					far, farEnded := cell.far.end()
					t.Errorf("%v: the wake was not delivered within %v (client ended %v %v, destination ended %v %v)",
						cell, propagation, clientEnded, client.err, farEnded, far.err)
				}
			}
			delivered[t.Name()] = arm
			t.Logf("%d cells: %d below the flow idle timeout delivered, %d ended at it",
				len(cells), countTrue(arm), len(cells)-len(arm))
		})
	}
	legacy, flip := delivered[t.Name()+"/"+legacyArm.name], delivered[t.Name()+"/"+flipArm.name]
	for cell, ok := range legacy {
		if ok && !flip[cell] {
			t.Errorf("%s: delivered under the legacy bundle, not under the flip bundle", cell)
		}
	}
}

func countTrue(m map[string]bool) int {
	n := 0
	for _, v := range m {
		if v {
			n++
		}
	}
	return n
}

// runIdleWake wakes a cell in its direction and records whether the wake
// arrived within the bound. The destination's half is driven here too, so
// a send is acknowledged as it would be by a real server.
func runIdleWake(cell *idleCell, within time.Duration) {
	start := time.Now()
	deadline := start.Add(within)
	write := func(side *idleSide, tag byte) bool {
		_ = side.conn.SetWriteDeadline(deadline)
		_, err := side.conn.Write(idleMessage(tag))
		_ = side.conn.SetWriteDeadline(time.Time{})
		return err == nil
	}
	expect := func(side *idleSide, tag byte) bool {
		return side.expect(tag, time.Until(deadline)) == nil
	}
	if cell.push {
		if !write(cell.far, idlePush) || !expect(cell.client, idlePush) {
			return
		}
	} else {
		if !write(cell.client, idleSend) || !expect(cell.far, idleSend) ||
			!write(cell.far, idleSendAck) || !expect(cell.client, idleSendAck) {
			return
		}
	}
	cell.wake = time.Since(start)
	if cell.wake > within {
		return
	}
	cell.delivered = true
	// The application closes its side; the other must see it.
	_ = cell.client.conn.Close()
	cell.closed = (<-waitEnd(cell.far, within)).err != nil
}

// waitEnd delivers the side's end, or a nil-error end if it has not ended
// within the bound. Messages that arrive meanwhile are discarded.
func waitEnd(s *idleSide, within time.Duration) <-chan idleEnd {
	out := make(chan idleEnd, 1)
	go func() {
		timeout := time.After(max(within, 0))
		for {
			// An end that has already happened wins over the timeout.
			if end, ok := s.end(); ok {
				if end.err == nil {
					end.err = io.EOF
				}
				out <- end
				return
			}
			select {
			case end := <-s.ended:
				s.ended <- end
			case <-s.messages:
			case <-timeout:
				if _, ok := s.end(); ok {
					continue
				}
				out <- idleEnd{}
				return
			}
		}
	}()
	return out
}

// checkIdleEnds waits for every cell past the flow idle timeout to end at
// both applications, and checks none ended silently or late. crossed is
// when the clock passed the timeout.
func checkIdleEnds(t *testing.T, cells []*idleCell, idleTimeout time.Duration, crossed time.Time, within time.Duration) {
	t.Helper()
	deadline := crossed.Add(within)
	for _, cell := range cells {
		if cell.woken {
			continue // closed by its own application after its wake
		}
		if cell.survives(idleTimeout) {
			if end, ok := cell.client.end(); ok {
				t.Errorf("%v: the client's flow ended at the timeout, which it should not reach (%v)", cell, end.err)
			}
			continue
		}
		for name, side := range map[string]*idleSide{"client": cell.client, "destination": cell.far} {
			end := <-waitEnd(side, time.Until(deadline))
			if end.err == nil || end.at.After(deadline) {
				t.Errorf("%v: the %s application saw no EOF or RST within %v of the flow idle timeout: a silent death",
					cell, name, within)
			}
		}
	}
}

// A flow the client ends at its own idle timeout must end at the
// destination too, at once. The client used to close its streams without a
// word; the gateway took that for a path failure and held the destination
// open for a replacement lane that was never coming, 45 s, unless its own
// idle limit happened to fire first. W9b found it: with the default timeout
// on both ends whichever ticked first decided, and with a client timeout
// shorter than the gateway's the destination always waited. This pins the
// second case, on real time.
func TestAClientIdleTimeoutReachesTheDestinationWithinFiveSeconds(t *testing.T) {
	const idleTimeout = 2 * time.Second
	arm := workloadArm{
		name:   "client idle timeout",
		client: func(c *ClientConfig) { c.FlowIdleTimeout = idleTimeout },
		server: func(*ServerConfig) {},
	}
	rig := newWorkloadRig(t, pathsim.Config{OneWayDelay: 10 * time.Millisecond, Seed: 3}, arm, nil)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	accepted := make(chan net.Conn, 1)
	go func() {
		if conn, err := listener.Accept(); err == nil {
			accepted <- conn
		}
	}()
	client := newIdleSide(rig.dial(listener.Addr().String()), 0)
	if _, err := client.conn.Write(idleMessage(idlePing)); err != nil {
		t.Fatal(err)
	}
	var far *idleSide
	select {
	case conn := <-accepted:
		t.Cleanup(func() { _ = conn.Close() })
		far = newIdleSide(conn, 0)
	case <-time.After(10 * time.Second):
		t.Fatal("the destination never saw the connection")
	}
	if err := far.expect(idlePing, 5*time.Second); err != nil {
		t.Fatal(err)
	}

	clientEnd := <-waitEnd(client, idleTimeout+10*time.Second)
	if clientEnd.err == nil {
		t.Fatalf("the client did not end the flow at its %v idle timeout", idleTimeout)
	}
	farEnd := <-waitEnd(far, 10*time.Second)
	if farEnd.err == nil {
		t.Fatal("the destination saw no EOF or RST within 10 s of the client's idle timeout")
	}
	if lag := farEnd.at.Sub(clientEnd.at); lag > 5*time.Second {
		t.Fatalf("the destination saw the flow end %v after the client application did, want within 5 s", lag.Round(time.Millisecond))
	}
}
