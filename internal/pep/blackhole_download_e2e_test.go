package pep

import (
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bojieli/queqiao/internal/pathsim"
)

// The live impaired campaign's blackhole trial as the gateway sees it: a
// path that is carrying downloads when it is dropped completely, for 8 s and
// then for 25 s. TestBlackholeCampaignAgainstLegacy has an upload and thin
// flows only, so the gateway's sender is nearly idle when the path goes and
// has nothing to rebuild when it returns. Here two downloads fill a
// 20 Mbit/s, 360 ms path, as the campaign's background transfer and fetches
// do, and what is measured is what the campaign measures after each restore:
// how soon a flow that was in flight moves again, how soon a new connection
// gets its first byte, and how much the downloads carry in the seconds that
// follow.

const (
	bhdWarm     = 14 * time.Second // past the first minimum-round-trip expiry
	bhdBetween  = 16 * time.Second
	bhdAfter    = 12 * time.Second // measured after each restore
	bhdResumeAt = 256 << 10        // "moving again": this much more arrived
	// The flip bundle's bounds: how soon after the 8 s drop its downloads
	// move, and how much they carry in the ten seconds after each restore.
	bhdResumeMax  = 3 * time.Second
	bhdCarriedMin = 12 // megabytes
)

func bhdPath(seed int64) pathsim.Config {
	return pathsim.Config{OneWayDelay: 180 * time.Millisecond, RateBytesPerSec: 20_000_000 / 8, QueueBytes: 256 << 10, Seed: seed}
}

// bhdDestination writes without end to a connection whose first byte is 'D'
// and echoes any other.
func bhdDestination(l net.Listener) {
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
			if first[0] != 'D' {
				_, _ = conn.Write(first[:])
				_, _ = io.Copy(conn, conn)
				return
			}
			chunk := make([]byte, 64<<10)
			for {
				if _, err := conn.Write(chunk); err != nil {
					return
				}
			}
		}()
	}
}

// bhdRestore is what followed one restore.
type bhdRestore struct {
	// resumed is how long after the restore the downloads had carried
	// bhdResumeAt more, and reopened how long until a connection opened after
	// it had its first byte. Negative: not within the measured time.
	resumed, reopened time.Duration
	// mbps is the downloads' rate in each second after the restore.
	mbps []float64
}

func (r bhdRestore) carried(seconds int) float64 {
	var sum float64
	for _, v := range r.mbps[:min(seconds, len(r.mbps))] {
		sum += v
	}
	return sum / 8 // megabytes
}

type bhdTrial struct {
	arm      string
	before   float64 // Mbit/s in the 4 s before the first drop
	restores [2]bhdRestore
	trace    []string
}

func (b bhdTrial) String() string {
	s := fmt.Sprintf("%-6s before %.1f Mbit/s", b.arm, b.before)
	for i, r := range b.restores {
		s += fmt.Sprintf(" | restore %d: moving after %.2fs, new connection's first byte %.2fs, %.1f MB in 5 s, %.1f MB in 10 s",
			i+1, r.resumed.Seconds(), r.reopened.Seconds(), r.carried(5), r.carried(10))
	}
	return s
}

func runBlackholeDownload(t *testing.T, arm workloadArm, seed int64) bhdTrial {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go bhdDestination(listener)
	destination := listener.Addr().String()
	rig := newWorkloadRig(t, bhdPath(seed), arm, nil)

	var got atomic.Int64
	for range 2 {
		conn := rig.dial(destination)
		if _, err := conn.Write([]byte{'D'}); err != nil {
			t.Fatal(err)
		}
		go func() {
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
	res := bhdTrial{arm: arm.name}
	trace := os.Getenv("QQ_BHD_TRACE") != ""
	began := time.Now()
	note := func(event string) {
		if trace {
			res.trace = append(res.trace, fmt.Sprintf("t=%5.1fs %s", time.Since(began).Seconds(), event))
		}
	}
	if trace {
		stop := make(chan struct{})
		t.Cleanup(func() { close(stop) })
		go func() {
			last := got.Load()
			lastUp, lastDown := rig.relay.Stats()
			for {
				select {
				case <-stop:
					return
				case <-time.After(500 * time.Millisecond):
				}
				now := got.Load()
				up, down := rig.relay.Stats()
				note(fmt.Sprintf("%5.1f Mbit/s | packets on the path: client sent %d (%d through), gateway sent %d (%d through)%s",
					float64(now-last)*16/1e6, up.PacketsIn-lastUp.PacketsIn, up.PacketsOut-lastUp.PacketsOut,
					down.PacketsIn-lastDown.PacketsIn, down.PacketsOut-lastDown.PacketsOut,
					qpTelemetry(0, 0)[len("t= 0.0s queue   0ms"):]))
				last, lastUp, lastDown = now, up, down
			}
		}()
	}

	time.Sleep(bhdWarm - 4*time.Second)
	mark := got.Load()
	time.Sleep(4 * time.Second)
	res.before = float64(got.Load()-mark) * 8 / 4 / 1e6
	for i, drop := range []time.Duration{8 * time.Second, 25 * time.Second} {
		note("DROP")
		rig.relay.SetLossRate(1, 1)
		time.Sleep(drop)
		rig.relay.SetLossRate(0, 0)
		note("RESTORE")
		restored, at := time.Now(), got.Load()
		r := bhdRestore{resumed: -1, reopened: -1}
		reopened := make(chan time.Duration, 1)
		go func() {
			conn, err := dialSOCKS(rig.socks, destination)
			if err != nil {
				return
			}
			defer conn.Close()
			_ = conn.SetDeadline(time.Now().Add(bhdAfter))
			var b [1]byte
			if _, err := conn.Write([]byte{'e'}); err != nil {
				return
			}
			if _, err := io.ReadFull(conn, b[:]); err == nil {
				reopened <- time.Since(restored)
			}
		}()
		last := at
		for s := 1; time.Since(restored) < bhdAfter; {
			time.Sleep(10 * time.Millisecond)
			now := got.Load()
			if r.resumed < 0 && now-at >= bhdResumeAt {
				r.resumed = time.Since(restored)
			}
			if time.Since(restored) >= time.Duration(s)*time.Second {
				r.mbps = append(r.mbps, float64(now-last)*8/1e6)
				last = now
				s++
			}
		}
		select {
		case r.reopened = <-reopened:
		default:
		}
		res.restores[i] = r
		if i == 0 {
			time.Sleep(bhdBetween - bhdAfter)
		}
	}
	return res
}

// TestBlackholeLoadedDownloadRecovers reports, for the legacy bundle and the
// flip bundle, how a loaded download path comes back from an 8 s and a 25 s
// drop, and holds the flip bundle to bounds on it. QQ_BHD_TRACE=1 logs the downloads' rate and the path controllers'
// state every half second; QQ_BHD_ARM runs one arm and QQ_BHD_REPEAT repeats
// each.
func TestBlackholeLoadedDownloadRecovers(t *testing.T) {
	if testing.Short() {
		t.Skip("wall-clock impairment campaign")
	}
	requireStableImpairmentClock(t)
	repeat := 1
	if n, err := strconv.Atoi(os.Getenv("QQ_BHD_REPEAT")); err == nil && n > 1 {
		repeat = n
	}
	for i, arm := range workloadArms() {
		if only := os.Getenv("QQ_BHD_ARM"); only != "" && only != arm.name {
			continue
		}
		for n := range repeat {
			r := runBlackholeDownload(t, arm, int64(61+i+10*n))
			t.Log(r)
			for _, line := range r.trace {
				t.Log("  " + line)
			}
			if arm.name != flipArm.name {
				continue
			}
			// Before the fixes this test came with, the flip bundle's
			// downloads moved 5.5 to 8 s after the 8 s drop and carried 0.4
			// to 10 MB in the ten seconds after it; with them, 1.3 to 1.5 s
			// and about 20 MB (the path carries 25 MB in ten seconds).
			if first := r.restores[0].resumed; first < 0 || first > bhdResumeMax {
				t.Errorf("flip: the downloads were moving %.2fs after the 8 s drop (bound %v)", first.Seconds(), bhdResumeMax)
			}
			for j, restore := range r.restores {
				if mb := restore.carried(10); mb < bhdCarriedMin {
					t.Errorf("flip: %.1f MB in the 10 s after restore %d (bound %.0f MB)", mb, j+1, float64(bhdCarriedMin))
				}
			}
		}
	}
}
