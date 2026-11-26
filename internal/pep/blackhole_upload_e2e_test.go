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
)

// The other direction of TestBlackholeLoadedDownloadRecovers: a client that
// is uploading when the path is dropped for 8 s. The live campaign's
// background upload posts objects with a 10 s socket timeout, so an upload
// that does not move again within a second or two of the path's return is
// one the application gives up on. What counts as moving is what the
// destination receives, not what the client's socket accepts: the tunnel's
// client buffers.

const (
	bhuWarm      = 14 * time.Second
	bhuAfter     = 10 * time.Second
	bhuResumeAt  = 64 << 10
	bhuResumeMax = 1500 * time.Millisecond
)

// bhuDestination counts what it is sent into received, and for a connection
// whose first byte is 'D' also writes without end.
func bhuDestination(l net.Listener, received *atomic.Int64) {
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
			if first[0] == 'D' {
				chunk := make([]byte, 64<<10)
				for {
					if _, err := conn.Write(chunk); err != nil {
						return
					}
				}
			}
			buf := make([]byte, 64<<10)
			for {
				n, err := conn.Read(buf)
				received.Add(int64(n))
				if err != nil {
					return
				}
			}
		}()
	}
}

type bhuTrial struct {
	arm, mix string
	before   float64 // Mbit/s arriving in the 4 s before the drop
	resumed  time.Duration
	mb5      float64 // megabytes arriving in the 5 s after the restore
	trace    []string
}

func (b bhuTrial) String() string {
	return fmt.Sprintf("%-6s %-22s before %.1f Mbit/s | upload arriving again %.2fs after the restore | %.1f MB in the next 5 s",
		b.arm, b.mix, b.before, b.resumed.Seconds(), b.mb5)
}

// runBlackholeUpload runs one upload across an 8 s drop, beside a download
// if withDownload, and reports when the destination was receiving again.
func runBlackholeUpload(t *testing.T, arm workloadArm, withDownload bool, lead time.Duration, seed int64) bhuTrial {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	var received atomic.Int64
	go bhuDestination(listener, &received)
	destination := listener.Addr().String()
	rig := newWorkloadRig(t, bhdPath(seed), arm, nil)

	res := bhuTrial{arm: arm.name, mix: "upload alone", resumed: -1}
	if withDownload {
		res.mix = "upload beside a download"
		down := rig.dial(destination)
		if _, err := down.Write([]byte{'D'}); err != nil {
			t.Fatal(err)
		}
		go func() { _, _ = io.Copy(io.Discard, down) }()
	}
	up := rig.dial(destination)
	go func() {
		chunk := make([]byte, 64<<10)
		chunk[0] = 'U'
		for {
			if _, err := up.Write(chunk); err != nil {
				return
			}
		}
	}()
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
			last := received.Load()
			lastUp, lastDown := rig.relay.Stats()
			for {
				select {
				case <-stop:
					return
				case <-time.After(500 * time.Millisecond):
				}
				now := received.Load()
				u, d := rig.relay.Stats()
				note(fmt.Sprintf("%5.1f Mbit/s arriving | packets on the path: client sent %d (%d through), gateway sent %d (%d through)%s",
					float64(now-last)*16/1e6, u.PacketsIn-lastUp.PacketsIn, u.PacketsOut-lastUp.PacketsOut,
					d.PacketsIn-lastDown.PacketsIn, d.PacketsOut-lastDown.PacketsOut, bhuTelemetry()))
				last, lastUp, lastDown = now, u, d
			}
		}()
	}
	time.Sleep(bhuWarm - 4*time.Second + lead)
	mark := received.Load()
	time.Sleep(4 * time.Second)
	res.before = float64(received.Load()-mark) * 8 / 4 / 1e6
	note("DROP")
	rig.relay.SetLossRate(1, 1)
	time.Sleep(8 * time.Second)
	rig.relay.SetLossRate(0, 0)
	note("RESTORE")
	restored, at := time.Now(), received.Load()
	for time.Since(restored) < bhuAfter {
		time.Sleep(10 * time.Millisecond)
		now := received.Load()
		if res.resumed < 0 && now-at >= bhuResumeAt {
			res.resumed = time.Since(restored)
		}
		if res.mb5 == 0 && time.Since(restored) >= 5*time.Second {
			res.mb5 = float64(now-at) / 1e6
		}
	}
	return res
}

// bhuTelemetry is both ends' path controllers, the client's first.
func bhuTelemetry() string {
	s := ""
	pathAuthorities.mu.Lock()
	defer pathAuthorities.mu.Unlock()
	for _, side := range []string{"cl", "gw"} {
		for name, c := range pathAuthorities.controllers {
			isClient := len(name) >= len(clientPathPrincipal) && name[:len(clientPathPrincipal)] == clientPathPrincipal
			if c.Attachments() == 0 || isClient != (side == "cl") {
				continue
			}
			tel, st := c.Telemetry(), c.Stats()
			s += fmt.Sprintf(" | %s m%d bw %.1fMb cwnd %dk hi %dk flight %dk pace %.1fMb", side, tel.Mode,
				float64(tel.MaxBandwidth)*8/1e6, tel.CongestionWindow/1000, tel.InflightBound/1000, st.Flight/1000, float64(tel.PacingRate)*8/1e6)
		}
	}
	return s
}

// TestBlackholeUploadResumes reports, for the legacy bundle and the flip
// bundle, how soon after an 8 s drop an upload is arriving at its
// destination again, alone and beside a download, at three offsets of the
// drop against the transport's timers, and holds the flip bundle to
// bhuResumeMax. QQ_BHD_TRACE, QQ_BHD_ARM and
// QQ_BHD_REPEAT apply as in TestBlackholeLoadedDownloadRecovers.
func TestBlackholeUploadResumes(t *testing.T) {
	if testing.Short() {
		t.Skip("wall-clock impairment campaign")
	}
	requireStableImpairmentClock(t)
	repeat := 1
	if n, err := strconv.Atoi(os.Getenv("QQ_BHD_REPEAT")); err == nil && n > 1 {
		repeat = n
	}
	leads := []time.Duration{0, 700 * time.Millisecond, 1400 * time.Millisecond}
	for i, arm := range workloadArms() {
		if only := os.Getenv("QQ_BHD_ARM"); only != "" && only != arm.name {
			continue
		}
		for n := range repeat {
			for j, lead := range leads {
				r := runBlackholeUpload(t, arm, j%2 == 1 || n%2 == 1, lead, int64(81+i+10*n+j))
				t.Log(r)
				for _, line := range r.trace {
					t.Log("  " + line)
				}
				// The flip bundle's uploads arrived again 0.6 to 1.0 s after
				// the restore in twelve runs; the legacy bundle's, 1.3 to
				// 7.3 s.
				if arm.name == flipArm.name && (r.resumed < 0 || r.resumed > bhuResumeMax) {
					t.Errorf("flip, %s: the upload was arriving again %.2fs after the restore (bound %v)", r.mix, r.resumed.Seconds(), bhuResumeMax)
				}
			}
		}
	}
}
