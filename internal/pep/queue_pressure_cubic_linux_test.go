//go:build linux

package pep

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/bojieli/queqiao/internal/pathsim"
)

// A kernel CUBIC competitor for the shared-bottleneck bed.
//
// The bed's other loss-based arm is the QUIC stack's stock NewReno, which is
// not what a path's other users run. This one is the kernel's own TCP with
// its CUBIC, end to end: a sender and a receiver socket whose segments and
// acknowledgements cross the same emulated link, queue and loss process as
// the QUIC leaves' packets.
//
// The kernel will not route a connection between two of its own addresses
// anywhere but loopback, so the connection is made to an address that is not
// local. A TUN device carries 10.9.0.1/24 and a route to 10.9.1.0/24; the
// receiver dials 10.9.1.2 and the kernel hands each packet to the device. The
// test reads it, sends it across the link, and writes it back with its source
// and destination addresses exchanged, which makes it a packet from 10.9.1.2
// to the listener on 10.9.0.1, and the reply likewise. Exchanging the two
// addresses leaves both the IP and the TCP checksum as they were. The device
// has no offloads, so every packet is one segment of at most the MTU.
//
// Creating the device needs CAP_NET_ADMIN, so the test runs itself again in
// a user and network namespace of its own, where it has it, and is skipped
// where unprivileged user namespaces are not allowed. Nothing outside that
// namespace is touched.

const (
	qpNetnsEnv     = "QQ_QP_NETNS"
	qpTunName      = "qqcubic0"
	qpTunLocal     = "10.9.0.1"
	qpTunMirror    = "10.9.1.2"
	qpTunMTU       = 1500
	qpCubicWindow  = 40 * time.Second
	qpTCPBufferMax = 64 << 20
)

const dcKernelCubicName = "kernel-cubic"

// TestSharedBottleneckAgainstKernelCUBIC reports the share a path authority,
// and for reference the legacy bundle, takes of the shared link from two
// kernel CUBIC connections, and the queue they hold between them.
// QQ_QP_PAIR, QQ_QP_REPEAT, QQ_QP_WINDOW and QQ_QP_TELEMETRY apply as in
// TestSharedBottleneckQueuePressure.
func TestSharedBottleneckAgainstKernelCUBIC(t *testing.T) {
	requireDCBed(t)
	if os.Getenv(qpNetnsEnv) == "" {
		runInOwnNetns(t)
		return
	}
	tun, err := openQPTun()
	if err != nil {
		t.Skipf("no TUN device in this namespace: %v", err)
	}
	defer tun.Close()
	var mirror atomic.Pointer[qpMirror]
	go mirrorTun(tun, &mirror)
	only := os.Getenv("QQ_QP_PAIR")
	repeat := 1
	if n, err := strconv.Atoi(os.Getenv("QQ_QP_REPEAT")); err == nil && n > 1 {
		repeat = n
	}
	for _, arm := range []dcArm{dcNewArm, dcLegacyArm} {
		if only != "" && only != arm.name+"/"+dcKernelCubicName {
			continue
		}
		for range repeat {
			r := runAgainstKernelCUBIC(t, arm, &mirror)
			t.Logf("PAIR %s", r)
			if testing.Verbose() {
				for _, line := range r.telemetry {
					t.Log("  " + line)
				}
			}
		}
	}
}

// runInOwnNetns runs this test again in a new user and network namespace and
// reports its output.
func runInOwnNetns(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("ip"); err != nil {
		t.Skip("no ip command to configure the TUN device with")
	}
	args := []string{"-test.run=^" + t.Name() + "$", "-test.timeout=30m"}
	if testing.Verbose() {
		args = append(args, "-test.v")
	}
	cmd := exec.Command(os.Args[0], args...)
	cmd.Env = append(os.Environ(), qpNetnsEnv+"=1")
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Cloneflags:  syscall.CLONE_NEWUSER | syscall.CLONE_NEWNET,
		UidMappings: []syscall.SysProcIDMap{{ContainerID: 0, HostID: os.Getuid(), Size: 1}},
		GidMappings: []syscall.SysProcIDMap{{ContainerID: 0, HostID: os.Getgid(), Size: 1}},
	}
	out, err := cmd.CombinedOutput()
	for _, line := range strings.Split(strings.TrimRight(string(out), "\n"), "\n") {
		t.Log(line)
	}
	var exit *exec.ExitError
	switch {
	case err == nil:
	case errors.As(err, &exit):
		t.Fatalf("the test failed in its own namespace: %v", err)
	default:
		// The namespace could not be created: unprivileged user namespaces
		// are off, or a sandbox forbids them.
		t.Skipf("cannot run in a user and network namespace: %v", err)
	}
}

// openQPTun brings loopback up, creates the TUN device and routes the mirror
// network to it, in the namespace the test is running in.
func openQPTun() (*os.File, error) {
	fd, err := unix.Open("/dev/net/tun", unix.O_RDWR|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	ifr, err := unix.NewIfreq(qpTunName)
	if err != nil {
		_ = unix.Close(fd)
		return nil, err
	}
	ifr.SetUint16(unix.IFF_TUN | unix.IFF_NO_PI)
	if err := unix.IoctlIfreq(fd, unix.TUNSETIFF, ifr); err != nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("TUNSETIFF: %w", err)
	}
	tun := os.NewFile(uintptr(fd), "/dev/net/tun")
	for _, args := range [][]string{
		{"link", "set", "lo", "up"},
		{"addr", "add", qpTunLocal + "/24", "dev", qpTunName},
		{"link", "set", qpTunName, "up", "mtu", strconv.Itoa(qpTunMTU), "txqueuelen", "10000"},
		{"route", "add", "10.9.1.0/24", "dev", qpTunName},
	} {
		if out, err := exec.Command("ip", args...).CombinedOutput(); err != nil {
			_ = tun.Close()
			return nil, fmt.Errorf("ip %s: %v: %s", strings.Join(args, " "), err, strings.TrimSpace(string(out)))
		}
	}
	// A window of the pipe and the queue is about 4 MiB; the namespace's
	// default ceilings would bind before the link did. These are the
	// namespace's own.
	limit := fmt.Sprintf("4096 131072 %d", qpTCPBufferMax)
	for _, name := range []string{"tcp_wmem", "tcp_rmem"} {
		if err := os.WriteFile("/proc/sys/net/ipv4/"+name, []byte(limit), 0); err != nil {
			_ = tun.Close()
			return nil, fmt.Errorf("%s: %w", name, err)
		}
	}
	return tun, nil
}

// qpMirror is where the device's packets go for the run in progress: across
// path, those from senderPort downstream and the rest upstream.
type qpMirror struct {
	path       *pathsim.PacketPath
	senderPort int
}

// mirrorTun carries the device's packets across the current run's path, each
// written back with its addresses exchanged, and drops them between runs. It
// returns when the device is closed.
func mirrorTun(tun *os.File, current *atomic.Pointer[qpMirror]) {
	deliver := func(p []byte) { _, _ = tun.Write(p) }
	buf := make([]byte, 65536)
	for {
		n, err := tun.Read(buf)
		if err != nil {
			return
		}
		to := current.Load()
		// IPv4 carrying TCP, with its ports in reach.
		if to == nil || n < 24 || buf[0]>>4 != 4 || buf[9] != unix.IPPROTO_TCP {
			continue
		}
		header := int(buf[0]&0x0f) * 4
		if header < 20 || n < header+4 {
			continue
		}
		p := make([]byte, n)
		copy(p, buf[:n])
		var src [4]byte
		copy(src[:], p[12:16])
		copy(p[12:16], p[16:20])
		copy(p[16:20], src[:])
		if int(p[header])<<8|int(p[header+1]) == to.senderPort {
			to.path.Down(p, deliver)
		} else {
			to.path.Up(p, deliver)
		}
	}
}

// runAgainstKernelCUBIC runs one leaf of arm, with its two downloads, beside
// two kernel CUBIC connections sending as fast as they are let.
func runAgainstKernelCUBIC(t *testing.T, arm dcArm, mirror *atomic.Pointer[qpMirror]) qpResult {
	t.Helper()
	bed := newDCBed(t, arm)
	defer bed.close()
	leg := qpLeg()
	link := pathsim.NewBottleneck(leg)
	leaf := bed.leafAs(1, arm, func(listen, target string) (*pathsim.Relay, error) { return link.Attach(listen, target, leg) })

	listener, err := net.Listen("tcp4", net.JoinHostPort(qpTunLocal, "0"))
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	port := listener.Addr().(*net.TCPAddr).Port
	path := link.AttachPackets(leg)
	mirror.Store(&qpMirror{path: path, senderPort: port})
	defer func() {
		mirror.Store(nil)
		path.Close()
	}()

	stop := make(chan struct{})
	var wg sync.WaitGroup
	var got [2]atomic.Int64
	var conns []net.Conn
	defer func() {
		for _, conn := range conns {
			_ = conn.Close()
		}
		wg.Wait()
	}()
	chunk := make([]byte, 256<<10)
	for range qpFlowsPerSender {
		// The receiver dials; the handshake crosses the emulated link.
		receiver, err := (&net.Dialer{Timeout: 10 * time.Second}).Dial("tcp4", net.JoinHostPort(qpTunMirror, strconv.Itoa(port)))
		if err != nil {
			t.Fatalf("kernel TCP across the emulated link: %v", err)
		}
		conns = append(conns, receiver)
		sender, err := listener.Accept()
		if err != nil {
			t.Fatal(err)
		}
		conns = append(conns, sender)
		if name, err := congestionControlOf(sender.(*net.TCPConn)); err != nil || name != "cubic" {
			t.Fatalf("the kernel sender runs %q, not cubic (%v)", name, err)
		}
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, _ = io.Copy(countingWriter{&got[1]}, receiver)
		}()
		// The sender writes as fast as it is let until it is closed.
		go func() {
			defer wg.Done()
			for {
				if _, err := sender.Write(chunk); err != nil {
					return
				}
			}
		}()
	}
	for range qpFlowsPerSender {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = leaf.download(stop, &got[0])
		}()
	}
	res := measureQueuePressure(link, &got, [2]string{arm.name, dcKernelCubicName}, qpCubicWindow)
	close(stop)
	return res
}

// congestionControlOf sets conn to CUBIC and returns what the kernel then
// reports it is running.
func congestionControlOf(conn *net.TCPConn) (string, error) {
	raw, err := conn.SyscallConn()
	if err != nil {
		return "", err
	}
	var name string
	var sockErr error
	if err := raw.Control(func(fd uintptr) {
		if sockErr = unix.SetsockoptString(int(fd), unix.IPPROTO_TCP, unix.TCP_CONGESTION, "cubic"); sockErr != nil {
			return
		}
		name, sockErr = unix.GetsockoptString(int(fd), unix.IPPROTO_TCP, unix.TCP_CONGESTION)
	}); err != nil {
		return "", err
	}
	return strings.TrimRight(name, "\x00"), sockErr
}

type countingWriter struct{ n *atomic.Int64 }

func (w countingWriter) Write(p []byte) (int, error) {
	w.n.Add(int64(len(p)))
	return len(p), nil
}
