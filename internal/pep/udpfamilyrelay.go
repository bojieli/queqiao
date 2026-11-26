package pep

import (
	"errors"
	"net"
	"os"
	"sync"
	"time"
)

// familyUDPRelay is the gateway's direct UDP relay socket for one
// association, as one socket per address family rather than one dual-stack
// socket (#197).
//
// On macOS the kernel can give a dual-stack wildcard socket an ephemeral port
// that an IPv4 socket elsewhere on the host already holds, and the older IPv4
// socket then receives every datagram sent to that port: the relay never sees
// the destination's answers. A udp4 socket is allocated from the same table as
// those IPv4 sockets, so it cannot be given a port one of them holds, and a
// udp6 socket is IPv6-only, so IPv4 datagrams never reach it. Linux checks
// both families when it picks a dual-stack port, so it never had the problem;
// the relay is the same on every platform so that one implementation is
// tested everywhere.
//
// One association can send to IPv4 and IPv6 destinations alike, so the relay
// sends each datagram on the socket of its destination's family. The IPv4
// socket is opened with the relay, which keeps the open-time failure and gives
// LocalAddr an address; the IPv6 socket is opened on the first IPv6
// destination, so a relay that only ever sees IPv4 stays on udp4. A reader
// goroutine per socket hands datagrams to ReadFromUDP, which applies the
// relay's read deadline itself; a datagram read but not yet taken stays in the
// relay, so a relay retained for a replacement association loses nothing.
type familyUDPRelay struct {
	// listen opens a socket of one family; a test may replace it.
	listen func(network string) (*net.UDPConn, error)

	mu    sync.Mutex
	conns [2]*net.UDPConn // familyIPv4, familyIPv6
	// readDeadline is ReadFromUDP's deadline; deadlineChanged is closed and
	// replaced whenever it changes, to wake a waiting read.
	readDeadline    time.Time
	deadlineChanged chan struct{}
	writeDeadline   time.Time
	closed          bool

	reads chan relayRead
	done  chan struct{}
}

const (
	familyIPv4 = iota
	familyIPv6
)

// relayRead is one datagram, or read error, from a family's socket. The
// reader waits on release before it reuses buf.
type relayRead struct {
	buf     []byte
	addr    *net.UDPAddr
	err     error
	release chan struct{}
}

func listenUDPFamily(network string) (*net.UDPConn, error) {
	if network == "udp4" {
		return net.ListenUDP(network, &net.UDPAddr{IP: net.IPv4zero})
	}
	return net.ListenUDP(network, &net.UDPAddr{IP: net.IPv6unspecified})
}

// openFamilyUDPRelay opens a relay with its IPv4 socket, or, on a host that
// cannot open one, its IPv6 socket.
func openFamilyUDPRelay(listen func(network string) (*net.UDPConn, error)) (*familyUDPRelay, error) {
	if listen == nil {
		listen = listenUDPFamily
	}
	r := &familyUDPRelay{
		listen:          listen,
		deadlineChanged: make(chan struct{}),
		reads:           make(chan relayRead),
		done:            make(chan struct{}),
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, err := r.connLocked(familyIPv4); err != nil {
		if _, err6 := r.connLocked(familyIPv6); err6 != nil {
			return nil, errors.Join(err, err6)
		}
	}
	return r, nil
}

func udpFamily(ip net.IP) int {
	if ip.To4() != nil {
		return familyIPv4
	}
	return familyIPv6
}

// connLocked returns the socket of a family, opening it on first use.
func (r *familyUDPRelay) connLocked(family int) (*net.UDPConn, error) {
	if r.closed {
		return nil, net.ErrClosed
	}
	if conn := r.conns[family]; conn != nil {
		return conn, nil
	}
	network := "udp4"
	if family == familyIPv6 {
		network = "udp6"
	}
	conn, err := r.listen(network)
	if err != nil {
		return nil, err
	}
	if !r.writeDeadline.IsZero() {
		_ = conn.SetWriteDeadline(r.writeDeadline)
	}
	r.conns[family] = conn
	go r.readLoop(conn)
	return conn, nil
}

func (r *familyUDPRelay) readLoop(conn *net.UDPConn) {
	buf := make([]byte, 65535)
	release := make(chan struct{}, 1)
	for {
		n, addr, err := conn.ReadFromUDP(buf)
		if err != nil && errors.Is(err, net.ErrClosed) {
			return
		}
		select {
		case r.reads <- relayRead{buf: buf[:n], addr: addr, err: err, release: release}:
		case <-r.done:
			return
		}
		select {
		case <-release:
		case <-r.done:
			return
		}
	}
}

// LocalAddr is the IPv4 socket's address, or the IPv6 socket's on a host
// without IPv4.
func (r *familyUDPRelay) LocalAddr() net.Addr {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, conn := range r.conns {
		if conn != nil {
			return conn.LocalAddr()
		}
	}
	return &net.UDPAddr{}
}

func (r *familyUDPRelay) SetReadDeadline(deadline time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return net.ErrClosed
	}
	r.readDeadline = deadline
	close(r.deadlineChanged)
	r.deadlineChanged = make(chan struct{})
	return nil
}

func (r *familyUDPRelay) SetWriteDeadline(deadline time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return net.ErrClosed
	}
	r.writeDeadline = deadline
	for _, conn := range r.conns {
		if conn != nil {
			_ = conn.SetWriteDeadline(deadline)
		}
	}
	return nil
}

// ReadFromUDP returns the next datagram from either family's socket, or a
// timeout error at the read deadline.
func (r *familyUDPRelay) ReadFromUDP(p []byte) (int, *net.UDPAddr, error) {
	for {
		r.mu.Lock()
		deadline, changed, closed := r.readDeadline, r.deadlineChanged, r.closed
		r.mu.Unlock()
		if closed {
			return 0, nil, &net.OpError{Op: "read", Net: "udp", Err: net.ErrClosed}
		}
		var (
			timer   *time.Timer
			expired <-chan time.Time
		)
		if !deadline.IsZero() {
			wait := time.Until(deadline)
			if wait <= 0 {
				return 0, nil, &net.OpError{Op: "read", Net: "udp", Err: os.ErrDeadlineExceeded}
			}
			timer = time.NewTimer(wait)
			expired = timer.C
		}
		stop := func() {
			if timer != nil {
				timer.Stop()
			}
		}
		select {
		case read := <-r.reads:
			stop()
			n := copy(p, read.buf)
			addr, err := read.addr, read.err
			// Buffered: the reader may already have stopped on Close.
			read.release <- struct{}{}
			return n, addr, err
		case <-expired:
			return 0, nil, &net.OpError{Op: "read", Net: "udp", Err: os.ErrDeadlineExceeded}
		case <-changed:
			stop()
		case <-r.done:
			stop()
			return 0, nil, &net.OpError{Op: "read", Net: "udp", Err: net.ErrClosed}
		}
	}
}

// WriteToUDP sends on the socket of the destination's family, opening it on
// first use.
func (r *familyUDPRelay) WriteToUDP(p []byte, addr *net.UDPAddr) (int, error) {
	if addr == nil {
		return 0, &net.OpError{Op: "write", Net: "udp", Err: errors.New("missing address")}
	}
	r.mu.Lock()
	conn, err := r.connLocked(udpFamily(addr.IP))
	r.mu.Unlock()
	if err != nil {
		return 0, &net.OpError{Op: "write", Net: "udp", Addr: addr, Err: err}
	}
	return conn.WriteToUDP(p, addr)
}

func (r *familyUDPRelay) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	r.closed = true
	close(r.done)
	var errs []error
	for _, conn := range r.conns {
		if conn != nil {
			errs = append(errs, conn.Close())
		}
	}
	return errors.Join(errs...)
}
