package pep

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"time"

	"github.com/bojieli/queqiao/internal/socks5"
)

// serverUDPRelay is the gateway-side UDP socket as seen by one Queqiao
// association. A direct relay is one kernel socket. A SOCKS5 relay additionally
// owns the TCP control connection whose lifetime defines UDP ASSOCIATE.
type serverUDPRelay interface {
	LocalAddr() net.Addr
	SetReadDeadline(time.Time) error
	SetWriteDeadline(time.Time) error
	ReadFromUDP([]byte) (int, *net.UDPAddr, error)
	WriteToUDP([]byte, *net.UDPAddr) (int, error)
	Close() error
}

type namedUDPRelay interface {
	ReadFromDestination([]byte) (int, string, error)
	WriteToDestination([]byte, string) (int, error)
}

type socks5UDPRelay struct {
	control *net.TCPConn
	conn    *net.UDPConn
	relay   *net.UDPAddr
}

func (r *socks5UDPRelay) LocalAddr() net.Addr { return r.conn.LocalAddr() }
func (r *socks5UDPRelay) SetReadDeadline(deadline time.Time) error {
	return r.conn.SetReadDeadline(deadline)
}
func (r *socks5UDPRelay) SetWriteDeadline(deadline time.Time) error {
	return r.conn.SetWriteDeadline(deadline)
}

func (r *socks5UDPRelay) ReadFromDestination(packet []byte) (int, string, error) {
	for {
		n, source, err := r.conn.ReadFromUDP(packet)
		if err != nil {
			return 0, "", err
		}
		if !udpAddrEqual(source, r.relay) {
			continue
		}
		datagram, err := socks5.ReadUDPDatagram(packet[:n])
		if err != nil {
			continue
		}
		if len(datagram.Payload) > len(packet) {
			return 0, "", errors.New("SOCKS5 UDP response exceeds relay buffer")
		}
		copy(packet, datagram.Payload)
		return len(datagram.Payload), datagram.Destination, nil
	}
}

func (r *socks5UDPRelay) ReadFromUDP(packet []byte) (int, *net.UDPAddr, error) {
	for {
		n, destination, err := r.ReadFromDestination(packet)
		if err != nil {
			return 0, nil, err
		}
		host, port, err := net.SplitHostPort(destination)
		if err != nil {
			continue
		}
		ip := net.ParseIP(host)
		if ip == nil {
			continue
		}
		portNumber, err := strconv.Atoi(port)
		if err != nil || portNumber < 1 || portNumber > 65535 {
			continue
		}
		return n, &net.UDPAddr{IP: ip, Port: portNumber}, nil
	}
}

func (r *socks5UDPRelay) WriteToUDP(payload []byte, destination *net.UDPAddr) (int, error) {
	return r.WriteToDestination(payload, destination.String())
}

func (r *socks5UDPRelay) WriteToDestination(payload []byte, destination string) (int, error) {
	var packet bytes.Buffer
	if err := socks5.WriteUDPDatagram(&packet, destination, payload); err != nil {
		return 0, err
	}
	if _, err := r.conn.WriteToUDP(packet.Bytes(), r.relay); err != nil {
		return 0, err
	}
	return len(payload), nil
}

func (r *socks5UDPRelay) Close() error {
	udpErr := r.conn.Close()
	tcpErr := r.control.Close()
	if udpErr != nil {
		return udpErr
	}
	return tcpErr
}

func dialSOCKS5TCP(ctx context.Context, upstream, destination string, timeout time.Duration) (net.Conn, error) {
	control, _, err := openSOCKS5Control(ctx, upstream, socks5.CommandConnect, destination, timeout)
	if err != nil {
		return nil, err
	}
	return control, nil
}

func openSOCKS5UDPRelay(ctx context.Context, upstream string, timeout time.Duration) (serverUDPRelay, error) {
	controlConn, bound, err := openSOCKS5Control(ctx, upstream, socks5.CommandUDPAssociate, "0.0.0.0:0", timeout)
	if err != nil {
		return nil, err
	}
	control, ok := controlConn.(*net.TCPConn)
	if !ok {
		_ = controlConn.Close()
		return nil, errors.New("SOCKS5 upstream control connection is not TCP")
	}
	host, _, err := net.SplitHostPort(bound)
	if err != nil {
		_ = control.Close()
		return nil, errors.New("SOCKS5 upstream returned an invalid UDP relay address")
	}
	if ip := net.ParseIP(host); ip != nil && ip.IsUnspecified() {
		host, _, err = net.SplitHostPort(control.RemoteAddr().String())
		if err != nil {
			_ = control.Close()
			return nil, errors.New("SOCKS5 upstream has no usable UDP relay address")
		}
		_, port, _ := net.SplitHostPort(bound)
		bound = net.JoinHostPort(host, port)
	}
	relay, err := net.ResolveUDPAddr("udp", bound)
	if err != nil || relay.Port == 0 || relay.IP == nil || !relay.IP.IsLoopback() {
		_ = control.Close()
		return nil, errors.New("loopback SOCKS5 upstream returned a non-loopback or invalid UDP relay address")
	}
	udpConn, err := net.ListenUDP("udp", &net.UDPAddr{})
	if err != nil {
		_ = control.Close()
		return nil, fmt.Errorf("open SOCKS5 UDP socket: %w", err)
	}
	result := &socks5UDPRelay{control: control, conn: udpConn, relay: relay}
	go func() {
		var unexpected [1]byte
		_, _ = io.ReadFull(control, unexpected[:])
		// RFC 1928 defines the TCP connection's lifetime as the UDP
		// association's lifetime. Wake the packet reader if the upstream ends
		// that control connection; Close is safe to repeat during normal cleanup.
		_ = udpConn.Close()
	}()
	return result, nil
}

func openSOCKS5Control(ctx context.Context, upstream string, command byte, destination string, timeout time.Duration) (net.Conn, string, error) {
	dialer := net.Dialer{Timeout: timeout, KeepAlive: 30 * time.Second}
	conn, err := dialer.DialContext(ctx, "tcp", upstream)
	if err != nil {
		return nil, "", fmt.Errorf("connect to SOCKS5 upstream: %w", err)
	}
	deadline := time.Now().Add(timeout)
	if contextDeadline, ok := ctx.Deadline(); ok && contextDeadline.Before(deadline) {
		deadline = contextDeadline
	}
	_ = conn.SetDeadline(deadline)
	stopContextWatch := context.AfterFunc(ctx, func() {
		// DialContext only covers the TCP connection attempt. Once connected,
		// interrupt a SOCKS server that stalls during method negotiation or the
		// request reply. The watcher is stopped before a successful connection
		// is returned, so the caller may cancel its dial context without closing
		// the established destination flow.
		_ = conn.SetDeadline(time.Now())
	})
	bound, err := socks5.ClientRequest(conn, command, destination)
	stopContextWatch()
	if contextErr := ctx.Err(); contextErr != nil {
		_ = conn.Close()
		return nil, "", fmt.Errorf("SOCKS5 upstream handshake: %w", contextErr)
	}
	if err != nil {
		_ = conn.Close()
		return nil, "", err
	}
	_ = conn.SetDeadline(time.Time{})
	return conn, bound, nil
}
