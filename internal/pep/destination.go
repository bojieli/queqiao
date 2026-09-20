package pep

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strconv"
	"time"
)

type DestinationPolicy struct {
	AllowPrivate bool
	DialTimeout  time.Duration
}

var additionallyForbidden = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("192.88.99.0/24"),
	netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"),
	netip.MustParsePrefix("240.0.0.0/4"),
	// IPv4-translation prefixes can encode a private IPv4 destination in an
	// address that netip classifies as global IPv6. Reject them at this layer
	// instead of relying on every NAT64 or 6to4 relay to enforce RFC filtering.
	netip.MustParsePrefix("64:ff9b::/96"),
	netip.MustParsePrefix("64:ff9b:1::/48"),
	netip.MustParsePrefix("2002::/16"),
	// Current IANA non-global special-purpose ranges that otherwise satisfy
	// IsGlobalUnicast. Loopback, ULA, link-local, and multicast are handled by
	// the standard netip predicates below.
	netip.MustParsePrefix("100::/64"),
	netip.MustParsePrefix("100:0:0:1::/64"),
	netip.MustParsePrefix("2001:2::/48"),
	netip.MustParsePrefix("2001:10::/28"),
	netip.MustParsePrefix("2001:db8::/32"),
	netip.MustParsePrefix("3fff::/20"),
	netip.MustParsePrefix("5f00::/16"),
}

func (p DestinationPolicy) DialContext(ctx context.Context, destination string) (net.Conn, error) {
	host, port, err := parseDestination(destination)
	if err != nil {
		return nil, err
	}
	timeout := p.DialTimeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var addresses []net.IPAddr
	if parsed := net.ParseIP(host); parsed != nil {
		addresses = []net.IPAddr{{IP: parsed}}
	} else {
		addresses, err = net.DefaultResolver.LookupIPAddr(ctx, host)
		if err != nil {
			return nil, errors.New("destination resolution failed")
		}
	}
	if len(addresses) == 0 {
		return nil, errors.New("destination did not resolve")
	}

	dialer := net.Dialer{Timeout: timeout, KeepAlive: 30 * time.Second}
	var lastErr error
	var candidates []string
	for _, candidate := range addresses {
		if !p.AllowPrivate && !publicDestinationIP(candidate.IP) {
			lastErr = errors.New("destination address is not public")
			continue
		}
		candidates = append(candidates, net.JoinHostPort(candidate.IP.String(), strconv.Itoa(port)))
	}
	if len(candidates) == 0 {
		return nil, lastErr
	}
	return dialDestinationCandidates(ctx, candidates, func(ctx context.Context, address string) (net.Conn, error) {
		return dialer.DialContext(ctx, "tcp", address)
	})
}

func dialDestinationCandidates(ctx context.Context, addresses []string, dial func(context.Context, string) (net.Conn, error)) (net.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(addresses) < 2 {
		return dialDestinationSerial(ctx, addresses, dial)
	}
	// The addresses have already passed policy validation. Race only concrete
	// addresses so a second DNS lookup cannot rebind a public name to a private
	// destination. Two staggered workers bound sockets per OPEN; a healthy
	// first address costs no speculative connection.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	type result struct {
		conn net.Conn
		err  error
	}
	results := make(chan result)
	start := func(offset int) {
		candidates := make([]string, 0, (len(addresses)+1)/2)
		for i := offset; i < len(addresses); i += 2 {
			candidates = append(candidates, addresses[i])
		}
		go func() {
			conn, err := dialDestinationSerial(ctx, candidates, dial)
			select {
			case results <- result{conn, err}:
			case <-ctx.Done():
				if conn != nil {
					_ = conn.Close()
				}
			}
		}()
	}
	start(0)
	delay := 250 * time.Millisecond
	if deadline, ok := ctx.Deadline(); ok {
		delay = min(delay, max(0, time.Until(deadline)/2))
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	timerC := timer.C
	active := 1
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timerC:
			timerC = nil
			active++
			start(1)
		case r := <-results:
			if r.err == nil {
				return r.conn, nil
			}
			active--
			if timerC != nil {
				// An exhausted worker should not wait for the stagger timer.
				timerC = nil
				timer.Stop()
				active++
				start(1)
			}
			if active == 0 {
				return nil, r.err
			}
		}
	}
}

func dialDestinationSerial(ctx context.Context, addresses []string, dial func(context.Context, string) (net.Conn, error)) (net.Conn, error) {
	var lastErr error
	for i, address := range addresses {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		attemptCtx := ctx
		cancel := func() {}
		if deadline, ok := ctx.Deadline(); ok && i+1 < len(addresses) {
			// Reserve time for later addresses instead of letting a blackholed
			// candidate consume the whole flow-open deadline.
			attemptCtx, cancel = context.WithTimeout(ctx, time.Until(deadline)/time.Duration(len(addresses)-i))
		}
		conn, dialErr := dial(attemptCtx, address)
		cancel()
		if dialErr == nil {
			return conn, nil
		}
		lastErr = dialErr
	}
	if lastErr == nil {
		lastErr = errors.New("destination unavailable")
	}
	return nil, fmt.Errorf("destination unavailable: %w", lastErr)
}

// ResolveUDPAddr validates and resolves a destination using exactly the same
// public-address policy as TCP CONNECT. It deliberately returns a concrete
// address: the server performs DNS resolution at the US egress and does not
// let the client influence a later DNS rebinding or private-address hop.
func (p DestinationPolicy) ResolveUDPAddr(ctx context.Context, destination string) ([]*net.UDPAddr, error) {
	host, port, err := parseDestination(destination)
	if err != nil {
		return nil, err
	}
	timeout := p.DialTimeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	resolveCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var addresses []net.IPAddr
	if parsed := net.ParseIP(host); parsed != nil {
		addresses = []net.IPAddr{{IP: parsed}}
	} else {
		addresses, err = net.DefaultResolver.LookupIPAddr(resolveCtx, host)
		if err != nil {
			return nil, errors.New("destination resolution failed")
		}
	}
	result := make([]*net.UDPAddr, 0, len(addresses))
	for _, candidate := range addresses {
		if !p.AllowPrivate && !publicDestinationIP(candidate.IP) {
			continue
		}
		result = append(result, &net.UDPAddr{IP: append(net.IP(nil), candidate.IP...), Port: port})
	}
	if len(result) == 0 {
		return nil, errors.New("destination address is not public or did not resolve")
	}
	return result, nil
}

func parseDestination(destination string) (string, int, error) {
	host, portText, err := net.SplitHostPort(destination)
	if err != nil || host == "" {
		return "", 0, errors.New("invalid destination")
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 {
		return "", 0, errors.New("invalid destination port")
	}
	return host, port, nil
}

func publicDestinationIP(ip net.IP) bool {
	addr, ok := netip.AddrFromSlice(ip)
	if !ok {
		return false
	}
	addr = addr.Unmap()
	if !addr.IsGlobalUnicast() || addr.IsPrivate() || addr.IsLoopback() || addr.IsLinkLocalUnicast() || addr.IsLinkLocalMulticast() || addr.IsMulticast() || addr.IsUnspecified() {
		return false
	}
	for _, prefix := range additionallyForbidden {
		if prefix.Contains(addr) {
			return false
		}
	}
	return true
}
