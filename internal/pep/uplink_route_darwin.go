package pep

import (
	"context"
	"net"
	"net/netip"
	"os"
	"sort"
	"strings"
	"syscall"

	"golang.org/x/net/route"
)

// The socket is bound to one address family. Churn in the other family
// neither changes that socket's route nor invalidates its live connections.
func sameUplinkFamily(source string, address route.Addr) bool {
	ip, err := netip.ParseAddr(source)
	if err != nil {
		return true
	}
	switch address.(type) {
	case *route.Inet4Addr:
		return ip.Unmap().Is4()
	case *route.Inet6Addr:
		return ip.Is6() && !ip.Is4In6()
	default:
		return false
	}
}

func uplinkAddressChanged(m *route.InterfaceAddrMessage, source string) bool {
	return len(m.Addrs) > syscall.RTAX_IFA && sameUplinkFamily(source, m.Addrs[syscall.RTAX_IFA])
}

func defaultRouteGateway(m *route.RouteMessage, source string) string {
	if m.Err != nil || len(m.Addrs) <= syscall.RTAX_GATEWAY {
		return ""
	}
	var zero bool
	switch a := m.Addrs[syscall.RTAX_DST].(type) {
	case *route.Inet4Addr:
		zero = a.IP == [4]byte{}
	case *route.Inet6Addr:
		zero = a.IP == [16]byte{}
	}
	if !zero || !sameUplinkFamily(source, m.Addrs[syscall.RTAX_DST]) {
		return ""
	}
	switch a := m.Addrs[syscall.RTAX_GATEWAY].(type) {
	case *route.Inet4Addr:
		return net.IP(a.IP[:]).String()
	case *route.Inet6Addr:
		return net.IP(a.IP[:]).String()
	}
	return ""
}

func uplinkGateway(index int, source string) string {
	rib, err := route.FetchRIB(syscall.AF_UNSPEC, route.RIBTypeRoute, 0)
	if err != nil {
		return ""
	}
	messages, err := route.ParseRIB(route.RIBTypeRoute, rib)
	if err != nil {
		return ""
	}
	var gateways []string
	seen := make(map[string]bool)
	for _, message := range messages {
		if m, ok := message.(*route.RouteMessage); ok && m.Index == index {
			if gateway := defaultRouteGateway(m, source); gateway != "" && !seen[gateway] {
				seen[gateway] = true
				gateways = append(gateways, gateway)
			}
		}
	}
	sort.Strings(gateways)
	return strings.Join(gateways, ",")
}

// Listen only to the physical uplink. TUN route and address churn must not
// invalidate the connection which keeps that TUN alive.
func (c *Client) uplinkEvents(ctx context.Context) <-chan struct{} {
	fd, err := syscall.Socket(syscall.AF_ROUTE, syscall.SOCK_RAW, 0)
	if err != nil {
		return nil
	}
	if err = syscall.SetNonblock(fd, true); err != nil {
		_ = syscall.Close(fd) // Best-effort cleanup after initialization failed.
		return nil
	}
	file := os.NewFile(uintptr(fd), "queqiao-uplink-events")
	events := make(chan struct{}, 1)
	go func() {
		defer file.Close()
		stop := context.AfterFunc(ctx, func() { _ = file.Close() })
		defer stop()
		buffer := make([]byte, 64*1024)
		previous := c.uplinkInterface()
		flags := make(map[int]int)
		for {
			n, err := file.Read(buffer)
			if err != nil {
				return
			}
			messages, err := route.ParseRIB(route.RIBTypeRoute, buffer[:n])
			if err != nil {
				continue
			}
			current := c.uplinkInterface()
			source, _ := c.currentUplinkState()
			matches := func(index int) bool {
				return current != nil && current.Index == index || previous != nil && previous.Index == index
			}
			for _, message := range messages {
				relevant := false
				switch m := message.(type) {
				case *route.InterfaceMessage:
					if matches(m.Index) {
						old, known := flags[m.Index]
						relevant = !known || old != m.Flags
						flags[m.Index] = m.Flags
					}
				case *route.InterfaceAddrMessage:
					relevant = matches(m.Index) && uplinkAddressChanged(m, source)
				case *route.RouteMessage:
					relevant = matches(m.Index) && (m.Type == syscall.RTM_ADD || m.Type == syscall.RTM_DELETE || m.Type == syscall.RTM_CHANGE) && defaultRouteGateway(m, source) != "" && (c.cfg.LocalAddress == "" || m.Flags&syscall.RTF_IFSCOPE != 0)
				}
				if relevant {
					notifyActivity(events)
				}
			}
			if current != nil {
				previous = current
			}
		}
	}()
	return events
}
