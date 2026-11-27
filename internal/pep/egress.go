package pep

import (
	"fmt"
	"math"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	wancongestion "github.com/bojieli/queqiao/internal/congestion"
)

// EgressCapacity is a gateway's --egress-capacity (docs/DESIGN-EGRESS-JOINT-LIMIT.md
// §3): the ceiling on what its path authorities send out of one network
// interface together. The zero value is auto: the interface's link speed with
// a margin where it can be read, learned from the aggregate otherwise.
type EgressCapacity struct {
	// Off disables the joint limit.
	Off bool
	// BytesPerSecond is the configured ceiling, zero when none.
	BytesPerSecond uint64
}

// ParseEgressCapacity reads "auto" (or empty), "off", or a rate in bits per
// second with an optional k, m or g suffix (and an optional "bit" or "bps").
func ParseEgressCapacity(s string) (EgressCapacity, error) {
	switch v := strings.ToLower(strings.TrimSpace(s)); v {
	case "", "auto":
		return EgressCapacity{}, nil
	case "off", "none":
		return EgressCapacity{Off: true}, nil
	default:
		v = strings.TrimSuffix(strings.TrimSuffix(strings.TrimSuffix(v, "ps"), "it"), "b")
		mult := 1.0
		switch {
		case strings.HasSuffix(v, "k"):
			mult, v = 1e3, strings.TrimSuffix(v, "k")
		case strings.HasSuffix(v, "m"):
			mult, v = 1e6, strings.TrimSuffix(v, "m")
		case strings.HasSuffix(v, "g"):
			mult, v = 1e9, strings.TrimSuffix(v, "g")
		}
		n, err := strconv.ParseFloat(v, 64)
		if err != nil || n <= 0 || math.IsInf(n, 0) || math.IsNaN(n) || n*mult/8 > float64(math.MaxInt64) {
			return EgressCapacity{}, fmt.Errorf("--egress-capacity %q: want auto, off, or a rate such as 1g or 940m", s)
		}
		return EgressCapacity{BytesPerSecond: uint64(n * mult / 8)}, nil
	}
}

// validateEgressCapacity refuses a configured rate that nothing would apply:
// the joint limit is part of congestion-scope=path on a gateway.
func validateEgressCapacity(c EgressCapacity, scope CongestionScope) error {
	if c.BytesPerSecond > 0 && scope != CongestionScopePath {
		return fmt.Errorf("--egress-capacity with a rate requires --congestion-scope=%s", CongestionScopePath)
	}
	return nil
}

// ValidateEgressCapacity is validateEgressCapacity for the command line.
func ValidateEgressCapacity(c EgressCapacity, scope CongestionScope) error {
	return validateEgressCapacity(c, scope)
}

// egressGroups holds the gateway's joint limits, one per egress interface.
var egressGroups = &egressRegistry{groups: make(map[string]*wancongestion.EgressLimit)}

type egressRegistry struct {
	mu     sync.Mutex
	groups map[string]*wancongestion.EgressLimit
}

// group returns the joint limit of the interface that owns the source address
// egressHost, creating it on first use. The first configuration to reach an
// interface is the one it keeps.
func (r *egressRegistry) group(egressHost string, c EgressCapacity) *wancongestion.EgressLimit {
	if c.Off {
		return nil
	}
	name := interfaceOf(egressHost)
	r.mu.Lock()
	defer r.mu.Unlock()
	if g, ok := r.groups[name]; ok {
		return g
	}
	cfg := wancongestion.EgressConfig{Configured: c.BytesPerSecond}
	if iface := interfaceName(egressHost); iface != "" {
		cfg.LinkSpeed = linkSpeedBytesPerSecond(iface)
	}
	g := wancongestion.NewEgressLimit(cfg)
	r.groups[name] = g
	return g
}

func (r *egressRegistry) forget() {
	r.mu.Lock()
	clear(r.groups)
	r.mu.Unlock()
}

// interfaceOf names the group of a source address: its interface, or the
// address itself when no interface owns it.
func interfaceOf(host string) string {
	if name := interfaceName(host); name != "" {
		return "if:" + name
	}
	return "addr:" + host
}

// interfaceName is the name of the interface that holds host, or "".
func interfaceName(host string) string {
	ip := net.ParseIP(host)
	if ip == nil {
		return ""
	}
	ifaces, err := net.Interfaces()
	if err != nil {
		return ""
	}
	for _, iface := range ifaces {
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, a := range addrs {
			var got net.IP
			switch v := a.(type) {
			case *net.IPNet:
				got = v.IP
			case *net.IPAddr:
				got = v.IP
			}
			if got != nil && got.Equal(ip) {
				return iface.Name
			}
		}
		if ip.IsLoopback() && iface.Flags&net.FlagLoopback != 0 {
			// Every 127/8 address is the loopback interface's.
			return iface.Name
		}
	}
	return ""
}

// linkSpeedBytesPerSecond reads the interface's speed from sysfs, in bytes per
// second, zero where it cannot be read: not Linux, a virtual or wireless
// interface (the file is absent or holds -1), or a read error.
func linkSpeedBytesPerSecond(iface string) uint64 {
	data, err := os.ReadFile(filepath.Join("/sys/class/net", iface, "speed"))
	if err != nil {
		return 0
	}
	return parseLinkSpeed(string(data))
}

// parseLinkSpeed reads sysfs's speed file, which is in Mbit/s.
func parseLinkSpeed(s string) uint64 {
	mbit, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil || mbit <= 0 {
		return 0
	}
	return uint64(mbit) * 1_000_000 / 8
}
