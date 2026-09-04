package pep

import (
	"context"
	"net"
	"testing"
)

func TestResolveUDPAddrBoundTakesLiteralAddressesWithoutALookup(t *testing.T) {
	// A numbered endpoint needs no resolver at all, and must not be made to
	// depend on one: this is the path every literal-endpoint provider takes.
	for _, remote := range []string{"203.0.113.7:12540", "[2001:db8::1]:12540"} {
		addr, err := resolveUDPAddrBound(context.Background(), remote, "if:nonexistent0", nil)
		if err != nil {
			t.Fatalf("%s: %v", remote, err)
		}
		if got := addr.String(); got != remote {
			t.Errorf("resolved %s to %s", remote, got)
		}
	}
}

func TestResolveUDPAddrBoundReportsAMalformedEndpoint(t *testing.T) {
	if _, err := resolveUDPAddrBound(context.Background(), "no-port-here", "", nil); err == nil {
		t.Error("an endpoint without a port must be reported")
	}
}

func TestResolveUDPAddrBoundReportsAnUnusableLocalAddress(t *testing.T) {
	// A named endpoint needs the bound resolver, so a local-address spec that
	// cannot be resolved has to surface here rather than quietly falling back
	// to an unbound lookup — the fallback is the bug this path exists to close.
	_, err := resolveUDPAddrBound(context.Background(), "gateway.example:12540", "if:nonexistent0", nil)
	if err == nil {
		t.Fatal("an unresolvable local address must be reported")
	}
	if _, ok := err.(*net.DNSError); ok {
		t.Errorf("reported a lookup failure (%v), want the local-address failure", err)
	}
}
