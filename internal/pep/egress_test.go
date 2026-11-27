package pep

import "testing"

func TestParseEgressCapacity(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want EgressCapacity
		bad  bool
	}{
		{"", EgressCapacity{}, false},
		{"auto", EgressCapacity{}, false},
		{"off", EgressCapacity{Off: true}, false},
		{"1g", EgressCapacity{BytesPerSecond: 125_000_000}, false},
		{"940m", EgressCapacity{BytesPerSecond: 117_500_000}, false},
		{"500Mbit", EgressCapacity{BytesPerSecond: 62_500_000}, false},
		{"2.5gbps", EgressCapacity{BytesPerSecond: 312_500_000}, false},
		{"8000000", EgressCapacity{BytesPerSecond: 1_000_000}, false},
		{"fast", EgressCapacity{}, true},
		{"-1g", EgressCapacity{}, true},
		{"0", EgressCapacity{}, true},
	} {
		got, err := ParseEgressCapacity(tc.in)
		if (err != nil) != tc.bad || got != tc.want {
			t.Errorf("ParseEgressCapacity(%q) = %+v, %v; want %+v (bad=%v)", tc.in, got, err, tc.want, tc.bad)
		}
	}
}

func TestEgressCapacityNeedsPathScope(t *testing.T) {
	rate := EgressCapacity{BytesPerSecond: 1}
	if err := ValidateEgressCapacity(rate, CongestionScopeConnection); err == nil {
		t.Error("a configured rate was accepted without --congestion-scope=path")
	}
	if err := ValidateEgressCapacity(rate, CongestionScopePath); err != nil {
		t.Error(err)
	}
	for _, c := range []EgressCapacity{{}, {Off: true}} {
		if err := ValidateEgressCapacity(c, CongestionScopeConnection); err != nil {
			t.Errorf("%+v refused under connection scope: %v", c, err)
		}
	}
}

func TestParseLinkSpeed(t *testing.T) {
	for in, want := range map[string]uint64{"1000\n": 125_000_000, "10000": 1_250_000_000, "-1\n": 0, "": 0, "unknown": 0, "0": 0} {
		if got := parseLinkSpeed(in); got != want {
			t.Errorf("parseLinkSpeed(%q) = %d, want %d", in, got, want)
		}
	}
}

func TestLoopbackAddressesShareOneEgressGroup(t *testing.T) {
	a, b := interfaceOf("127.0.0.41"), interfaceOf("127.0.0.42")
	if a != b || a == "addr:127.0.0.41" {
		t.Fatalf("loopback addresses are in groups %q and %q, want one interface group", a, b)
	}
}

func TestEgressRegistryKeepsOneGroupPerInterfaceAndHonoursOff(t *testing.T) {
	g := egressGroups.group("127.0.0.7", EgressCapacity{})
	if g == nil || g != egressGroups.group("127.0.0.8", EgressCapacity{}) {
		t.Fatal("two loopback addresses did not share one group")
	}
	if egressGroups.group("127.0.0.7", EgressCapacity{Off: true}) != nil {
		t.Fatal("off still returned a group")
	}
	egressGroups.forget()
}
