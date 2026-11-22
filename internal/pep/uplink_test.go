package pep

import (
	"errors"
	"testing"
	"time"
)

func TestSuspendBoundaryUsesTransportIdleBudget(t *testing.T) {
	var state suspendWatchState
	if state.observe(time.Hour, nil) {
		t.Fatal("initial clock reading reset a new client")
	}
	if state.observe(time.Hour+laneDeadPathDetection-time.Second, nil) {
		t.Fatal("short sleep reset a live transport")
	}
	if state.observe(0, errors.New("clock unavailable")) || state.observe(0, nil) {
		t.Fatal("failed or regressed clock observation reset the pool")
	}
	if !state.observe(time.Hour+2*laneDeadPathDetection-time.Second, nil) {
		t.Fatal("sleep crossing the transport idle budget was missed")
	}
	if state.observe(time.Hour+2*laneDeadPathDetection-time.Second, nil) {
		t.Fatal("same resume reset the pool twice")
	}
}

func TestUplinkReconnectWithTheSameAddressStartsANewPath(t *testing.T) {
	state := uplinkWatchState{known: "192.0.2.10"}

	if _, changed := state.observe("", true); changed {
		t.Fatal("losing the address acted before a replacement path was usable")
	}
	from, changed := state.observe("192.0.2.10", false)
	if !changed || from != "192.0.2.10" {
		t.Fatalf("same-address reconnect = changed %v from %q, want a new path", changed, from)
	}
	if _, changed := state.observe("192.0.2.10", false); changed {
		t.Fatal("stable address changed the path twice")
	}
}

func TestUplinkAddressChangeStartsANewPath(t *testing.T) {
	state := uplinkWatchState{known: "192.0.2.10"}
	from, changed := state.observe("198.51.100.20", false)
	if !changed || from != "192.0.2.10" || state.known != "198.51.100.20" {
		t.Fatalf("address change = changed %v from %q known %q", changed, from, state.known)
	}
}

func TestInconclusiveUplinkProbeDoesNotChurnAStablePath(t *testing.T) {
	state := uplinkWatchState{known: "192.0.2.10"}
	if _, changed := state.observe("", false); changed {
		t.Fatal("inconclusive observation changed the path")
	}
	if _, changed := state.observe("192.0.2.10", false); changed {
		t.Fatal("one inconclusive probe made the same address look new")
	}
}

func TestInitiallyUnavailableUplinkWarmsWhenItAppears(t *testing.T) {
	state := uplinkWatchState{}
	if _, changed := state.observe("", true); changed {
		t.Fatal("unavailable initial path changed before it became usable")
	}
	if _, changed := state.observe("192.0.2.10", false); !changed {
		t.Fatal("first usable path was not detected")
	}
}

func TestOnlyConfiguredDynamicBindingMakesAnEmptyProbeDefinitive(t *testing.T) {
	dynamic := &Client{cfg: ClientConfig{LocalAddress: "if:queqiao-interface-that-does-not-exist"}}
	if address, unavailable := dynamic.currentUplinkState(); address != "" || !unavailable {
		t.Fatalf("missing configured interface = address %q unavailable %v", address, unavailable)
	}

	inconclusive := &Client{cfg: ClientConfig{RemoteAddr: "127.0.0.1:not-a-port"}}
	if address, unavailable := inconclusive.currentUplinkState(); address != "" || unavailable {
		t.Fatalf("failed unbound probe = address %q unavailable %v", address, unavailable)
	}
}
