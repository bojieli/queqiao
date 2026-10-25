package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestNormalizeRelayListen(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"12541", "0.0.0.0:12541"},
		{":12541", "0.0.0.0:12541"},
		{"127.0.0.1:12541", "127.0.0.1:12541"},
		{"[::1]:12541", "[::1]:12541"},
	} {
		got, err := normalizeRelayListen(tc.in)
		if err != nil {
			t.Errorf("%q: %v", tc.in, err)
			continue
		}
		if got != tc.want {
			t.Errorf("%q = %q, want %q", tc.in, got, tc.want)
		}
	}
	for _, bad := range []string{"", "0", "65536", "http", "host:http"} {
		if _, err := normalizeRelayListen(bad); err == nil {
			t.Errorf("listen %q was accepted", bad)
		}
	}
}

func TestRelayEntryValidate(t *testing.T) {
	base := relayEntry{Name: "p", Kind: "queqiao", Listen: "12541", Upstream: "94.74.126.245:12541"}
	if err := base.validate(); err != nil {
		t.Fatalf("valid entry rejected: %v", err)
	}
	// empty kind defaults to queqiao
	e := base
	e.Kind = ""
	if err := e.validate(); err != nil {
		t.Fatalf("empty kind rejected: %v", err)
	}
	if e.Kind != relayKindQueqiao {
		t.Errorf("empty kind defaulted to %q", e.Kind)
	}

	bad := map[string]relayEntry{
		"no name":      {Kind: "queqiao", Listen: "12541", Upstream: "h:1"},
		"bad kind":     {Name: "p", Kind: "udp", Listen: "12541", Upstream: "h:1"},
		"bad listen":   {Name: "p", Kind: "queqiao", Listen: "0", Upstream: "h:1"},
		"bad upstream": {Name: "p", Kind: "queqiao", Listen: "12541", Upstream: "nope"},
		"neg timeout":  {Name: "p", Kind: "queqiao", Listen: "12541", Upstream: "h:1", DialTimeout: duration(-time.Second)},
		"tcp tuning":   {Name: "p", Kind: "tcp", Listen: "12541", Upstream: "h:1", DialTimeout: duration(time.Second)},
	}
	for name, e := range bad {
		if err := e.validate(); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
}

func TestLoadRelayManifest(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "relays.json")

	// Valid manifest: two relays, one queqiao and one tcp.
	valid := `{"version":1,"relays":[
		{"name":"queqiao-cn-a","kind":"queqiao","listen":"12541","upstream":"94.74.126.245:12541","dialTimeout":"3s","idleTimeout":"15m","maxConnections":8192},
		{"name":"vless-cn-a","kind":"tcp","listen":"18443","upstream":"94.74.126.245:18443"}]}`
	if err := os.WriteFile(path, []byte(valid), 0o600); err != nil {
		t.Fatal(err)
	}
	m, err := loadRelayManifest(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Relays) != 2 {
		t.Fatalf("got %d relays, want 2", len(m.Relays))
	}
	if m.Relays[0].DialTimeout.or(time.Second) != 3*time.Second {
		t.Errorf("dialTimeout = %v, want 3s", m.Relays[0].DialTimeout)
	}
	if m.Relays[0].MaxConnections != 8192 {
		t.Errorf("maxConnections = %d, want 8192", m.Relays[0].MaxConnections)
	}

	// Rejections: wrong version, unknown field, duplicate name, empty list.
	for name, body := range map[string]string{
		"wrong version": `{"version":2,"relays":[{"name":"a","listen":"1","upstream":"h:1"}]}`,
		"unknown field": `{"version":1,"oops":true,"relays":[{"name":"a","listen":"1","upstream":"h:1"}]}`,
		"duplicate":     `{"version":1,"relays":[{"name":"a","listen":"1","upstream":"h:1"},{"name":"a","listen":"2","upstream":"h:2"}]}`,
		"empty":         `{"version":1,"relays":[]}`,
	} {
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := loadRelayManifest(path); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
}

func TestRenderRelayUnits(t *testing.T) {
	// queqiao relay -> one hardened .service with the tuning flags.
	q := relayEntry{Name: "queqiao-cn-a", Kind: "queqiao", Listen: "12541", Upstream: "94.74.126.245:12541",
		DialTimeout: duration(3 * time.Second), IdleTimeout: duration(15 * time.Minute), MaxConnections: 8192}
	unit, text := renderRelayUnit("/usr/local/bin/queqiaod", q)
	if unit != "queqiao-cn-a.service" {
		t.Errorf("unit = %q", unit)
	}
	for _, want := range []string{"relay run", "--listen 0.0.0.0:12541", "--upstream 94.74.126.245:12541",
		"--dial-timeout 3s", "--idle-timeout 15m0s", "--max-connections 8192", "DynamicUser=yes", "WantedBy=multi-user.target"} {
		if !strings.Contains(text, want) {
			t.Errorf("queqiao unit missing %q", want)
		}
	}

	// tcp relay -> a .socket + .service pair for systemd-socket-proxyd.
	files := relayUnitFiles("/usr/local/bin/queqiaod", relayEntry{Name: "vless-cn-a", Kind: "tcp", Listen: "18443", Upstream: "94.74.126.245:18443"})
	if _, ok := files["vless-cn-a.socket"]; !ok {
		t.Error("tcp relay missing .socket unit")
	}
	if _, ok := files["vless-cn-a.service"]; !ok {
		t.Error("tcp relay missing .service unit")
	}
	sock := files["vless-cn-a.socket"]
	if !strings.Contains(sock, "ListenStream=0.0.0.0:18443") || !strings.Contains(sock, "NoDelay=true") {
		t.Error("socket unit missing ListenStream/NoDelay")
	}
	svc := files["vless-cn-a.service"]
	if !strings.Contains(svc, "systemd-socket-proxyd 94.74.126.245:18443") {
		t.Error("service unit missing systemd-socket-proxyd upstream")
	}
}
