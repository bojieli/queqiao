package main

import (
	"context"
	"errors"
	"io"
	"net"
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

// relayClockSlack is how much earlier than its timeout a test may see the
// relay close a connection: the relay stamps a byte when it reads it, a
// moment before the test's own clock reading for the same byte.
const relayClockSlack = 50 * time.Millisecond

// relayedEcho starts an echo server and a TCP relay in front of it with the
// given idle timeout, and returns a connection through the relay.
func relayedEcho(t *testing.T, idle time.Duration) net.Conn {
	t.Helper()
	echo, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = echo.Close() })
	go func() {
		for {
			conn, err := echo.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				_, _ = io.Copy(conn, conn)
			}()
		}
	}()
	front, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() { cancel(); _ = front.Close() })
	go func() { _ = serveRelayTCP(ctx, front, echo.Addr().String(), 3*time.Second, idle, 16) }()
	conn, err := net.Dial("tcp", front.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// A TCP connection through the relay that keeps carrying bytes outlives
// --idle-timeout, several times over; it used to be cut at that age however
// busy it was (#253). Once it goes quiet it is closed one timeout later, and
// not before.
func TestRelayTCPIdleTimeoutIsResetByTraffic(t *testing.T) {
	const idle = time.Second
	conn := relayedEcho(t, idle)
	opened := time.Now()
	msg := []byte("still here")
	buf := make([]byte, len(msg))
	var last time.Time
	for time.Since(opened) < 7*idle/2 {
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		if _, err := conn.Write(msg); err != nil {
			t.Fatalf("a busy connection was cut %v after it opened: %v", time.Since(opened), err)
		}
		if _, err := io.ReadFull(conn, buf); err != nil {
			t.Fatalf("a busy connection was cut %v after it opened: %v", time.Since(opened), err)
		}
		last = time.Now()
		time.Sleep(idle / 5)
	}
	// Quiet from here: the relay closes it, one idle timeout after the last
	// byte.
	_ = conn.SetReadDeadline(time.Now().Add(idle + 5*time.Second))
	n, err := conn.Read(buf)
	quiet := time.Since(last)
	if err == nil || n != 0 {
		t.Fatalf("read %d bytes (%v) from a connection that should have been closed for idleness", n, err)
	}
	var timeout net.Error
	if errors.As(err, &timeout) && timeout.Timeout() {
		t.Fatalf("an idle connection was still open %v after its last byte", quiet)
	}
	if quiet < idle-relayClockSlack {
		t.Fatalf("closed %v after its last byte, before the %v idle timeout", quiet, idle)
	}
}

// One direction is enough: a connection that only receives (a download) is
// as busy as one that only sends.
func TestRelayTCPIdleTimeoutCountsEitherDirection(t *testing.T) {
	const idle = time.Second
	sink, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()
	stop := make(chan struct{})
	defer close(stop)
	// The upstream sends without ever being sent anything.
	go func() {
		conn, err := sink.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			select {
			case <-stop:
				return
			case <-time.After(idle / 5):
			}
			if _, err := conn.Write([]byte("x")); err != nil {
				return
			}
		}
	}()
	front, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer func() { cancel(); _ = front.Close() }()
	go func() { _ = serveRelayTCP(ctx, front, sink.Addr().String(), 3*time.Second, idle, 16) }()
	conn, err := net.Dial("tcp", front.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	opened := time.Now()
	buf := make([]byte, 16)
	for time.Since(opened) < 3*idle {
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		if _, err := conn.Read(buf); err != nil {
			t.Fatalf("a connection that was receiving was cut %v after it opened: %v", time.Since(opened), err)
		}
	}
}

// A connection that never carries a byte is closed at the idle timeout.
func TestRelayTCPClosesAnIdleConnection(t *testing.T) {
	const idle = 500 * time.Millisecond
	conn := relayedEcho(t, idle)
	opened := time.Now()
	_ = conn.SetReadDeadline(time.Now().Add(idle + 5*time.Second))
	n, err := conn.Read(make([]byte, 1))
	age := time.Since(opened)
	var timeout net.Error
	if err == nil || n != 0 || (errors.As(err, &timeout) && timeout.Timeout()) {
		t.Fatalf("an idle connection was still open after %v (read %d, %v)", age, n, err)
	}
	if age < idle-relayClockSlack {
		t.Fatalf("an idle connection was closed after %v, before the %v idle timeout", age, idle)
	}
}
