package main

// relay implements a TCP+UDP port forwarder and a systemd installer for it.
//
// A queqiao deployment is often fronted by one or more relays (a "CN" or "HK"
// hop) that forward a local port to a gateway, and each such hop previously
// meant hand-writing a systemd unit with the right flags. This subcommand
// renders, installs, and manages those units so a whole relay host is described
// by a small manifest and stood up with one command.
//
// Two service kinds are supported:
//
//   - "queqiao": a TCP+UDP forwarder (this binary, `relay run`) suitable for
//     the queqiao control channel, which needs both transports.
//   - "tcp": a plain TCP forwarder rendered as a systemd-socket-proxyd unit,
//     suitable for a VLESS/TCP leg that needs no UDP.
//
// Both are socket-independent, hardened systemd services; "tcp" is
// socket-activated (ListenStream) because systemd-socket-proxyd expects a
// passed socket, while "queqiao" is a long-running process that owns both the
// TCP and UDP listeners.

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// relayEntry is one forwarded port. In a manifest a host is described by a list
// of these.
type relayEntry struct {
	// Name is the service unit base name (e.g. "queqiao-cn-proxy-a").
	Name string `json:"name"`
	// Kind is "queqiao" (TCP+UDP, this binary) or "tcp" (TCP,
	// systemd-socket-proxyd).
	Kind string `json:"kind"`
	// Listen is the local port or host:port to bind ("12541" or ":12541").
	Listen string `json:"listen"`
	// Upstream is the host:port to forward to.
	Upstream string `json:"upstream"`
	// Tuning for the queqiao forwarder.
	DialTimeout    duration `json:"dialTimeout,omitempty"`
	IdleTimeout    duration `json:"idleTimeout,omitempty"`
	MaxConnections int      `json:"maxConnections,omitempty"`
}

// relayManifest groups the relays for one host.
type relayManifest struct {
	Version int          `json:"version"`
	Relays  []relayEntry `json:"relays"`
}

// duration is a time.Duration that marshals as a Go duration string.
type duration time.Duration

func (d duration) MarshalJSON() ([]byte, error) { return json.Marshal(time.Duration(d).String()) }

func (d *duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	*d = duration(v)
	return nil
}

func (d duration) or(def time.Duration) time.Duration {
	if d <= 0 {
		return def
	}
	return time.Duration(d)
}

func orInt(v, def int) int {
	if v <= 0 {
		return def
	}
	return v
}

const (
	relayKindQueqiao = "queqiao"
	relayKindTCP     = "tcp"

	defaultRelayDialTimeout    = 10 * time.Second
	defaultRelayIdleTimeout    = 2 * time.Minute
	defaultRelayMaxConnections = 2048
)

// validate normalizes and checks one entry.
func (e *relayEntry) validate() error {
	if e.Name == "" {
		return errors.New("relay name is required")
	}
	if strings.ContainsAny(e.Name, " \t/") || strings.HasPrefix(e.Name, "-") {
		return fmt.Errorf("relay name %q must be a simple unit-safe token", e.Name)
	}
	switch e.Kind {
	case "":
		e.Kind = relayKindQueqiao
	case relayKindQueqiao, relayKindTCP:
	default:
		return fmt.Errorf("relay %q kind %q is not %q or %q", e.Name, e.Kind, relayKindQueqiao, relayKindTCP)
	}
	if _, err := normalizeRelayListen(e.Listen); err != nil {
		return fmt.Errorf("relay %q listen: %w", e.Name, err)
	}
	if _, _, err := net.SplitHostPort(e.Upstream); err != nil {
		return fmt.Errorf("relay %q upstream: %w", e.Name, err)
	}
	if e.DialTimeout < 0 || e.IdleTimeout < 0 {
		return fmt.Errorf("relay %q timeouts must not be negative", e.Name)
	}
	if e.MaxConnections < 0 {
		return fmt.Errorf("relay %q maxConnections must not be negative", e.Name)
	}
	if e.Kind == relayKindTCP && (e.DialTimeout != 0 || e.IdleTimeout != 0 || e.MaxConnections != 0) {
		return fmt.Errorf("relay %q is kind %q; dial/idle/maxConnections apply only to the queqiao forwarder", e.Name, relayKindTCP)
	}
	return nil
}

// normalizeRelayListen accepts "12541", ":12541", or "host:port" and returns a
// concrete host:port. A bare port binds all interfaces.
func normalizeRelayListen(address string) (string, error) {
	if address == "" {
		return "", errors.New("listen address is required")
	}
	if !strings.Contains(address, ":") {
		port, err := strconv.Atoi(address)
		if err != nil || port < 1 || port > 65535 {
			return "", fmt.Errorf("listen port %q must be a port between 1 and 65535", address)
		}
		return "0.0.0.0:" + strconv.Itoa(port), nil
	}
	host, portText, err := net.SplitHostPort(address)
	if err != nil {
		return "", errors.New("listen must be a port or host:port")
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 {
		return "", errors.New("listen port must be numeric between 1 and 65535")
	}
	if host == "" {
		host = "0.0.0.0"
	}
	return net.JoinHostPort(host, strconv.Itoa(port)), nil
}

func runRelay(args []string) error {
	if len(args) == 0 {
		return errors.New("relay requires a subcommand: run, install, uninstall, status")
	}
	switch args[0] {
	case "run":
		return relayRun(args[1:])
	case "install":
		return relayInstall(args[1:])
	case "uninstall":
		return relayUninstall(args[1:])
	case "status":
		return relayStatus(args[1:])
	default:
		return fmt.Errorf("unknown relay subcommand %q (want run, install, uninstall, status)", args[0])
	}
}

// --- relay run (the forwarder) ---------------------------------------------

func relayRun(args []string) error {
	fs := flag.NewFlagSet("relay run", flag.ContinueOnError)
	listen := fs.String("listen", "", "local TCP and UDP bind address (required)")
	upstream := fs.String("upstream", "", "fixed gateway host:port (required)")
	dialTimeout := fs.Duration("dial-timeout", defaultRelayDialTimeout, "upstream connection and UDP write timeout")
	idleTimeout := fs.Duration("idle-timeout", defaultRelayIdleTimeout, "inactive connection/association expiry")
	maxConns := fs.Int("max-connections", defaultRelayMaxConnections, "maximum TCP connections and UDP associations per transport")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *listen == "" || *upstream == "" {
		return errors.New("relay run requires -listen and -upstream")
	}
	return relayForward(*listen, *upstream, *dialTimeout, *idleTimeout, *maxConns)
}

func relayForward(listenAddr, upstream string, dialTimeout, idleTimeout time.Duration, maxConns int) error {
	normalized, err := normalizeRelayListen(listenAddr)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	errs := make(chan error, 2)
	go func() { errs <- relayTCP(ctx, normalized, upstream, dialTimeout, idleTimeout, maxConns) }()
	go func() { errs <- relayUDP(ctx, normalized, upstream, dialTimeout, idleTimeout, maxConns) }()
	<-ctx.Done()
	<-ctx.Done() // wait for cancellation to propagate; either goroutine error is reported below
	select {
	case err := <-errs:
		return err
	default:
		return nil
	}
}

func relayTCP(ctx context.Context, listenAddr, upstream string, dialTimeout, idleTimeout time.Duration, maxConns int) error {
	lc := net.ListenConfig{}
	ln, err := lc.Listen(ctx, "tcp", listenAddr)
	if err != nil {
		return fmt.Errorf("tcp listen %s: %w", listenAddr, err)
	}
	defer func() { _ = ln.Close() }()
	sem := make(chan struct{}, maxConns)
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		select {
		case sem <- struct{}{}:
		default:
			_ = conn.Close()
			continue
		}
		go func() {
			defer func() { <-sem }()
			defer func() { _ = conn.Close() }()
			d := net.Dialer{Timeout: dialTimeout}
			up, err := d.DialContext(ctx, "tcp", upstream)
			if err != nil {
				return
			}
			defer func() { _ = up.Close() }()
			// Disable Nagle so the queqiao handshake's small writes are not
			// delayed across relay hops.
			if tc, ok := conn.(*net.TCPConn); ok {
				_ = tc.SetNoDelay(true)
			}
			if tc, ok := up.(*net.TCPConn); ok {
				_ = tc.SetNoDelay(true)
			}
			relayCopy(conn, up, idleTimeout)
		}()
	}
}

func relayCopy(a, b net.Conn, idle time.Duration) {
	done := make(chan struct{}, 2)
	cp := func(dst, src net.Conn) {
		_, _ = io.Copy(dst, src)
		if t, ok := dst.(*net.TCPConn); ok {
			_ = t.CloseWrite()
		}
		done <- struct{}{}
	}
	go cp(a, b)
	go cp(b, a)
	timer := time.NewTimer(idle)
	defer timer.Stop()
	for i := 0; i < 2; i++ {
		select {
		case <-done:
		case <-timer.C:
			_ = a.Close()
			_ = b.Close()
			return
		}
	}
}

func relayUDP(ctx context.Context, listenAddr, upstream string, dialTimeout, idleTimeout time.Duration, maxConns int) error {
	lc := net.ListenConfig{}
	pc, err := lc.ListenPacket(ctx, "udp", listenAddr)
	if err != nil {
		return fmt.Errorf("udp listen %s: %w", listenAddr, err)
	}
	defer func() { _ = pc.Close() }()

	type assoc struct {
		up     *net.UDPConn
		last   time.Time
		closed bool
	}
	var mu sync.Mutex
	assocs := map[string]*assoc{}

	// sweep idle associations
	go func() {
		t := time.NewTicker(idleTimeout / 2)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				mu.Lock()
				for k, a := range assocs {
					if time.Since(a.last) > idleTimeout {
						_ = a.up.Close()
						delete(assocs, k)
					}
				}
				mu.Unlock()
			}
		}
	}()

	buf := make([]byte, 65535)
	for {
		n, raddr, err := pc.ReadFrom(buf)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		key := raddr.String()
		mu.Lock()
		a := assocs[key]
		if a == nil {
			if len(assocs) >= maxConns {
				mu.Unlock()
				continue
			}
			rudp, err := net.ResolveUDPAddr("udp", upstream)
			if err != nil {
				mu.Unlock()
				continue
			}
			uc, err := net.DialUDP("udp", nil, rudp)
			if err != nil {
				mu.Unlock()
				continue
			}
			a = &assoc{up: uc}
			assocs[key] = a
			go func(a *assoc, raddr net.Addr) {
				rbuf := make([]byte, 65535)
				for {
					m, err := a.up.Read(rbuf)
					if err != nil {
						return
					}
					mu.Lock()
					a.last = time.Now()
					mu.Unlock()
					if _, err := pc.WriteTo(rbuf[:m], raddr); err != nil {
						return
					}
				}
			}(a, raddr)
		}
		a.last = time.Now()
		mu.Unlock()
		_, _ = a.up.Write(buf[:n])
	}
}

// --- systemd install -------------------------------------------------------

const systemdDir = "/etc/systemd/system"

// renderRelayUnit returns the systemd unit text for one relay entry.
func renderRelayUnit(binary string, e relayEntry) (unitName, text string) {
	switch e.Kind {
	case relayKindTCP:
		// systemd-socket-proxyd expects a passed socket: render .socket + .service.
		return e.Name + ".socket", renderSocketProxyUnit(e)
	default:
		return e.Name + ".service", renderQueqiaoRelayUnit(binary, e)
	}
}

func renderQueqiaoRelayUnit(binary string, e relayEntry) string {
	listen, _ := normalizeRelayListen(e.Listen)
	var b strings.Builder
	fmt.Fprintf(&b, "[Unit]\nDescription=queqiao relay %s (%s -> %s)\nAfter=network-online.target\nWants=network-online.target\n\n", e.Name, listen, e.Upstream)
	fmt.Fprintf(&b, "[Service]\nType=simple\n")
	fmt.Fprintf(&b, "ExecStart=%s relay run --listen %s --upstream %s --dial-timeout %s --idle-timeout %s --max-connections %d\n",
		binary, listen, e.Upstream,
		e.DialTimeout.or(defaultRelayDialTimeout), e.IdleTimeout.or(defaultRelayIdleTimeout),
		orInt(e.MaxConnections, defaultRelayMaxConnections))
	b.WriteString("Restart=on-failure\nRestartSec=3\nDynamicUser=yes\nNoNewPrivileges=yes\nProtectSystem=strict\nProtectHome=yes\nPrivateTmp=yes\nLimitNOFILE=16384\n\n")
	b.WriteString("[Install]\nWantedBy=multi-user.target\n")
	return b.String()
}

func renderSocketProxyUnit(e relayEntry) string {
	listen, _ := normalizeRelayListen(e.Listen)
	var b strings.Builder
	fmt.Fprintf(&b, "[Unit]\nDescription=TCP relay %s (%s -> %s)\n\n", e.Name, listen, e.Upstream)
	fmt.Fprintf(&b, "[Socket]\nListenStream=%s\nNoDelay=true\n\n", listen)
	b.WriteString("[Install]\nWantedBy=sockets.target\n")
	return b.String()
}

// socket-activated proxyd also needs a .service; return it separately.
func renderSocketProxyService(e relayEntry) string {
	var b strings.Builder
	fmt.Fprintf(&b, "[Unit]\nDescription=TCP relay %s (-> %s)\n\n", e.Name, e.Upstream)
	fmt.Fprintf(&b, "[Service]\nExecStart=/usr/lib/systemd/systemd-socket-proxyd %s\nDynamicUser=yes\nNoNewPrivileges=yes\nProtectSystem=strict\nProtectHome=yes\nPrivateTmp=yes\n", e.Upstream)
	return b.String()
}

func relayInstall(args []string) error {
	fs := flag.NewFlagSet("relay install", flag.ContinueOnError)
	manifestPath := fs.String("manifest", "", "relay manifest JSON to install (all entries)")
	name := fs.String("name", "", "single relay unit name")
	kind := fs.String("kind", relayKindQueqiao, "queqiao (TCP+UDP) or tcp (systemd-socket-proxyd)")
	listen := fs.String("listen", "", "local bind port/host:port")
	upstream := fs.String("upstream", "", "upstream host:port")
	dialTimeout := fs.Duration("dial-timeout", defaultRelayDialTimeout, "upstream/UDP write timeout (queqiao kind)")
	idleTimeout := fs.Duration("idle-timeout", defaultRelayIdleTimeout, "association expiry (queqiao kind)")
	maxConns := fs.Int("max-connections", defaultRelayMaxConnections, "max connections/associations (queqiao kind)")
	dir := fs.String("dir", systemdDir, "systemd unit directory")
	binary := fs.String("binary", os.Args[0], "path to the queqiaod binary for the unit ExecStart")
	dryRun := fs.Bool("dry-run", false, "print the unit(s) without writing")
	enable := fs.Bool("enable", false, "systemctl enable --now after writing")
	if err := fs.Parse(args); err != nil {
		return err
	}

	var entries []relayEntry
	if *manifestPath != "" {
		manifest, err := loadRelayManifest(*manifestPath)
		if err != nil {
			return err
		}
		entries = manifest.Relays
	} else {
		if *name == "" || *listen == "" || *upstream == "" {
			return errors.New("relay install requires -manifest, or -name, -listen, and -upstream")
		}
		entries = []relayEntry{{
			Name: *name, Kind: *kind, Listen: *listen, Upstream: *upstream,
			DialTimeout: duration(*dialTimeout), IdleTimeout: duration(*idleTimeout), MaxConnections: *maxConns,
		}}
	}

	for i := range entries {
		if err := entries[i].validate(); err != nil {
			return err
		}
	}
	for _, e := range entries {
		files := relayUnitFiles(*binary, e)
		for unitName, content := range files {
			if *dryRun {
				fmt.Printf("--- %s ---\n%s\n", unitName, content)
				continue
			}
			path := filepath.Join(*dir, unitName)
			if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
				return fmt.Errorf("write %s: %w", path, err)
			}
			fmt.Println("wrote", path)
		}
		if !*dryRun && *enable {
			if err := systemctl("daemon-reload"); err != nil {
				return err
			}
			for unitName := range files {
				unit := strings.TrimSuffix(strings.TrimSuffix(unitName, ".service"), ".socket")
				suffix := ""
				if strings.HasSuffix(unitName, ".socket") {
					suffix = ".socket"
				}
				if err := systemctl("enable", "--now", unit+suffix); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// relayUnitFiles returns every unit file an entry needs (name -> content).
func relayUnitFiles(binary string, e relayEntry) map[string]string {
	if e.Kind == relayKindTCP {
		return map[string]string{
			e.Name + ".socket":  renderSocketProxyUnit(e),
			e.Name + ".service": renderSocketProxyService(e),
		}
	}
	return map[string]string{e.Name + ".service": renderQueqiaoRelayUnit(binary, e)}
}

func relayUninstall(args []string) error {
	fs := flag.NewFlagSet("relay uninstall", flag.ContinueOnError)
	name := fs.String("name", "", "relay unit name to remove")
	dir := fs.String("dir", systemdDir, "systemd unit directory")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *name == "" {
		return errors.New("relay uninstall requires -name")
	}
	_ = systemctl("disable", "--now", *name+".socket")
	_ = systemctl("disable", "--now", *name+".service")
	for _, suffix := range []string{".service", ".socket"} {
		path := filepath.Join(*dir, *name+suffix)
		if err := os.Remove(path); err == nil {
			fmt.Println("removed", path)
		}
	}
	return systemctl("daemon-reload")
}

func relayStatus(args []string) error {
	fs := flag.NewFlagSet("relay status", flag.ContinueOnError)
	dir := fs.String("dir", systemdDir, "systemd unit directory")
	if err := fs.Parse(args); err != nil {
		return err
	}
	matches, _ := filepath.Glob(filepath.Join(*dir, "queqiao-*.service"))
	matches2, _ := filepath.Glob(filepath.Join(*dir, "*-relay.service"))
	matches = append(matches, matches2...)
	if len(matches) == 0 {
		fmt.Println("no relay units found in", *dir)
		return nil
	}
	for _, m := range matches {
		unit := filepath.Base(m)
		out, _ := exec.Command("systemctl", "is-active", unit).Output()
		fmt.Printf("%-40s %s\n", unit, strings.TrimSpace(string(out)))
	}
	return nil
}

func systemctl(args ...string) error {
	cmd := exec.Command("systemctl", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("systemctl %s: %w", strings.Join(args, " "), err)
	}
	return nil
}

func loadRelayManifest(path string) (relayManifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return relayManifest{}, fmt.Errorf("read relay manifest %q: %w", path, err)
	}
	// Permissive version pass, then a strict decode (same approach as the
	// provider manifest) so a newer manifest reports a version problem rather
	// than an unknown-field error.
	var envelope struct {
		Version int `json:"version"`
	}
	if err := json.Unmarshal(data, &envelope); err != nil {
		return relayManifest{}, fmt.Errorf("decode relay manifest: %w", err)
	}
	if envelope.Version != 1 {
		return relayManifest{}, fmt.Errorf("unsupported relay manifest version %d", envelope.Version)
	}
	dec := json.NewDecoder(strings.NewReader(string(data)))
	dec.DisallowUnknownFields()
	var manifest relayManifest
	if err := dec.Decode(&manifest); err != nil {
		return relayManifest{}, fmt.Errorf("decode relay manifest: %w", err)
	}
	if len(manifest.Relays) == 0 {
		return relayManifest{}, errors.New("relay manifest must contain at least one relay")
	}
	seen := map[string]bool{}
	for i := range manifest.Relays {
		if err := manifest.Relays[i].validate(); err != nil {
			return relayManifest{}, err
		}
		if seen[manifest.Relays[i].Name] {
			return relayManifest{}, fmt.Errorf("duplicate relay name %q", manifest.Relays[i].Name)
		}
		seen[manifest.Relays[i].Name] = true
	}
	return manifest, nil
}
