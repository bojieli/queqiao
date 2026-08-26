package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"runtime"
	"time"

	"github.com/bojieli/queqiao/internal/identity"
	"github.com/bojieli/queqiao/internal/profile"
)

// doctor answers one question: would this host run this profile the way the
// deployment guide assumes it will.
//
// It exists because the guide's first instruction is to do the free things
// first, and the free things are worth more than the transport on the median
// request. Measured on the China-US path this profile was built for, a client
// that reuses its connection and sets tcp_slow_start_after_idle=0 takes a real
// 355KB inference upload to 225.8ms, against 295.0ms through the tunnel. An
// operator who deploys without doing that is paying for a tunnel to be slower
// than a sysctl, and nothing in the system told them.
//
// So the checks are the preconditions rather than the plumbing. Whether the
// gateway answers matters less than whether the host is in the state the
// measurements assumed, because a gateway that does not answer is obvious and
// a kernel default is not.

type check struct {
	Name   string `json:"name"`
	Status string `json:"status"`
	Detail string `json:"detail"`
}

type report struct {
	Version   string    `json:"version"`
	OS        string    `json:"os"`
	Arch      string    `json:"arch"`
	Profile   string    `json:"path_profile"`
	CheckedAt time.Time `json:"checked_at"`
	OK        bool      `json:"ok"`
	Checks    []check   `json:"checks"`
}

func runDoctorCommand(args []string) error {
	fs := flag.NewFlagSet("doctor", flag.ContinueOnError)
	profilePath := fs.String("profile", "", "client profile to check, for the gateway endpoint")
	pathProfile := fs.String("path-profile", "", "path profile to check against, empty for the default")
	endpoint := fs.String("endpoint", "", "gateway host:port, when no client profile is available")
	asJSON := fs.Bool("json", false, "emit the report as JSON")
	timeout := fs.Duration("timeout", 10*time.Second, "per-check timeout")
	if err := fs.Parse(args); err != nil {
		return err
	}

	selected, err := profile.ByName(*pathProfile)
	if err != nil {
		return err
	}
	r := report{
		Version: version, OS: runtime.GOOS, Arch: runtime.GOARCH,
		Profile: selected.Name, CheckedAt: time.Now().UTC(), OK: true,
	}
	r.Checks = append(r.Checks, checkProfile(selected))
	r.Checks = append(r.Checks, hostChecks(selected)...)

	target := *endpoint
	if target == "" && *profilePath != "" {
		p, loadErr := identity.LoadClientProfile(*profilePath)
		if loadErr != nil {
			r.Checks = append(r.Checks, check{Name: "client_profile", Status: "fail", Detail: loadErr.Error()})
		} else {
			r.Checks = append(r.Checks, check{Name: "client_profile", Status: "pass",
				Detail: fmt.Sprintf("%s via %s", p.Name, p.Endpoint)})
			target = p.Endpoint
		}
	}
	if target != "" {
		ctx, cancel := context.WithTimeout(context.Background(), *timeout)
		r.Checks = append(r.Checks, checkReachable(ctx, target)...)
		cancel()
	} else {
		r.Checks = append(r.Checks, check{Name: "gateway", Status: "warn",
			Detail: "no --profile or --endpoint given, so nothing was dialled"})
	}

	for _, c := range r.Checks {
		if c.Status == "fail" {
			r.OK = false
		}
	}
	if *asJSON {
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(r); err != nil {
			return err
		}
	} else {
		printReport(r)
	}
	if !r.OK {
		return errors.New("one or more checks failed")
	}
	return nil
}

func printReport(r report) {
	fmt.Printf("queqiao doctor  profile=%s  %s/%s\n\n", r.Profile, r.OS, r.Arch)
	for _, c := range r.Checks {
		mark := "ok  "
		switch c.Status {
		case "fail":
			mark = "FAIL"
		case "warn":
			mark = "warn"
		}
		fmt.Printf("  %s  %-24s %s\n", mark, c.Name, c.Detail)
	}
	fmt.Println()
	if r.OK {
		fmt.Println("No failures. A pass here is a host in the state the measurements assumed,")
		fmt.Println("not a promise about the path: run cmd/pathprobe to learn what the path does.")
		return
	}
	fmt.Println("Something above will not behave the way the deployment guide assumes.")
}

// checkProfile reports what was selected and how far it has been qualified,
// because an experimental profile qualified on one path is a different claim
// from the default one and the difference should not live only in a document.
func checkProfile(p profile.Profile) check {
	status := "pass"
	detail := fmt.Sprintf("%s (%s)", p.Name, p.Level)
	if p.Level != profile.LevelSupported {
		status = "warn"
		detail = fmt.Sprintf("%s is %s. Precondition: %s. Evidence: %s",
			p.Name, p.Level, p.Precondition, p.Evidence)
	}
	return check{Name: "path_profile", Status: status, Detail: detail}
}

// checkReachable dials the gateway both ways the transport can use it. A host
// where QUIC is blocked and TCP is not still works, on the fallback, and an
// operator seeing the latency that implies should be told which one they got.
func checkReachable(ctx context.Context, endpoint string) []check {
	out := make([]check, 0, 2)
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "tcp", endpoint)
	if err != nil {
		out = append(out, check{Name: "gateway_tcp", Status: "fail", Detail: err.Error()})
	} else {
		_ = conn.Close()
		out = append(out, check{Name: "gateway_tcp", Status: "pass", Detail: endpoint})
	}
	// A UDP dial does not prove the far side answers, only that the local
	// stack has a route and nothing refused the socket outright. Saying so is
	// better than implying a handshake that did not happen.
	udp, err := dialer.DialContext(ctx, "udp", endpoint)
	if err != nil {
		out = append(out, check{Name: "gateway_udp", Status: "warn",
			Detail: fmt.Sprintf("%v; the transport would fall back to TCP", err)})
	} else {
		_ = udp.Close()
		out = append(out, check{Name: "gateway_udp", Status: "pass",
			Detail: "a route exists; this does not prove the gateway answered"})
	}
	return out
}

// readSysctl is a file read rather than a shell out, so it behaves the same
// under a service manager that gives the process no PATH.
func readSysctl(name string) (string, error) {
	data, err := os.ReadFile("/proc/sys/" + name)
	if err != nil {
		return "", err
	}
	return string(trimTrailingNewline(data)), nil
}

func trimTrailingNewline(b []byte) []byte {
	for len(b) > 0 && (b[len(b)-1] == '\n' || b[len(b)-1] == '\r' || b[len(b)-1] == ' ' || b[len(b)-1] == '\t') {
		b = b[:len(b)-1]
	}
	return b
}
