package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bojieli/queqiao/internal/metrics"
	"github.com/bojieli/queqiao/internal/pep"
)

func parseRuntimeForTest(t *testing.T, client bool, args ...string) runtimeOptions {
	t.Helper()
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var opts runtimeOptions
	bindRuntimeFlags(fs, &opts, client)
	if err := fs.Parse(args); err != nil {
		t.Fatal(err)
	}
	if err := validateRuntime(opts, client); err != nil {
		t.Fatal(err)
	}
	return opts
}

func TestClientDefaultsNeedOnlyAnImportedProfile(t *testing.T) {
	opts := parseRuntimeForTest(t, true)
	if opts.listen != "127.0.0.1:12080" || opts.transport != "auto" || opts.maxSessions != 2048 || opts.maxPendingOpens != 256 || opts.logFile != "auto" || opts.logFormat != "json" || opts.telemetryLogInterval != 5*time.Second {
		t.Fatalf("unexpected client defaults: %+v", opts)
	}
}

func TestClientListenerMustBeLiteralLoopback(t *testing.T) {
	for _, address := range []string{"127.0.0.1:1080", "127.0.0.2:0", "[::1]:1080"} {
		opts := parseRuntimeForTest(t, true)
		opts.listen = address
		if err := validateRuntime(opts, true); err != nil {
			t.Errorf("loopback listener %q rejected: %v", address, err)
		}
	}
	for _, address := range []string{":1080", "0.0.0.0:1080", "[::]:1080", "192.168.1.2:1080", "localhost:1080"} {
		opts := parseRuntimeForTest(t, true)
		opts.listen = address
		if err := validateRuntime(opts, true); err == nil {
			t.Errorf("non-literal-loopback listener %q accepted", address)
		}
	}
}

func TestServerDefaultsUseBothTransports(t *testing.T) {
	opts := parseRuntimeForTest(t, false)
	if opts.listen != ":443" || opts.transport != "auto" || opts.outboundSOCKS5 != "" || opts.logFile != "auto" || opts.logFormat != "json" || opts.telemetryLogInterval != 5*time.Second {
		t.Fatalf("unexpected server defaults: %+v", opts)
	}
}

func TestServerOutboundSOCKS5MustBeLoopback(t *testing.T) {
	for _, address := range []string{"127.0.0.1:1080", "127.0.0.2:2080", "[::1]:1080"} {
		opts := parseRuntimeForTest(t, false, "--outbound-socks5", address)
		if opts.outboundSOCKS5 != address {
			t.Fatalf("outbound SOCKS5 = %q, want %q", opts.outboundSOCKS5, address)
		}
	}
	for _, address := range []string{"localhost:1080", "0.0.0.0:1080", "192.0.2.1:1080", "127.0.0.1:0", "127.0.0.1"} {
		fs := flag.NewFlagSet("test", flag.ContinueOnError)
		fs.SetOutput(io.Discard)
		var opts runtimeOptions
		bindRuntimeFlags(fs, &opts, false)
		if err := fs.Parse([]string{"--outbound-socks5", address}); err != nil {
			t.Fatal(err)
		}
		if err := validateRuntime(opts, false); err == nil {
			t.Errorf("unsafe SOCKS5 upstream %q accepted", address)
		}
	}
}

func TestRuntimeBoundsRejectUnsafeValues(t *testing.T) {
	for _, args := range [][]string{
		{"--tcp-fallback-lanes", "17"},
		{"--max-sessions", "0"},
		{"--max-pending-opens", "0"},
		{"--max-pending-opens", "65537"},
		{"--fallback-grace", "0s"},
		{"--dial-timeout", "0s"},
		{"--handshake-timeout", "-1s"},
		{"--flow-idle-timeout", "2h", "--flow-max-lifetime", "1h"},
		{"--local-address", "if:"},
		{"--log-format", "xml"},
		{"--log-level", "verbose"},
		{"--log-max-size-mib", "0"},
		{"--log-max-backups", "101"},
		{"--telemetry-log-interval", "500ms"},
		{"--log-file", "none", "--log-stderr=false"},
	} {
		fs := flag.NewFlagSet("test", flag.ContinueOnError)
		fs.SetOutput(io.Discard)
		var opts runtimeOptions
		bindRuntimeFlags(fs, &opts, true)
		if err := fs.Parse(args); err != nil {
			continue
		}
		if err := validateRuntime(opts, true); err == nil {
			t.Fatalf("unsafe options accepted: %v", args)
		}
	}
}

func TestPendingOpenLimitIsIndependentOfTotalSessionLimit(t *testing.T) {
	opts := parseRuntimeForTest(t, true, "--max-sessions", "128", "--max-pending-opens", "256")
	if opts.maxSessions != 128 || opts.maxPendingOpens != 256 {
		t.Fatalf("admission limits = %d/%d, want 128/256", opts.maxSessions, opts.maxPendingOpens)
	}
}

func TestFallbackWindowsRemainConfigurable(t *testing.T) {
	opts := parseRuntimeForTest(t, true, "--fallback-delay", "25ms", "--fallback-grace", "3s")
	if opts.fallbackDelay != 25*time.Millisecond || opts.fallbackGrace != 3*time.Second {
		t.Fatalf("fallback windows = %v/%v", opts.fallbackDelay, opts.fallbackGrace)
	}
}

func TestLegacyModeInterfaceIsGone(t *testing.T) {
	if err := run([]string{"--mode", "local"}); err == nil {
		t.Fatal("legacy mode/secret interface was accepted")
	}
}

func TestEnrollmentURIAllowsFollowingFlags(t *testing.T) {
	err := runEnroll([]string{"queqiao://invalid", "--profile", "profile.json"})
	if err == nil || strings.Contains(err.Error(), "at most one") || strings.Contains(err.Error(), "unexpected arguments") {
		t.Fatalf("share URI prevented following flags from being parsed: %v", err)
	}
}

func TestClientMissingConfigurationExplainsEnrollment(t *testing.T) {
	t.Setenv("QUEQIAO_LOG_DIR", t.TempDir())
	err := runClient(nil)
	if err == nil || !strings.Contains(err.Error(), "--profile") || !strings.Contains(err.Error(), "--providers") || !strings.Contains(err.Error(), "queqiaod enroll") {
		t.Fatalf("missing client configuration produced unhelpful error: %v", err)
	}
}

func TestClientProfileAndProvidersAreMutuallyExclusive(t *testing.T) {
	// These checks sit above openRuntimeLogger today, but that is not a
	// property the test should depend on: without this the first refactor which
	// logs a validation error writes into the real platform log directory.
	t.Setenv("QUEQIAO_LOG_DIR", t.TempDir())
	err := runClient([]string{"--profile", "profile.json", "--providers", "providers.json"})
	if err == nil || !strings.Contains(err.Error(), "mutually exclusive") {
		t.Fatalf("profile and providers were not rejected together: %v", err)
	}
	err = runClient([]string{"--providers", "providers.json", "--listen", "127.0.0.1:1081"})
	if err == nil || !strings.Contains(err.Error(), "cannot be used") {
		t.Fatalf("global listener was accepted with providers: %v", err)
	}
}

func TestClientAndServerCreateSeparateRuntimeLogs(t *testing.T) {
	directory := t.TempDir()
	t.Setenv("QUEQIAO_LOG_DIR", directory)
	for _, role := range []string{"client", "server"} {
		opts := parseRuntimeForTest(t, role == "client", "--log-stderr=false")
		logger, sink, err := openRuntimeLogger(opts, role)
		if err != nil {
			t.Fatal(err)
		}
		logger.Info("role-specific record")
		if err := sink.Close(); err != nil {
			t.Fatal(err)
		}
		payload, err := os.ReadFile(filepath.Join(directory, role+".log"))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Contains(payload, []byte(`"role":"`+role+`"`)) || !bytes.Contains(payload, []byte(`"msg":"runtime logging initialized"`)) {
			t.Fatalf("%s log is missing runtime metadata: %s", role, payload)
		}
	}
}

func TestPerformanceSnapshotIsMachineReadable(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, nil))
	registry := metrics.New()
	registry.FlowStarted()
	registry.ObserveQUIC(1, metrics.QUICObservation{
		Lanes: 1, SmoothedRTT: 210 * time.Millisecond, ControllerKind: "bbr-tuic",
		ControllerPacingRate: 9999,
	})
	registry.AddQUICConnectionCounters(metrics.QUICConnectionCounters{
		BytesSent: 1234, BytesReceived: 5678,
		PacketsSent: 123, PacketsReceived: 119, LossObservedPackets: 3,
	})
	logPerformanceSnapshot(logger, registry.Snapshot(), 5*time.Second)
	var record map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(output.Bytes()), &record); err != nil {
		t.Fatal(err)
	}
	for key, want := range map[string]any{
		"msg": "performance snapshot", "type": "metrics", "telemetry_schema": float64(1),
		"sample_interval_seconds": float64(5), "queqiao_active_flows": float64(1),
		"queqiao_quic_smoothed_rtt_seconds": 0.21, "queqiao_quic_bytes_received": float64(5678),
		"queqiao_quic_packets_sent": float64(123), "queqiao_quic_packets_received": float64(119),
		"queqiao_quic_controller_kind": "bbr-tuic", "queqiao_quic_controller_pacing_rate_bytes_per_second": float64(9999),
	} {
		if record[key] != want {
			t.Fatalf("%s = %#v, want %#v in %#v", key, record[key], want, record)
		}
	}
}

// Every lane-join refusal reason is in the flat snapshot. A reason missing
// from it vanishes from log-based totals, and refusals reclassified out of
// unknown_session (issue #159) would look like refusals stopping.
func TestPerformanceSnapshotCarriesEveryLaneJoinRefusal(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, nil))
	registry := metrics.New()
	for reason := metrics.LaneJoinRefusal(0); reason < metrics.LaneJoinReasons; reason++ {
		for range int(reason) + 1 {
			registry.LaneJoinRefused(reason)
		}
	}
	logPerformanceSnapshot(logger, registry.Snapshot(), 5*time.Second)
	var record map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(output.Bytes()), &record); err != nil {
		t.Fatal(err)
	}
	for reason := metrics.LaneJoinRefusal(0); reason < metrics.LaneJoinReasons; reason++ {
		key := "queqiao_lane_join_refused_" + reason.String() + "_total"
		if got, want := record[key], float64(int(reason)+1); got != want {
			t.Errorf("%s = %#v, want %v", key, got, want)
		}
	}
}

func TestRuntimeConfigurationLogsRoleSpecificControls(t *testing.T) {
	for _, test := range []struct {
		role   string
		client bool
		want   string
		absent string
	}{
		{role: "client", client: true, want: "local_address", absent: "tcp_congestion"},
		{role: "server", client: false, want: "tcp_congestion", absent: "local_address"},
	} {
		t.Run(test.role, func(t *testing.T) {
			var output bytes.Buffer
			logger := slog.New(slog.NewJSONHandler(&output, nil))
			logRuntimeConfiguration(logger, parseRuntimeForTest(t, test.client), test.client)
			var record map[string]any
			if err := json.Unmarshal(bytes.TrimSpace(output.Bytes()), &record); err != nil {
				t.Fatal(err)
			}
			if record["msg"] != "runtime configuration" || record["config_schema"] != float64(1) || record["transport"] != "auto" {
				t.Fatalf("incomplete runtime configuration: %#v", record)
			}
			// Validation tooling written during the migration reads the
			// switches an endpoint ran from this field.
			if record["bundle"] != pep.EffectiveBundle(!test.client) {
				t.Fatalf("%s configuration names bundle %v", test.role, record["bundle"])
			}
			if _, ok := record[test.want]; !ok {
				t.Fatalf("%s configuration is missing %q: %#v", test.role, test.want, record)
			}
			if _, ok := record[test.absent]; ok {
				t.Fatalf("%s configuration unexpectedly contains %q: %#v", test.role, test.absent, record)
			}
		})
	}
}

func TestWireVersionFlags(t *testing.T) {
	if got := parseRuntimeForTest(t, false).wireVersions; got != "1,2" {
		t.Fatalf("--accept-wire defaults to %q, want 1,2", got)
	}
	if got := parseRuntimeForTest(t, true).wireVersions; got != "2,1" {
		t.Fatalf("--wire-versions defaults to %q, want 2,1", got)
	}
	if got := parseRuntimeForTest(t, false, "--accept-wire", "2").wireVersions; got != "2" {
		t.Fatalf("--accept-wire 2 parsed as %q", got)
	}
	for _, client := range []bool{false, true} {
		for _, bad := range []string{"3", "1,1", "", "x"} {
			opts := parseRuntimeForTest(t, client)
			opts.wireVersions = bad
			if err := validateRuntime(opts, client); err == nil {
				t.Errorf("client=%v accepted wire versions %q", client, bad)
			}
		}
	}
	// release.yml and validate_release.py match `wire=2` as its own token.
	if wireAccept() != "1,2" {
		t.Fatalf("wire_accept = %q, want 1,2", wireAccept())
	}
}

// parseRuntimeArgs parses and validates, as the client and server commands
// do.
func parseRuntimeArgs(client bool, args ...string) (runtimeOptions, error) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var opts runtimeOptions
	bindRuntimeFlags(fs, &opts, client)
	if err := fs.Parse(args); err != nil {
		return opts, err
	}
	return opts, validateRuntime(opts, client)
}

// The path-authority migration ended with one architecture (design §14
// stage 11): its switches, the --bundle escape hatch, the congestion laws
// only the legacy bundle could host and the unpooled client are gone from
// both roles, and a command line that still names one is refused at start-up
// rather than silently ignored.
func TestMigrationSwitchesAreGone(t *testing.T) {
	for _, client := range []bool{true, false} {
		for _, flagName := range []string{"bundle", "cc-api", "sched", "congestion-scope", "coded-retry", "mem", "classifier",
			"recovery", "congestion", "brutal-bytes-per-sec", "adaptive-min-bytes-per-sec", "adaptive-max-bytes-per-sec",
			"topology", "topology-fanout", "quic-pool", "gateway-admission"} {
			if _, err := parseRuntimeArgs(client, "--"+flagName+"=x"); err == nil {
				t.Errorf("client=%v accepted --%s", client, flagName)
			}
		}
	}
}

// Every profile, dc-long-haul included, starts on both roles: there is one
// bundle, and the profile no longer restricts it.
func TestEveryProfileStartsOnBothRoles(t *testing.T) {
	for _, client := range []bool{true, false} {
		for _, name := range []string{"", "wan-shared-bottleneck", "dc-long-haul"} {
			if _, err := parseRuntimeArgs(client, "--path-profile="+name); err != nil {
				t.Errorf("client=%v profile %q: %v", client, name, err)
			}
		}
	}
}

// The carrier-liveness observer is off by default, only records, and
// refuses thresholds it cannot honour.
func TestCarrierShadowFlags(t *testing.T) {
	for _, client := range []bool{true, false} {
		if opts := parseRuntimeForTest(t, client); opts.carrierShadowConfig().Enabled {
			t.Fatalf("carrier shadow on by default (client=%v)", client)
		}
		opts := parseRuntimeForTest(t, client, "--carrier-shadow", "--carrier-shadow-ks=3", "--carrier-shadow-kp=5")
		if cfg := opts.carrierShadowConfig(); !cfg.Enabled || cfg.SuspectPTOs != 3 || cfg.ReplacePTOs != 5 {
			t.Fatalf("carrier shadow config = %+v", cfg)
		}
		for _, args := range [][]string{
			{"--carrier-shadow", "--carrier-shadow-ks=4", "--carrier-shadow-kp=3"},
			{"--carrier-shadow", "--carrier-shadow-kp=99"},
			{"--carrier-shadow-ks=4294967297"},
		} {
			if _, err := parseRuntimeArgs(client, args...); err == nil {
				t.Errorf("client=%v %v accepted", client, args)
			}
		}
	}
}

// The carrier manager's thresholds (design §10) are validated at start-up.
func TestRecoveryFlags(t *testing.T) {
	for _, client := range []bool{true, false} {
		opts := parseRuntimeForTest(t, client, "--recovery-ks=3", "--recovery-kp=6", "--recovery-skew=10s", "--recovery-handoff-budget=3s")
		if cfg := opts.carrierRecoveryConfig(); cfg.SuspectPTOs != 3 || cfg.ReplacePTOs != 6 || cfg.DetectionSkew != 10*time.Second || cfg.HandoffBudget != 3*time.Second {
			t.Fatalf("carrier recovery config = %+v", cfg)
		}
		for _, args := range [][]string{
			{"--recovery-ks=5", "--recovery-kp=2"},
			{"--recovery-skew=40s", "--recovery-handoff-budget=10s"},
			{"--recovery-ks=65"},
		} {
			if _, err := parseRuntimeArgs(client, args...); err == nil {
				t.Errorf("client=%v %v accepted", client, args)
			}
		}
	}
}
