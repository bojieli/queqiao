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
	if opts.listen != "127.0.0.1:12080" || !opts.quicPool || opts.transport != "auto" || opts.maxSessions != 2048 || opts.maxPendingOpens != 256 || opts.logFile != "auto" || opts.logFormat != "json" || opts.telemetryLogInterval != 5*time.Second {
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

// An unsupported bundle is refused by flag validation, before the process
// logs its configuration or renews anything.
func TestRuntimeValidationRejectsAnUnsupportedBundle(t *testing.T) {
	for _, client := range []bool{true, false} {
		if opts := parseRuntimeForTest(t, client, "--cc-api=v2"); opts.ccAPI != "v2" {
			t.Fatalf("cc-api=v2 not parsed: %q", opts.ccAPI)
		}
		for _, args := range [][]string{
			{"--cc-api=v3"},
			{"--cc-api=v2", "--congestion=reno"},
			{"--cc-api=v2", "--path-profile=dc-long-haul"},
		} {
			fs := flag.NewFlagSet("test", flag.ContinueOnError)
			fs.SetOutput(io.Discard)
			var opts runtimeOptions
			bindRuntimeFlags(fs, &opts, client)
			if err := fs.Parse(args); err != nil {
				t.Fatal(err)
			}
			if err := validateRuntime(opts, client); err == nil {
				t.Errorf("client=%v %v accepted", client, args)
			}
		}
	}
}

// The memory-ownership bundle (design §14 stage 3) is independent: either
// value starts on either role, and any other spelling is refused at start-up.
func TestMemoryBundleIsValidatedAtStartup(t *testing.T) {
	for _, client := range []bool{true, false} {
		if opts := parseRuntimeForTest(t, client); opts.memory != "legacy" {
			t.Fatalf("default memory bundle = %q", opts.memory)
		}
		if opts := parseRuntimeForTest(t, client, "--mem", "owned"); opts.memory != "owned" {
			t.Fatalf("memory bundle = %q", opts.memory)
		}
		opts := parseRuntimeForTest(t, client)
		opts.memory = "shared"
		if err := validateRuntime(opts, client); err == nil {
			t.Fatalf("unknown memory bundle accepted (client=%v)", client)
		}
	}
}

// The coded-retry bundle (design §14 stage 4) requires cc-api=v2, and any
// other spelling is refused at start-up.
func TestCodedRetryBundleIsValidatedAtStartup(t *testing.T) {
	for _, client := range []bool{true, false} {
		if opts := parseRuntimeForTest(t, client); opts.codedRetry != "legacy" {
			t.Fatalf("default coded-retry bundle = %q", opts.codedRetry)
		}
		if opts := parseRuntimeForTest(t, client, "--cc-api=v2", "--coded-retry=sticky"); opts.codedRetry != "sticky" {
			t.Fatalf("coded-retry bundle = %q", opts.codedRetry)
		}
		for _, args := range [][]string{
			{"--coded-retry=sticky"},
			{"--cc-api=v1", "--coded-retry=sticky"},
			{"--cc-api=v2", "--coded-retry=eager"},
		} {
			fs := flag.NewFlagSet("test", flag.ContinueOnError)
			fs.SetOutput(io.Discard)
			var opts runtimeOptions
			bindRuntimeFlags(fs, &opts, client)
			if err := fs.Parse(args); err != nil {
				t.Fatal(err)
			}
			if err := validateRuntime(opts, client); err == nil {
				t.Errorf("client=%v %v accepted", client, args)
			}
		}
	}
}
