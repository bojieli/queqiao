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
	if err := resolveBundle(fs, &opts, client); err != nil {
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

// parseRuntimeArgs parses, resolves the bundle and validates, as the client
// and server commands do.
func parseRuntimeArgs(client bool, args ...string) (runtimeOptions, error) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var opts runtimeOptions
	bindRuntimeFlags(fs, &opts, client)
	if err := fs.Parse(args); err != nil {
		return opts, err
	}
	if err := resolveBundle(fs, &opts, client); err != nil {
		return opts, err
	}
	return opts, validateRuntime(opts, client)
}

// legacy prefixes a command line with the legacy bundle, the base the
// per-switch validation below was written against.
func legacy(args ...string) []string {
	return append([]string{"--bundle=legacy"}, args...)
}

// Since stage 10 (design §14) both roles default to the path-authority
// bundle: the stage 1-8 switches plus classifier=phased, gateway-admission
// owned on a gateway and topology=single on a client.
func TestBothRolesDefaultToThePathAuthorityBundle(t *testing.T) {
	for _, client := range []bool{true, false} {
		opts := parseRuntimeForTest(t, client)
		got := pep.Bundle(pep.CCAPIVersion(opts.ccAPI), pep.MemoryMode(opts.memory), pep.CodedRetryMode(opts.codedRetry),
			pep.GatewayAdmission(opts.gatewayAdmission), pep.SchedMode(opts.sched), pep.ClassifierMode(opts.classifierMode),
			pep.CongestionScope(opts.congestionScope), pep.Topology(opts.topology), pep.RecoveryMode(opts.recovery))
		want := "cc-api=v2,sched=class,congestion=path,recovery=carrier,coded-retry=sticky,mem=owned,gateway-admission=owned,classifier=phased"
		if client {
			want = "cc-api=v2,sched=class,congestion=path,topology=single,recovery=carrier,coded-retry=sticky,mem=owned,classifier=phased"
		}
		if got != want || opts.bundle != "path-authority" {
			t.Fatalf("client=%v default bundle %q (%s), want %q", client, got, opts.bundle, want)
		}
		// The default bundle is what the library calls it.
		s, err := pep.SwitchesOf(pep.DefaultBundle, !client)
		if err != nil {
			t.Fatal(err)
		}
		if opts.ccAPI != string(s.CCAPI) || opts.sched != string(s.Sched) || opts.congestionScope != string(s.CongestionScope) ||
			opts.codedRetry != string(s.CodedRetry) || opts.memory != string(s.Memory) || opts.classifierMode != string(s.Classifier) ||
			opts.recovery != string(s.Recovery) || opts.topology != string(s.Topology) || opts.gatewayAdmission != string(s.GatewayAdmission) {
			t.Fatalf("client=%v flags %+v differ from the library's default bundle %+v", client, opts, s)
		}
	}
}

// --bundle=legacy is the rollback of the soak release: every switch off at
// once, on both roles, with any switch given explicitly still winning and
// the combination still validated.
func TestLegacyBundleTurnsEverySwitchOff(t *testing.T) {
	for _, client := range []bool{true, false} {
		opts := parseRuntimeForTest(t, client, "--bundle=legacy")
		got := pep.Bundle(pep.CCAPIVersion(opts.ccAPI), pep.MemoryMode(opts.memory), pep.CodedRetryMode(opts.codedRetry),
			pep.GatewayAdmission(opts.gatewayAdmission), pep.SchedMode(opts.sched), pep.ClassifierMode(opts.classifierMode),
			pep.CongestionScope(opts.congestionScope), pep.Topology(opts.topology), pep.RecoveryMode(opts.recovery))
		if got != "legacy" {
			t.Fatalf("client=%v --bundle=legacy runs %q", client, got)
		}
		if client && opts.topology != "isolated" || !client && opts.gatewayAdmission != "legacy" {
			t.Fatalf("client=%v --bundle=legacy left topology %q admission %q", client, opts.topology, opts.gatewayAdmission)
		}
		// An explicit independent switch is kept.
		if opts := parseRuntimeForTest(t, client, "--bundle=legacy", "--mem=owned"); opts.memory != "owned" || opts.ccAPI != "v1" {
			t.Fatalf("client=%v explicit switch under legacy: mem %q cc-api %q", client, opts.memory, opts.ccAPI)
		}
		// The bundle name is case-insensitive; an unknown one is refused.
		if opts := parseRuntimeForTest(t, client, "--bundle=LEGACY"); opts.ccAPI != "v1" {
			t.Fatalf("client=%v --bundle=LEGACY: cc-api %q", client, opts.ccAPI)
		}
		for _, args := range [][]string{{"--bundle=v2"}, {"--bundle="}, {"--bundle=legacy", "--recovery=carrier"}} {
			if _, err := parseRuntimeArgs(client, args...); err == nil && args[0] != "--bundle=" {
				t.Errorf("client=%v %v accepted", client, args)
			}
		}
		// An empty --bundle is the default.
		if opts, err := parseRuntimeArgs(client, "--bundle="); err != nil || opts.ccAPI != "v2" {
			t.Errorf("client=%v --bundle= : %v cc-api %q", client, err, opts.ccAPI)
		}
	}
}

// A fan-out was only ever an explicit option of topology=single; under the
// default bundle it needs per-flow recovery back.
func TestFanOutUnderTheDefaultBundleNeedsFlowRecovery(t *testing.T) {
	if _, err := parseRuntimeArgs(true, "--topology-fanout=2"); err == nil || !strings.Contains(err.Error(), "--recovery=flow") {
		t.Fatalf("fan-out on the default bundle: %v", err)
	}
	if opts, err := parseRuntimeArgs(true, "--topology-fanout=2", "--recovery=flow"); err != nil || opts.topologyFanOut != 2 {
		t.Fatalf("fan-out with recovery=flow: %v", err)
	}
}

// One switch can be rolled back on its own when nothing that depends on it
// is on (design §14, "Rollback"); the validator refuses the rest.
func TestSingleSwitchRollbackUnderTheDefaultBundle(t *testing.T) {
	for _, client := range []bool{true, false} {
		for _, args := range [][]string{
			{"--recovery=flow"},
			{"--mem=legacy"},
			{"--classifier=legacy"},
			{"--recovery=flow", "--coded-retry=legacy"},
		} {
			if _, err := parseRuntimeArgs(client, args...); err != nil {
				t.Errorf("client=%v %v refused: %v", client, args, err)
			}
		}
		for _, args := range [][]string{
			{"--coded-retry=legacy"},          // recovery=carrier needs it
			{"--congestion-scope=connection"}, // recovery=carrier needs it
			{"--sched=legacy"},                // congestion=path needs it
			{"--cc-api=v1"},                   // everything needs it
		} {
			if _, err := parseRuntimeArgs(client, args...); err == nil {
				t.Errorf("client=%v %v accepted", client, args)
			}
		}
	}
	if _, err := parseRuntimeArgs(false, "--gateway-admission=legacy"); err != nil {
		t.Errorf("gateway admission rollback refused: %v", err)
	}
	if _, err := parseRuntimeArgs(true, "--topology=isolated"); err == nil {
		t.Error("topology=isolated accepted beside recovery=carrier")
	}
	if _, err := parseRuntimeArgs(true, "--topology=isolated", "--recovery=flow"); err != nil {
		t.Errorf("topology rollback with recovery=flow refused: %v", err)
	}
}

// A command line written for the old defaults names a setting only the
// legacy bundle runs; start-up refuses it and says how to keep it.
func TestLegacyOnlySettingsPointAtTheLegacyBundle(t *testing.T) {
	for _, tc := range []struct {
		client bool
		args   []string
	}{
		{true, []string{"--congestion=reno"}},
		{false, []string{"--congestion=bbr"}},
		{false, []string{"--congestion=brutal", "--brutal-bytes-per-sec=1000000"}},
		{true, []string{"--quic-pool=false"}},
	} {
		_, err := parseRuntimeArgs(tc.client, tc.args...)
		if err == nil || !strings.Contains(err.Error(), "--bundle=legacy") {
			t.Errorf("client=%v %v: %v, want a pointer at --bundle=legacy", tc.client, tc.args, err)
		}
		if _, err := parseRuntimeArgs(tc.client, legacy(tc.args...)...); err != nil {
			t.Errorf("client=%v %v under the legacy bundle: %v", tc.client, tc.args, err)
		}
	}
}

func TestGatewayAdmissionIsValidatedAtStartup(t *testing.T) {
	if opts := parseRuntimeForTest(t, false); opts.gatewayAdmission != "owned" {
		t.Fatalf("default gateway admission = %q, want owned", opts.gatewayAdmission)
	}
	if opts := parseRuntimeForTest(t, false, legacy()...); effectiveGatewayAdmission(opts.gatewayAdmission) != "legacy" {
		t.Fatalf("legacy-bundle gateway admission = %q, want legacy", opts.gatewayAdmission)
	}
	if opts := parseRuntimeForTest(t, false, legacy("--gateway-admission", "owned")...); opts.gatewayAdmission != "owned" {
		t.Fatalf("gateway admission = %q, want owned", opts.gatewayAdmission)
	}
	if _, err := parseRuntimeArgs(false, "--gateway-admission", "shared"); err == nil {
		t.Fatal("an unknown gateway admission was accepted")
	}
}

// An unsupported bundle is refused by flag validation, before the process
// logs its configuration or renews anything. The combinations are spelled
// on top of the legacy bundle, one switch at a time, as they were built.
func TestRuntimeValidationRejectsAnUnsupportedBundle(t *testing.T) {
	for _, client := range []bool{true, false} {
		if opts := parseRuntimeForTest(t, client, legacy("--cc-api=v2")...); opts.ccAPI != "v2" {
			t.Fatalf("cc-api=v2 not parsed: %q", opts.ccAPI)
		}
		if opts := parseRuntimeForTest(t, client, legacy("--cc-api=v2", "--sched=class")...); opts.sched != "class" {
			t.Fatalf("sched=class not parsed: %q", opts.sched)
		}
		if opts := parseRuntimeForTest(t, client, legacy()...); opts.sched != "legacy" {
			t.Fatalf("the legacy bundle's sched is %q, want legacy", opts.sched)
		}
		if opts := parseRuntimeForTest(t, client, legacy("--cc-api=v2", "--sched=class", "--congestion-scope=path")...); opts.congestionScope != "path" {
			t.Fatalf("congestion-scope=path not parsed: %q", opts.congestionScope)
		}
		if opts := parseRuntimeForTest(t, client, legacy()...); opts.congestionScope != "connection" {
			t.Fatalf("the legacy bundle's congestion scope is %q, want connection", opts.congestionScope)
		}
		for _, args := range [][]string{
			{"--cc-api=v3"},
			{"--cc-api=v2", "--congestion=reno"},
			{"--cc-api=v2", "--path-profile=dc-long-haul"},
			{"--sched=class"},
			{"--cc-api=v1", "--sched=class"},
			{"--cc-api=v2", "--sched=fifo"},
			{"--cc-api=v2", "--sched="},
			{"--congestion-scope=path"},
			{"--cc-api=v2", "--congestion-scope=path"},
			{"--sched=class", "--congestion-scope=path"},
			{"--cc-api=v2", "--sched=class", "--congestion-scope=lane"},
			{"--cc-api=v2", "--sched=class", "--congestion-scope="},
			{"--cc-api=v2", "--sched=class", "--congestion-scope=path", "--path-profile=dc-long-haul"},
			{"--cc-api=v2", "--sched=class", "--congestion-scope=path", "--congestion=bbr"},
			{"--classifier=phased", "--path-profile=dc-long-haul"},
			{"--cc-api=v2", "--sched=class", "--congestion-scope=path", "--coded-retry=sticky", "--path-profile=dc-long-haul"},
		} {
			if _, err := parseRuntimeArgs(client, legacy(args...)...); err == nil {
				t.Errorf("client=%v %v accepted", client, args)
			}
		}
	}
}

// dc-long-haul starts on its whole new-architecture bundle (design §11.3,
// stage 9), with classifier=phased, on both roles; since stage 10 that is
// what it runs with no switch flags at all, and --bundle=legacy keeps the
// bundle it ran before.
func TestRuntimeValidationAcceptsTheDCBundle(t *testing.T) {
	for _, client := range []bool{true, false} {
		args := []string{"--path-profile=dc-long-haul", "--cc-api=v2", "--sched=class", "--congestion-scope=path",
			"--coded-retry=sticky", "--recovery=carrier", "--classifier=phased", "--mem=owned"}
		if client {
			args = append(args, "--topology=single")
		} else {
			args = append(args, "--gateway-admission=owned")
		}
		if _, err := parseRuntimeArgs(client, args...); err != nil {
			t.Errorf("client=%v %v refused: %v", client, args, err)
		}
		if _, err := parseRuntimeArgs(client, legacy(args...)...); err != nil {
			t.Errorf("client=%v %v over the legacy bundle refused: %v", client, args, err)
		}
		// No switch flags: the default bundle is the whole DC bundle.
		if _, err := parseRuntimeArgs(client, "--path-profile=dc-long-haul"); err != nil {
			t.Errorf("client=%v dc-long-haul on the default bundle refused: %v", client, err)
		}
		if opts, err := parseRuntimeArgs(client, "--path-profile=dc-long-haul", "--bundle=legacy"); err != nil || opts.classifierMode != "legacy" {
			t.Errorf("client=%v dc-long-haul on the legacy bundle: %v (classifier %q)", client, err, opts.classifierMode)
		}
		// Without carrier recovery it is a partial bundle, explicit or by
		// rolling back one switch from the default.
		for _, partial := range [][]string{append(args, "--recovery=flow"), {"--path-profile=dc-long-haul", "--recovery=flow"}} {
			if _, err := parseRuntimeArgs(client, partial...); err == nil || !strings.Contains(err.Error(), "dc-long-haul") {
				t.Errorf("client=%v partial DC bundle %v: got %v", client, partial, err)
			}
		}
	}
}

// The memory-ownership bundle (design §14 stage 3) is independent: either
// value starts on either role and with either bundle, and any other spelling
// is refused at start-up.
func TestMemoryBundleIsValidatedAtStartup(t *testing.T) {
	for _, client := range []bool{true, false} {
		if opts := parseRuntimeForTest(t, client); opts.memory != "owned" {
			t.Fatalf("default memory bundle = %q", opts.memory)
		}
		if opts := parseRuntimeForTest(t, client, legacy()...); opts.memory != "legacy" {
			t.Fatalf("legacy memory bundle = %q", opts.memory)
		}
		if opts := parseRuntimeForTest(t, client, legacy("--mem", "owned")...); opts.memory != "owned" {
			t.Fatalf("memory bundle = %q", opts.memory)
		}
		if opts := parseRuntimeForTest(t, client, "--mem", "legacy"); opts.memory != "legacy" {
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
		if opts := parseRuntimeForTest(t, client); opts.codedRetry != "sticky" {
			t.Fatalf("default coded-retry bundle = %q", opts.codedRetry)
		}
		if opts := parseRuntimeForTest(t, client, legacy()...); opts.codedRetry != "legacy" {
			t.Fatalf("legacy coded-retry bundle = %q", opts.codedRetry)
		}
		if opts := parseRuntimeForTest(t, client, legacy("--cc-api=v2", "--coded-retry=sticky")...); opts.codedRetry != "sticky" {
			t.Fatalf("coded-retry bundle = %q", opts.codedRetry)
		}
		for _, args := range [][]string{
			legacy("--coded-retry=sticky"),
			legacy("--cc-api=v1", "--coded-retry=sticky"),
			legacy("--cc-api=v2", "--coded-retry=eager"),
			{"--coded-retry=eager"},
		} {
			if _, err := parseRuntimeArgs(client, args...); err == nil {
				t.Errorf("client=%v %v accepted", client, args)
			}
		}
	}
}

// The carrier-liveness observer is off by default, works with every bundle
// (it only records), and refuses thresholds it cannot honour.
func TestCarrierShadowFlags(t *testing.T) {
	for _, client := range []bool{true, false} {
		if opts := parseRuntimeForTest(t, client); opts.carrierShadowConfig().Enabled {
			t.Fatalf("carrier shadow on by default (client=%v)", client)
		}
		opts := parseRuntimeForTest(t, client, "--carrier-shadow", "--carrier-shadow-ks=3", "--carrier-shadow-kp=5")
		if cfg := opts.carrierShadowConfig(); !cfg.Enabled || cfg.SuspectPTOs != 3 || cfg.ReplacePTOs != 5 {
			t.Fatalf("carrier shadow config = %+v", cfg)
		}
		parseRuntimeForTest(t, client, legacy("--carrier-shadow", "--cc-api=v2", "--coded-retry=sticky")...)
		parseRuntimeForTest(t, client, legacy("--carrier-shadow")...)
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

// The recovery bundle (design §14 stage 8): on by default on both ends
// since stage 10; carrier needs the path controller and the sticky coded
// retry, and on a client the single topology; its thresholds are validated.
func TestRecoveryFlags(t *testing.T) {
	full := []string{"--cc-api=v2", "--sched=class", "--congestion-scope=path", "--coded-retry=sticky", "--classifier=phased"}
	for _, client := range []bool{true, false} {
		if opts := parseRuntimeForTest(t, client); opts.recovery != "carrier" {
			t.Fatalf("recovery %q by default (client=%v)", opts.recovery, client)
		}
		if opts := parseRuntimeForTest(t, client, legacy()...); opts.recovery != "flow" {
			t.Fatalf("recovery %q on the legacy bundle (client=%v)", opts.recovery, client)
		}
		args := legacy(full...)
		if client {
			args = append(args, "--topology=single")
		}
		args = append(args, "--recovery=carrier", "--recovery-ks=3", "--recovery-kp=6", "--recovery-skew=10s", "--recovery-handoff-budget=3s")
		opts := parseRuntimeForTest(t, client, args...)
		if cfg := opts.carrierRecoveryConfig(); cfg.SuspectPTOs != 3 || cfg.ReplacePTOs != 6 || cfg.DetectionSkew != 10*time.Second || cfg.HandoffBudget != 3*time.Second {
			t.Fatalf("carrier recovery config = %+v", cfg)
		}
		// The thresholds apply to the default bundle without naming it.
		if cfg := parseRuntimeForTest(t, client, "--recovery-ks=3", "--recovery-kp=6").carrierRecoveryConfig(); cfg.SuspectPTOs != 3 || cfg.ReplacePTOs != 6 {
			t.Fatalf("carrier recovery config on the default bundle = %+v", cfg)
		}
		bad := [][]string{
			legacy("--recovery=carrier"),
			{"--recovery=bogus"},
			legacy(append(append([]string{}, full[:3]...), "--recovery=carrier")...),
			legacy(append(append([]string{}, full...), "--topology=single", "--recovery=carrier", "--recovery-ks=5", "--recovery-kp=2")...),
			legacy(append(append([]string{}, full...), "--topology=single", "--recovery=carrier", "--recovery-skew=40s", "--recovery-handoff-budget=10s")...),
			{"--recovery-ks=5", "--recovery-kp=2"},
		}
		if client {
			bad = append(bad, legacy(append(append([]string{}, full...), "--recovery=carrier")...))
		}
		for _, args := range bad {
			if _, err := parseRuntimeArgs(client, args...); err == nil {
				t.Errorf("client=%v %v accepted", client, args)
			}
		}
	}
}

// The topology bundle (design §14 stage 7) is a client switch: single needs
// the path controller and the pool, and a fan-out needs single.
func TestTopologyIsValidatedAtStartup(t *testing.T) {
	path := []string{"--bundle=legacy", "--cc-api=v2", "--sched=class", "--congestion-scope=path"}
	if opts := parseRuntimeForTest(t, true); opts.topology != "single" || opts.topologyFanOut != 0 {
		t.Fatalf("default topology %q fan-out %d", opts.topology, opts.topologyFanOut)
	}
	if opts := parseRuntimeForTest(t, true, legacy()...); opts.topology != "isolated" || opts.topologyFanOut != 0 {
		t.Fatalf("legacy topology %q fan-out %d", opts.topology, opts.topologyFanOut)
	}
	if opts := parseRuntimeForTest(t, true, append(path, "--topology=single", "--classifier=phased")...); opts.topology != "single" {
		t.Fatalf("topology=single not parsed: %q", opts.topology)
	}
	if opts := parseRuntimeForTest(t, true, append(path, "--topology=single", "--topology-fanout=4")...); opts.topologyFanOut != 4 {
		t.Fatalf("fan-out not parsed: %d", opts.topologyFanOut)
	}
	for _, args := range [][]string{
		legacy("--topology=single"),
		legacy("--cc-api=v2", "--sched=class", "--topology=single"),
		append(path, "--topology=single", "--quic-pool=false"),
		append(path, "--topology=ring"),
		append(path, "--topology="),
		append(path, "--topology-fanout=2"),
		append(path, "--topology=single", "--topology-fanout=10"),
		append(path, "--topology=single", "--topology-fanout=-1"),
		{"--topology=single", "--quic-pool=false"},
	} {
		if _, err := parseRuntimeArgs(true, args...); err == nil {
			t.Errorf("%v accepted", args)
		}
	}
	// A gateway has no topology flag: it never isolates, and it attaches an
	// isolating client's connections to one controller either way.
	if _, err := parseRuntimeArgs(false, "--topology=single"); err == nil {
		t.Fatal("the gateway accepted --topology")
	}
}

func TestEgressCapacityIsAGatewayFlagUnderPathScope(t *testing.T) {
	path := []string{"--cc-api=v2", "--sched=class", "--congestion-scope=path"}
	if opts := parseRuntimeForTest(t, false); opts.egressCapacity != "auto" {
		t.Fatalf("the default egress capacity is %q, want auto", opts.egressCapacity)
	}
	for _, v := range []string{"auto", "off", "1g", "940m"} {
		if opts := parseRuntimeForTest(t, false, append([]string{"--egress-capacity=" + v}, path...)...); opts.egressCapacity != v {
			t.Fatalf("egress-capacity=%s parsed as %q", v, opts.egressCapacity)
		}
	}
	// A configured rate does nothing under connection scope, so it is refused
	// rather than ignored; auto and off are inert there. The scope is named
	// explicitly: these lines are parsed without the bundle resolution, and
	// since stage 10 the flags' own defaults are the path-authority bundle.
	for _, tc := range []struct {
		args []string
		ok   bool
	}{
		{[]string{"--egress-capacity=1g"}, true},
		{[]string{"--egress-capacity=1g", "--congestion-scope=connection", "--recovery=flow"}, false},
		{[]string{"--egress-capacity=fast", "--congestion-scope=connection", "--recovery=flow"}, false},
		{append([]string{"--egress-capacity=fast"}, path...), false},
		{[]string{"--egress-capacity=off", "--congestion-scope=connection", "--recovery=flow"}, true},
		{[]string{"--egress-capacity=auto", "--congestion-scope=connection", "--recovery=flow"}, true},
	} {
		fs := flag.NewFlagSet("test", flag.ContinueOnError)
		fs.SetOutput(io.Discard)
		var opts runtimeOptions
		bindRuntimeFlags(fs, &opts, false)
		if err := fs.Parse(tc.args); err != nil {
			t.Fatal(err)
		}
		if err := validateRuntime(opts, false); (err == nil) != tc.ok {
			t.Errorf("%v: validation error %v, want ok=%v", tc.args, err, tc.ok)
		}
	}
	// A client has no egress to share.
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var opts runtimeOptions
	bindRuntimeFlags(fs, &opts, true)
	if err := fs.Parse([]string{"--egress-capacity=1g"}); err == nil {
		t.Error("a client accepted --egress-capacity")
	}
}
