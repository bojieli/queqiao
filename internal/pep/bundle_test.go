package pep

import (
	"context"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/apernet/quic-go"

	wancongestion "github.com/bojieli/queqiao/internal/congestion"
	"github.com/bojieli/queqiao/internal/identity"
	"github.com/bojieli/queqiao/internal/pathmodel"
	"github.com/bojieli/queqiao/internal/profile"
)

// The configuration validator accepts exactly the bundles the migration
// supports and rejects every other combination at start-up.
func TestBundleValidatorRejectsUnsupportedCombinations(t *testing.T) {
	access, err := profile.ByName("wan-shared-bottleneck")
	if err != nil {
		t.Fatal(err)
	}
	longHaul, err := profile.ByName(dcLongHaulProfile)
	if err != nil {
		t.Fatal(err)
	}
	accepted := []struct {
		api CCAPIVersion
		cc  CongestionControlKind
		p   profile.Profile
	}{
		{"", CongestionErasure, access},
		{CCAPIV1, CongestionReno, access},
		{CCAPIV1, CongestionBBR, longHaul},
		{CCAPIV1, CongestionErasure, longHaul},
		{CCAPIV2, CongestionErasure, access},
	}
	for _, c := range accepted {
		api := c.api
		if err := validateBundle(&api, c.cc, c.p); err != nil {
			t.Errorf("cc-api %q, congestion %q, profile %q rejected: %v", c.api, c.cc, c.p.Name, err)
		}
		if c.api == "" && api != CCAPIV1 {
			t.Errorf("the default cc-api is %q, want %q", api, CCAPIV1)
		}
	}
	rejected := []struct {
		api  CCAPIVersion
		cc   CongestionControlKind
		p    profile.Profile
		want string
	}{
		{"v3", CongestionErasure, access, "unsupported cc-api"},
		{CCAPIV2, CongestionReno, access, "requires --congestion=erasure"},
		{CCAPIV2, CongestionBBR, access, "requires --congestion=erasure"},
		{CCAPIV2, CongestionBBRTUIC, access, "requires --congestion=erasure"},
		{CCAPIV2, CongestionAdaptive, access, "requires --congestion=erasure"},
		{CCAPIV2, CongestionBrutal, access, "requires --congestion=erasure"},
		{CCAPIV2, CongestionErasure, longHaul, "legacy bundle"},
	}
	for _, c := range rejected {
		api := c.api
		err := validateBundle(&api, c.cc, c.p)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("cc-api %q, congestion %q, profile %q: got %v, want an error containing %q", c.api, c.cc, c.p.Name, err, c.want)
		}
	}
}

// sched=class is the stage-2 bundle: it requires cc-api=v2, and the default
// is legacy.
func TestSchedValidatorRequiresCCAPIV2(t *testing.T) {
	for _, c := range []struct {
		sched SchedMode
		api   CCAPIVersion
		want  string
	}{
		{"", CCAPIV1, ""},
		{"", CCAPIV2, ""},
		{SchedLegacy, CCAPIV1, ""},
		{SchedLegacy, CCAPIV2, ""},
		{SchedClass, CCAPIV2, ""},
		{SchedClass, CCAPIV1, "requires cc-api=v2"},
		{SchedClass, "", "requires cc-api=v2"},
		{"fifo", CCAPIV2, "unsupported sched"},
	} {
		sched := c.sched
		err := validateSched(&sched, c.api)
		if c.want == "" {
			if err != nil {
				t.Errorf("sched %q with cc-api %q rejected: %v", c.sched, c.api, err)
			}
			if c.sched == "" && sched != SchedLegacy {
				t.Errorf("the default sched is %q, want %q", sched, SchedLegacy)
			}
		} else if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("sched %q with cc-api %q: got %v, want an error containing %q", c.sched, c.api, err, c.want)
		}
		if exported := ValidateSched(c.sched, c.api); (exported == nil) != (c.want == "") {
			t.Errorf("ValidateSched(%q, %q) = %v, disagrees with the constructors", c.sched, c.api, exported)
		}
	}
	if got := Bundle(CCAPIV2, MemoryLegacy, CodedRetryLegacy, GatewayAdmissionLegacy, SchedClass, ClassifierLegacy, CongestionScopeConnection, ""); got != "cc-api=v2,sched=class" {
		t.Errorf("bundle %q", got)
	}
	if got := Bundle(CCAPIV2, MemoryOwned, CodedRetrySticky, GatewayAdmissionOwned, SchedClass, ClassifierLegacy, CongestionScopeConnection, ""); got != "cc-api=v2,sched=class,coded-retry=sticky,mem=owned,gateway-admission=owned" {
		t.Errorf("bundle %q", got)
	}
	if got := Bundle(CCAPIV1, MemoryLegacy, CodedRetryLegacy, GatewayAdmissionLegacy, SchedLegacy, ClassifierLegacy, CongestionScopeConnection, ""); got != "legacy" {
		t.Errorf("bundle %q, want legacy", got)
	}
	// The stage 6/7 campaigns run sched=class with the phased classifier in
	// both arms; the two switches compose.
	if got := Bundle(CCAPIV2, MemoryLegacy, CodedRetryLegacy, GatewayAdmissionLegacy, SchedClass, ClassifierPhased, CongestionScopeConnection, ""); got != "cc-api=v2,sched=class,classifier=phased" {
		t.Errorf("sched=class with classifier=phased = %q", got)
	}
}

func TestClientAndServerRejectAnUnsupportedBundle(t *testing.T) {
	certificate, credentials := testCertificate(t)
	if _, err := NewClient(ClientConfig{
		ListenAddr: "127.0.0.1:0", RemoteAddr: "127.0.0.1:1", Credentials: credentials,
		Congestion: CongestionReno, CCAPI: CCAPIV2,
	}); err == nil {
		t.Fatal("client accepted cc-api=v2 with reno")
	}
	if _, err := NewServer(ServerConfig{
		ListenAddr: "127.0.0.1:0", Credentials: certificate, EnableQUIC: true,
		Congestion: CongestionBBR, CCAPI: CCAPIV2,
	}); err == nil {
		t.Fatal("server accepted cc-api=v2 with bbr")
	}
	if _, err := NewServer(ServerConfig{
		ListenAddr: "127.0.0.1:0", Credentials: certificate, EnableQUIC: true, CCAPI: CCAPIV2,
	}); err != nil {
		t.Fatalf("server rejected cc-api=v2 with the default controller: %v", err)
	}
	if _, err := NewClient(ClientConfig{
		ListenAddr: "127.0.0.1:0", RemoteAddr: "127.0.0.1:1", Credentials: credentials, Sched: SchedClass,
	}); err == nil {
		t.Fatal("client accepted sched=class without cc-api=v2")
	}
	if _, err := NewServer(ServerConfig{
		ListenAddr: "127.0.0.1:0", Credentials: certificate, EnableQUIC: true, Sched: SchedClass,
	}); err == nil {
		t.Fatal("server accepted sched=class without cc-api=v2")
	}
	if _, err := NewServer(ServerConfig{
		ListenAddr: "127.0.0.1:0", Credentials: certificate, EnableQUIC: true, CCAPI: CCAPIV2, Sched: SchedClass,
	}); err != nil {
		t.Fatalf("server rejected sched=class with cc-api=v2: %v", err)
	}
}

// Under cc-api=v2 every QUIC connection's erasure controller is driven
// through the PathSender API; under v1 it is the ErasureSender itself, as on
// main.
func TestCCAPISelectsHowTheErasureControllerIsDriven(t *testing.T) {
	serverCredentials, clientCredentials := testCertificate(t)
	serverTLS, err := identity.ServerTLSConfig(serverCredentials, defaultALPN, false)
	if err != nil {
		t.Fatal(err)
	}
	clientTLS, err := tlsClientConfig(clientCredentials, nil)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := quic.ListenAddr("127.0.0.1:0", serverTLS, quicConfig(flowWindows{}))
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	received := make(chan int64, 4)
	go func() {
		for {
			conn, err := listener.Accept(ctx)
			if err != nil {
				return
			}
			go func() { <-ctx.Done(); _ = conn.CloseWithError(0, "") }()
			go func() {
				stream, err := conn.AcceptStream(ctx)
				if err != nil {
					return
				}
				n, _ := io.Copy(io.Discard, stream)
				received <- n
			}()
		}
	}()
	for _, api := range []CCAPIVersion{CCAPIV1, CCAPIV2} {
		conn, err := quic.DialAddr(ctx, listener.Addr().String(), clientTLS, quicConfig(flowWindows{}))
		if err != nil {
			t.Fatal(err)
		}
		// Start from an empty model: other tests in this package report to the
		// same loopback path.
		pathmodel.Forget(peerKey(conn))
		controller := configureQUICController(conn, congestionConfig{kind: CongestionErasure, ccAPI: api})
		switch c := controller.(type) {
		case *wancongestion.ErasureSender:
			if api != CCAPIV1 {
				t.Fatalf("cc-api %s installed the ErasureSender directly", api)
			}
		case *wancongestion.ErasurePathSender:
			if api != CCAPIV2 {
				t.Fatalf("cc-api %s installed a PathSender", api)
			}
			// The connection keeps working on it.
			stream, err := conn.OpenStreamSync(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := stream.Write(make([]byte, 64<<10)); err != nil {
				t.Fatal(err)
			}
			if err := stream.Close(); err != nil {
				t.Fatal(err)
			}
			select {
			case n := <-received:
				if n != 64<<10 {
					t.Fatalf("the peer received %d bytes, want %d", n, 64<<10)
				}
			case <-ctx.Done():
				t.Fatal("the transfer did not complete under the PathSender")
			}
			deadline := time.Now().Add(10 * time.Second)
			for c.Telemetry().Samples == 0 {
				if time.Now().After(deadline) {
					t.Fatal("the PathSender's control law never received a delivery sample")
				}
				time.Sleep(5 * time.Millisecond)
			}
			// The connection's other path readers (coded FEC, the control
			// generation's erasure test) share one reference that follows a
			// rekey, and only a rekey.
			readers := pathModelOf(conn)
			if readers != pathModelOf(conn) || readers.model.Load() != pathmodel.Shared(peerKey(conn)) {
				t.Fatal("the connection's path readers do not share its current model")
			}
			rekeyPathModel(conn, "elsewhere->"+conn.RemoteAddr().String())
			if readers.model.Load() != pathmodel.Shared("elsewhere->"+conn.RemoteAddr().String()) {
				t.Fatal("the readers did not follow the rekey")
			}
			// A migration whose resolved key differs from the one the
			// controller was chosen for gets a controller for the new path.
			rekey := erasureRekey(conn, congestionConfig{kind: CongestionErasure, ccAPI: api}, "another-uplink->"+conn.RemoteAddr().String())
			if rekey(conn.LocalAddr(), conn.LocalAddr(), conn.RemoteAddr(), conn.RemoteAddr()) == nil {
				t.Fatal("a migration to another path kept the old path's controller")
			}
			if rekey(conn.LocalAddr(), conn.LocalAddr(), conn.RemoteAddr(), conn.RemoteAddr()) != nil {
				t.Fatal("the key was not updated on replacement")
			}
		default:
			t.Fatalf("cc-api %s installed %T", api, controller)
		}
		_ = conn.CloseWithError(0, "")
	}
}

// A migration that keeps the path key keeps the controller; the comparison is
// against the key the controller was chosen for.
func TestErasureRekeyKeepsTheControllerOnTheSamePath(t *testing.T) {
	local := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 10), Port: 5000}
	oldRemote := &net.UDPAddr{IP: net.IPv4(198, 51, 100, 7), Port: 443}
	newRemote := &net.UDPAddr{IP: net.IPv4(198, 51, 100, 7), Port: 61000}
	rekey := erasureRekey(nil, congestionConfig{kind: CongestionErasure, ccAPI: CCAPIV2}, pathKey(local, oldRemote))
	if next := rekey(local, local, oldRemote, newRemote); next != nil {
		t.Fatal("a port-only rebinding replaced the controller")
	}
}

// congestion=path is the stage-6 bundle: it requires cc-api=v2 and
// sched=class, and the default is one controller per connection.
func TestCongestionScopeValidatorRequiresStagesOneAndTwo(t *testing.T) {
	for _, c := range []struct {
		scope CongestionScope
		api   CCAPIVersion
		sched SchedMode
		want  string
	}{
		{"", CCAPIV1, SchedLegacy, ""},
		{"", CCAPIV2, SchedClass, ""},
		{CongestionScopeConnection, CCAPIV2, SchedLegacy, ""},
		{CongestionScopePath, CCAPIV2, SchedClass, ""},
		{CongestionScopePath, CCAPIV1, SchedLegacy, "requires cc-api=v2"},
		{CongestionScopePath, "", "", "requires cc-api=v2"},
		{CongestionScopePath, CCAPIV2, SchedLegacy, "requires sched=class"},
		{CongestionScopePath, CCAPIV2, "", "requires sched=class"},
		{"lane", CCAPIV2, SchedClass, "unsupported congestion scope"},
	} {
		scope := c.scope
		sched := c.sched
		if sched == "" {
			sched = SchedLegacy
		}
		api := c.api
		if api == "" {
			api = CCAPIV1
		}
		err := validateCongestionScope(&scope, api, sched)
		if c.want == "" {
			if err != nil {
				t.Errorf("scope %q with cc-api %q, sched %q rejected: %v", c.scope, c.api, c.sched, err)
			}
			if c.scope == "" && scope != CongestionScopeConnection {
				t.Errorf("the default scope is %q", scope)
			}
		} else if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("scope %q with cc-api %q, sched %q: got %v, want an error containing %q", c.scope, c.api, c.sched, err, c.want)
		}
		if exported := ValidateCongestionScope(c.scope, c.api, c.sched); (exported == nil) != (c.want == "") {
			t.Errorf("ValidateCongestionScope(%q, %q, %q) = %v, disagrees", c.scope, c.api, c.sched, exported)
		}
	}
	if got := Bundle(CCAPIV2, MemoryLegacy, CodedRetryLegacy, GatewayAdmissionLegacy, SchedClass, ClassifierLegacy, CongestionScopePath, ""); got != "cc-api=v2,sched=class,congestion=path" {
		t.Errorf("bundle %q", got)
	}
	certificate, credentials := testCertificate(t)
	if _, err := NewClient(ClientConfig{
		ListenAddr: "127.0.0.1:0", RemoteAddr: "127.0.0.1:1", Credentials: credentials,
		CCAPI: CCAPIV2, CongestionScope: CongestionScopePath,
	}); err == nil {
		t.Fatal("client accepted congestion=path without sched=class")
	}
	if _, err := NewServer(ServerConfig{
		ListenAddr: "127.0.0.1:0", Credentials: certificate, EnableQUIC: true, CongestionScope: CongestionScopePath,
	}); err == nil {
		t.Fatal("server accepted congestion=path without cc-api=v2")
	}
	if _, err := NewServer(ServerConfig{
		ListenAddr: "127.0.0.1:0", Credentials: certificate, EnableQUIC: true,
		CCAPI: CCAPIV2, Sched: SchedClass, CongestionScope: CongestionScopePath,
	}); err != nil {
		t.Fatalf("server rejected the congestion=path bundle: %v", err)
	}
}
