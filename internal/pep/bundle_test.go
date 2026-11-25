package pep

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/apernet/quic-go"

	wancongestion "github.com/bojieli/queqiao/internal/congestion"
	"github.com/bojieli/queqiao/internal/identity"
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
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() {
		for {
			conn, err := listener.Accept(ctx)
			if err != nil {
				return
			}
			go func() { <-ctx.Done(); _ = conn.CloseWithError(0, "") }()
		}
	}()
	for _, api := range []CCAPIVersion{CCAPIV1, CCAPIV2} {
		conn, err := quic.DialAddr(ctx, listener.Addr().String(), clientTLS, quicConfig(flowWindows{}))
		if err != nil {
			t.Fatal(err)
		}
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
			deadline := time.Now().Add(3 * time.Second)
			for c.Telemetry().Samples == 0 {
				if time.Now().After(deadline) {
					t.Fatal("the PathSender's control law never received a delivery sample")
				}
				time.Sleep(5 * time.Millisecond)
			}
		default:
			t.Fatalf("cc-api %s installed %T", api, controller)
		}
		_ = conn.CloseWithError(0, "")
	}
}

// A migration that keeps the path key keeps the controller; erasureRekey is
// asked only for a new path.
func TestErasureRekeyKeepsTheControllerOnTheSamePath(t *testing.T) {
	rekey := erasureRekey(nil, congestionConfig{kind: CongestionErasure, ccAPI: CCAPIV2})
	local := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 10), Port: 5000}
	oldRemote := &net.UDPAddr{IP: net.IPv4(198, 51, 100, 7), Port: 443}
	newRemote := &net.UDPAddr{IP: net.IPv4(198, 51, 100, 7), Port: 61000}
	if next := rekey(local, local, oldRemote, newRemote); next != nil {
		t.Fatal("a port-only rebinding replaced the controller")
	}
}
