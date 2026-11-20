package identity

import (
	"context"
	"crypto/tls"
	"net"
	"testing"
	"time"
)

// A relay in front of a gateway needs invitations that dial the relay but
// still pin the gateway. Operators used to get them by decoding an invitation
// and editing its endpoint by hand.
func TestInvitationForPathDialsThePathAndPinsTheGateway(t *testing.T) {
	now := time.Now()
	provider := testProvider(t, "gateway.example:443", now)
	account, err := provider.Store.AddAccount("alice", time.Time{}, AccountLimits{}, now)
	if err != nil {
		t.Fatal(err)
	}
	path := InvitationPath{Name: "example-hk", Endpoint: "relay.example:12548"}
	uri, invitation, err := provider.CreateInvitationForPath(account.ID, path, time.Hour, now)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ParseInvitation(uri, now)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Endpoint != "relay.example:12548" || parsed.ProviderName != "example-hk" {
		t.Fatalf("invitation = %q at %q, want example-hk at relay.example:12548", parsed.ProviderName, parsed.Endpoint)
	}
	if parsed.ProviderID != provider.Metadata.ProviderID || parsed.GatewayID != provider.Metadata.GatewayID ||
		parsed.RootPin != provider.Metadata.RootPin {
		t.Fatal("a path invitation must pin the same provider, gateway and root")
	}
	if invitation.Token == "" {
		t.Fatal("the invitation carries no token")
	}

	// Each path is its own one-time invitation.
	other, _, err := provider.CreateInvitationForPath(account.ID, InvitationPath{}, time.Hour, now)
	if err != nil {
		t.Fatal(err)
	}
	plain, err := ParseInvitation(other, now)
	if err != nil {
		t.Fatal(err)
	}
	if plain.Token == parsed.Token {
		t.Fatal("two invitations share one token")
	}
	if plain.Endpoint != "gateway.example:443" || plain.ProviderName != "Example Provider" {
		t.Fatalf("an empty path changed the invitation: %q at %q", plain.ProviderName, plain.Endpoint)
	}
}

// A bad path is refused before a token is issued, so nothing is left
// outstanding for an invitation nobody received.
func TestInvalidInvitationPathIssuesNoToken(t *testing.T) {
	now := time.Now()
	provider := testProvider(t, "gateway.example:443", now)
	account, err := provider.Store.AddAccount("alice", time.Time{}, AccountLimits{}, now)
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []InvitationPath{
		{Endpoint: "relay.example"},
		{Endpoint: "relay.example:0"},
		{Name: " padded "},
	} {
		if _, _, err := provider.CreateInvitationForPath(account.ID, path, time.Hour, now); err == nil {
			t.Fatalf("path %+v was accepted", path)
		}
	}
	if invites := provider.Store.Invites(account.ID, now); len(invites) != 0 {
		t.Fatalf("%d invitations outstanding after refused paths", len(invites))
	}
}

// The whole point of a path: enroll through a relay address that is not the
// provider's own endpoint, keep that route in the profile, and renew over it.
// Clients compare the enrollment response's endpoint with the invitation's
// and build the profile from the response, so the provider must answer with
// the route the invitation was issued for.
func TestEnrollmentThroughAPathKeepsThePathAndRenews(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	// The provider's own endpoint is not where anything listens; only the
	// path's endpoint, standing in for a relay, reaches the gateway.
	provider := testProvider(t, "gateway.example:443", time.Now())
	account, err := provider.Store.AddAccount("alice", time.Time{}, AccountLimits{}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	route := InvitationPath{Name: "example-hk", Endpoint: listener.Addr().String()}
	_, invitation, err := provider.CreateInvitationForPath(account.ID, route, time.Hour, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	serverConfig, _ := ServerTLSConfig(provider.ServerCredentials(), "queqiao/1", true)
	service := EnrollmentService{Provider: provider}
	accept := func(handle func(*tls.Conn)) {
		raw, acceptErr := listener.Accept()
		if acceptErr != nil {
			return
		}
		defer raw.Close()
		conn := tls.Server(raw, serverConfig)
		_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
		if conn.Handshake() == nil {
			handle(conn)
		}
	}
	go accept(func(conn *tls.Conn) { _, _ = service.Serve(conn) })
	options := DialOptions{Timeout: 3 * time.Second, LocalAddress: "127.0.0.1"}
	profile, err := EnrollWithOptions(context.Background(), invitation, "phone", options)
	if err != nil {
		t.Fatalf("enrollment through a path failed: %v", err)
	}
	if profile.Endpoint != route.Endpoint || profile.Name != route.Name {
		t.Fatalf("profile = %q at %q, want the path %q at %q", profile.Name, profile.Endpoint, route.Name, route.Endpoint)
	}
	if profile.ProviderID != provider.Metadata.ProviderID || profile.GatewayID != provider.Metadata.GatewayID {
		t.Fatal("a path profile must pin the same provider and gateway")
	}

	// Certificate validity has one-second resolution, and a renewal must
	// extend it; renewing in the issuing second could not.
	time.Sleep(1100 * time.Millisecond)
	go accept(func(conn *tls.Conn) {
		principal, principalErr := PrincipalFromTLS(conn.ConnectionState())
		if principalErr == nil {
			_, _ = service.Renew(conn, principal)
		}
	})
	renewed, err := RenewProfileWithOptions(context.Background(), profile, options)
	if err != nil {
		t.Fatalf("renewal over the path failed: %v", err)
	}
	if renewed.Endpoint != route.Endpoint || renewed.Name != route.Name || renewed.DeviceID != profile.DeviceID {
		t.Fatalf("renewal moved the profile to %q at %q", renewed.Name, renewed.Endpoint)
	}
}

// An invitation without a path still answers with the provider's own values.
func TestEnrollmentWithoutAPathAnswersWithTheProvider(t *testing.T) {
	now := time.Now()
	provider := testProvider(t, "127.0.0.1:443", now)
	account, err := provider.Store.AddAccount("alice", time.Time{}, AccountLimits{}, now)
	if err != nil {
		t.Fatal(err)
	}
	record, _, err := provider.Store.CreateInvite(account.ID, time.Hour, now)
	if err != nil {
		t.Fatal(err)
	}
	if record.Name != "" || record.Endpoint != "" {
		t.Fatalf("a plain invitation recorded a route: %+v", record)
	}
}
