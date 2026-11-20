package identity

import (
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
