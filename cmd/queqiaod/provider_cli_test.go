package main

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/bojieli/queqiao/internal/identity"
)

func providerStateForTest(t *testing.T) string {
	t.Helper()
	state := filepath.Join(t.TempDir(), "provider")
	if err := runProvider([]string{"init", "--state", state, "--name", "test", "--endpoint", "127.0.0.1:443"}); err != nil {
		t.Fatalf("provider init: %v", err)
	}
	return state
}

func accountForTest(t *testing.T, state, name string) identity.Account {
	t.Helper()
	provider, err := loadProviderRequired(state)
	if err != nil {
		t.Fatal(err)
	}
	account, ok := provider.Store.FindAccount(name)
	if !ok {
		t.Fatalf("account %q is missing", name)
	}
	return account
}

// A new account must be able to browse. The flow default is high because one
// flow is one connection, and the limit an operator reaches for when they mean
// "this account is for a few devices" is the device count beside it.
func TestAddUserDefaultsAdmitOrdinaryBrowsing(t *testing.T) {
	state := providerStateForTest(t)
	if err := runProvider([]string{"add-user", "--state", state, "--name", "alice"}); err != nil {
		t.Fatal(err)
	}
	account := accountForTest(t, state, "alice")
	if account.MaxFlows != identity.DefaultAccountMaxFlows || account.MaxClients != identity.DefaultAccountMaxClients {
		t.Fatalf("defaults = %+v, want flows %d and clients %d",
			account.Limits(), identity.DefaultAccountMaxFlows, identity.DefaultAccountMaxClients)
	}
	if account.MaxFlows < 1024 {
		t.Fatalf("default flow ceiling %d is too low for a browser", account.MaxFlows)
	}
}

// The old name kept working, but it must not be settable alongside the new one:
// the two would name one limit twice, and picking a winner picks a policy.
func TestAddUserAcceptsTheFormerFlagNameButNotBoth(t *testing.T) {
	state := providerStateForTest(t)
	if err := runProvider([]string{"add-user", "--state", state, "--name", "alice", "--max-sessions", "64"}); err != nil {
		t.Fatal(err)
	}
	if account := accountForTest(t, state, "alice"); account.MaxFlows != 64 {
		t.Fatalf("max flows = %d, want the value given as --max-sessions", account.MaxFlows)
	}
	err := runProvider([]string{"add-user", "--state", state, "--name", "bob", "--max-sessions", "64", "--max-flows", "64"})
	if err == nil {
		t.Fatal("both spellings of the flow limit were accepted at once")
	}
}

// Correcting one limit must not silently rewrite the other, and an account
// whose limit was set too low must be correctable without being deleted.
func TestSetUserLimitsChangesOnlyWhatIsNamed(t *testing.T) {
	state := providerStateForTest(t)
	if err := runProvider([]string{"add-user", "--state", state, "--name", "alice", "--max-flows", "16", "--max-clients", "2"}); err != nil {
		t.Fatal(err)
	}
	if err := runProvider([]string{"set-user-limits", "--state", state, "--user", "alice", "--max-flows", "0"}); err != nil {
		t.Fatal(err)
	}
	account := accountForTest(t, state, "alice")
	if account.MaxFlows != 0 {
		t.Fatalf("max flows = %d, want the gateway limit", account.MaxFlows)
	}
	if account.MaxClients != 2 {
		t.Fatalf("max clients = %d, want the unnamed limit left alone", account.MaxClients)
	}
	if err := runProvider([]string{"set-user-limits", "--state", state, "--user", "alice"}); err == nil {
		t.Fatal("set-user-limits with no limit named was accepted")
	}
	if err := runProvider([]string{"set-user-limits", "--state", state, "--user", "nobody", "--max-clients", "4"}); err == nil {
		t.Fatal("limits were set on an unknown user")
	}
}

func TestInvitationPathsParse(t *testing.T) {
	routes, err := parseInvitationPaths(nil)
	if err != nil || len(routes) != 1 || routes[0] != (identity.InvitationPath{}) {
		t.Fatalf("no --path = %+v, %v; want one invitation with the provider's own values", routes, err)
	}
	routes, err = parseInvitationPaths([]string{"rtx-pro=", "rtx-pro-hk=203.0.113.9:12548", "=198.51.100.1:443"})
	if err != nil {
		t.Fatal(err)
	}
	want := []identity.InvitationPath{
		{Name: "rtx-pro"},
		{Name: "rtx-pro-hk", Endpoint: "203.0.113.9:12548"},
		{Endpoint: "198.51.100.1:443"},
	}
	if len(routes) != len(want) {
		t.Fatalf("routes = %+v, want %+v", routes, want)
	}
	for i := range want {
		if routes[i] != want[i] {
			t.Fatalf("route %d = %+v, want %+v", i, routes[i], want[i])
		}
	}
	if _, err := parseInvitationPaths([]string{"hk=203.0.113.9:1", "hk=203.0.113.9:1"}); err == nil {
		t.Fatal("a repeated --path was accepted")
	}
	for _, bad := range []string{"rtx-pro-hk", "x=no-port", "x=host:99999", "a\nb=203.0.113.9:1"} {
		if _, err := parseInvitationPaths([]string{bad}); err == nil {
			t.Fatalf("--path %q was accepted", bad)
		}
	}
}

// One run with several paths issues one invitation per path and nothing when
// any path is invalid.
func TestProviderInviteIssuesOneInvitationPerPath(t *testing.T) {
	state := providerStateForTest(t)
	if err := runProvider([]string{"add-user", "--state", state, "--name", "alice"}); err != nil {
		t.Fatal(err)
	}
	err := runProvider([]string{"invite", "--state", state, "--user", "alice",
		"--path", "direct=", "--path", "hk=203.0.113.9:12548", "--path", "broken"})
	if err == nil {
		t.Fatal("an invalid --path was accepted")
	}
	account := accountForTest(t, state, "alice")
	provider, err := loadProviderRequired(state)
	if err != nil {
		t.Fatal(err)
	}
	if n := len(provider.Store.Invites(account.ID, time.Now())); n != 0 {
		t.Fatalf("%d invitations issued by a run with an invalid path", n)
	}
	if err := runProvider([]string{"invite", "--state", state, "--user", "alice",
		"--path", "direct=", "--path", "hk=203.0.113.9:12548"}); err != nil {
		t.Fatal(err)
	}
	provider, err = loadProviderRequired(state)
	if err != nil {
		t.Fatal(err)
	}
	if n := len(provider.Store.Invites(account.ID, time.Now())); n != 2 {
		t.Fatalf("%d invitations outstanding, want one per path", n)
	}
}
