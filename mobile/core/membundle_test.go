package mobilecore

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"path/filepath"
	"testing"
	"time"

	"github.com/bojieli/queqiao/internal/identity"
	"github.com/bojieli/queqiao/internal/pep"
)

// Since stage 10 the tunnel runs the path-authority bundle, mem=owned
// included. The memory bundle can still be set back to legacy on its own,
// since it is independent of the other switches; unknown values are refused
// and change nothing.
func TestSessionMemoryBundleDefaultsToOwned(t *testing.T) {
	s := &Session{}
	if got := s.memoryBundle(); got != pep.MemoryOwned {
		t.Fatalf("default bundle %q", got)
	}
	if err := s.SetMemoryBundle("legacy"); err != nil {
		t.Fatal(err)
	}
	if err := s.SetMemoryBundle("bogus"); err == nil {
		t.Fatal("unknown bundle accepted")
	}
	if got := s.memoryBundle(); got != pep.MemoryLegacy {
		t.Fatalf("bundle after a refused change %q", got)
	}
}

// Each mobile resource profile starts a client on the default bundle, with
// either memory bundle: its windows, budgets and pool pass the memory
// validator under topology=single and recovery=carrier.
func TestMobileProfilesStartOnTheDefaultBundle(t *testing.T) {
	credentials := testClientCredentials(t)
	want, err := pep.SwitchesOf(pep.DefaultBundle, false)
	if err != nil {
		t.Fatal(err)
	}
	for _, limits := range []mobileResourceLimits{iosResourceLimits, androidResourceLimits} {
		for _, memory := range []pep.MemoryMode{pep.MemoryOwned, pep.MemoryLegacy} {
			cfg := tunnelClientConfig(limits, memory)
			if cfg.CCAPI != want.CCAPI || cfg.Sched != want.Sched || cfg.CongestionScope != want.CongestionScope ||
				cfg.CodedRetry != want.CodedRetry || cfg.Classifier != want.Classifier || cfg.Recovery != want.Recovery ||
				cfg.Topology != want.Topology || cfg.Memory != memory {
				t.Fatalf("%s: config switches %+v, want %+v with mem=%s", limits.name, cfg, want, memory)
			}
			cfg.ListenAddr, cfg.RemoteAddr = "127.0.0.1:0", "192.0.2.1:443"
			cfg.Credentials = credentials
			client, err := pep.NewClient(cfg)
			if err != nil {
				t.Fatalf("%s mem=%s: %v", limits.name, memory, err)
			}
			_ = client
		}
	}
}

func testClientCredentials(t *testing.T) identity.ClientCredentials {
	t.Helper()
	now := time.Now()
	provider, err := identity.InitProvider(filepath.Join(t.TempDir(), "provider"), "mobile-test", "192.0.2.1:443", now)
	if err != nil {
		t.Fatal(err)
	}
	account, err := provider.Store.AddAccount("mobile-test", time.Time{}, identity.AccountLimits{}, now)
	if err != nil {
		t.Fatal(err)
	}
	_, invitation, err := provider.CreateInvitation(account.ID, time.Hour, now)
	if err != nil {
		t.Fatal(err)
	}
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	_, device, err := provider.Store.ConsumeInvite(invitation.Token, "mobile-test", publicKey, now)
	if err != nil {
		t.Fatal(err)
	}
	certificatePEM, err := provider.IssueDevice(account.ID, device.ID, publicKey, now)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := tls.X509KeyPair(certificatePEM, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}))
	if err != nil {
		t.Fatal(err)
	}
	return identity.ClientCredentials{
		ProviderID: provider.Metadata.ProviderID, GatewayID: provider.Metadata.GatewayID,
		Certificate: certificate, Root: provider.RootCert, RootPin: provider.Metadata.RootPin,
	}
}
