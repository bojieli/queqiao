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

// Each mobile resource profile starts a client: its windows, budgets and
// receive-window credit pass the memory validator under the one architecture
// the tunnel runs (one pooled connection per path, carrier recovery).
func TestMobileProfilesStartAClient(t *testing.T) {
	credentials := testClientCredentials(t)
	for _, limits := range []mobileResourceLimits{iosResourceLimits, androidResourceLimits} {
		cfg := tunnelClientConfig(limits)
		cfg.ListenAddr, cfg.RemoteAddr = "127.0.0.1:0", "192.0.2.1:443"
		cfg.Credentials = credentials
		if _, err := pep.NewClient(cfg); err != nil {
			t.Fatalf("%s: %v", limits.name, err)
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
