package identity

import (
	"crypto/tls"
	"io"
	"net"
	"slices"
	"strings"
	"testing"
	"time"
)

// negotiate runs one handshake and returns the protocol both ends agreed on.
func negotiate(t *testing.T, serverConfig, clientConfig *tls.Config) (string, error) {
	t.Helper()
	serverRaw, clientRaw := net.Pipe()
	defer serverRaw.Close()
	defer clientRaw.Close()
	server := tls.Server(serverRaw, serverConfig)
	client := tls.Client(clientRaw, clientConfig)
	deadline := time.Now().Add(3 * time.Second)
	_ = server.SetDeadline(deadline)
	_ = client.SetDeadline(deadline)
	serverResult := make(chan error, 1)
	go func() { serverResult <- server.Handshake() }()
	clientErr := client.Handshake()
	if clientErr != nil {
		// Unblock the server half so its handshake returns.
		_ = clientRaw.Close()
	} else {
		// A TLS 1.3 client finishes before the server has checked its
		// certificate; read on so the server's alert is not stuck in the pipe.
		go func() { _, _ = io.Copy(io.Discard, client) }()
	}
	serverErr := <-serverResult
	if clientErr != nil {
		return "", clientErr
	}
	if serverErr != nil {
		return "", serverErr
	}
	got, served := client.ConnectionState().NegotiatedProtocol, server.ConnectionState().NegotiatedProtocol
	if got != served {
		t.Fatalf("client negotiated %q, server %q", got, served)
	}
	return got, nil
}

// TestDataALPNNegotiationMatrix pins how a gateway that accepts several wire
// versions picks one, and that doing so does not loosen the isolation of the
// enrollment and renewal protocols.
//
// The gateway's preference decides: Go's TLS server takes the first of its
// own protocols that the client offers, whatever order the client lists them
// in. A client offering only one version gets that version or no connection,
// which is what lets a client pin a flow's replacement lanes.
func TestDataALPNNegotiationMatrix(t *testing.T) {
	now := time.Now()
	provider := testProvider(t, "127.0.0.1:443", now)
	account, _ := provider.Store.AddAccount("alice", time.Time{}, AccountLimits{}, now)
	profile := localProfile(t, provider, account, "laptop", now)
	credentials, err := profile.Credentials()
	if err != nil {
		t.Fatal(err)
	}
	const v1, v2 = "queqiao/1", "queqiao/2"
	servers := [][]string{{v2}, {v1}, {v2, v1}, {v1, v2}}
	clients := [][]string{{v2}, {v1}, {v2, v1}, {v1, v2}, {"queqiao/3"}}
	for _, serverALPNs := range servers {
		for _, allowEnrollment := range []bool{false, true} {
			serverConfig, err := ServerTLSConfigProtocols(provider.ServerCredentials(), serverALPNs, allowEnrollment)
			if err != nil {
				t.Fatal(err)
			}
			for _, clientALPNs := range clients {
				name := strings.Join(serverALPNs, ",") + "<-" + strings.Join(clientALPNs, ",")
				clientConfig, err := ClientTLSConfigProtocols(credentials, clientALPNs)
				if err != nil {
					t.Fatal(err)
				}
				want := ""
				for _, alpn := range serverALPNs {
					if slices.Contains(clientALPNs, alpn) {
						want = alpn
						break
					}
				}
				got, err := negotiate(t, serverConfig, clientConfig)
				if want == "" {
					if err == nil {
						t.Fatalf("%s (enrollment %v): negotiated %q with no common version", name, allowEnrollment, got)
					}
					continue
				}
				if err != nil || got != want {
					t.Fatalf("%s (enrollment %v): got %q, %v; want %q", name, allowEnrollment, got, err, want)
				}
			}

			// Enrollment is reached only when offered alone, and only on a
			// gateway that enrolls. Offered beside a data version it is
			// ignored and the data version is negotiated, still under mutual
			// TLS: offering it does not relax the device certificate check.
			enroll := EnrollmentTLSConfig(provider.Metadata.RootPin, provider.Metadata.ProviderID, provider.Metadata.GatewayID)
			got, err := negotiate(t, serverConfig, enroll)
			if allowEnrollment && (err != nil || got != EnrollmentALPN) {
				t.Fatalf("%v: lone enrollment offer got %q, %v", serverALPNs, got, err)
			}
			if !allowEnrollment && err == nil {
				t.Fatalf("%v: a gateway without enrollment negotiated %q", serverALPNs, got)
			}
			mixed := enroll.Clone()
			mixed.NextProtos = []string{EnrollmentALPN, serverALPNs[0]}
			if got, err := negotiate(t, serverConfig, mixed); err == nil {
				t.Fatalf("%v: enrollment offered beside a data version completed %q without a device certificate", serverALPNs, got)
			}
			withDevice, _ := ClientTLSConfigProtocols(credentials, serverALPNs)
			withDevice.NextProtos = append([]string{EnrollmentALPN, RenewalALPN}, serverALPNs...)
			if got, err := negotiate(t, serverConfig, withDevice); err != nil || got != serverALPNs[0] {
				t.Fatalf("%v: control protocols beside data ones got %q, %v; want the data protocol", serverALPNs, got, err)
			}
			renew, err := RenewalTLSConfig(credentials)
			if err != nil {
				t.Fatal(err)
			}
			got, err = negotiate(t, serverConfig, renew)
			if allowEnrollment && (err != nil || got != RenewalALPN) {
				t.Fatalf("%v: lone renewal offer got %q, %v", serverALPNs, got, err)
			}
			if !allowEnrollment && err == nil {
				t.Fatalf("%v: a gateway without renewal negotiated %q", serverALPNs, got)
			}
		}
	}
}

func TestDataALPNOffersAreValidated(t *testing.T) {
	now := time.Now()
	provider := testProvider(t, "127.0.0.1:443", now)
	account, _ := provider.Store.AddAccount("alice", time.Time{}, AccountLimits{}, now)
	credentials, err := localProfile(t, provider, account, "laptop", now).Credentials()
	if err != nil {
		t.Fatal(err)
	}
	for _, bad := range [][]string{nil, {""}, {"queqiao/2", "queqiao/2"}, {"queqiao/2", EnrollmentALPN}, {RenewalALPN}} {
		if _, err := ServerTLSConfigProtocols(provider.ServerCredentials(), bad, true); err == nil {
			t.Fatalf("gateway accepted data protocols %q", bad)
		}
		if _, err := ClientTLSConfigProtocols(credentials, bad); err == nil {
			t.Fatalf("client offered data protocols %q", bad)
		}
	}
}
