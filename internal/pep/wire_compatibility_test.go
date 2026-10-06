package pep

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/apernet/quic-go"
	"github.com/bojieli/queqiao/internal/identity"
	"github.com/bojieli/queqiao/internal/protocol"
)

// The old peer fixture offers only the published v1 ALPN. Both directions
// must fail in the authenticated carrier, before any logical flow exists.
func TestDataWireCompatibility(t *testing.T) {
	serverCredentials, clientCredentials := testCertificate(t)
	for _, carrier := range []string{"TCP", "QUIC"} {
		for _, serverALPN := range []string{"queqiao/1", protocol.DataALPN} {
			for _, clientALPN := range []string{"queqiao/1", protocol.DataALPN} {
				t.Run(fmt.Sprintf("%s/server-%s/client-%s", carrier, serverALPN, clientALPN), func(t *testing.T) {
					ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
					defer cancel()
					serverTLS, err := identity.ServerTLSConfig(serverCredentials, serverALPN, false)
					if err != nil {
						t.Fatal(err)
					}
					clientTLS, err := identity.ClientTLSConfig(clientCredentials, clientALPN)
					if err != nil {
						t.Fatal(err)
					}
					var negotiated string
					if carrier == "TCP" {
						listener, e := net.Listen("tcp4", "127.0.0.1:0")
						if e != nil {
							t.Fatal(e)
						}
						defer listener.Close()
						serverDone := make(chan struct{})
						go func() {
							defer close(serverDone)
							raw, e := listener.Accept()
							if e != nil {
								return
							}
							defer raw.Close()
							_ = tls.Server(raw, serverTLS).HandshakeContext(ctx)
						}()
						var raw net.Conn
						raw, err = (&tls.Dialer{Config: clientTLS}).DialContext(ctx, "tcp", listener.Addr().String())
						if err == nil {
							negotiated = raw.(*tls.Conn).ConnectionState().NegotiatedProtocol
							_ = raw.Close()
						}
						<-serverDone
					} else {
						listener, e := quic.ListenAddr("127.0.0.1:0", serverTLS, quicConfig(flowWindows{}))
						if e != nil {
							t.Fatal(e)
						}
						defer listener.Close()
						conn, e := quic.DialAddr(ctx, listener.Addr().String(), clientTLS, quicConfig(flowWindows{}))
						err = e
						if err == nil {
							negotiated = conn.ConnectionState().TLS.NegotiatedProtocol
							_ = conn.CloseWithError(0, "")
						}
					}
					if serverALPN == clientALPN {
						if err != nil || negotiated != clientALPN {
							t.Fatalf("matching versions: negotiated=%q err=%v", negotiated, err)
						}
					} else if err == nil || !strings.Contains(strings.ToLower(err.Error()), "no application protocol") {
						t.Fatalf("mixed versions must fail at ALPN negotiation: %v", err)
					}
				})
			}
		}
	}
}
