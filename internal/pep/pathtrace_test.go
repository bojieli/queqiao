package pep

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/apernet/quic-go"
	"github.com/bojieli/queqiao/internal/identity"
)

// The validation harness confirms which path-tree leaves a gateway saw, and
// that they were QUIC, from the flow-completion record. With the campaign
// trace on, a QUIC carrier names its egress and egress->peer key exactly as
// resolvePath keys the tree; otherwise nothing is added.
func TestFlowRecordNamesItsPathLeafOnlyUnderTheTrace(t *testing.T) {
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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	accepted := make(chan *quic.Conn, 1)
	go func() {
		c, err := listener.Accept(ctx)
		if err == nil {
			accepted <- c
		}
	}()
	client, err := quic.DialAddr(ctx, listener.Addr().String(), clientTLS, quicConfig(flowWindows{}))
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseWithError(0, "")
	server := <-accepted
	defer server.CloseWithError(0, "")
	lane := &quicStreamConn{conn: server}

	prev := laneTrace.Load()
	defer laneTrace.Store(prev)

	laneTrace.Store(false)
	if got := pathTraceFields(lane); got != nil {
		t.Fatalf("without the trace the record must not change, got %v", got)
	}

	laneTrace.Store(true)
	got := pathTraceFields(lane)
	egress, key := resolvePath(server.LocalAddr(), server.RemoteAddr())
	want := []any{"path_egress", egress, "path_key", key}
	if len(got) != len(want) {
		t.Fatalf("fields = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("fields = %v, want %v", got, want)
		}
	}
	if key != "127.0.0.1->127.0.0.1" {
		t.Fatalf("loopback leaf key = %q", key)
	}

	// A TCP carrier is not a node of the QUIC path tree.
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	if got := pathTraceFields(a); got != nil {
		t.Fatalf("a TCP carrier named a path leaf: %v", got)
	}
}
