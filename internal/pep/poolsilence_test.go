package pep

import (
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/bojieli/queqiao/internal/pathsim"
)

// responseDestination answers each request with as many bytes as the request
// asked for: a few bytes up, a response body down, as a page fetch is.
func responseDestination(listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		go func() {
			defer conn.Close()
			var size [4]byte
			if _, err := io.ReadFull(conn, size[:]); err != nil {
				return
			}
			_, _ = conn.Write(make([]byte, binary.BigEndian.Uint32(size[:])))
		}()
	}
}

// tryFetch opens one flow and reads a response of the given size, for a
// caller that will retry, as a browser does.
func tryFetch(socks string, destination net.Listener, size int, budget time.Duration) error {
	conn, err := trySocksDial(socks, destination, budget)
	if err != nil {
		return err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(budget))
	var request [4]byte
	binary.BigEndian.PutUint32(request[:], uint32(size))
	if _, err := conn.Write(request[:]); err != nil {
		return err
	}
	_, err = io.ReadFull(conn, make([]byte, size))
	return err
}

// The connection a client pools can die while the path does not: a NAT
// rebinds, a middlebox drops one port. QUIC declares it dead only after its
// idle timeout, and until then every new request is opened on it and waits.
// A page load issued in that window hangs for the whole timeout though a fresh
// connection would have worked at once.
func TestARequestAfterThePooledConnectionDiesDoesNotWaitForTheIdleTimeout(t *testing.T) {
	if testing.Short() {
		t.Skip("brings up QUIC across an emulated path")
	}
	path := pathsim.Config{OneWayDelay: 25 * time.Millisecond, Seed: 3}
	socks, destination := codedPairWith(t, true, &path, responseDestination)
	if err := tryFetch(socks, destination, 20_000, 20*time.Second); err != nil {
		t.Fatalf("the healthy pool did not carry a request: %v", err)
	}

	lastRelay.BlackholeSources()
	start := time.Now()
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		if err = tryFetch(socks, destination, 20_000, 30*time.Second); err == nil {
			break
		}
		t.Logf("attempt %d failed after %v: %v", attempt+1, time.Since(start).Round(time.Millisecond), err)
	}
	took := time.Since(start)
	t.Logf("a request after the pooled connection died completed in %v", took.Round(time.Millisecond))
	if err != nil {
		t.Fatalf("no request completed after the pooled connection died: %v", err)
	}
	if took > 8*time.Second {
		t.Fatalf("a request after the pooled connection died took %v; it waited out the "+
			"connection's idle timeout instead of moving to a fresh one", took)
	}
}
