package pep

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/apernet/quic-go"
)

// A dial to a gateway with several addresses races them: the next starts
// while the first is still handshaking. Every racing candidate is a
// connection the gateway counts against the device, so a standby dial with
// one free slot must dial them one at a time (#222).
func TestAddressRaceLimitBoundsConcurrentCandidates(t *testing.T) {
	candidates := []*net.UDPAddr{
		{IP: net.IPv4(192, 0, 2, 1), Port: 443},
		{IP: net.IPv4(192, 0, 2, 2), Port: 443},
		{IP: net.IPv4(192, 0, 2, 3), Port: 443},
	}
	race := func(ctx context.Context) (peak int, dialled int) {
		var mu sync.Mutex
		active := 0
		dial := func(ctx context.Context, addr *net.UDPAddr) (*quic.Conn, net.PacketConn, error) {
			mu.Lock()
			active++
			dialled++
			peak = max(peak, active)
			mu.Unlock()
			// Longer than the 250 ms stagger, so an unlimited race starts the
			// next candidate before this one answers.
			select {
			case <-time.After(400 * time.Millisecond):
			case <-ctx.Done():
			}
			mu.Lock()
			active--
			mu.Unlock()
			return nil, nil, errors.New("unreachable")
		}
		ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		if _, _, err := raceAddressCandidates(ctx, candidates, dial); err == nil {
			t.Fatal("every candidate failed, yet the race succeeded")
		}
		mu.Lock()
		defer mu.Unlock()
		return peak, dialled
	}
	if peak, dialled := race(context.Background()); peak != 2 || dialled != len(candidates) {
		t.Fatalf("an ordinary dial raced %d candidates at once and tried %d; want 2 and %d", peak, dialled, len(candidates))
	}
	if peak, dialled := race(withAddressRaceLimit(context.Background(), 1)); peak != 1 || dialled != len(candidates) {
		t.Fatalf("a dial limited to one candidate raced %d at once and tried %d; want 1 and %d", peak, dialled, len(candidates))
	}
}

// A standby dial races only as many candidates as the device has free
// connection slots beside its live carriers, and always at least one.
func TestStandbyAddressRaceCountsAgainstTheDeviceLimit(t *testing.T) {
	for _, tc := range []struct{ budget, live, want int }{
		{2, 1, 1}, // iOS: the pooled carrier and one secondary slot
		{3, 1, 2},
		{9, 1, 2}, // never more than an ordinary dial races
		{3, 2, 1}, // a draining carrier still holding flows counts
		{2, 2, 1},
	} {
		if got := standbyAddressRace(tc.budget, tc.live); got != tc.want {
			t.Errorf("budget %d with %d live: race %d, want %d", tc.budget, tc.live, got, tc.want)
		}
	}
}
