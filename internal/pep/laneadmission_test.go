package pep

import (
	"context"
	"net"
	"testing"
	"time"

	wancongestion "github.com/bojieli/queqiao/internal/congestion"
)

// A QUIC stream takes a write as soon as its buffer has room, so admission
// that counts only what the writer has not yet handed over let a slow lane go
// on taking chunks at the rate the fast lanes freed them. Measured live, 1.2
// to 1.5 MB sat behind a lane pacing 0.7 to 1 MB/s; each chunk outlived the
// reissue deadline before it left and was copied to another lane. A QUIC lane
// may now commit what it can deliver in four of its own round trips.
func TestASlowQUICLaneCommitsOnlyWhatItsRateCarries(t *testing.T) {
	local, remote := net.Pipe()
	t.Cleanup(func() { _ = local.Close(); _ = remote.Close() })
	flow := newMultipathFlow(context.Background(), local, [16]byte{1}, 7, 0, 0, 0, nil, nil)
	slow := &mpLane{id: 1, kind: TransportQUIC, fc: newFrameConn(&statsConn{stats: laneTransportStats{
		smoothedRTT: 200 * time.Millisecond,
		controller:  wancongestion.ControllerTelemetry{PacingRate: 1_000_000, CongestionWindow: 4 << 20},
	}})}
	fast := &mpLane{id: 2, kind: TransportQUIC, fc: newFrameConn(&statsConn{stats: laneTransportStats{
		smoothedRTT: 200 * time.Millisecond,
		controller:  wancongestion.ControllerTelemetry{PacingRate: 50_000_000, CongestionWindow: 16 << 20},
	}})}
	flow.lanes[slow.id], flow.lanes[fast.id] = slow, fast
	admission := &laneAdmission{flow: flow}

	if got, want := admission.Outstanding(slow.id, 0), 800_000; got != want {
		t.Fatalf("a QUIC lane pacing 1 MB/s at 200 ms may commit %d bytes, want four round trips' worth, %d", got, want)
	}
	if got, want := admission.Outstanding(fast.id, 0), 40_000_000; got != want {
		t.Fatalf("a QUIC lane pacing 50 MB/s at 200 ms may commit %d bytes, want %d", got, want)
	}
	// A lane with nothing to size a bound from is not bounded here.
	cold := &mpLane{id: 3, kind: TransportQUIC, fc: newFrameConn(&statsConn{})}
	flow.lanes[cold.id] = cold
	if got := admission.Outstanding(cold.id, 0); got != 0 {
		t.Fatalf("a lane with no rate is bounded at %d", got)
	}
	// A trickle still gets a few chunks, so its estimate can grow.
	trickle := &mpLane{id: 4, kind: TransportQUIC, fc: newFrameConn(&statsConn{stats: laneTransportStats{
		smoothedRTT: 200 * time.Millisecond,
		controller:  wancongestion.ControllerTelemetry{PacingRate: 10_000},
	}})}
	flow.lanes[trickle.id] = trickle
	if got, want := admission.Outstanding(trickle.id, 0), laneOutstandingChunks*flow.chunkSize; got != want {
		t.Fatalf("a lane pacing a trickle may commit %d, want the floor %d", got, want)
	}
	flow.chunkSize = 128 << 10
	if got, want := admission.Outstanding(trickle.id, 0), laneOutstandingChunks*(128<<10); got != want {
		t.Fatalf("with 128 KiB chunks the floor is %d, want %d chunks' worth, %d", got, laneOutstandingChunks, want)
	}
	// The scheduler counts payload, and a lane compensating for erasure
	// puts more on the wire than arrives: losing half its own packets, 2 MB/s
	// of pacing is 1 MB/s of payload -- whatever the pool's erasure says,
	// which can be another destination's.
	lossy := &mpLane{id: 5, kind: TransportQUIC, fc: newFrameConn(&statsConn{stats: laneTransportStats{
		smoothedRTT: 200 * time.Millisecond, packetsSent: 1000,
		controller: wancongestion.ControllerTelemetry{PacingRate: 2_000_000, Erasure: 0.1, PacketsLostObserved: 500},
	}})}
	flow.lanes[lossy.id] = lossy
	if got, want := admission.Outstanding(lossy.id, 0), 800_000; got != want {
		t.Fatalf("a lane pacing 2 MB/s over 50%% erasure may commit %d payload bytes, want %d", got, want)
	}
}
