package pep

import (
	"sync"

	"github.com/bojieli/queqiao/internal/classifier"
)

// Topology (design docs/DESIGN-PATH-AUTHORITY.md §7, §14 stage 7).
//
// Under topology=single a client keeps every flow a stream on one preferred
// QUIC connection per path: the pooled control generation. It never isolates
// a bulk flow reactively, because the path controller's class arbitration
// (§4.3), executed by the packet builder, already keeps bulk bytes from
// trapping control and interactive work, which is what isolation bought with
// a second connection and controller. Connections beyond the preferred one
// exist only
//
//   - during a handover: a replacement control generation
//     (supersedeSilentGeneration) beside the one it replaces;
//   - for per-flow rescue, which still dials (stage 8 moves it to the carrier
//     manager); and
//   - for an explicit fan-out (ClientConfig.TopologyFanOut).
//
// Under congestion=path, which topology=single requires, every one of them
// is an attachment to the same PathController, so none of them adds a
// congestion controller or claims capacity.
//
// An old client that isolates is the gateway's concern and needs nothing
// here: the gateway attaches all of a device's data connections to one
// controller (stage 6).

// singleTopology reports whether this client keeps its flows on one
// connection per path.
func (c *Client) singleTopology() bool { return c.cfg.Topology == TopologySingle }

// fanOutConnections is the number of connections an explicit fan-out may add
// beside the preferred one: zero unless one was configured.
func (c *Client) fanOutConnections() int {
	if !c.singleTopology() || c.cfg.TopologyFanOut <= 1 {
		return 0
	}
	return c.cfg.TopologyFanOut - 1
}

// secondConnectionWanted decides whether a flow's data should move from the
// pooled connection to a secondary one, once the manager's own pacing (the
// decision interval, the attempt budget and back-off) allows asking.
//
// Under topology=isolated this is today's reactive isolation: a BULK flow, or
// one prewarmed as bulk, that shares the pooled connection with another flow.
// Isolating a lone flow buys nothing and was measured to cost about 8 %.
//
// Under topology=single it is the fan-out hook and nothing else. The design
// keeps fan-out as a carrier-manager policy that stays off until the
// single-connection ceiling (one connection plateaued at 292 Mbit/s against
// 536 for four on an unconstrained 200 ms path) is attributed, and leaves its
// automatic trigger open (§16 Q3). So the only trigger built is the explicit
// one: with a fan-out configured, the same condition as isolation, within
// the fan-out's connection budget (maxBulkConns). An automatic trigger
// belongs here, as a further condition on a measured ceiling.
func (c *Client) secondConnectionWanted(snapshot flowSnapshot) bool {
	if c.singleTopology() && c.fanOutConnections() == 0 {
		return false
	}
	// Isolation earns its cost only while another flow shares the control
	// connection. A bulk transfer alone on it has nothing to protect, and
	// moving it would spend a handshake and a fresh congestion window for no
	// benefit: measured on an otherwise idle path that costs about 8% of bulk
	// goodput.
	if c.quicPoolActive.Load() <= 1 {
		return false
	}
	return snapshot.Class == classifier.ClassBulk || shouldPrewarmBulkLane(snapshot)
}

// Receive-window credit for the preferred connection.
//
// Under isolation each of up to maxBulk bulk flows had a connection, and so a
// connection-level receive window, of its own; on one connection they share
// one. Where the stream window already bounds a flow far below the connection
// window (desktop: 64 MiB streams under a 128 MiB connection that auto-tunes,
// 2.7 Gbit/s at 400 ms) nothing changes. Where the windows are fixed and
// small (mobile) the shared connection window could become the aggregate's
// bound before the streams' windows do: on Android's 1 MiB streams and 4 MiB
// connection, eight bulk flows reach 4 MiB in flight on one connection
// against 6 MiB across the pooled connection and the two isolated ones. So
// the preferred connection's ceiling grows by one stream window for every
// isolated connection single topology does not open, which restores the
// receive capacity isolation had.
//
// That growth is credit drawn from one client-wide ledger, not a property of
// each dial. A retired generation keeps draining while a flow still borrows
// it, so successive supersessions can leave several preferred connections
// alive at once, and each carries whatever it was dialled with for life. The
// ledger holds at most the unopened connections' own windows, k x connection,
// across every live preferred connection: so the sum over every live
// connection on the path -- preferred generations, draining or current, at
// their base window plus their credit, and fan-out, bulk and rescue
// connections at the base window -- never exceeds what isolation could hold
// on the same connections plus its k bulk connections. A dial takes what is
// left, up to its full growth; a connection's credit returns when it closes.
// After a handover whose predecessor still drains, the replacement may
// therefore start with less than the full growth; the next preferred
// connection after the drain ends gets it back.

// windowCredit is the ledger.
type windowCredit struct {
	mu   sync.Mutex
	held uint64
}

// reservePreferredWindows returns the receive windows for a new preferred
// (pooled control) connection, with the credit it was granted, and the
// function that returns that credit, which the caller runs when the
// connection ends (or at once if the dial fails). Without topology=single it
// is the configured windows and a no-op.
func (c *Client) reservePreferredWindows() (flowWindows, func()) {
	w := c.windows()
	unopened := c.memoryLimits.maxBulkConnections - c.fanOutConnections()
	if !c.singleTopology() || unopened <= 0 {
		return w, func() {}
	}
	_, _, streamMax, connectionMax, _ := w.resolved()
	want := uint64(unopened) * min(streamMax, connectionMax)
	budget := uint64(unopened) * connectionMax
	if connectionMax >= maxConnectionReceiveWindow {
		return w, func() {}
	}
	want = min(want, maxConnectionReceiveWindow-connectionMax)
	c.windowCredit.mu.Lock()
	grant := min(want, budget-min(budget, c.windowCredit.held))
	c.windowCredit.held += grant
	c.windowCredit.mu.Unlock()
	if grant == 0 {
		return w, func() {}
	}
	var once sync.Once
	return growConnectionWindow(w, grant), func() {
		once.Do(func() {
			c.windowCredit.mu.Lock()
			c.windowCredit.held -= grant
			c.windowCredit.mu.Unlock()
		})
	}
}

// growConnectionWindow raises the connection receive ceiling by extra. A
// fixed window (initial equal to maximum) stays fixed at the new value; an
// auto-tuned one keeps its initial value.
func growConnectionWindow(w flowWindows, extra uint64) flowWindows {
	if extra == 0 {
		return w
	}
	_, connection, _, connectionMax, _ := w.resolved()
	if connection == connectionMax {
		w.connection = connectionMax + extra
	}
	w.maxConnection = connectionMax + extra
	return w
}
