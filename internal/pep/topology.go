package pep

import "github.com/bojieli/queqiao/internal/classifier"

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

// singleConnectionWindows is the receive windows of a client that keeps its
// flows on one connection.
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
// the connection ceiling grows by one stream window for every isolated
// connection single topology does not open, which restores the receive
// capacity isolation had without exceeding the receive commitment it made
// (each of those connections could hold a whole connection window). It never
// grows past the desktop ceiling. A fixed window (initial equal to maximum)
// stays fixed at the new value; an auto-tuned one keeps its initial value.
func singleConnectionWindows(w flowWindows, notOpened int) flowWindows {
	if notOpened <= 0 {
		return w
	}
	_, connection, streamMax, connectionMax, _ := w.resolved()
	perFlow := min(streamMax, connectionMax)
	grown := connectionMax + uint64(notOpened)*perFlow
	if grown > maxConnectionReceiveWindow {
		grown = maxConnectionReceiveWindow
	}
	if grown <= connectionMax {
		return w
	}
	if connection == connectionMax {
		w.connection = grown
	}
	w.maxConnection = grown
	return w
}
