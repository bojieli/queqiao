package pathmodel

import (
	"sort"
	"strings"
	"sync"
)

// A path is a chain of segments, and only one of them is the bottleneck.
//
// PathModel answers what one endpoint pair is doing, which is the right
// question when every flow crosses the same difficult segment and nothing
// beyond it matters. That is the deployment this project was built for, and on
// it the endpoint pair and the bottleneck are the same thing.
//
// They stop being the same thing as soon as flows go to different places. A
// host calling three services in two regions shares its uplink, its egress
// gateway and the first stretch of backbone with all of them, and shares
// nothing else with any of them. Modelling that as one path per destination
// makes every destination rediscover the shared segment; modelling it as one
// path for everything makes a congested peering point to one region throttle
// traffic to another.
//
// Neither is wrong so much as both are the same mistake at different
// granularities: assuming the answer instead of measuring it. What is actually
// true is a tree rooted at the egress, and a flow's budget is set by the
// tightest node along its own root-to-leaf path.
//
// The tree is built from the node type rather than replacing it. Each node is
// an ordinary PathModel measuring the traffic that crosses it, so a chain of
// one behaves exactly as a single model does today, and every existing caller
// is on that path unchanged.

// Key names where a flow goes, at the three granularities that are cheap to
// know before the flow starts.
//
// Empty fields are skipped rather than treated as a distinct group, so a
// caller that knows only the endpoint pair gets a chain of one and today's
// behaviour exactly.
type Key struct {
	// Egress identifies the local side: the uplink or source interface. It is
	// shared by every flow this host sends, whatever the destination.
	Egress string
	// Group is the destination's region, autonomous system, or prefix --
	// whatever granularity the deployment has reason to believe is shared. The
	// plan this implements bootstraps it statically and refines it by
	// correlating congestion signals; this is the bootstrap.
	Group string
	// Dest is the endpoint pair itself, the most specific granularity and the
	// one the existing model is keyed by.
	Dest string
}

// chainKeys renders the root-to-leaf node keys, most general first. Each level
// includes the levels above it so that two destinations in different groups
// never share a leaf key.
func (k Key) chainKeys() []string {
	var out []string
	if k.Egress != "" {
		out = append(out, "egress|"+k.Egress)
	}
	if k.Group != "" {
		out = append(out, "group|"+k.Egress+"|"+k.Group)
	}
	if k.Dest != "" {
		out = append(out, "dest|"+k.Egress+"|"+k.Group+"|"+k.Dest)
	}
	return out
}

// confidentSamples is how much evidence a node needs before its estimate may
// constrain a flow.
//
// It is the same hundred the path prewarm sends, chosen for the same reason: a
// loss proportion measured from ten packets has a standard error near fifteen
// points at the rates this project sees, and a hundred brings it under five. A
// node below this bar is not silent -- it still contributes what it has seen to
// the pool -- it simply may not be the reason a flow is slowed down.
const confidentSamples = 100

var (
	treeMu sync.Mutex
	tree   = make(map[string]*PathModel)
)

// NodeReport is one node's state, for the operator asking why a flow is being
// held where it is.
type NodeReport struct {
	Key     string
	Members int
	State   State
	// Binding reports whether this node is the one setting the chain's Share.
	// A hierarchical model that cannot say which segment is responsible is a
	// hierarchical model that cannot be debugged, and the whole reason for
	// building it was to stop assuming the answer.
	Binding bool
}

// InspectTree renders every node the process knows, sorted, for diagnostics.
func InspectTree() []NodeReport {
	treeMu.Lock()
	keys := make([]string, 0, len(tree))
	for k := range tree {
		keys = append(keys, k)
	}
	nodes := make([]*PathModel, len(keys))
	sort.Strings(keys)
	for i, k := range keys {
		nodes[i] = tree[k]
	}
	treeMu.Unlock()

	out := make([]NodeReport, 0, len(keys))
	for i, k := range keys {
		out = append(out, NodeReport{
			Key: k, Members: nodes[i].Members(), State: nodes[i].Current(),
		})
	}
	return out
}

// ForgetTree drops every node whose key begins with prefix, which is how an
// uplink change discards what was measured about the network that is gone
// without discarding what is known about destinations reached another way.
func ForgetTree(prefix string) {
	treeMu.Lock()
	defer treeMu.Unlock()
	for k := range tree {
		if prefix == "" || strings.Contains(k, prefix) {
			delete(tree, k)
		}
	}
}

// ResetTree drops every node. It exists for tests, for the same reason Reset
// does: in a process where every path is loopback, models that could not
// affect each other on a real network share one key here.
func ResetTree() {
	treeMu.Lock()
	defer treeMu.Unlock()
	tree = make(map[string]*PathModel)
}

// Model is what a consumer needs from a path: what is known, and a way to
// contribute to it.
//
// A single node and a chain of them both satisfy it, which is what lets the
// hierarchy be introduced without every consumer learning about hierarchies.
// A consumer asks what it is allowed to do and reports what it saw; whether
// the answer came from one segment or from the tightest of several is the
// model's business.
type Model interface {
	Report(Member, Observation) State
	Current() State
}

var _ Model = (*PathModel)(nil)
