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

// Chain is one flow's view of the tree: the nodes it crosses, root first.
type Chain struct {
	nodes []*PathModel
	keys  []string
}

// Nodes is how many segments this chain distinguishes. A chain of one is the
// single-model case.
func (c *Chain) Nodes() int {
	if c == nil {
		return 0
	}
	return len(c.nodes)
}

// Leaf is the most specific node, which is the one a caller wanting today's
// behaviour should use directly.
func (c *Chain) Leaf() *PathModel {
	if c == nil || len(c.nodes) == 0 {
		return nil
	}
	return c.nodes[len(c.nodes)-1]
}

// Report records an observation against every node the flow crosses, and
// returns what the chain as a whole permits.
//
// Every node hears the report because every node carried the traffic. What
// differs is what each is entitled to conclude: the leaf saw the whole path,
// so its erasure is the total, while an ancestor pools across destinations and
// therefore measures only what they have in common.
func (c *Chain) Report(member Member, o Observation) State {
	if c == nil || len(c.nodes) == 0 {
		return State{}
	}
	states := make([]State, len(c.nodes))
	for i, n := range c.nodes {
		states[i] = n.Report(member, o)
	}
	return combine(states)
}

// Current is what the chain already knows, without contributing to it.
func (c *Chain) Current() State {
	if c == nil || len(c.nodes) == 0 {
		return State{}
	}
	states := make([]State, len(c.nodes))
	for i, n := range c.nodes {
		states[i] = n.Current()
	}
	return combine(states)
}

// combine reduces a root-to-leaf sequence of node states to the one a flow
// should act on.
//
// The rules differ per field because the fields mean different things along a
// path, and applying one rule to all of them is how a hierarchy becomes worse
// than no hierarchy.
//
//   - Share and Seed take the minimum. A flow may not exceed the tightest
//     segment it crosses, and the tightest is whichever that turns out to be.
//   - Erasure and BurstFactor take the maximum. Erasure accumulates along a
//     path: a leaf that has seen more than its ancestors is measuring its own
//     tail as well as the shared part, and an ancestor that has seen more than
//     a barely-measured leaf is better evidence about the part they share.
//     Sizing a code for the smaller of the two is how a flow ends up carrying
//     no parity into a channel that erases.
//   - RoundTrip takes the maximum. Each node measures the flows crossing it,
//     and a leaf traverses everything its ancestors do plus its own tail, so
//     the longest is the one that describes this flow's path.
//   - ObservedSamples takes the maximum, because it is a statement about how
//     much is known rather than a constraint.
func combine(states []State) State {
	var out State
	for _, s := range states {
		if s.Erasure > out.Erasure {
			out.Erasure = s.Erasure
		}
		if s.BurstFactor > out.BurstFactor {
			out.BurstFactor = s.BurstFactor
		}
		if s.ObservedSamples > out.ObservedSamples {
			out.ObservedSamples = s.ObservedSamples
		}
		if s.RoundTrip > out.RoundTrip {
			out.RoundTrip = s.RoundTrip
		}
		// A node that has not measured enough may not be the reason a flow is
		// capped. Without this rule the first flow to a new destination is
		// throttled by a leaf that has seen nothing, which is the failure this
		// tree exists to avoid rather than to introduce at a finer grain.
		if s.ObservedSamples < confidentSamples {
			continue
		}
		if s.Share > 0 && (out.Share == 0 || s.Share < out.Share) {
			out.Share = s.Share
		}
		if s.Seed > 0 && (out.Seed == 0 || s.Seed < out.Seed) {
			out.Seed = s.Seed
		}
	}
	if out.BurstFactor < 1 {
		out.BurstFactor = 1
	}
	// A chain that constrained nothing still seeds from whatever any node
	// knows, because a seed is not a ceiling: starting a lane from a
	// well-measured ancestor's rate is strictly better than starting it from
	// zero, even when no node is confident enough to impose a cap.
	if out.Seed == 0 {
		for _, s := range states {
			if s.Seed > 0 && (out.Seed == 0 || s.Seed < out.Seed) {
				out.Seed = s.Seed
			}
		}
	}
	return out
}

var (
	treeMu sync.Mutex
	tree   = make(map[string]*PathModel)
)

// SharedChain returns the chain of models a flow to this key crosses.
//
// Nodes are created on demand and shared by every flow that crosses them,
// which is the whole point: the second destination in a group inherits what
// the first measured about the segment they share.
func SharedChain(k Key) *Chain {
	keys := k.chainKeys()
	if len(keys) == 0 {
		return &Chain{}
	}
	c := &Chain{nodes: make([]*PathModel, 0, len(keys)), keys: keys}
	treeMu.Lock()
	defer treeMu.Unlock()
	for _, key := range keys {
		m, ok := tree[key]
		if !ok {
			m = NewPathModel()
			tree[key] = m
		}
		c.nodes = append(c.nodes, m)
	}
	return c
}

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

// Inspect renders the chain root-first, naming the node that binds.
func (c *Chain) Inspect() []NodeReport {
	if c == nil {
		return nil
	}
	out := make([]NodeReport, 0, len(c.nodes))
	combined := c.Current()
	for i, n := range c.nodes {
		s := n.Current()
		out = append(out, NodeReport{
			Key: c.keys[i], Members: n.Members(), State: s,
			Binding: combined.Share > 0 && s.Share == combined.Share &&
				s.ObservedSamples >= confidentSamples,
		})
	}
	return out
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

var (
	_ Model = (*PathModel)(nil)
	_ Model = (*Chain)(nil)
)
