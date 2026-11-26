package pathmodel

// Knowledge is one path authority's view of the tree: the authority's own
// model as the leaf, with the tree's nodes above it supplying priors and
// nothing else (docs/DESIGN-PATH-AUTHORITY.md §11.3).
//
// A Chain, which the legacy bundle uses, combines its nodes into a limit: the
// tightest node's share caps every member below it, and the largest erasure
// anywhere on the chain sizes compensation and coding for as long as the flow
// lives. Under the path authority neither is allowed. One authority per leaf
// owns the rate (I1), its estimators hold only its own samples (I3), and a
// sibling leaf's channel is not this leaf's. What the tree may still say is
// what the leaf has not measured yet: a second destination reached over the
// same uplink starts from what the first measured about the segment they
// share, and a second identity on an endpoint pair starts from what the first
// measured about that pair.
//
// So the view is the leaf's own state once the leaf has measured enough to be
// believed (confidentSamples), and until then the leaf's state with its
// erasure, burst factor and round trip raised to what the confident ancestors
// know, by the same rule the chain uses for those fields (the largest, since
// a leaf crosses everything its ancestors do). Share and seed are never
// reported: the authority divides nothing and takes no seed.
//
// Every node still hears every report, so the ancestors keep pooling what
// their leaves measure and remain priors for the next leaf.
type Knowledge struct {
	leaf      *PathModel
	ancestors []*PathModel
	keys      []string
}

var _ Model = (*Knowledge)(nil)

// SharedKnowledge returns the knowledge view of an authority whose own model
// is leaf and whose position in the tree is k. The ancestors are the tree's
// shared nodes for k (the same nodes a legacy chain for k crosses), so every
// authority and every legacy lane crossing a segment pools into one node for
// it. A nil leaf is not a view; the caller gets nil.
func SharedKnowledge(k Key, leaf *PathModel) *Knowledge {
	if leaf == nil {
		return nil
	}
	if k.Group != "" {
		k.Group = GroupOf(k.Group)
	}
	keys := k.chainKeys()
	kn := &Knowledge{leaf: leaf, ancestors: make([]*PathModel, 0, len(keys)), keys: keys}
	treeMu.Lock()
	defer treeMu.Unlock()
	for _, key := range keys {
		m, ok := tree[key]
		if !ok {
			m = NewPathModel()
			tree[key] = m
		}
		kn.ancestors = append(kn.ancestors, m)
	}
	return kn
}

// Leaf is the authority's own model.
func (k *Knowledge) Leaf() *PathModel {
	if k == nil {
		return nil
	}
	return k.leaf
}

// Ancestors is how many tree nodes supply priors.
func (k *Knowledge) Ancestors() int {
	if k == nil {
		return 0
	}
	return len(k.ancestors)
}

// Report records the observation at the leaf and at every ancestor, and
// returns the view.
func (k *Knowledge) Report(member Member, o Observation) State {
	if k == nil {
		return State{}
	}
	leaf := k.leaf.Report(member, o)
	ancestors := make([]State, len(k.ancestors))
	for i, n := range k.ancestors {
		ancestors[i] = n.Report(member, o)
	}
	return prior(leaf, ancestors)
}

// Current is the view, without contributing to it.
func (k *Knowledge) Current() State {
	if k == nil {
		return State{}
	}
	ancestors := make([]State, len(k.ancestors))
	for i, n := range k.ancestors {
		ancestors[i] = n.Current()
	}
	return prior(k.leaf.Current(), ancestors)
}

// prior is the leaf's state, with the confident ancestors' erasure, burst
// factor and round trip standing in while the leaf is not itself confident.
// Share and Seed are dropped in every case.
func prior(leaf State, ancestors []State) State {
	out := leaf
	out.Share, out.Seed = 0, 0
	if leaf.ObservedSamples >= confidentSamples {
		return out
	}
	for _, s := range ancestors {
		if s.ObservedSamples < confidentSamples {
			// An ancestor that has not measured enough is no better a prior
			// than the leaf's own few samples.
			continue
		}
		if s.Erasure > out.Erasure {
			out.Erasure = s.Erasure
		}
		if s.BurstFactor > out.BurstFactor {
			out.BurstFactor = s.BurstFactor
		}
		if s.RoundTrip > out.RoundTrip {
			out.RoundTrip = s.RoundTrip
		}
		if s.ObservedSamples > out.ObservedSamples {
			out.ObservedSamples = s.ObservedSamples
		}
	}
	if out.BurstFactor < 1 {
		out.BurstFactor = 1
	}
	return out
}
