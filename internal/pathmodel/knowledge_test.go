package pathmodel

import (
	"testing"
	"time"
)

// The tree under the path authority is knowledge only (design §11.3): a new
// leaf starts from what its confident ancestors know, its own measurement
// replaces that prior as soon as it is believable, and no share or seed ever
// leaves the view.

func TestKnowledgeGivesANewLeafItsAncestorsPrior(t *testing.T) {
	ResetTree()
	Reset()
	t.Cleanup(func() { ResetTree(); Reset() })
	// A first leaf behind the uplink measures a 14% channel at 200 ms.
	first := SharedKnowledge(Key{Egress: "wan0", Dest: "wan0->peer-a"}, Shared("a"))
	first.Report(1, Observation{Erasure: 0.14, BurstFactor: 1.2, ObservedSamples: 500, Delivered: 0, RoundTrip: 200 * time.Millisecond})

	// A second leaf behind the same uplink has measured nothing.
	second := SharedKnowledge(Key{Egress: "wan0", Dest: "wan0->peer-b"}, Shared("b"))
	got := second.Current()
	if got.Erasure != 0.14 || got.BurstFactor != 1.2 || got.RoundTrip != 200*time.Millisecond {
		t.Fatalf("a new leaf's prior = %+v, want the uplink's 14%%, 1.2 and 200 ms", got)
	}
	if got.Share != 0 || got.Seed != 0 {
		t.Fatalf("a prior carried share %v and seed %v; the authority takes neither (I3)", got.Share, got.Seed)
	}
	// Its own leaf model holds nothing but its own samples.
	if leaf := second.Leaf().Current(); leaf.Erasure != 0 || leaf.ObservedSamples != 0 {
		t.Fatalf("the prior was written into the leaf: %+v", leaf)
	}
}

func TestKnowledgeDropsThePriorOnceTheLeafIsConfident(t *testing.T) {
	ResetTree()
	Reset()
	t.Cleanup(func() { ResetTree(); Reset() })
	lossy := SharedKnowledge(Key{Egress: "wan0", Dest: "wan0->peer-a"}, Shared("a"))
	lossy.Report(1, Observation{Erasure: 0.20, BurstFactor: 2, ObservedSamples: 5000, RoundTrip: 300 * time.Millisecond})

	clean := SharedKnowledge(Key{Egress: "wan0", Dest: "wan0->peer-b"}, Shared("b"))
	// Below the confidence bar the sibling's channel stands in.
	early := clean.Report(2, Observation{Erasure: 0, BurstFactor: 1, ObservedSamples: confidentSamples - 1, RoundTrip: 100 * time.Millisecond})
	if early.Erasure == 0 {
		t.Fatalf("an unconfident leaf ignored a confident ancestor: %+v", early)
	}
	// At the bar the leaf's own clean channel is the answer, whatever the
	// uplink's pooled figure says. A chain would keep the larger erasure for
	// the life of the flow; that is the coupling the authority must not have.
	late := clean.Report(2, Observation{Erasure: 0, BurstFactor: 1, ObservedSamples: confidentSamples, RoundTrip: 100 * time.Millisecond})
	if late.Erasure != 0 || late.BurstFactor != 1 || late.RoundTrip != 100*time.Millisecond {
		t.Fatalf("a confident leaf's view = %+v, want its own 0%%, 1 and 100 ms", late)
	}
	chain := SharedChain(Key{Egress: "wan0", Dest: "wan0->peer-b"})
	if c := chain.Current(); c.Erasure == 0 {
		t.Fatalf("control: the legacy chain over the same nodes should still report the uplink's erasure, got %+v", c)
	}
}

func TestKnowledgeIgnoresUnconfidentAncestors(t *testing.T) {
	ResetTree()
	Reset()
	t.Cleanup(func() { ResetTree(); Reset() })
	a := SharedKnowledge(Key{Egress: "wan0", Dest: "wan0->peer-a"}, Shared("a"))
	a.Report(1, Observation{Erasure: 0.5, BurstFactor: 3, ObservedSamples: 10, RoundTrip: time.Second})
	b := SharedKnowledge(Key{Egress: "wan0", Dest: "wan0->peer-b"}, Shared("b"))
	if got := b.Current(); got.Erasure != 0 || got.RoundTrip != 0 || got.BurstFactor != 1 {
		t.Fatalf("ten samples became a prior: %+v", got)
	}
}

// Ancestors pool every leaf's reports, and the endpoint-pair node is the one
// a legacy chain to the same pair crosses, so knowledge measured under either
// bundle is one node's.
func TestKnowledgeSharesTheLegacyChainsNodes(t *testing.T) {
	ResetTree()
	Reset()
	t.Cleanup(func() { ResetTree(); Reset() })
	k := Key{Egress: "wan0", Dest: "wan0->peer-a"}
	kn := SharedKnowledge(k, Shared("principal-1|wan0->peer-a"))
	if kn.Ancestors() != 2 {
		t.Fatalf("ancestors = %d, want the uplink and the endpoint pair", kn.Ancestors())
	}
	kn.Report(7, Observation{Erasure: 0.1, BurstFactor: 1, ObservedSamples: 400, RoundTrip: 150 * time.Millisecond})
	chain := SharedChain(k)
	if got := chain.Current(); got.Erasure != 0.1 || got.ObservedSamples != 400 {
		t.Fatalf("the chain's nodes did not hear the authority: %+v", got)
	}
	if got := Shared("principal-1|wan0->peer-a").Current(); got.Erasure != 0.1 {
		t.Fatalf("the leaf did not hear its own report: %+v", got)
	}
	if SharedKnowledge(k, nil) != nil {
		t.Fatal("a view without a leaf")
	}
}
