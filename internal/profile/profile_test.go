package profile

import (
	"strings"
	"testing"

	"github.com/bojieli/queqiao/internal/classifier"
)

// The default must stay the profile whose published measurements describe it.
// A deployment that does not choose gets the behaviour the README claims, and
// changing that silently would invalidate every result in the comparison table.
func TestDefaultIsTheSupportedAccessLinkProfile(t *testing.T) {
	d := Default()
	if d.Name != "wan-shared-bottleneck" {
		t.Fatalf("default profile is %q", d.Name)
	}
	if d.Level != LevelSupported {
		t.Errorf("default profile level is %q, want supported", d.Level)
	}
	if d.Classifier != classifier.DefaultConfig() {
		t.Error("default profile does not carry the documented classifier config")
	}
}

// An unknown name must fail. Falling back to the default would run a different
// policy than the operator asked for, and nothing in the logs would say so --
// which is the precise failure this package exists to prevent.
func TestUnknownProfileIsRefusedAndListsTheAlternatives(t *testing.T) {
	_, err := ByName("dc-longhaul") // plausible typo
	if err == nil {
		t.Fatal("an unknown profile name was accepted")
	}
	for _, want := range Names() {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error does not mention known profile %q: %v", want, err)
		}
	}
}

func TestEmptyNameSelectsTheDefault(t *testing.T) {
	p, err := ByName("")
	if err != nil {
		t.Fatalf("empty name rejected: %v", err)
	}
	if p.Name != Default().Name {
		t.Errorf("empty name gave %q", p.Name)
	}
}

// Every profile has to state a precondition a reader can find false, and name
// the measurements that justify its constants. A profile without either is a
// set of tuned numbers with no argument behind them, which is what the
// admission rule exists to keep out.
func TestEveryProfileStatesItsPreconditionAndEvidence(t *testing.T) {
	for _, name := range Names() {
		p, err := ByName(name)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if len(p.Precondition) < 40 {
			t.Errorf("%s: precondition is too thin to falsify: %q", name, p.Precondition)
		}
		if !strings.HasPrefix(p.Evidence, "docs/") {
			t.Errorf("%s: evidence %q does not name a characterisation document", name, p.Evidence)
		}
		if p.Classifier.BulkBytes == 0 || p.Classifier.NewBytes == 0 {
			t.Errorf("%s: incomplete classifier config would silently fall back", name)
		}
	}
}

// The datacenter profile's whole purpose is that one request is not a
// transfer. If its threshold ever drops back to the access-link value, the
// regression this profile was created to fix returns.
func TestDatacenterProfileDoesNotCallOneRequestBulk(t *testing.T) {
	p, err := ByName("dc-long-haul")
	if err != nil {
		t.Fatal(err)
	}
	if p.Level != LevelExperimental {
		t.Errorf("dc-long-haul level is %q; it has been qualified on one path", p.Level)
	}
	if p.Classifier.BulkBytes <= 8<<20 {
		t.Errorf("BulkBytes is %d: a multi-megabyte request would classify as a transfer",
			p.Classifier.BulkBytes)
	}
	if p.Classifier.BulkIdleGapVeto == 0 {
		t.Error("the idle veto is off, so repeated requests can still accumulate into bulk")
	}
	if classifier.DefaultConfig().BulkIdleGapVeto != 0 {
		t.Error("the veto leaked into the access-link default, changing published behaviour")
	}
}
