package mobilecore

import (
	"testing"

	"github.com/bojieli/queqiao/internal/pep"
)

// The memory-ownership bundle is an opt-in on mobile, where the shared
// budgets it governs exist. Unknown values are refused and change nothing.
func TestSessionMemoryBundleIsOptIn(t *testing.T) {
	s := &Session{}
	if got := s.memoryBundle(); got != pep.MemoryLegacy {
		t.Fatalf("default bundle %q", got)
	}
	if err := s.SetMemoryBundle("owned"); err != nil {
		t.Fatal(err)
	}
	if err := s.SetMemoryBundle("bogus"); err == nil {
		t.Fatal("unknown bundle accepted")
	}
	if got := s.memoryBundle(); got != pep.MemoryOwned {
		t.Fatalf("bundle after a refused change %q", got)
	}
}
