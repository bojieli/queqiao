package pep

import "testing"

func TestStaleRecoverySnapshotCannotClearNewEpisode(t *testing.T) {
	for _, ids := range [][2]uint64{{0, 0}, {7, 7}, {7, 8}} {
		f := &multipathFlow{}
		f.setRescueReplacement(ids[0])
		old := f.rescueHint.Load()
		f.setRescueReplacement(ids[1])
		current := f.rescueHint.Load()
		f.clearRescueReplacementSnapshot(old)
		if got, ok := f.rescueReplacement(); !ok || got != ids[1] {
			t.Fatal("stale callback cleared newer recovery episode")
		}
		f.clearRescueReplacementSnapshot(current)
		if _, ok := f.rescueReplacement(); ok {
			t.Fatal("current episode did not clear")
		}
	}
}
