//go:build race

package pep

// workloadRaceEnabled is whether the race detector is on. It slows the
// endpoints several-fold, and not equally between bundles, so the workload
// tests judge their comparative bounds only without it.
const workloadRaceEnabled = true
