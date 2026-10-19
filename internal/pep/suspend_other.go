//go:build !darwin && !linux

package pep

import "time"

// Preserve address-based uplink detection where a suspend clock is not
// available. Do not infer sleep from changes to the wall clock.
func systemSuspendTime() (time.Duration, error) { return 0, nil }
