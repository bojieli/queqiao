package pep

import (
	"time"

	"golang.org/x/sys/unix"
)

// Darwin's RAW monotonic clock includes sleep; UPTIME_RAW (also used by
// Go's monotonic clock) excludes it. Neither follows wall-clock adjustments.
func systemSuspendTime() (time.Duration, error) {
	var continuous, awake unix.Timespec
	if err := unix.ClockGettime(unix.CLOCK_UPTIME_RAW, &awake); err != nil {
		return 0, err
	}
	if err := unix.ClockGettime(unix.CLOCK_MONOTONIC_RAW, &continuous); err != nil {
		return 0, err
	}
	return time.Duration(continuous.Nano() - awake.Nano()), nil
}
