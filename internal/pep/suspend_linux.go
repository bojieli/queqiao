package pep

import (
	"time"

	"golang.org/x/sys/unix"
)

func systemSuspendTime() (time.Duration, error) {
	var continuous, awake unix.Timespec
	if err := unix.ClockGettime(unix.CLOCK_MONOTONIC, &awake); err != nil {
		return 0, err
	}
	if err := unix.ClockGettime(unix.CLOCK_BOOTTIME, &continuous); err != nil {
		return 0, err
	}
	return time.Duration(continuous.Nano() - awake.Nano()), nil
}
