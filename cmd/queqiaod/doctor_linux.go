//go:build linux

package main

import (
	"fmt"

	"github.com/bojieli/queqiao/internal/profile"
)

// hostChecks reports the kernel settings the measurements assumed.
//
// tcp_slow_start_after_idle is the one that matters most and it is the one
// nothing else will tell you about. Linux defaults it to 1, which throws the
// congestion window away after any idle gap longer than a retransmission
// timeout, so a connection held open across a pause is open without being
// warm. Measured on the path this profile was built for, a 300KB burst on an
// idle connection took 941ms with the default and 209ms without it, and a real
// inference upload went from 789.9ms to 225.8ms.
func hostChecks(p profile.Profile) []check {
	out := make([]check, 0, 2)
	if value, err := readSysctl("net/ipv4/tcp_slow_start_after_idle"); err != nil {
		out = append(out, check{Name: "tcp_slow_start_after_idle", Status: "warn", Detail: err.Error()})
	} else if value == "0" {
		out = append(out, check{Name: "tcp_slow_start_after_idle", Status: "pass",
			Detail: "0, so a held-open connection keeps its window across a pause"})
	} else {
		out = append(out, check{Name: "tcp_slow_start_after_idle", Status: "warn",
			Detail: "1, the kernel default. Every idle gap past an RTO throws the congestion " +
				"window away, so a connection held open across a pause is open without being " +
				"warm. Measured worth 4.5x on a 300KB burst. Set net.ipv4.tcp_slow_start_after_idle=0"})
	}
	if value, err := readSysctl("net/ipv4/tcp_congestion_control"); err == nil {
		out = append(out, check{Name: "tcp_congestion_control", Status: "pass",
			Detail: fmt.Sprintf("%s, for traffic that does not go through the tunnel", value)})
	}
	return out
}
