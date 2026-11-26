//go:build unix

package pep

import (
	"os"
	"os/exec"
	"syscall"
	"testing"
)

const fdLimitChildEnv = "QUEQIAO_TEST_FD_LIMIT_CHILD"

// TestIdleReaderTestFitsA512DescriptorLimit runs the socket-based idle
// reader test (#169) in a child process limited to 512 open descriptors, the
// limit of some CI runners. At the full iOS scale it kept 200 proxied flows
// open, about 800 descriptors, and failed during setup before it tested
// anything (#174).
func TestIdleReaderTestFitsA512DescriptorLimit(t *testing.T) {
	if os.Getenv(fdLimitChildEnv) != "" {
		t.Skip("already in the descriptor-limited child")
	}
	var limit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &limit); err != nil {
		t.Skipf("getrlimit: %v", err)
	}
	if limit.Max < 512 {
		t.Skipf("hard descriptor limit %d is below 512", limit.Max)
	}
	// The shell lowers both the soft and the hard limit, so the Go runtime
	// in the child cannot raise it again.
	cmd := exec.Command("/bin/sh", "-c", `ulimit -n 512 && exec "$0" -test.run='^TestIdleReadersCannotStarveAnActiveRequestUnderIOSLimits$' -test.count=1 -test.v`, os.Args[0])
	cmd.Env = append(os.Environ(), fdLimitChildEnv+"=1")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("idle reader test under a 512-descriptor limit: %v\n%s", err, out)
	}
}
