package pep

import "fmt"

// CodedRetryMode selects the coded-retry bundle of the path-authority
// migration (docs/DESIGN-PATH-AUTHORITY.md §8.1, §14 stage 4).
//
// Under CodedRetrySticky a chunk sent on coded datagrams is the code's to
// repair only until a deadline, and that deadline starts when the datagram
// carrying the chunk's last source symbol is registered in a packet, not when
// the frame is queued: the coded path's queue and the connection's datagram
// queue are not the path, and nothing is lost while waiting in them. A chunk
// whose deadline passes, whose datagrams are discarded, or that the peer's
// ranges show missing is retried on the flow's stream, and every later
// attempt of that chunk stays there (the sticky reliable bit). The stall
// watchdog's byte frontier remains beside it until stage 11.
//
// It needs the datagram outcomes of the cc-api=v2 fork API, so it requires
// cc-api=v2.
type CodedRetryMode string

const (
	// CodedRetryLegacy is today's behaviour: a coded attempt's deadline counts
	// from dispatch, a retry is coded again unless the stall watchdog pinned
	// its bytes to the stream.
	CodedRetryLegacy CodedRetryMode = "legacy"
	// CodedRetrySticky is the stage-4 bundle.
	CodedRetrySticky CodedRetryMode = "sticky"
)

// ParseCodedRetryMode accepts the configuration spelling of the bundle. The
// empty string is the legacy default.
func ParseCodedRetryMode(value string) (CodedRetryMode, error) {
	switch CodedRetryMode(value) {
	case "", CodedRetryLegacy:
		return CodedRetryLegacy, nil
	case CodedRetrySticky:
		return CodedRetrySticky, nil
	}
	return "", fmt.Errorf("coded-retry bundle must be %s or %s, not %q", CodedRetryLegacy, CodedRetrySticky, value)
}

// ValidateCodedRetry reports whether the coded-retry bundle may run with the
// given controller API. It normalises nothing; ccAPI empty is v1.
func ValidateCodedRetry(mode CodedRetryMode, ccAPI CCAPIVersion) error {
	parsed, err := ParseCodedRetryMode(string(mode))
	if err != nil {
		return err
	}
	if parsed == CodedRetrySticky && ccAPI != CCAPIV2 {
		return fmt.Errorf("coded-retry=%s requires cc-api=%s", CodedRetrySticky, CCAPIV2)
	}
	return nil
}

// resolveCodedRetry normalises a configured mode and checks it against the
// (already normalised) controller API.
func resolveCodedRetry(mode *CodedRetryMode, ccAPI CCAPIVersion) error {
	parsed, err := ParseCodedRetryMode(string(*mode))
	if err != nil {
		return err
	}
	if err := ValidateCodedRetry(parsed, ccAPI); err != nil {
		return err
	}
	*mode = parsed
	return nil
}
