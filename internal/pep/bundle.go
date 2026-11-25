package pep

import (
	"fmt"

	"github.com/bojieli/queqiao/internal/profile"
)

// CCAPIVersion selects how a QUIC connection's congestion controller is
// driven. It is the switch of the first migration bundle of the path-authority
// design (docs/DESIGN-PATH-AUTHORITY.md, §14 stage 1).
type CCAPIVersion string

const (
	// CCAPIV1 drives the controller through quic-go's per-packet callbacks:
	// today's behaviour, and the legacy bundle.
	CCAPIV1 CCAPIVersion = "v1"
	// CCAPIV2 drives today's ErasureSender, one per connection, through the
	// fork's PathSender API. It activates exactly three behaviour changes:
	// non-eliciting packets are not debited from the pacer; the controller is
	// kept across a connection migration; exempt obligations (ACK-only
	// packets, PTO probes, liveness PINGs, path validation) have a build path
	// while the connection is window-blocked or paced. The fork's keep-alive
	// schedule, transport progress export and datagram tags are present and
	// unused.
	CCAPIV2 CCAPIVersion = "v2"
)

// dcLongHaulProfile is the profile that stays on today's code throughout the
// migration (§0 decision 9).
const dcLongHaulProfile = "dc-long-haul"

// validateBundle rejects a combination of switches the migration does not
// support. The empty version is the default, v1.
func validateBundle(ccAPI *CCAPIVersion, congestion CongestionControlKind, p profile.Profile) error {
	if *ccAPI == "" {
		*ccAPI = CCAPIV1
	}
	switch *ccAPI {
	case CCAPIV1:
		return nil
	case CCAPIV2:
	default:
		return fmt.Errorf("unsupported cc-api %q (want %s or %s)", *ccAPI, CCAPIV1, CCAPIV2)
	}
	// Stage 1 hosts today's erasure law only. reno is quic-go's own controller
	// and cannot be hosted at all; the other laws plug in with the path
	// controller of a later stage.
	if congestion != CongestionErasure {
		return fmt.Errorf("cc-api=%s requires --congestion=%s, not %q", CCAPIV2, CongestionErasure, congestion)
	}
	if p.Name == dcLongHaulProfile {
		return fmt.Errorf("cc-api=%s is not supported with the %s profile, which stays on the legacy bundle", CCAPIV2, dcLongHaulProfile)
	}
	return nil
}

// Bundle names the switches in effect, for publishing an endpoint's effective
// configuration.
func Bundle(ccAPI CCAPIVersion) string {
	if ccAPI == CCAPIV2 {
		return "cc-api=v2"
	}
	return "legacy"
}
