package pep

import (
	"fmt"
	"strings"

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

// SchedMode selects how a QUIC connection orders the classes of work it sends.
// It is the switch of the second migration bundle (§14 stage 2), which
// requires the first.
type SchedMode string

const (
	// SchedLegacy fills packets as quic-go always has: datagrams first, then
	// control frames, then every stream round-robin. Today's behaviour.
	SchedLegacy SchedMode = "legacy"
	// SchedClass has each connection's controller arbitrate between the
	// CONTROL, INTERACTIVE and BULK classes with a weighted deficit round
	// robin (§4.3), and the packet builder execute its decision. Flow
	// control frames are CONTROL, and a flow's DATA ahead of a waiting
	// control frame is boosted to CONTROL; DATA of NEW and INTERACTIVE flows
	// and the datagram substrate are INTERACTIVE; DATA of BULK flows is BULK.
	// It requires cc-api=v2.
	SchedClass SchedMode = "class"
)

// validateSched rejects a scheduler the migration does not support with the
// given controller API. The empty mode is the default, legacy.
func validateSched(sched *SchedMode, ccAPI CCAPIVersion) error {
	if *sched == "" {
		*sched = SchedLegacy
	}
	switch *sched {
	case SchedLegacy:
		return nil
	case SchedClass:
	default:
		return fmt.Errorf("unsupported sched %q (want %s or %s)", *sched, SchedLegacy, SchedClass)
	}
	if ccAPI != CCAPIV2 {
		// The arbiter lives in the PathSender, and only cc-api=v2 installs
		// one; validateBundle has already checked the rest of v2's bundle.
		return fmt.Errorf("sched=%s requires cc-api=%s", SchedClass, CCAPIV2)
	}
	return nil
}

// ValidateSched reports whether a scheduler is supported with a controller
// API. Check the API with ValidateBundle first.
func ValidateSched(sched SchedMode, ccAPI CCAPIVersion) error {
	if ccAPI == "" {
		ccAPI = CCAPIV1
	}
	return validateSched(&sched, ccAPI)
}

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

// ValidateBundle reports whether a combination of switches is supported.
func ValidateBundle(ccAPI CCAPIVersion, congestion CongestionControlKind, p profile.Profile) error {
	return validateBundle(&ccAPI, congestion, p)
}

// Bundle names the switches in effect, for publishing an endpoint's effective
// configuration: "legacy" when every switch is off. admission is a gateway's
// (stage 5, independent of the others); a client passes the empty value.
// sched is the class scheduler (stage 2).
func Bundle(ccAPI CCAPIVersion, memory MemoryMode, codedRetry CodedRetryMode, admission GatewayAdmission, sched SchedMode) string {
	var on []string
	if ccAPI == CCAPIV2 {
		on = append(on, "cc-api=v2")
	}
	if sched == SchedClass {
		on = append(on, "sched=class")
	}
	if codedRetry == CodedRetrySticky {
		on = append(on, "coded-retry=sticky")
	}
	if memory == MemoryOwned {
		on = append(on, "mem=owned")
	}
	if admission == GatewayAdmissionOwned {
		on = append(on, "gateway-admission=owned")
	}
	if len(on) == 0 {
		return "legacy"
	}
	return strings.Join(on, ",")
}
