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

// CongestionScope selects what one congestion controller governs. It is the
// switch of the sixth migration bundle (§14 stage 6, congestion=path), which
// requires the first two.
type CongestionScope string

const (
	// CongestionScopeConnection gives every QUIC connection its own
	// controller, coordinated through the shared path model's shares and
	// seeds. Today's behaviour.
	CongestionScopeConnection CongestionScope = "connection"
	// CongestionScopePath gives each path and sending direction one
	// controller, a PathController (§4), which every connection on the path
	// attaches to and asks to admit each packet. It owns the rate, the
	// window, the erasure estimate, the minimum round trip and class
	// arbitration; nothing is divided, so there are no shares, seeds or
	// ramps. It works with isolation on: a flow's isolated or pooled
	// connections, and an old client's, are attachments to the same
	// controller. It requires cc-api=v2 and sched=class.
	CongestionScopePath CongestionScope = "path"
)

// validateCongestionScope rejects a scope the migration does not support
// with the given controller API and scheduler. The empty scope is the
// default, connection.
func validateCongestionScope(scope *CongestionScope, ccAPI CCAPIVersion, sched SchedMode) error {
	if *scope == "" {
		*scope = CongestionScopeConnection
	}
	switch *scope {
	case CongestionScopeConnection:
		return nil
	case CongestionScopePath:
	default:
		return fmt.Errorf("unsupported congestion scope %q (want %s or %s)", *scope, CongestionScopeConnection, CongestionScopePath)
	}
	if ccAPI != CCAPIV2 {
		return fmt.Errorf("congestion=%s requires cc-api=%s", CongestionScopePath, CCAPIV2)
	}
	if sched != SchedClass {
		// The path controller owns class arbitration (§4.3); there is no
		// path controller that leaves packet filling to round robin.
		return fmt.Errorf("congestion=%s requires sched=%s", CongestionScopePath, SchedClass)
	}
	return nil
}

// ValidateCongestionScope reports whether a congestion scope is supported
// with a controller API and scheduler. Check those with ValidateBundle and
// ValidateSched first.
func ValidateCongestionScope(scope CongestionScope, ccAPI CCAPIVersion, sched SchedMode) error {
	if ccAPI == "" {
		ccAPI = CCAPIV1
	}
	if sched == "" {
		sched = SchedLegacy
	}
	return validateCongestionScope(&scope, ccAPI, sched)
}

// dcLongHaulProfile is the hierarchical profile (§11.3). It ran only today's
// code until stage 9; it now runs either the legacy bundle or the whole new
// architecture, and nothing in between (validateProfileBundle).
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
	// dc-long-haul accepts v2 only as part of the whole new architecture,
	// which validateProfileBundle checks once every switch is known.
	return nil
}

// validateProfileBundle rejects a bundle a profile does not support, once
// every switch has been validated and normalised on its own. Only the
// hierarchical dc-long-haul profile restricts the bundle (design §11.3, §14
// stage 9). It runs exactly two:
//
//   - the legacy bundle: every switch off, the per-connection erasure
//     senders reporting to the path tree's chain, and the legacy classifier
//     its thresholds were tuned against;
//   - the new architecture: cc-api=v2, sched=class, congestion=path,
//     coded-retry=sticky and recovery=carrier, and on a client
//     topology=single. One path authority per leaf (the authority's key is
//     the endpoint pair and the sender's identity), the tree used only as
//     knowledge: priors for erasure, burst and round trip while a leaf has not
//     measured enough, never a share, seed or joint limit. classifier=phased
//     is accepted here.
//
// Every intermediate combination is refused: a v2 per-connection sender
// under the chain, a path controller beside per-flow recovery, isolation
// beside a path controller. None of them was designed for the hierarchy and
// none is what the stage-9 gate measures. mem=owned and
// gateway-admission=owned are independent and accepted with either bundle.
// A topology fan-out is refused with recovery=carrier (validateRecovery);
// the profile gets no exception (§7: fan-out stays off until the
// single-connection ceiling is identified).
func validateProfileBundle(p profile.Profile, ccAPI CCAPIVersion, sched SchedMode, scope CongestionScope, codedRetry CodedRetryMode, topology Topology, recovery RecoveryMode, classifierMode ClassifierMode, gateway bool) error {
	if p.Name != dcLongHaulProfile {
		return nil
	}
	if ccAPI != CCAPIV2 {
		if classifierMode == ClassifierPhased {
			// The profile's classifier thresholds (32 MiB, the idle veto)
			// were tuned against the sticky class, and the legacy bundle
			// keeps them exactly.
			return fmt.Errorf("classifier=%s with the %s profile requires its new-architecture bundle (%s)", ClassifierPhased, dcLongHaulProfile, dcBundleSpelling(gateway))
		}
		return nil
	}
	whole := sched == SchedClass && scope == CongestionScopePath && codedRetry == CodedRetrySticky && recovery == RecoveryCarrier
	if !gateway {
		whole = whole && topology == TopologySingle
	}
	if !whole {
		return fmt.Errorf("the %s profile runs the legacy bundle or its whole new-architecture bundle (%s), nothing in between", dcLongHaulProfile, dcBundleSpelling(gateway))
	}
	return nil
}

// dcBundleSpelling names dc-long-haul's new-architecture bundle in flags.
func dcBundleSpelling(gateway bool) string {
	s := "--cc-api=v2 --sched=class --congestion-scope=path --coded-retry=sticky --recovery=carrier"
	if !gateway {
		s += " --topology=single"
	}
	return s
}

// ValidateProfileBundle reports whether a profile supports a client's
// (gateway false) or gateway's (gateway true) whole bundle. Check every
// switch on its own first; empty values are their defaults.
func ValidateProfileBundle(p profile.Profile, ccAPI CCAPIVersion, sched SchedMode, scope CongestionScope, codedRetry CodedRetryMode, topology Topology, recovery RecoveryMode, classifierMode ClassifierMode, gateway bool) error {
	if ccAPI == "" {
		ccAPI = CCAPIV1
	}
	if sched == "" {
		sched = SchedLegacy
	}
	if scope == "" {
		scope = CongestionScopeConnection
	}
	if codedRetry == "" {
		codedRetry = CodedRetryLegacy
	}
	if topology == "" && !gateway {
		topology = TopologyIsolated
	}
	if recovery == "" {
		recovery = RecoveryFlow
	}
	if classifierMode == "" {
		classifierMode = ClassifierLegacy
	}
	return validateProfileBundle(p, ccAPI, sched, scope, codedRetry, topology, recovery, classifierMode, gateway)
}

// ValidateBundle reports whether a combination of switches is supported.
func ValidateBundle(ccAPI CCAPIVersion, congestion CongestionControlKind, p profile.Profile) error {
	return validateBundle(&ccAPI, congestion, p)
}

// Bundle names the switches in effect, for publishing an endpoint's effective
// configuration: "legacy" when every switch is off. admission is a gateway's
// (stage 5, independent of the others); a client passes the empty value.
// sched is the class scheduler (stage 2); classifierMode is the
// flow-classifier bundle; scope is the path controller switch (stage 6);
// topology is a client's placement of flows on connections (stage 7), and a
// gateway passes the empty topology; recovery is the recovery owner (stage
// 8).
func Bundle(ccAPI CCAPIVersion, memory MemoryMode, codedRetry CodedRetryMode, admission GatewayAdmission, sched SchedMode, classifierMode ClassifierMode, scope CongestionScope, topology Topology, recovery RecoveryMode) string {
	var on []string
	if ccAPI == CCAPIV2 {
		on = append(on, "cc-api=v2")
	}
	if sched == SchedClass {
		on = append(on, "sched=class")
	}
	if scope == CongestionScopePath {
		on = append(on, "congestion=path")
	}
	if topology == TopologySingle {
		on = append(on, "topology=single")
	}
	if recovery == RecoveryCarrier {
		on = append(on, "recovery=carrier")
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
	if classifierMode == ClassifierPhased {
		on = append(on, "classifier=phased")
	}
	if len(on) == 0 {
		return "legacy"
	}
	return strings.Join(on, ",")
}

// Topology selects how a client places its flows on QUIC connections. It is
// the switch of the seventh migration bundle (§7, §14 stage 7,
// topology=single), which requires the sixth.
type Topology string

const (
	// TopologyIsolated is today's placement: flows open as streams on the
	// pooled connection, and a bulk flow that shares it with another flow
	// moves its data to a secondary connection of its own from the bounded
	// bulk pool (reactive isolation).
	TopologyIsolated Topology = "isolated"
	// TopologySingle keeps every flow a stream on one preferred QUIC
	// connection per path; nothing isolates. Priority between flows is the
	// path controller's class arbitration (§4.3), executed by the packet
	// builder, not a second connection and controller. Extra connections
	// exist only during a handover (a replacement generation), for per-flow
	// rescue, which still dials, and for an explicit fan-out
	// (ClientConfig.TopologyFanOut); each is an attachment to the same
	// PathController. It requires congestion=path, and the pooled connection.
	TopologySingle Topology = "single"
)

// maxTopologyFanOut bounds an explicit fan-out: the preferred connection
// plus at most every slot of the bulk pool.
const maxTopologyFanOut = 1 + isolatedBulkConns

// validateTopology rejects a topology the migration does not support with
// the given congestion scope, and a fan-out that is not an explicit request
// on top of topology=single. The empty topology is the default, isolated; a
// fan-out of zero or one is off.
func validateTopology(topology *Topology, scope CongestionScope, fanOut int) error {
	if *topology == "" {
		*topology = TopologyIsolated
	}
	switch *topology {
	case TopologyIsolated:
		if fanOut > 1 {
			// Isolation already spreads bulk flows over the bulk pool;
			// a fan-out is a policy of the single topology only.
			return fmt.Errorf("topology fan-out requires topology=%s", TopologySingle)
		}
	case TopologySingle:
		if scope != CongestionScopePath {
			// Without one authority per path the flows' isolated
			// connections are what keep bulk out of the interactive
			// congestion window; removing them needs the path
			// controller's class arbitration (§4.3, §7).
			return fmt.Errorf("topology=%s requires congestion=%s", TopologySingle, CongestionScopePath)
		}
	default:
		return fmt.Errorf("unsupported topology %q (want %s or %s)", *topology, TopologyIsolated, TopologySingle)
	}
	if fanOut < 0 || fanOut > maxTopologyFanOut {
		return fmt.Errorf("topology fan-out must be between 0 and %d connections per path", maxTopologyFanOut)
	}
	return nil
}

// ValidateTopology reports whether a client topology and fan-out are
// supported with a congestion scope. Check the scope with
// ValidateCongestionScope first.
func ValidateTopology(topology Topology, scope CongestionScope, fanOut int) error {
	if scope == "" {
		scope = CongestionScopeConnection
	}
	return validateTopology(&topology, scope, fanOut)
}

// RecoveryMode selects who owns QUIC recovery. It is the switch of the
// eighth migration bundle (§10, §14 stage 8, recovery=carrier), which
// requires the seventh and the fourth.
type RecoveryMode string

const (
	// RecoveryFlow is today's recovery: every flow's lane manager rescues its
	// own lanes, with a dial+JOIN race (per-flow QUIC rescue dials) when its
	// stall watchdog demotes a lane or its last lane fails, and the pooled
	// connection is superseded on a packet-count silence heuristic.
	RecoveryFlow RecoveryMode = "flow"
	// RecoveryCarrier makes the client's carrier manager the single owner of
	// carrier bindings and QUIC replacement (§10.2-§10.4): a carrier is
	// replaced only on its own transport evidence (probe timeouts without
	// progress), by promoting one verified standby; a flow whose stream
	// failed is rebound by one JOIN on the eligible carrier, the same one if
	// it is live; missing flow credit on a live stream only moves coded
	// bytes to the stream. Per-flow QUIC rescue dials and lane demotion are
	// off, the adaptive keep-alive schedule is on, and every unbound flow
	// carries a binding deadline derived from the gateway's grace. On a
	// gateway it disables lane demotion, activates the keep-alive schedule
	// and binds a verification grant to PROBE replies; the gateway never
	// dials (§10.1). It requires congestion=path and coded-retry=sticky, and
	// on a client topology=single.
	RecoveryCarrier RecoveryMode = "carrier"
)

// validateRecovery rejects a recovery owner the migration does not support
// with the rest of the bundle. The empty mode is the default, flow. A gateway
// passes the empty topology and no fan-out.
func validateRecovery(recovery *RecoveryMode, scope CongestionScope, codedRetry CodedRetryMode, topology Topology, fanOut int, gateway bool) error {
	if *recovery == "" {
		*recovery = RecoveryFlow
	}
	switch *recovery {
	case RecoveryFlow:
		return nil
	case RecoveryCarrier:
	default:
		return fmt.Errorf("unsupported recovery %q (want %s or %s)", *recovery, RecoveryFlow, RecoveryCarrier)
	}
	if scope != CongestionScopePath {
		// A standby is an attachment to the same path controller, and a
		// suspect carrier's attachment becomes PROBING there (§4.4).
		return fmt.Errorf("recovery=%s requires congestion=%s", RecoveryCarrier, CongestionScopePath)
	}
	if codedRetry != CodedRetrySticky {
		// Missing credit on a live stream moves coded bytes to the stream by
		// the registration deadline and the sticky bit (§8.1, §10.4); the
		// watchdog no longer rescues them.
		return fmt.Errorf("recovery=%s requires coded-retry=%s", RecoveryCarrier, CodedRetrySticky)
	}
	if gateway {
		return nil
	}
	if topology != TopologySingle {
		// The carrier manager owns one carrier per path; isolated bulk
		// connections are per-flow bindings it does not own.
		return fmt.Errorf("recovery=%s requires topology=%s", RecoveryCarrier, TopologySingle)
	}
	if fanOut > 1 {
		// A fan-out's extra connections come from the bulk pool, outside the
		// carrier set (§7 leaves fan-out as a later carrier-manager policy).
		return fmt.Errorf("recovery=%s does not support a topology fan-out", RecoveryCarrier)
	}
	return nil
}

// ValidateRecovery reports whether a recovery owner is supported with the
// rest of a client's (gateway false) or gateway's (gateway true) bundle.
// Check the other switches first.
func ValidateRecovery(recovery RecoveryMode, scope CongestionScope, codedRetry CodedRetryMode, topology Topology, fanOut int, gateway bool) error {
	if scope == "" {
		scope = CongestionScopeConnection
	}
	if topology == "" && !gateway {
		topology = TopologyIsolated
	}
	return validateRecovery(&recovery, scope, codedRetry, topology, fanOut, gateway)
}

// BundleName names a whole migration bundle (design §14). Since stage 10 the
// path-authority bundle is the default of both roles; the legacy bundle stays
// selectable, as one switch, for the release that soaks the flip.
type BundleName string

const (
	// BundlePathAuthority is the stage 1-8 bundle plus classifier=phased:
	// cc-api=v2, sched=class, congestion=path, coded-retry=sticky,
	// recovery=carrier, mem=owned and classifier=phased on both roles,
	// gateway-admission=owned on a gateway and topology=single on a client.
	// It is the default since stage 10.
	BundlePathAuthority BundleName = "path-authority"
	// BundleLegacy is every switch off: the behaviour before the migration.
	// It is the rollback for the soak release and is deleted at stage 11.
	BundleLegacy BundleName = "legacy"
)

// DefaultBundle is the bundle an endpoint runs when nothing selects another.
const DefaultBundle = BundlePathAuthority

// ParseBundleName accepts a bundle's name. The empty name is the default.
func ParseBundleName(value string) (BundleName, error) {
	switch BundleName(strings.ToLower(strings.TrimSpace(value))) {
	case "":
		return DefaultBundle, nil
	case BundlePathAuthority:
		return BundlePathAuthority, nil
	case BundleLegacy:
		return BundleLegacy, nil
	}
	return "", fmt.Errorf("unsupported bundle %q (want %s or %s)", value, BundlePathAuthority, BundleLegacy)
}

// BundleSwitches is the value of every migration switch in one bundle. A
// gateway's Topology is empty; a client's GatewayAdmission is empty.
type BundleSwitches struct {
	CCAPI            CCAPIVersion
	Sched            SchedMode
	CongestionScope  CongestionScope
	CodedRetry       CodedRetryMode
	Memory           MemoryMode
	Classifier       ClassifierMode
	Recovery         RecoveryMode
	Topology         Topology
	GatewayAdmission GatewayAdmission
}

// SwitchesOf returns a named bundle's switches for a client (gateway false)
// or a gateway (gateway true). Every switch is spelled out, so a caller that
// applies them does not depend on what an empty value means.
func SwitchesOf(name BundleName, gateway bool) (BundleSwitches, error) {
	var s BundleSwitches
	switch name {
	case BundlePathAuthority:
		s = BundleSwitches{
			CCAPI: CCAPIV2, Sched: SchedClass, CongestionScope: CongestionScopePath,
			CodedRetry: CodedRetrySticky, Memory: MemoryOwned, Classifier: ClassifierPhased,
			Recovery: RecoveryCarrier, Topology: TopologySingle, GatewayAdmission: GatewayAdmissionOwned,
		}
	case BundleLegacy:
		s = BundleSwitches{
			CCAPI: CCAPIV1, Sched: SchedLegacy, CongestionScope: CongestionScopeConnection,
			CodedRetry: CodedRetryLegacy, Memory: MemoryLegacy, Classifier: ClassifierLegacy,
			Recovery: RecoveryFlow, Topology: TopologyIsolated, GatewayAdmission: GatewayAdmissionLegacy,
		}
	default:
		return BundleSwitches{}, fmt.Errorf("unsupported bundle %q (want %s or %s)", name, BundlePathAuthority, BundleLegacy)
	}
	if gateway {
		s.Topology = ""
	} else {
		s.GatewayAdmission = ""
	}
	return s, nil
}

// ApplyToClient sets every switch of a client configuration.
func (s BundleSwitches) ApplyToClient(cfg *ClientConfig) {
	cfg.CCAPI, cfg.Sched, cfg.CongestionScope = s.CCAPI, s.Sched, s.CongestionScope
	cfg.CodedRetry, cfg.Memory, cfg.Classifier = s.CodedRetry, s.Memory, s.Classifier
	cfg.Recovery, cfg.Topology = s.Recovery, s.Topology
}

// ApplyToServer sets every switch of a gateway configuration.
func (s BundleSwitches) ApplyToServer(cfg *ServerConfig) {
	cfg.CCAPI, cfg.Sched, cfg.CongestionScope = s.CCAPI, s.Sched, s.CongestionScope
	cfg.CodedRetry, cfg.Memory, cfg.Classifier = s.CodedRetry, s.Memory, s.Classifier
	cfg.Recovery, cfg.GatewayAdmission = s.Recovery, s.GatewayAdmission
}
