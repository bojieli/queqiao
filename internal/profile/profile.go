// Package profile names the deployments this transport is known to fit, and
// carries the policy each one needs.
//
// The project's promise has always been specific: it helps when many flows
// cross one difficult segment whose behaviour has been measured. Widening that
// promise to a second regime is only honest if the second regime is stated as
// precisely as the first, with its own measurements and its own admitted
// limits -- otherwise two sharp claims become one vague one, which is how every
// general-purpose accelerator stopped being able to say who it was for.
//
// A profile is therefore a named bundle of a precondition, the measurements
// that justify it, a release level, and the policy constants that differ. It is
// deliberately not a configuration file of tunables: a deployment picks the
// deployment it is, not each knob independently, and a knob whose value is only
// correct in one regime belongs to that regime rather than to the reader.
package profile

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/bojieli/queqiao/internal/classifier"
)

// Level is how much a profile has been qualified. It exists per profile rather
// than per release so that an experimental regime cannot borrow the confidence
// of a stable one by sharing a version number with it.
type Level string

const (
	// LevelSupported is the deployment the published measurements describe.
	LevelSupported Level = "supported"
	// LevelExperimental means the mechanisms run but the regime has not been
	// qualified across enough paths to claim a result.
	LevelExperimental Level = "experimental"
)

// Profile is one named deployment shape.
type Profile struct {
	Name string
	// Precondition states what must be true for this profile to apply, in a
	// form a reader can check and find false. A profile whose precondition
	// cannot fail is not a precondition.
	Precondition string
	// Evidence names the characterisation document that justifies the
	// constants below. A profile without one has not earned them.
	Evidence string
	Level    Level
	// Classifier is the flow classification policy. It differs between regimes
	// because what counts as bulk differs: on a shared access link, bulk is
	// whatever would starve an interactive flow, and on a datacenter leg
	// carrying only requests there is no bulk to protect anything from.
	Classifier classifier.Config
	// HierarchicalPath models the path as a chain of segments -- the uplink,
	// then the peer -- rather than as one endpoint pair, and lets a flow do
	// only what the tighter segment permits.
	//
	// It is off for the access-link profile because that deployment has one
	// peer, so the two models are identical by construction and the extra node
	// would measure nothing while changing the numbers the published results
	// were taken with.
	HierarchicalPath bool
}

// wanSharedBottleneck is the deployment this project was built for and the one
// its published results describe: a client and a trusted gateway whose shared
// segment is the dominant bottleneck for every flow crossing it.
func wanSharedBottleneck() Profile {
	return Profile{
		Name: "wan-shared-bottleneck",
		Precondition: "many application flows share one client-to-gateway " +
			"segment, and that segment is the dominant bottleneck for all of them",
		Evidence:   "docs/PATH-CHARACTER-20260813.md",
		Level:      LevelSupported,
		Classifier: classifier.DefaultConfig(),
	}
}

// dcLongHaul is a long leg between two hosts the same operator runs, carrying
// request/response traffic whose payloads are hundreds of kilobytes.
//
// Two things differ from the access-link case and both follow from the same
// observation: there is no bulk traffic here to protect anything from. Every
// flow is a latency-critical burst, so a classifier that demotes the largest
// of them is not protecting an interactive flow, it is penalising the only
// kind of flow present. Measured on the China-US datacenter path, that
// demotion cost a factor of two on repeated megabyte requests while every
// other case gained between five and seventeen times.
func dcLongHaul() Profile {
	c := classifier.DefaultConfig()
	// Bulk has to mean something different here, and the thresholds have to say
	// what.
	//
	// On an access link, 128KB is a reasonable place to start suspecting that a
	// flow will starve an interactive one, because the link is small. On a
	// datacenter leg, 128KB is one ordinary request. Bulk on this profile is a
	// checkpoint pull or a model download -- tens of megabytes, sustained over
	// many seconds -- and the thresholds are set so that a five-megabyte
	// inference request cannot be mistaken for one.
	c.BulkBytes = 32 << 20
	c.BulkMinimumAge = 10 * time.Second
	// Thresholds alone would still latch on a long-lived connection that has
	// carried enough requests to add up. A request that goes quiet between
	// bursts is a caller waiting on a response, whatever its running total, so
	// an observed idle disqualifies the flow permanently. On the access-link
	// profile the veto stays off: there a slow bulk transfer still needs to
	// unlock lanes, and its slowness is the path's doing rather than the
	// application's.
	c.BulkIdleGapVeto = 1 * time.Second
	return Profile{
		Name:             "dc-long-haul",
		HierarchicalPath: true,
		Precondition: "both endpoints are operated by the same party, the leg " +
			"between them is long, and every flow on it is a latency-critical " +
			"request rather than a transfer seeking throughput",
		Evidence:   "docs/PATH-CHARACTER-DC-20260826.md",
		Level:      LevelExperimental,
		Classifier: c,
	}
}

var all = map[string]func() Profile{
	"wan-shared-bottleneck": wanSharedBottleneck,
	"dc-long-haul":          dcLongHaul,
}

// Default is the profile a deployment gets when it does not choose, and it is
// the one whose measurements are published. A new regime must be asked for.
func Default() Profile { return wanSharedBottleneck() }

// ByName resolves a profile, listing the alternatives when it cannot. An
// unknown profile is an error rather than a fallback to the default: silently
// running a different policy than the operator named is the failure this
// package exists to prevent.
func ByName(name string) (Profile, error) {
	if strings.TrimSpace(name) == "" {
		return Default(), nil
	}
	f, ok := all[name]
	if !ok {
		return Profile{}, fmt.Errorf("unknown profile %q; known profiles are %s",
			name, strings.Join(Names(), ", "))
	}
	return f(), nil
}

// Names lists every profile, sorted, for help text and error messages.
func Names() []string {
	out := make([]string, 0, len(all))
	for k := range all {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
