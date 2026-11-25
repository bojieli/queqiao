// Package classifier implements the transport-independent flow classifier.
// It intentionally uses observable flow statistics rather than HTTPS
// decryption or a TLS MITM.
package classifier

import (
	"sync"
	"time"
)

// Class is the scheduling class assigned to a flow.
type Class uint8

const (
	ClassNew Class = iota
	ClassInteractive
	ClassBulk
)

func (c Class) String() string {
	switch c {
	case ClassNew:
		return "new"
	case ClassInteractive:
		return "interactive"
	case ClassBulk:
		return "bulk"
	default:
		return "unknown"
	}
}

// Config controls transition thresholds. Values are deliberately explicit so
// deployments can tune them from measurements rather than hiding policy in
// the implementation.
type Config struct {
	NewBytes            uint64
	NewAge              time.Duration
	BulkBytes           uint64
	BulkRateBytesPerSec float64
	BulkMinimumAge      time.Duration
	InteractiveMaxRate  float64
	InteractiveIdleGap  time.Duration
	// BulkIdleGapVeto disqualifies a flow from ever becoming bulk once it has
	// been observed idle for this long, that many times. Zero disables the
	// veto, which is the behaviour every deployment had before it existed.
	//
	// Cumulative bytes cannot separate a bulk transfer from a series of
	// requests on one connection: three 300KB exchanges and a 900KB download
	// weigh the same, and the classifier latches on the first to cross the
	// threshold. Rate cannot separate them either, because a request burst is
	// briefly faster than the bulk floor, not slower.
	//
	// Duty cycle does separate them. A flow seeking throughput does not stop
	// asking for it; one that voluntarily goes quiet for a second and then
	// resumes is a caller waiting on something, whatever its byte total. The
	// veto is sticky for the same reason the bulk decision is sticky: the
	// evidence is about what kind of flow this is, and it does not expire when
	// the next burst starts.
	BulkIdleGapVeto time.Duration
	// BulkIdleGapVetoEpisodes is how many separate idle gaps it takes. Zero
	// means one, which is what the veto did when it was first written.
	//
	// One is too few, and the measurement that says so is that a transfer
	// stalling once anywhere in its first BulkBytes is vetoed for the rest of
	// its life. That window is the beginning of every transfer, which is where
	// a stall is most likely: a cold source, an authorization round trip, a
	// cache miss. On the datacenter profile a 240MB pull that paused for a
	// second-and-a-half at 12MB spent the remaining 228MB classified
	// interactive, which means coded and holding one lane.
	//
	// What separates the two cases is not whether a flow went quiet but
	// whether going quiet is what it does. Two separate episodes is the
	// smallest evidence that idling is a pattern rather than an event. It
	// still protects the case the veto exists for by a wide margin: a session
	// of a few hundred kilobytes per exchange needs roughly ninety exchanges
	// to reach this profile's byte floor, and it produces an episode on the
	// second one.
	BulkIdleGapVetoEpisodes int

	// Phased splits the class into two (the classifier=phased bundle).
	//
	// Class keeps today's meaning: a sticky fact about the flow, "this flow
	// has been a bulk transfer". Lane choice, bulk isolation and pre-warm read
	// it, because re-isolating a flow back and forth costs a lane each way.
	//
	// Current is what the flow is doing now. It leaves BULK once the flow has
	// carried payload with no bulk evidence for BulkExitWindows windows, and re-enters it once the
	// sender has held a large backlog for BulkReenterBacklog (or four smoothed
	// round trips, whichever is longer). The frame's class byte, the lane
	// queue, the aggregate budget's interactive flag and the coding choice
	// read it.
	//
	// Without Phased, Current is Class, which is the legacy bundle.
	Phased bool
	// BulkExitWindow and BulkExitWindows set how a flow that is currently
	// BULK leaves it: after BulkExitWindows distinct windows of BulkExitWindow
	// in which it carried payload and showed no bulk evidence. Evidence is a
	// send backlog over BulkBacklogBytes, or a recent window over the
	// small-exchange rate in either direction, and it restarts the count.
	//
	// A window with no payload neither counts nor resets: an idle gap is not
	// evidence about what the flow is, so a transfer whose source stalls is
	// still BULK when it resumes (TestBulkClassDoesNotFlap). Two one-second
	// windows match the exchange window the evidence itself is measured
	// over: slow to leave, so a transfer's short stall does not flap it, and
	// quick enough that a session is INTERACTIVE again two to four seconds
	// after its burst ends. Small writes do not wait for it: the sparse-write
	// rule sends them INTERACTIVE at once.
	BulkExitWindow  time.Duration
	BulkExitWindows int
	// BulkReenterBacklog is how long a flow that has left BULK must hold a
	// send backlog over BulkBacklogBytes, without a break, before it is BULK
	// again. It is the same second a new flow is given before the bulk test
	// can fire (BulkMinimumAge): a prompt or a page that drains inside it is
	// carried as an exchange, and a transfer that does not is a transfer.
	BulkReenterBacklog time.Duration
	// BulkBacklogBytes is the send backlog -- bytes read from the application
	// and not yet acknowledged by the peer -- above which a flow is
	// transferring rather than conversing. It is the small-exchange budget of
	// one window: a conversation is never that far ahead of its peer, and a
	// backlogged transfer holds at least its read-ahead bound, which is larger.
	BulkBacklogBytes uint64
}

func DefaultConfig() Config {
	return Config{
		NewBytes: 64 * 1024,
		NewAge:   3 * time.Second,
		// Demote after a modest byte budget so a path whose single-lane goodput
		// is below the bulk-rate floor can still unlock a pre-warmed independent
		// lane early in a large transfer. Interactive bursts are excluded
		// separately below, and hysteresis keeps a demoted flow in BULK once it
		// crosses this boundary.
		BulkBytes: 128 * 1024,
		// Retained as a configuration/documentation hook for future rate-aware
		// policies. The current classifier does not require a minimum rate: a
		// slow bulk transfer must still be able to unlock additional lanes.
		BulkRateBytesPerSec: 256 * 1024,
		BulkMinimumAge:      1 * time.Second,
		InteractiveMaxRate:  1 * 1024 * 1024,
		InteractiveIdleGap:  250 * time.Millisecond,
		BulkExitWindow:      1 * time.Second,
		BulkExitWindows:     2,
		BulkReenterBacklog:  1 * time.Second,
		BulkBacklogBytes:    64 * 1024,
	}
}

// Observation contains statistics accumulated by the flow observer. Rates
// are bytes per second over a recent, implementation-defined window.
type Observation struct {
	BytesUp, BytesDown       uint64
	UpRate, DownRate         float64
	Age                      time.Duration
	SinceLastPayload         time.Duration
	Bidirectional            bool
	SmallBidirectionalBursts bool
	// SendBacklog is what this endpoint has read from its application and
	// the peer has not yet acknowledged. Only the phased classifier reads it.
	SendBacklog uint64
	// SmoothedRTT is the flow's current round-trip estimate, zero before one
	// exists. Only the phased classifier reads it.
	SmoothedRTT time.Duration
	// CarriedPayload says this observation is of bytes just carried rather
	// than a re-examination. Only the phased classifier reads it.
	CarriedPayload bool
}

// Classifier is stateful. Once a flow becomes bulk it remains bulk until it
// closes; this hysteresis prevents queue policy from flapping during a short
// idle gap in a large transfer.
// A Classifier is read and written from different goroutines: a flow observes
// its own traffic on whichever goroutine carried the bytes, and asks for the
// class wherever a scheduling decision is made. The class is one word, and one
// word is exactly the size of value a race detector finds and a reader gets
// half of.
type Classifier struct {
	// idleEpisodes counts the times this flow has gone quiet long enough to
	// count under BulkIdleGapVeto, and inIdle debounces a single gap that
	// several consecutive observations land inside. Counting observations
	// rather than episodes would make the veto a function of how often the
	// scheduler happened to tick during one pause.
	//
	// Once the count reaches the threshold the conclusion is sticky, in the
	// same way the bulk decision is: both are about what kind of flow this is
	// rather than about its present moment.
	idleEpisodes int
	inIdle       bool
	cfg          Config
	mu           sync.Mutex
	class        Class

	// current is the phased classifier's present-tense class; without Phased
	// it is always class. quietWindows counts the distinct exit windows that
	// carried payload since bulk evidence was last seen, and lastQuietWindow
	// is the latest of them (by index from the flow's start). backlogSince
	// is the age at which the present unbroken send backlog began, and
	// inBacklog says whether there is one.
	current          Class
	quietWindows     int
	firstQuietWindow int64
	lastQuietWindow  int64
	backlogSince     time.Duration
	inBacklog        bool
	// exits and reentries count the phased transitions, for telemetry.
	exits, reentries uint64
}

// Declare sets the class a flow starts in, from something known before it
// carried anything: what produced it.
//
// It is a starting point rather than a promise. The classifier goes on judging
// the flow by what it does, so a process declared interactive that turns out
// to be moving a checkpoint is still demoted -- the declaration buys the first
// second, which is the window inference cannot cover and which a request
// shorter than it spends entirely inside.
//
// Declaring bulk is sticky in the same way an inferred demotion is, because it
// is the same conclusion reached earlier. Declaring interactive is not: it
// says where to begin, not where to stay.
func (c *Classifier) Declare(class Class) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.class == ClassBulk {
		return
	}
	switch class {
	case ClassInteractive, ClassBulk:
		c.class = class
		c.current = class
	}
}

func New(cfg Config) *Classifier {
	if cfg.NewBytes == 0 || cfg.NewAge <= 0 || cfg.BulkBytes == 0 ||
		cfg.BulkRateBytesPerSec <= 0 || cfg.BulkMinimumAge <= 0 {
		phased := cfg.Phased
		cfg = DefaultConfig()
		cfg.Phased = phased
	}
	defaults := DefaultConfig()
	if cfg.BulkExitWindow <= 0 {
		cfg.BulkExitWindow = defaults.BulkExitWindow
	}
	if cfg.BulkExitWindows <= 0 {
		cfg.BulkExitWindows = defaults.BulkExitWindows
	}
	if cfg.BulkReenterBacklog <= 0 {
		cfg.BulkReenterBacklog = defaults.BulkReenterBacklog
	}
	if cfg.BulkBacklogBytes == 0 {
		cfg.BulkBacklogBytes = defaults.BulkBacklogBytes
	}
	return &Classifier{cfg: cfg, class: ClassNew, current: ClassNew}
}

// Class is the sticky class: once BULK, BULK for the life of the flow. Lane
// choice and bulk isolation read it.
func (c *Classifier) Class() Class {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.class
}

// Current is what the flow is doing now. Under the legacy bundle it is Class.
func (c *Classifier) Current() Class {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.current
}

// Phased reports whether this classifier keeps a current class apart from the
// sticky one.
func (c *Classifier) Phased() bool { return c.cfg.Phased }

// PhaseTransitions reports how many times the current class left BULK and
// re-entered it after the flow first became BULK.
func (c *Classifier) PhaseTransitions() (exits, reentries uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.exits, c.reentries
}

// Observe advances the flow class. The caller should call this at a bounded
// cadence (for example, once per scheduler tick), not once per packet.
//
// It returns the current class: under the legacy bundle that is the sticky
// class, under the phased bundle what the flow is doing now.
func (c *Classifier) Observe(o Observation) Class {
	c.mu.Lock()
	defer c.mu.Unlock()
	before := c.class
	c.observeSticky(o)
	if !c.cfg.Phased {
		c.current = c.class
		return c.current
	}
	c.observeCurrent(o, before)
	return c.current
}

// observeCurrent advances the phased classifier's present-tense class. The
// sticky class has already been advanced from the same observation; before is
// what it was until then.
//
// A flow that has never been BULK is whatever the sticky class says, so the
// first second of every flow and the first entry into BULK are exactly the
// legacy bundle's: a download is BULK at the same instant it is today. Only a
// flow that has been BULK can be anything else.
func (c *Classifier) observeCurrent(o Observation, before Class) {
	backlogged := o.SendBacklog > c.cfg.BulkBacklogBytes
	if backlogged {
		if !c.inBacklog {
			c.inBacklog = true
			c.backlogSince = o.Age
		}
	} else {
		c.inBacklog = false
	}
	evidence := backlogged || !o.SmallBidirectionalBursts
	switch {
	case c.class != ClassBulk:
		c.current = c.class
	case before != ClassBulk:
		// Entered BULK on this observation, by the legacy test.
		c.current = ClassBulk
		c.quietWindows = 0
	case c.current == ClassBulk:
		// Leaving needs payload-carrying windows with neither a backlog nor a
		// recent window over the small-exchange rate. A transfer that is
		// backlogged never has one, however slowly its path drains it.
		switch {
		case evidence:
			c.quietWindows = 0
		case o.CarriedPayload:
			window := int64(o.Age / c.cfg.BulkExitWindow)
			if c.quietWindows == 0 {
				c.firstQuietWindow = window
			}
			if c.quietWindows == 0 || window != c.lastQuietWindow {
				c.quietWindows++
				c.lastQuietWindow = window
			}
			// Distinct windows alone could be two writes a millisecond apart
			// on either side of a boundary; the stretch must also span the
			// windows it counts.
			if c.quietWindows >= c.cfg.BulkExitWindows &&
				window-c.firstQuietWindow >= int64(c.cfg.BulkExitWindows) {
				c.current = ClassInteractive
				c.quietWindows = 0
				c.exits++
			}
		}
	default:
		// Re-entering needs the sender to have been behind for a sustained
		// stretch: a prompt or a page that drains within it stays an
		// exchange. Four round trips is the floor on a long path, where a
		// one-second backlog is only a few flights.
		hold := c.cfg.BulkReenterBacklog
		if rtts := 4 * o.SmoothedRTT; rtts > hold {
			hold = rtts
		}
		if c.inBacklog && o.Age-c.backlogSince >= hold {
			c.current = ClassBulk
			c.quietWindows = 0
			c.reentries++
		}
	}
}

// observeSticky advances the sticky class, which is the whole of the legacy
// classifier.
func (c *Classifier) observeSticky(o Observation) {
	if c.class == ClassBulk {
		return
	}
	if c.cfg.BulkIdleGapVeto > 0 {
		if o.SinceLastPayload >= c.cfg.BulkIdleGapVeto {
			if !c.inIdle {
				c.idleEpisodes++
				c.inIdle = true
			}
		} else {
			c.inIdle = false
		}
	}
	if c.isBulk(o) {
		c.class = ClassBulk
		return
	}
	if c.class == ClassNew && o.Age >= c.cfg.NewAge {
		if c.isInteractive(o) || o.BytesUp+o.BytesDown >= c.cfg.NewBytes {
			c.class = ClassInteractive
		}
	}
}

func (c *Classifier) isBulk(o Observation) bool {
	if c.vetoed() {
		return false
	}
	return o.Age >= c.cfg.BulkMinimumAge &&
		o.BytesUp+o.BytesDown >= c.cfg.BulkBytes &&
		(!o.Bidirectional || !o.SmallBidirectionalBursts)
}

// vetoed reports whether this flow has gone quiet often enough to be
// disqualified from bulk.
func (c *Classifier) vetoed() bool {
	if c.cfg.BulkIdleGapVeto <= 0 {
		return false
	}
	return c.idleEpisodes >= c.vetoEpisodes()
}

// vetoEpisodes is the configured episode count, treating zero as one so that a
// deployment that set only the duration keeps the behaviour it had.
func (c *Classifier) vetoEpisodes() int {
	if c.cfg.BulkIdleGapVetoEpisodes <= 0 {
		return 1
	}
	return c.cfg.BulkIdleGapVetoEpisodes
}

func (c *Classifier) isInteractive(o Observation) bool {
	if o.Bidirectional && o.SmallBidirectionalBursts {
		return true
	}
	return o.SinceLastPayload >= c.cfg.InteractiveIdleGap &&
		max(o.UpRate, o.DownRate) <= c.cfg.InteractiveMaxRate
}

func max(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}
