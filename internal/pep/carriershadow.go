package pep

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apernet/quic-go"
	"github.com/bojieli/queqiao/internal/metrics"
)

// The carrier-liveness observer is the shadow-mode groundwork for stage 8 of
// the path-authority design (docs/DESIGN-PATH-AUTHORITY.md §10, §14 stage 0
// "shadow carrier states", §15 calibration of k_s and k_p). It watches every
// QUIC connection that carries a flow, computes the carrier state the design
// would assign from the connection's transport-progress export (fork change
// 10: newly acknowledged eliciting packets and probe timeouts since the last
// such acknowledgement), and, whenever today's per-flow machinery acts, writes
// down what the design would have done instead.
//
// It is observation only. Nothing it computes is read by a recovery decision,
// a timer or a remedy, and it takes no lock a flow holds: the only state it
// shares with the data path is the connection's own progress snapshot, which
// the fork publishes under its own mutex.
//
// It runs only when configured (--carrier-shadow). Off, every hook below is a
// nil check.

// CarrierShadowConfig configures the carrier-liveness observer.
type CarrierShadowConfig struct {
	// Enabled turns the observer on.
	Enabled bool
	// SuspectPTOs is k_s: probe timeouts without transport progress before a
	// carrier is SUSPECT. Zero selects the default.
	SuspectPTOs uint32
	// ReplacePTOs is k_p: probe timeouts, the progress epoch unchanged since
	// SUSPECT, before the design would promote a verified standby. Zero
	// selects the default.
	ReplacePTOs uint32
	// Interval is how often every tracked carrier is sampled. Zero selects
	// the default; tests shorten it.
	Interval time.Duration
}

const (
	// The defaults only label live states. Calibration does not depend on
	// them: every episode is recorded with the number of probe timeouts it
	// reached, so any k can be evaluated afterwards (§10.2).
	defaultCarrierShadowSuspectPTOs = 2
	defaultCarrierShadowReplacePTOs = 4
	maxCarrierShadowPTOs            = 16
	// The sample interval decides how soon a state is seen, not what the
	// calibration counts: the transport counts every completed episode, with
	// its peak, where the episode ends (carrierProgress.EpisodesReaching), so
	// an episode that opens and closes between two samples, or whose last
	// expiries come after the last sample that saw it, is still counted at
	// the peak it reached.
	defaultCarrierShadowInterval = 50 * time.Millisecond
)

// ValidateCarrierShadow reports whether a pair of thresholds is usable. Zero
// selects a default.
func ValidateCarrierShadow(suspectPTOs, replacePTOs uint32) error {
	cfg := CarrierShadowConfig{SuspectPTOs: suspectPTOs, ReplacePTOs: replacePTOs}
	return cfg.resolve()
}

func (c *CarrierShadowConfig) resolve() error {
	if c.SuspectPTOs == 0 {
		c.SuspectPTOs = defaultCarrierShadowSuspectPTOs
	}
	if c.ReplacePTOs == 0 {
		c.ReplacePTOs = max(defaultCarrierShadowReplacePTOs, c.SuspectPTOs)
	}
	if c.SuspectPTOs > maxCarrierShadowPTOs || c.ReplacePTOs > maxCarrierShadowPTOs {
		return fmt.Errorf("carrier-shadow thresholds must be at most %d probe timeouts", maxCarrierShadowPTOs)
	}
	if c.ReplacePTOs < c.SuspectPTOs {
		return fmt.Errorf("carrier-shadow k_p (%d) must not be below k_s (%d)", c.ReplacePTOs, c.SuspectPTOs)
	}
	if c.Interval <= 0 {
		c.Interval = defaultCarrierShadowInterval
	}
	return nil
}

// carrierProgress is one coherent reading of a carrier's transport progress.
type carrierProgress struct {
	Epoch                uint64
	LastProgress         time.Time
	PTOExpiries          uint32
	ElicitingOutstanding bool
	SmoothedRTT          time.Duration
	// Episodes counts the carrier's completed probe-timeout episodes, and
	// EpisodesReaching[n-1] those whose peak reached n expiries, as the
	// transport counted them when each ended. Both only grow; a transport
	// that does not count them leaves them zero.
	Episodes         uint64
	EpisodesReaching [metrics.CarrierShadowPTOBuckets]uint64
	// LastEpisodePeak is the peak of the last completed episode.
	LastEpisodePeak uint32
}

// carrierProgressSource is a lane transport that is a carrier the observer
// can watch: today, a stream on a QUIC connection.
type carrierProgressSource interface {
	// carrierKey identifies the carrier; lanes on one connection share it.
	// Nil means the transport has no carrier to watch.
	carrierKey() any
	// carrierProgress reads the carrier's progress; ended is the reason the
	// carrier is gone, nil while it lives.
	carrierProgress() (progress carrierProgress, ended error)
}

func (c *quicStreamConn) carrierKey() any {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn
}

func (c *quicStreamConn) carrierProgress() (carrierProgress, error) {
	s := c.conn.ConnectionStats()
	p := carrierProgress{
		Epoch: s.ProgressEpoch, LastProgress: s.LastProgress, PTOExpiries: s.PTOExpiriesSinceProgress,
		ElicitingOutstanding: s.ElicitingOutstanding, SmoothedRTT: s.SmoothedRTT,
		Episodes: s.PTOEpisodes, EpisodesReaching: s.PTOEpisodesReaching, LastEpisodePeak: s.LastPTOEpisodePeak,
	}
	ctx := c.conn.Context()
	if ctx.Err() == nil {
		return p, nil
	}
	if cause := context.Cause(ctx); cause != nil {
		return p, cause
	}
	return p, ctx.Err()
}

// carrierEndState classifies why a carrier ended. An application close on
// either end is a decision, not evidence about the path.
func carrierEndState(ended error) metrics.CarrierShadowState {
	var app *quic.ApplicationError
	if errors.As(ended, &app) || errors.Is(ended, context.Canceled) {
		return metrics.CarrierShadowClosed
	}
	return metrics.CarrierShadowDead
}

func carrierEndReason(ended error) string {
	var (
		idle      *quic.IdleTimeoutError
		reset     *quic.StatelessResetError
		handshake *quic.HandshakeTimeoutError
		app       *quic.ApplicationError
		transport *quic.TransportError
	)
	switch {
	case errors.As(ended, &idle):
		return "idle_timeout"
	case errors.As(ended, &reset):
		return "stateless_reset"
	case errors.As(ended, &handshake):
		return "handshake_timeout"
	case errors.As(ended, &app):
		if app.Remote {
			return "peer_close"
		}
		return "local_close"
	case errors.As(ended, &transport):
		return "transport_error"
	case errors.Is(ended, context.Canceled):
		return "local_close"
	}
	return "other"
}

// carrierSample is one input to the state machine.
type carrierSample struct {
	At       time.Time
	Progress carrierProgress
	Ended    error
}

type carrierEventKind uint8

const (
	carrierTransition carrierEventKind = iota + 1
	carrierEpisodeEnd
)

// Episode outcomes.
const (
	episodeRecovered = "recovered"
	episodeDead      = "dead"
	// episodeClosed is censored: an application closed the carrier before
	// it either progressed or failed, so the episode says nothing about k.
	episodeClosed = "closed"
)

// carrierEvent is what one sample changed.
type carrierEvent struct {
	Kind carrierEventKind
	At   time.Time
	// Transition.
	From, To metrics.CarrierShadowState
	// FalseReplacement: a REPLACE carrier was acknowledged again.
	FalseReplacement bool
	Reason           string
	// Both: the probe timeouts of the current episode (at the transition, or
	// the most it reached when it ended), and how long the carrier had been
	// without transport progress.
	PTOs          uint32
	SinceProgress time.Duration
	Outcome       string
	// BetweenSamples says the episode was counted by the transport, among
	// several that ended between two samples or before the first one; its
	// duration is not known.
	BetweenSamples bool
	// DurationUpperBound says SinceProgress runs to the newest progress, and
	// more than one packet was acknowledged since the last sample: the ACK
	// that ended the episode may have come earlier.
	DurationUpperBound bool
	// PeakLowerBound says PTOs is the last bucket of the transport's
	// histogram, which counts that many or more.
	PeakLowerBound bool
	// Epoch is the carrier's progress epoch after the event. A stall and a
	// later transition with the same epoch belong to one no-progress
	// episode.
	Epoch uint64
}

// carrierMachine is the §10.2 state machine for one carrier, driven by
// progress samples. It is pure: no clock, no I/O.
type carrierMachine struct {
	suspectPTOs, replacePTOs uint32

	started bool
	state   metrics.CarrierShadowState
	epoch   uint64
	// lastProgress is the newest progress time seen, or the first sample's
	// time when the carrier has not progressed yet.
	lastProgress time.Time

	inEpisode  bool
	episodeMax uint32
	// The transport's episode counters at the last sample, so a sample that
	// shows progress also shows every episode that ended since.
	srcEpisodes uint64
	srcReaching [metrics.CarrierShadowPTOBuckets]uint64
	// srcSeeded says the counters were read when the carrier was first
	// watched, before its first sample.
	srcSeeded bool
	// Per-carrier histogram, index n-1 = episodes reaching n expiries.
	reached, recovered, dead [metrics.CarrierShadowPTOBuckets]uint64
	episodes                 uint64
}

func newCarrierMachine(suspectPTOs, replacePTOs uint32) carrierMachine {
	return carrierMachine{suspectPTOs: suspectPTOs, replacePTOs: replacePTOs}
}

func (m *carrierMachine) terminal() bool {
	return m.state == metrics.CarrierShadowDead || m.state == metrics.CarrierShadowClosed
}

func (m *carrierMachine) sinceProgress(now time.Time) time.Duration {
	if m.lastProgress.IsZero() || now.Before(m.lastProgress) {
		return 0
	}
	return now.Sub(m.lastProgress)
}

func (m *carrierMachine) transition(events []carrierEvent, at time.Time, to metrics.CarrierShadowState, reason string, ptos uint32) []carrierEvent {
	from := m.state
	m.state = to
	return append(events, carrierEvent{
		Kind: carrierTransition, At: at, From: from, To: to, Reason: reason, PTOs: ptos,
		FalseReplacement: from == metrics.CarrierShadowReplace && to == metrics.CarrierShadowActive,
		SinceProgress:    m.sinceProgress(at), Epoch: m.epoch,
	})
}

// endEpisode closes the current episode. silent is how long the carrier had
// been without transport progress when the episode was decided.
func (m *carrierMachine) endEpisode(events []carrierEvent, at time.Time, silent time.Duration, outcome string) []carrierEvent {
	if !m.inEpisode {
		return events
	}
	top := int(min(m.episodeMax, metrics.CarrierShadowPTOBuckets))
	for n := 0; n < top; n++ {
		switch outcome {
		case episodeRecovered:
			m.recovered[n]++
		case episodeDead:
			m.dead[n]++
		}
	}
	m.episodes++
	events = append(events, carrierEvent{Kind: carrierEpisodeEnd, At: at, PTOs: m.episodeMax, SinceProgress: silent, Outcome: outcome, Epoch: m.epoch})
	m.inEpisode, m.episodeMax = false, 0
	return events
}

// endRecoveredEpisodes closes, as recovered, every episode the transport
// says ended since the last sample, at the peak the transport counted. The
// episode the samples followed is one of them, with a peak at least what was
// sampled; without transport counters it is closed at its sampled peak.
func (m *carrierMachine) endRecoveredEpisodes(events []carrierEvent, s carrierSample, silent time.Duration, bound bool) []carrierEvent {
	if s.Progress.Episodes <= m.srcEpisodes {
		n := len(events)
		events = m.endEpisode(events, s.At, silent, episodeRecovered)
		for i := n; i < len(events); i++ {
			events[i].DurationUpperBound = bound
		}
		return events
	}
	ended := s.Progress.Episodes - m.srcEpisodes
	var reach [metrics.CarrierShadowPTOBuckets]uint64
	for n := range reach {
		if s.Progress.EpisodesReaching[n] > m.srcReaching[n] {
			reach[n] = s.Progress.EpisodesReaching[n] - m.srcReaching[n]
		}
	}
	m.srcEpisodes, m.srcReaching = s.Progress.Episodes, s.Progress.EpisodesReaching
	for n := range reach {
		counted := uint64(0)
		if m.inEpisode && uint32(n) < m.episodeMax {
			// The followed episode already counted this level as reached.
			counted = 1
		}
		if reach[n] > counted {
			m.reached[n] += reach[n] - counted
		}
		m.recovered[n] += reach[n]
	}
	m.episodes += ended
	m.inEpisode, m.episodeMax = false, 0
	// One event per episode, highest peak first: reach[n-1]-reach[n] of them
	// peaked at exactly n.
	var above uint64 // episodes that reached the next level up
	for i := len(reach) - 1; i >= 0; i-- {
		n := i + 1
		exactly := reach[i] - min(reach[i], above)
		above = reach[i]
		for range exactly {
			event := carrierEvent{Kind: carrierEpisodeEnd, At: s.At, PTOs: uint32(n), Outcome: episodeRecovered, Epoch: s.Progress.Epoch}
			if n == len(reach) {
				// The last bucket is that many or more: one episode's exact
				// peak is the transport's last one.
				if ended == 1 && s.Progress.LastEpisodePeak >= uint32(n) {
					event.PTOs = s.Progress.LastEpisodePeak
				} else {
					event.PeakLowerBound = true
				}
			}
			if ended == 1 && m.started {
				event.SinceProgress, event.DurationUpperBound = silent, bound
			} else {
				event.BetweenSamples = true
			}
			events = append(events, event)
		}
	}
	return events
}

// seed records the transport's episode counters when the carrier starts
// being watched, so its first sample counts episodes that ended since.
func (m *carrierMachine) seed(p carrierProgress) {
	m.srcEpisodes, m.srcReaching, m.srcSeeded = p.Episodes, p.EpisodesReaching, true
}

// countExpiries folds a sample's expiry count into the current episode,
// opening one if needed.
func (m *carrierMachine) countExpiries(pto uint32) {
	if pto == 0 {
		return
	}
	m.inEpisode = true
	if pto > m.episodeMax {
		for n := m.episodeMax; n < min(pto, metrics.CarrierShadowPTOBuckets); n++ {
			m.reached[n]++
		}
		m.episodeMax = pto
	}
}

// observe folds one sample in and returns what changed, appended to events.
func (m *carrierMachine) observe(s carrierSample, events []carrierEvent) []carrierEvent {
	if m.terminal() {
		return events
	}
	if !m.started {
		if m.srcSeeded {
			// Episodes that ended between the carrier being watched and
			// this first sample.
			events = m.endRecoveredEpisodes(events, s, 0, false)
		}
		m.started = true
		m.epoch = s.Progress.Epoch
		m.srcEpisodes, m.srcReaching = s.Progress.Episodes, s.Progress.EpisodesReaching
		m.lastProgress = s.At
		if !s.Progress.LastProgress.IsZero() {
			m.lastProgress = s.Progress.LastProgress
		}
	} else if s.Progress.Epoch > m.epoch {
		// Transport progress: an ack-eliciting packet was newly acknowledged.
		// The episode it ends lasted from the previous progress to this one.
		recoveredAt := s.At
		if !s.Progress.LastProgress.IsZero() {
			recoveredAt = s.Progress.LastProgress
		}
		silent := time.Duration(0)
		if !m.lastProgress.IsZero() && recoveredAt.After(m.lastProgress) {
			silent = recoveredAt.Sub(m.lastProgress)
		}
		// One acknowledged packet since the last sample: the newest
		// progress is the one that ended the episode.
		events = m.endRecoveredEpisodes(events, s, silent, s.Progress.Epoch-m.epoch > 1)
		m.epoch = s.Progress.Epoch
		m.lastProgress = recoveredAt
		if m.state != metrics.CarrierShadowActive {
			events = m.transition(events, s.At, metrics.CarrierShadowActive, "progress", 0)
		}
	}
	if s.Ended != nil {
		to := carrierEndState(s.Ended)
		outcome := episodeDead
		if to == metrics.CarrierShadowClosed {
			outcome = episodeClosed
		}
		// Expiries since the previous sample belong to the episode the end
		// decides, including one that only this terminal sample shows.
		m.countExpiries(s.Progress.PTOExpiries)
		events = m.endEpisode(events, s.At, m.sinceProgress(s.At), outcome)
		return m.transition(events, s.At, to, carrierEndReason(s.Ended), s.Progress.PTOExpiries)
	}
	pto := s.Progress.PTOExpiries
	if pto == 0 {
		return events
	}
	m.countExpiries(pto)
	if m.state == metrics.CarrierShadowActive && pto >= m.suspectPTOs {
		events = m.transition(events, s.At, metrics.CarrierShadowSuspect, "pto", pto)
	}
	// The epoch is unchanged since SUSPECT by construction: progress resets
	// the expiry count, and a sample that saw progress returned to ACTIVE
	// above before counting this episode's expiries.
	if m.state == metrics.CarrierShadowSuspect && pto >= m.replacePTOs {
		events = m.transition(events, s.At, metrics.CarrierShadowReplace, "pto", pto)
	}
	return events
}

// carrierShadow is one endpoint's observer.
type carrierShadow struct {
	cfg     CarrierShadowConfig
	role    string
	logger  *slog.Logger
	metrics *metrics.Registry

	mu       sync.Mutex
	carriers map[any]*shadowCarrier
	running  bool
}

type shadowCarrier struct {
	id      uint64
	src     carrierProgressSource
	tracked time.Time

	mu sync.Mutex
	m  carrierMachine
}

var carrierShadowSeq atomic.Uint64

// newCarrierShadow returns nil when the observer is off. cfg must be resolved.
func newCarrierShadow(cfg CarrierShadowConfig, role string, logger *slog.Logger, registry *metrics.Registry) *carrierShadow {
	if !cfg.Enabled {
		return nil
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &carrierShadow{cfg: cfg, role: role, logger: logger, metrics: registry, carriers: make(map[any]*shadowCarrier)}
}

// carrierObservation is the carrier under one lane at one moment.
type carrierObservation struct {
	tracked  bool
	id       uint64
	state    metrics.CarrierShadowState
	progress carrierProgress
	since    time.Duration
	epoch    uint64
	// at is when the carrier was read; records carry it so a reader can
	// order them by observation rather than by when they were logged.
	at time.Time
}

// live reports whether the observation is of a carrier that has not ended.
func (obs carrierObservation) live() bool {
	return obs.tracked && obs.state != metrics.CarrierShadowDead && obs.state != metrics.CarrierShadowClosed
}

func laneCarrierSource(lane *mpLane) carrierProgressSource {
	if lane == nil {
		return nil
	}
	return frameConnCarrierSource(lane.fc)
}

func frameConnCarrierSource(fc *frameConn) carrierProgressSource {
	if fc == nil {
		return nil
	}
	src, ok := fc.transport().(carrierProgressSource)
	if !ok || src.carrierKey() == nil {
		return nil
	}
	return src
}

// trackAssociation watches the carrier under a UDP association's stream. An
// association has no multipath flow and no lane to bind, so without this a
// carrier that only associations use would never be watched. Carriers are
// tracked once per connection, however many associations and lanes share
// one.
func (o *carrierShadow) trackAssociation(fc *frameConn) {
	if o == nil {
		return
	}
	if src := frameConnCarrierSource(fc); src != nil {
		o.trackLive(src)
	}
}

// track starts watching the carrier under src, once per carrier.
func (o *carrierShadow) track(src carrierProgressSource, progress carrierProgress) *shadowCarrier {
	key := src.carrierKey()
	o.mu.Lock()
	defer o.mu.Unlock()
	if c := o.carriers[key]; c != nil {
		return c
	}
	c := &shadowCarrier{id: carrierShadowSeq.Add(1), src: src, tracked: time.Now(), m: newCarrierMachine(o.cfg.SuspectPTOs, o.cfg.ReplacePTOs)}
	c.m.seed(progress)
	o.carriers[key] = c
	if !o.running {
		o.running = true
		go o.run()
	}
	return c
}

// run samples every tracked carrier until none is left. It exits when the
// last one ends, so an idle endpoint holds no timer.
func (o *carrierShadow) run() {
	ticker := time.NewTicker(o.cfg.Interval)
	defer ticker.Stop()
	var batch []*shadowCarrier
	for range ticker.C {
		batch = batch[:0]
		o.mu.Lock()
		for _, c := range o.carriers {
			batch = append(batch, c)
		}
		o.mu.Unlock()
		for i, c := range batch {
			batch[i] = nil // a retired carrier must not stay reachable from here
			if obs := o.sample(c); obs.state == metrics.CarrierShadowDead || obs.state == metrics.CarrierShadowClosed {
				o.mu.Lock()
				if o.carriers[c.src.carrierKey()] == c {
					delete(o.carriers, c.src.carrierKey())
				}
				o.mu.Unlock()
			}
		}
		o.mu.Lock()
		if len(o.carriers) == 0 {
			o.running = false
			o.mu.Unlock()
			return
		}
		o.mu.Unlock()
	}
}

// sample reads the carrier now, advances its machine, and reports the result.
func (o *carrierShadow) sample(c *shadowCarrier) carrierObservation {
	var buf [4]carrierEvent
	// Reading and applying a snapshot is one step under the carrier's lock,
	// so the sampler and a stall hook cannot apply snapshots out of order.
	c.mu.Lock()
	now := time.Now()
	progress, ended := c.src.carrierProgress()
	events := c.m.observe(carrierSample{At: now, Progress: progress, Ended: ended}, buf[:0])
	obs := carrierObservation{tracked: true, id: c.id, state: c.m.state, progress: progress, since: c.m.sinceProgress(now), epoch: c.m.epoch, at: now}
	var summary *carrierMachine
	if len(events) > 0 && c.m.terminal() {
		copied := c.m
		summary = &copied
	}
	c.mu.Unlock()
	for _, event := range events {
		o.emit(c, event, progress.SmoothedRTT)
	}
	if summary != nil {
		o.logger.Info("carrier shadow carrier ended", "endpoint", o.role, "carrier_id", c.id,
			"state", summary.state.String(), "lifetime", now.Sub(c.tracked),
			"episodes", summary.episodes, "pto_reached", summary.reached[:], "pto_recovered", summary.recovered[:],
			"pto_dead", summary.dead[:], "k_s", o.cfg.SuspectPTOs, "k_p", o.cfg.ReplacePTOs,
			"sample_interval", o.cfg.Interval)
	}
	return obs
}

func (o *carrierShadow) emit(c *shadowCarrier, event carrierEvent, srtt time.Duration) {
	switch event.Kind {
	case carrierTransition:
		o.metrics.CarrierShadowTransition(event.To, event.FalseReplacement)
		o.logger.Info("carrier shadow state", "endpoint", o.role, "carrier_id", c.id, "observed_unix_ms", event.At.UnixMilli(),
			"from", event.From.String(), "to", event.To.String(), "reason", event.Reason,
			"pto_since_progress", event.PTOs, "since_transport_progress", event.SinceProgress,
			"false_replacement", event.FalseReplacement, "transport_epoch", event.Epoch, "srtt", srtt,
			"k_s", o.cfg.SuspectPTOs, "k_p", o.cfg.ReplacePTOs)
	case carrierEpisodeEnd:
		if event.Outcome != episodeClosed {
			o.metrics.CarrierShadowEpisode(event.PTOs, event.Outcome == episodeRecovered)
		}
		// One-expiry episodes are routine tail probes; they are counted in
		// the metrics and in the carrier's closing summary, not logged.
		if event.PTOs >= 2 {
			fields := []any{"endpoint", o.role, "carrier_id", c.id, "observed_unix_ms", event.At.UnixMilli(),
				"max_pto", event.PTOs, "outcome", event.Outcome, "transport_epoch", event.Epoch}
			if event.PeakLowerBound {
				fields = append(fields, "max_pto_lower_bound", true)
			}
			if event.BetweenSamples {
				fields = append(fields, "between_samples", true)
			} else {
				fields = append(fields, "without_transport_progress", event.SinceProgress)
				if event.DurationUpperBound {
					fields = append(fields, "duration_upper_bound", true)
				}
			}
			o.logger.Info("carrier shadow episode", append(fields, "srtt", srtt)...)
		}
	}
}

// observeLane samples the carrier under a lane now. It is the observation a
// stall or a lane failure is compared against.
func (o *carrierShadow) observeLane(lane *mpLane) carrierObservation {
	if o == nil {
		return carrierObservation{}
	}
	src := laneCarrierSource(lane)
	if src == nil {
		return carrierObservation{}
	}
	c, ended := o.trackLive(src)
	if c == nil {
		// The carrier ended before it was ever watched, or was already
		// retired by the sampler: report how it ended without watching it
		// again, so its end is counted once.
		return ended
	}
	return o.sample(c)
}

// trackLive tracks a carrier that is still alive. For one that is not, it
// returns nil and the observation of its end.
func (o *carrierShadow) trackLive(src carrierProgressSource) (*shadowCarrier, carrierObservation) {
	o.mu.Lock()
	c := o.carriers[src.carrierKey()]
	o.mu.Unlock()
	if c != nil {
		return c, carrierObservation{}
	}
	progress, ended := src.carrierProgress()
	if ended != nil {
		return nil, carrierObservation{tracked: true, state: carrierEndState(ended), progress: progress}
	}
	return o.track(src, progress), carrierObservation{}
}

func (o *carrierShadow) flowFields(f *multipathFlow) []any {
	return o.idFields(f.sessionID, f.flowID)
}

func (o *carrierShadow) idFields(sessionID [16]byte, flowID uint64) []any {
	return []any{"endpoint", o.role, "session_id", fmt.Sprintf("%x", sessionID), "flow_id", flowID}
}

// noteJoinAttempt records the client starting one JOIN, with the lane ID the
// gateway will log when it arrives: the pair is the handoff transit J of
// §10.3 step 4, per attempt and per transport.
// ctx names the rescue round the attempt belongs to, if any (noteRescue).
func (o *carrierShadow) noteJoinAttempt(ctx context.Context, sessionID [16]byte, flowID, laneID uint64, kind TransportKind) {
	if o == nil {
		return
	}
	fields := append(o.idFields(sessionID, flowID), "lane_id", laneID, "transport", string(kind))
	if round, ok := ctx.Value(shadowRescueRoundKey{}).(uint64); ok {
		fields = append(fields, "rescue_round", round)
	}
	o.logger.Info("carrier shadow join attempt", fields...)
}

// shadowRescueRoundKey carries a rescue round's identifier to the JOIN
// attempts it makes, so a measurement attributes attempts to rounds rather
// than inferring it from timing.
type shadowRescueRoundKey struct{}

var shadowRescueRounds atomic.Uint64

// shadowRescueTriggerKey names what started a rescue round, as the lane
// manager decided it: a stall request beside a live lane, or the
// replacement of a lost one.
type shadowRescueTriggerKey struct{}

const (
	shadowRescueStall  = "stall"
	shadowRescueOutage = "outage"
)

func withShadowRescueTrigger(ctx context.Context, trigger string) context.Context {
	return context.WithValue(ctx, shadowRescueTriggerKey{}, trigger)
}

// noteJoinAfterEnd records a JOIN that reached the gateway after the flow's
// session had ended, which is refused.
func (o *carrierShadow) noteJoinAfterEnd(sessionID [16]byte, flowID, laneID uint64) {
	if o == nil {
		return
	}
	o.logger.Info("carrier shadow join arrived", append(o.idFields(sessionID, flowID), "lane_id", laneID, "in_outage", false, "session_ended", true)...)
}

// bindLane records which carrier a flow's lane rides, so a measurement can
// put a flow's failure observations beside its carrier's transitions.
func (o *carrierShadow) bindLane(f *multipathFlow, lane *mpLane) {
	if o == nil {
		return
	}
	src := laneCarrierSource(lane)
	if src == nil {
		return
	}
	c, _ := o.trackLive(src)
	if c == nil {
		return
	}
	o.logger.Info("carrier shadow lane bound", append(o.flowFields(f), "lane_id", lane.id, "carrier_id", c.id)...)
}

// designStallRemedy is what §10.4 does about a flow whose credit stopped:
// replace the carrier if the carrier is silent, otherwise move coded bytes to
// the stream and wait.
func designStallRemedy(obs carrierObservation, coded bool) metrics.CarrierShadowRemedy {
	return designStallRemedyFor(carrierShadowClient, obs, coded)
}

// designStallRemedyFor is role-aware: only the client dials or replaces
// (§10.1), so a gateway's carrier evidence is answered by waiting for the
// client's JOIN.
func designStallRemedyFor(role string, obs carrierObservation, coded bool) metrics.CarrierShadowRemedy {
	if role == carrierShadowGateway && obs.tracked && obs.state != metrics.CarrierShadowActive && obs.state != metrics.CarrierShadowClosed {
		return metrics.CarrierShadowAwaitClient
	}
	switch {
	case !obs.tracked:
		return metrics.CarrierShadowUntracked
	case obs.state == metrics.CarrierShadowClosed:
		return metrics.CarrierShadowCarrierClosed
	case obs.state == metrics.CarrierShadowSuspect:
		// SUSPECT dials one standby and moves nothing (§10.2).
		return metrics.CarrierShadowDialStandby
	case obs.state != metrics.CarrierShadowActive:
		return metrics.CarrierShadowReplaceCarrier
	case coded:
		return metrics.CarrierShadowWaitMoveCoded
	}
	return metrics.CarrierShadowWait
}

// designLaneFailureRemedy is what §10.4 does about a stream that failed
// definitively: rebind it on its live carrier, or replace a silent carrier.
func designLaneFailureRemedy(obs carrierObservation) metrics.CarrierShadowRemedy {
	return designLaneFailureRemedyFor(carrierShadowClient, obs)
}

// designLaneFailureRemedyFor is role-aware: a gateway whose stream fails puts
// the flow in its replacement grace and waits for the client (§10.4).
func designLaneFailureRemedyFor(role string, obs carrierObservation) metrics.CarrierShadowRemedy {
	if role == carrierShadowGateway && obs.tracked && obs.state != metrics.CarrierShadowClosed {
		return metrics.CarrierShadowAwaitClient
	}
	switch {
	case !obs.tracked:
		return metrics.CarrierShadowUntracked
	case obs.state == metrics.CarrierShadowClosed:
		return metrics.CarrierShadowCarrierClosed
	case obs.state == metrics.CarrierShadowActive:
		return metrics.CarrierShadowRebindStream
	case obs.state == metrics.CarrierShadowSuspect:
		// The flow becomes unbound and waits for its carrier to recover or
		// be replaced, whichever comes first (§10.3).
		return metrics.CarrierShadowWaitUnbound
	}
	return metrics.CarrierShadowReplaceCarrier
}

// legacyStallRemedy names what today's watchdog does at a stall. On a client
// it demotes the lane and asks the lane manager for a rescue; whether a dial
// follows is the manager's decision (its backoff, a remaining healthy lane),
// so the label is the request, and the dials actually made are the join
// attempts the client logs.
func (o *carrierShadow) legacyStallRemedy(f *multipathFlow) string {
	if f != nil && f.carrierRecovery {
		// recovery=carrier acts as the design does: no remedy for missing
		// credit on a live stream (§10.4).
		return "none"
	}
	if o.role == carrierShadowClient {
		return "demote_and_request_rescue"
	}
	// A gateway cannot dial a client; its stall signal has no listener.
	return "demote"
}

const (
	carrierShadowClient  = "client"
	carrierShadowGateway = "gateway"
)

// noteStall records a watchdog stall episode beside the carrier state. lane is
// the data lane the watchdog demoted; pendingSince is when the stalled work
// was first seen without progress.
func (o *carrierShadow) noteStall(f *multipathFlow, lane *mpLane, pendingSince time.Time, threshold time.Duration, coded, spare bool) {
	if o == nil {
		return
	}
	obs := o.observeLane(lane)
	remedy := designStallRemedyFor(o.role, obs, coded)
	// The transport counts as acknowledging when it acknowledged an eliciting
	// packet within the window the watchdog used to declare the flow silent,
	// or has nothing unacknowledged at all (a flow blocked on credit sends
	// nothing for QUIC to acknowledge).
	acked := obs.live() && (!obs.progress.ElicitingOutstanding || obs.since < threshold)
	o.metrics.CarrierShadowStall(remedy, acked)
	fields := append(o.flowFields(f),
		"carrier_id", obs.id, "carrier_state", carrierStateLabel(obs), "transport_acked", acked,
		"observed_unix_ms", observedMillis(obs),
		"pto_since_progress", obs.progress.PTOExpiries, "since_transport_progress", obs.since,
		"eliciting_outstanding", obs.progress.ElicitingOutstanding, "srtt", obs.progress.SmoothedRTT,
		"transport_epoch", obs.epoch, "design_remedy", remedy.String(), "legacy_remedy", o.legacyStallRemedy(f),
		"escape_coded", coded, "healthy_spare", spare,
		"stall_pending_for", time.Since(pendingSince), "threshold", threshold)
	if lane != nil {
		fields = append(fields, "lane_id", lane.id)
	}
	o.logger.Info("carrier shadow stall", fields...)
}

func observedMillis(obs carrierObservation) int64 {
	if obs.at.IsZero() {
		return time.Now().UnixMilli()
	}
	return obs.at.UnixMilli()
}

func carrierStateLabel(obs carrierObservation) string {
	if !obs.tracked {
		return "untracked"
	}
	return obs.state.String()
}

// noteLaneFailure records a lane failure with the carrier observed just
// before the lane was closed.
func (o *carrierShadow) noteLaneFailure(f *multipathFlow, lane *mpLane, err error, obs carrierObservation) {
	if o == nil {
		return
	}
	remedy := designLaneFailureRemedyFor(o.role, obs)
	o.metrics.CarrierShadowLaneFailure(remedy)
	o.logger.Info("carrier shadow lane failed", append(o.flowFields(f),
		"lane_id", lane.id, "carrier_id", obs.id, "carrier_state", carrierStateLabel(obs),
		"observed_unix_ms", observedMillis(obs),
		"pto_since_progress", obs.progress.PTOExpiries, "since_transport_progress", obs.since, "transport_epoch", obs.epoch,
		"design_remedy", remedy.String(), "healthy_lanes", len(f.healthyLanes()), "error", fmt.Sprint(err))...)
}

// noteOutage records the moment this endpoint starts its replacement grace
// for a flow with no lane, and when that grace ends. This is each side's
// failure-observation time for the recovery deadline: the gateway's grace is
// what a client's JOIN or TCP handoff has to beat.
// id pairs the outage with its end record: both are logged to the
// millisecond, and asynchronously, so neither time nor file order tells an
// outage that ended in the millisecond it started from one still open.
func (o *carrierShadow) noteOutage(f *multipathFlow, id uint64, now time.Time, grace time.Duration) {
	if o == nil {
		return
	}
	o.logger.Info("carrier shadow outage", append(o.flowFields(f), "outage_id", id,
		"observed_unix_ms", now.UnixMilli(), "grace", grace, "deadline_unix_ms", now.Add(grace).UnixMilli())...)
}

func (o *carrierShadow) noteOutageEnd(f *multipathFlow, id uint64, ended, deadline time.Time) {
	if o == nil {
		return
	}
	o.logger.Info("carrier shadow outage ended", append(o.flowFields(f), "outage_id", id,
		"observed_unix_ms", ended.UnixMilli(), "deadline_remaining", deadline.Sub(ended))...)
}

var shadowOutageSeq atomic.Uint64

// noteRescue records the client starting a rescue round: the start of the
// JOIN whose arrival the gateway records with noteJoinArrival.
// It returns ctx naming the round, for the JOIN attempts made under it.
func (o *carrierShadow) noteRescue(ctx context.Context, f *multipathFlow) context.Context {
	if o == nil {
		return ctx
	}
	round := shadowRescueRounds.Add(1)
	trigger, _ := ctx.Value(shadowRescueTriggerKey{}).(string)
	if trigger == "" {
		trigger = "unknown"
	}
	o.logger.Info("carrier shadow rescue started", append(o.flowFields(f),
		"healthy_lanes", len(f.healthyLanes()), "rescue_round", round, "trigger", trigger)...)
	return context.WithValue(ctx, shadowRescueRoundKey{}, round)
}

// noteRescueRequest records the client's watchdog handing a rescue request
// to the lane manager: one per request actually queued, which a stall
// episode may make several of, or none while a round is in flight. at is
// taken before the request was queued, so it orders before any round that
// answers it.
func (o *carrierShadow) noteRescueRequest(f *multipathFlow, at time.Time) {
	if o == nil || o.role != carrierShadowClient {
		return
	}
	o.logger.Info("carrier shadow rescue requested", append(o.flowFields(f), "observed_unix_ms", at.UnixMilli())...)
}

// noteJoinArrival records a JOIN reaching the gateway's admission, with
// whether it arrived inside an outage and how much of the grace was left.
func (o *carrierShadow) noteJoinArrival(f *multipathFlow, laneID uint64, arrived, deadline time.Time) {
	if o == nil {
		return
	}
	fields := append(o.flowFields(f), "lane_id", laneID, "in_outage", !deadline.IsZero(), "observed_unix_ms", arrived.UnixMilli())
	if !deadline.IsZero() {
		fields = append(fields, "deadline_remaining", deadline.Sub(arrived))
	}
	o.logger.Info("carrier shadow join arrived", fields...)
}

// activeCarriers is how many carriers are being watched, for tests.
func (o *carrierShadow) activeCarriers() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.carriers)
}
