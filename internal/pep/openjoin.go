package pep

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// A client writes OPEN and starts sending at once, before the gateway has
// dialled the destination and said OPEN_OK. If the lane carrying that OPEN dies
// first, the replacement JOIN can reach a gateway that never saw the OPEN, or
// one still dialling. The gateway answers the second case by holding the JOIN
// until the dial is decided. The first case still gets "unknown session", and
// these are how the client tells it from a session the gateway dropped
// (issue #159).
//
// The flow is not opened again. A missing OPEN_OK does not prove the
// destination never saw the application's bytes -- the gateway may have
// admitted the OPEN and then lost the session, or another gateway instance
// may have taken it -- and nothing on the wire today lets the client prove
// otherwise, so a replay could deliver a request twice.

// errLaneJoinOpenUnadmitted is a JOIN refused as an unknown session while the
// flow's own OPEN was still unconfirmed. It is a permanent refusal of the JOIN,
// but not evidence that a session was lost: the peer never had one.
var errLaneJoinOpenUnadmitted = fmt.Errorf("%w: flow open was never admitted", errLaneJoinRejected)

// errLaneJoinOpenFailed is a JOIN the gateway held for a pending OPEN that
// then failed, such as an unreachable destination. Nothing can be resumed or
// reopened.
var errLaneJoinOpenFailed = fmt.Errorf("%w: flow open failed", errLaneJoinRejected)

// errLaneJoinPending is the gateway still dialling the destination of the
// OPEN this JOIN names. It is transient, and unlike the lane ceiling it says
// nothing against trying the same JOIN again shortly.
var errLaneJoinPending = errors.New("lane join refused: session still opening")

// sessionPendingAnswer is the gateway's text for that refusal.
const sessionPendingAnswer = "session pending"

// errOpenFailed ends a flow whose OPEN the gateway saw fail, as a JOIN that
// waited on it was told.
var errOpenFailed = errors.New("peer could not open this flow")

// errOpenNotAdmitted ends a flow whose OPEN the peer never admitted. It is reported apart from errResumeRefused,
// whose session the peer did hold and dropped.
var errOpenNotAdmitted = errors.New("peer never admitted this flow's open")

// sessionEndedAnswer is how a gateway that held a session says it has ended.
const sessionEndedAnswer = "session ended"

// classifyUnadmittedRefusal marks a plain session refusal that arrived before
// OPEN_OK. A refusal with a code of its own -- capacity, TCP mode, a failed
// open -- keeps its meaning, and so does a gateway saying it held the session
// and it ended.
func classifyUnadmittedRefusal(flow *multipathFlow, err error) error {
	if err == nil || !flow.openConfirmationRequired.Load() || !errors.Is(err, errLaneJoinRejected) ||
		errors.Is(err, errLaneJoinTCPMode) || errors.Is(err, errLaneJoinOpenFailed) || errors.Is(err, errLaneJoinOpenUnadmitted) {
		return err
	}
	// Only the plain "unknown session" answer says the session is not
	// there; other protocol refusals are about the JOIN itself.
	if text := err.Error(); !strings.Contains(text, "unknown session") || strings.Contains(text, sessionEndedAnswer) {
		return err
	}
	return fmt.Errorf("%w (%w)", errLaneJoinOpenUnadmitted, err)
}

// errSessionExpiredWhileSuspended ends a flow whose session the peer dropped
// while this host was asleep for longer than the peer holds a silent session.
// Nothing went wrong on the path or at the gateway: the gateway saw its lanes
// go quiet, waited out its replacement grace, and ended the flow, and the
// suspended host did not notice until it woke and asked to resume. It is
// reported apart from errResumeRefused so these are not counted as a
// gateway losing sessions (issue #159, route C).
var errSessionExpiredWhileSuspended = errors.New("peer dropped this session while this host was suspended")

// suspendedSessionHold is the longest a gateway keeps a flow whose lanes have
// all gone silent: it first has to notice the silence, then waits out its
// whole replacement grace. A host suspended for longer than this comes back
// to sessions that may be gone.
const suspendedSessionHold = laneDeadPathDetection + laneReplacementWait

// notePeerHeard stamps the arrival of a frame from the peer.
func (f *multipathFlow) notePeerHeard() {
	f.peerClockOffset.Store(f.clockOffset(time.Now()))
}

// clockOffset is the wall clock minus the monotonic time since started.
func (f *multipathFlow) clockOffset(now time.Time) int64 {
	return now.UnixNano() - int64(now.Sub(f.started))
}

// suspendedFor is how long this host was suspended since the peer was last
// heard from. Wall-clock time keeps running while a host sleeps and the
// monotonic clock that time.Since reads does not, on Linux, Android, macOS and
// iOS alike, so their difference over that interval is the sleep. Sleeps
// before the peer was last heard from are left out: the peer evidently still
// held the session after them. A wall clock stepped backwards reads as none,
// and one stepped forwards by more than a minute reads as a sleep.
func (f *multipathFlow) suspendedFor() time.Duration {
	if f.started.IsZero() {
		return 0
	}
	if gap := time.Duration(f.clockOffset(time.Now()) - f.peerClockOffset.Load()); gap > 0 {
		return gap
	}
	return 0
}

// resumeRefusal is the error a flow fails with once the peer has refused to
// resume it.
func (f *multipathFlow) resumeRefusal() error {
	if f.openFailed.Load() {
		return errOpenFailed
	}
	if f.openUnadmitted.Load() {
		return errOpenNotAdmitted
	}
	if f.suspendedFor() >= suspendedSessionHold {
		return errSessionExpiredWhileSuspended
	}
	return errResumeRefused
}

// confirmOpenByJoin treats an admitted JOIN as the flow's open confirmation.
// The JOIN's OPEN_OK proves the session exists, and the OPEN's own may never
// come: the stream that would carry it can be the one that died.
func (f *multipathFlow) confirmOpenByJoin() {
	f.openConfirmMu.Lock()
	defer f.openConfirmMu.Unlock()
	if f.openConfirmationRequired.CompareAndSwap(true, false) {
		f.openConfirmedByJoin.Store(true)
	}
}

// noteRefusalCause records what a terminal JOIN refusal said about the
// flow's OPEN, for the error the flow then fails with.
func (f *multipathFlow) noteRefusalCause(err error) {
	switch {
	case errors.Is(err, errLaneJoinOpenFailed):
		f.openFailed.Store(true)
	case errors.Is(err, errLaneJoinOpenUnadmitted):
		f.openUnadmitted.Store(true)
	}
}
