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

// errLaneJoinOpenUnconfirmed is a JOIN refused as an unknown session while the
// flow's own OPEN was still unconfirmed. It is a permanent refusal of the JOIN.
// It does not establish what became of the OPEN: most often it never arrived,
// but a gateway can also admit an OPEN and lose the session -- by restarting,
// say -- before the acknowledgement reaches the client.
var errLaneJoinOpenUnconfirmed = fmt.Errorf("%w: flow open was never confirmed", errLaneJoinRejected)

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

// errOpenUnconfirmed ends a flow whose session the peer refused before the
// client ever saw its OPEN confirmed. It is reported apart from
// errResumeRefused, a session the client knows the peer held and dropped.
var errOpenUnconfirmed = errors.New("peer refused this flow before confirming its open")

// sessionEndedAnswer is how a gateway that held a session says it has ended.
const sessionEndedAnswer = "session ended"

// classifyUnconfirmedRefusal marks a plain session refusal that arrived before
// OPEN_OK. A refusal with a code of its own -- capacity, TCP mode, a failed
// open -- keeps its meaning, and so does a gateway saying it held the session
// and it ended.
func classifyUnconfirmedRefusal(flow *multipathFlow, err error) error {
	if err == nil || !flow.openConfirmationRequired.Load() || !errors.Is(err, errLaneJoinRejected) ||
		errors.Is(err, errLaneJoinTCPMode) || errors.Is(err, errLaneJoinOpenFailed) || errors.Is(err, errLaneJoinOpenUnconfirmed) {
		return err
	}
	// Only the plain "unknown session" answer says the session is not
	// there; other protocol refusals are about the JOIN itself.
	if text := err.Error(); !strings.Contains(text, "unknown session") || strings.Contains(text, sessionEndedAnswer) {
		return err
	}
	return fmt.Errorf("%w (%w)", errLaneJoinOpenUnconfirmed, err)
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

// readSuspendTotal reads the host's cumulative suspend time from the system
// suspend clock (boot time minus monotonic time on Linux and Android, the raw
// clocks on Darwin). It ignores wall-clock corrections, and where no such
// clock exists it reads zero, so a sleep is never inferred.
func (f *multipathFlow) readSuspendTotal() int64 {
	read := f.suspendClock
	if read == nil {
		read = systemSuspendTime
	}
	total, err := read()
	if err != nil || total < 0 {
		return -1
	}
	return int64(total)
}

func (f *multipathFlow) samplePeerSuspend() {
	now := time.Now()
	f.peerSuspendTotal.Store(f.readSuspendTotal())
	f.peerSampledAt.Store(int64(now.Sub(f.started)))
	f.peerSampledWall.Store(now.UnixNano())
}

// notePeerHeard records that a frame arrived from the peer. The suspend clock
// is read at most once a second of awake time -- a sleep shorter than that is
// far below the minute that matters here -- and at once on the first frame
// after a sleep. Awake time alone would miss that one: the monotonic clock
// does not run while the host sleeps, so the first frame after waking can
// fall inside the same second as the last one before it, and it is the
// evidence that the session survived the sleep. The wall clock does run, so
// wall time having moved further than awake time is the cheap signal that a
// sleep came between (a wall-clock step only costs an extra sample).
func (f *multipathFlow) notePeerHeard() {
	now := time.Now()
	awake := now.Sub(f.started) - time.Duration(f.peerSampledAt.Load())
	wall := time.Duration(now.UnixNano() - f.peerSampledWall.Load())
	if awake < time.Second && wall-awake < time.Second {
		return
	}
	f.samplePeerSuspend()
}

// suspendedFor is how long this host was suspended since the peer was last
// heard from. Sleeps before that are left out: the peer evidently still held
// the session after them.
func (f *multipathFlow) suspendedFor() time.Duration {
	then := f.peerSuspendTotal.Load()
	now := f.readSuspendTotal()
	if then < 0 || now < 0 || now <= then {
		return 0
	}
	return time.Duration(now - then)
}

// resumeRefusal is the error a flow fails with once the peer has refused to
// resume it.
func (f *multipathFlow) resumeRefusal() error {
	if f.openFailed.Load() {
		return errOpenFailed
	}
	if f.openUnconfirmed.Load() {
		return errOpenUnconfirmed
	}
	if f.sessionAbsent.Load() && f.suspendedFor() >= suspendedSessionHold {
		return errSessionExpiredWhileSuspended
	}
	return errResumeRefused
}

// confirmOpenByJoin treats an admitted JOIN as the flow's open confirmation.
// The JOIN's OPEN_OK proves the session exists, and the OPEN's own may never
// come: the stream that would carry it can be the one that died.
//
// Every admitted JOIN calls it, confirmed open or not: the JOIN's answer is
// also the peer heard from, which is what a later sleep is measured against.
func (f *multipathFlow) confirmOpenByJoin() {
	f.samplePeerSuspend()
	f.openConfirmMu.Lock()
	defer f.openConfirmMu.Unlock()
	if f.openConfirmationRequired.CompareAndSwap(true, false) {
		f.openConfirmedByJoin.Store(true)
	}
}

// noteRefusalCause records what a terminal JOIN refusal said about the
// flow's OPEN, for the error the flow then fails with.
func (f *multipathFlow) noteRefusalCause(err error) {
	if strings.Contains(err.Error(), "unknown session") {
		f.sessionAbsent.Store(true)
	}
	switch {
	case errors.Is(err, errLaneJoinOpenFailed):
		f.openFailed.Store(true)
	case errors.Is(err, errLaneJoinOpenUnconfirmed):
		f.openUnconfirmed.Store(true)
	}
}
