package pep

import (
	"fmt"
	"time"

	"github.com/bojieli/queqiao/internal/classifier"
	"github.com/bojieli/queqiao/internal/profile"
	"github.com/bojieli/queqiao/internal/protocol"
	"github.com/bojieli/queqiao/internal/stripe"
)

// ClassifierMode selects the flow-classifier bundle.
//
// Under ClassifierLegacy a flow that becomes BULK is BULK for life: an SSH
// session after one large output, a browser's HTTP/2 connection after a page,
// an LLM call on a reused connection, a messaging connection after a photo.
// Its later small writes are then sent as BULK DATA, uncoded.
//
// ClassifierPhased splits the class in two (workload model R1):
//
//   - the sticky class (everBulk) keeps today's meaning and still decides
//     lane choice, bulk isolation and pre-warm, which are expensive to undo;
//   - the current class (nowBulk) decides the DATA class byte, the lane
//     queue, the aggregate budget's interactive flag and coding. It leaves
//     BULK after two payload-carrying windows with no bulk evidence and
//     re-enters it after a sustained send backlog (classifier.Config);
//   - a sparse write -- at most sparseWriteBytes, onto an empty send queue,
//     within the sparse allowance -- is sent INTERACTIVE and coded whatever
//     the current class says (R1a).
//
// Each endpoint classifies the direction it sends, and no peer reads the
// class byte of DATA, so either end may run either mode: a phased client with
// a legacy gateway gets phased upstream DATA and legacy downstream DATA. Both
// ends must run phased for the downstream half (LLM deltas, SSH echoes,
// pushes). The class byte keeps the three values every peer already accepts.
type ClassifierMode string

const (
	// ClassifierLegacy is today's sticky classifier.
	ClassifierLegacy ClassifierMode = "legacy"
	// ClassifierPhased is the sticky/current split with the sparse-write rule.
	ClassifierPhased ClassifierMode = "phased"
)

// ParseClassifierMode accepts the configuration spelling. The empty string is
// the legacy default.
func ParseClassifierMode(value string) (ClassifierMode, error) {
	switch ClassifierMode(value) {
	case "", ClassifierLegacy:
		return ClassifierLegacy, nil
	case ClassifierPhased:
		return ClassifierPhased, nil
	}
	return "", fmt.Errorf("classifier must be %s or %s, not %q", ClassifierLegacy, ClassifierPhased, value)
}

// ValidateClassifier reports whether the classifier mode may run with the
// given profile. The dc-long-haul profile stays on the legacy bundle
// throughout the migration (design §0 decision 9), and its own classifier
// thresholds (32 MiB, idle veto) were tuned against the sticky class.
func ValidateClassifier(mode ClassifierMode, p profile.Profile) error {
	parsed, err := ParseClassifierMode(string(mode))
	if err != nil {
		return err
	}
	if parsed == ClassifierPhased && p.Name == dcLongHaulProfile {
		return fmt.Errorf("classifier=%s is not supported with the %s profile, which stays on the legacy bundle", ClassifierPhased, dcLongHaulProfile)
	}
	return nil
}

func resolveClassifier(mode *ClassifierMode, p profile.Profile) error {
	parsed, err := ParseClassifierMode(string(*mode))
	if err != nil {
		return err
	}
	if err := ValidateClassifier(parsed, p); err != nil {
		return err
	}
	*mode = parsed
	return nil
}

// phasedClassifierConfig applies the mode to a profile's classifier policy.
func phasedClassifierConfig(cfg classifier.Config, mode ClassifierMode) classifier.Config {
	cfg.Phased = mode == ClassifierPhased
	return cfg
}

// sparseWriteBytes is the largest write the sparse-write rule treats as a
// message rather than content: two packets of about 1,400 bytes. A keystroke
// (tens of bytes), an SSE delta (a few hundred with its TLS record), a push or
// a chat message all fit; a read from a backlogged socket is a whole chunk
// (32 KiB) and never does.
const sparseWriteBytes = 2 * 1400

// sparseAllowanceBytes bounds how much a flow may send through the sparse
// rule per recent window (one to two seconds): the small-exchange rate, 64
// KiB, about 256-512 kbit/s. That is two orders of magnitude above an LLM
// stream (40 deltas a second) or typing, and below any transfer, so an
// application that writes small pieces fast onto an idle queue is charged by
// its class like any other flow past this point.
const sparseAllowanceBytes = smallExchangeBytes

// backlogThresholdFor fits the phased classifier's backlog threshold to what
// the flow's scheduler can actually hold. The backlog -- bytes read from the
// application and not acknowledged -- cannot exceed the read-ahead the
// scheduler allows, and the scheduler stops reading at whichever of its
// bounds binds first:
//
//   - the per-flow send bytes (maxSendBytes);
//   - the chunk count (maxOutstanding). A backlogged source fills each read,
//     so this holds maxOutstanding x readSize bytes, where readSize is a
//     whole chunk, or only the staging buffer when the scheduler stages
//     reads: a staged read is extended only if the connection takes a read
//     deadline and the shared send memory has a whole chunk free, and under
//     contention neither is promised;
//   - under mem=owned, the autotuned retention bound, which can sit at its
//     startup floor of stripe.DefaultAutotuneStartupChunks chunks.
//
// A continuously backlogged flow dips below that capacity each time the peer
// acknowledges a read before the producer refills its slot, and a dip to the
// threshold or under it is recorded as a drain that ends the backlog
// stretch. The threshold is therefore half of what remains after one read is
// acknowledged: a blocked transfer stays above it between refills, and it is
// still far above any conversation's backlog. It only ever lowers base.
func backlogThresholdFor(base uint64, limits flowMemoryLimits, chunkSize, readSize int, memOwned bool) (threshold uint64, refill bool) {
	if base == 0 {
		base = classifier.DefaultConfig().BulkBacklogBytes
	}
	if readSize <= 0 || readSize > chunkSize {
		readSize = chunkSize
	}
	residual, ok := leastResidual(limits, chunkSize, readSize, memOwned)
	if !ok {
		return base, false
	}
	threshold = min(base, max(countBytes(residual/2), 1))
	return threshold, threshold < minBacklogThresholdBytes
}

// minBacklogThresholdBytes is the lowest backlog threshold that still sits
// above a conversation's unacknowledged bytes: an LLM stream of 40 deltas a
// second of about 200 bytes holds 8 KB in flight on a one-second path. Below
// it, bytes outstanding are not evidence of a transfer, and the flow uses
// refill evidence instead (see refillWindow).
const minBacklogThresholdBytes = 8 << 10

// refillWindow is how soon after an acknowledgement opens room a full read
// must follow for the flow to count as never having drained. A producer
// blocked for room is woken by the acknowledgement and reads at once from a
// socket that already holds the bytes, which takes well under a millisecond;
// twenty allows for a loaded scheduler. A conversation's next write comes at
// its own cadence and is not full.
const refillWindow = 20 * time.Millisecond

// leastResidual is the least a continuously backlogged flow can hold,
// unacknowledged, just after one acknowledgement and before the producer
// refills. One acknowledgement can release up to maxAckRelease: the peer
// coalesces until ackBytesThreshold is pending, and the chunk that crosses
// it can add one more. The worst case is a queue whose oldest read is a
// whole chunk and whose others are as small as the read path allows, with
// as much of it as possible released at once. Each scheduler bound limits
// what is left:
//
//   - the send bytes: the limit less one release;
//   - the chunk count: one whole chunk and maxOutstanding-1 minimum-size
//     reads, less one release;
//   - under mem=owned, the autotune floor less one release.
//
// It reports false when no bound applies.
func leastResidual(limits flowMemoryLimits, chunkSize, readSize int, memOwned bool) (int, bool) {
	if chunkSize <= 0 {
		return 0, false
	}
	release := maxAckRelease(chunkSize)
	residual, bounded := 0, false
	take := func(r int) {
		r = max(r, 0)
		if !bounded || r < residual {
			residual, bounded = r, true
		}
	}
	if limits.maxSendBytes > 0 {
		take(limits.maxSendBytes - release)
	}
	if limits.maxOutstanding > 0 {
		take(chunkSize + (limits.maxOutstanding-1)*readSize - release)
	}
	if memOwned {
		take(stripe.DefaultAutotuneStartupChunks*chunkSize - release)
	}
	return residual, bounded
}

// maxAckRelease is the most one acknowledgement can cover.
func maxAckRelease(chunkSize int) int { return ackBytesThreshold + chunkSize }

func minPositive(a, b int) int {
	if a <= 0 {
		return b
	}
	if b <= 0 || a < b {
		return a
	}
	return b
}

// fitBacklogThreshold applies backlogThresholdFor once the flow's chunk size,
// memory limits and memory bundle are all settled, which is when its send
// loop starts. It starts from the threshold the profile configured.
func (f *multipathFlow) fitBacklogThreshold() {
	if !f.phased() {
		return
	}
	readSize := f.chunkSize
	if f.sendMemory != nil {
		// The scheduler stages each read in stagingBytes and extends it only
		// when it can; size for the reads a contended flow is left with.
		readSize = min(readSize, stagingBytes)
	}
	threshold, refill := backlogThresholdFor(f.classifier.BacklogBytes(), f.memoryLimits, f.chunkSize, readSize, f.memOwned)
	f.classifier.SetBacklogBytes(threshold)
	f.ackMu.Lock()
	f.refillMode, f.fullReadBytes, f.refillThreshold = refill, readSize, threshold
	f.ackMu.Unlock()
}

// phased reports whether this flow runs the phased classifier.
func (f *multipathFlow) phased() bool {
	return f.classifier != nil && f.classifier.Phased()
}

// sparseBytes is the scheduler's sparse-write threshold: zero, which marks
// nothing, under the legacy bundle.
func (f *multipathFlow) sparseBytes() int {
	if !f.phased() {
		return 0
	}
	return sparseWriteBytes
}

// admitSparse charges a sparse write against the flow's sparse allowance. It
// is called by the stripe scheduler under its own lock and takes only
// recentMu.
func (f *multipathFlow) admitSparse(n int) bool {
	if n <= 0 || !f.phased() {
		return false
	}
	f.recentMu.Lock()
	defer f.recentMu.Unlock()
	f.advanceRecentLocked(f.clockNow())
	if f.sparseCurrent+f.sparsePrior+countBytes(n) > sparseAllowanceBytes {
		return false
	}
	f.sparseCurrent += countBytes(n)
	return true
}

// sendClass is the class byte a DATA frame carries. Under the legacy bundle
// it is the flow's class. Under the phased bundle it is the current class,
// except that a sparse write is INTERACTIVE.
func (f *multipathFlow) sendClass(sparse bool) protocol.Class {
	if sparse && f.phased() {
		return protocol.ClassInteractive
	}
	return protocol.Class(f.class.Load()) // #nosec G115 -- f.class only ever stores a protocol.Class.
}

// codingForClass is the coding preference of a frame of the given class.
// It is the policy prefersCodingOverRetransmission has always applied to the
// flow's class, applied to one frame's.
func (f *multipathFlow) codingForClass(class protocol.Class) bool {
	switch classifier.Class(class) {
	case classifier.ClassBulk:
		return false
	case classifier.ClassInteractive:
		return true
	}
	return f.bytesUp.Load()+f.bytesDown.Load() <= codedFlowBytes
}

// frameCoding is the phased bundle's per-frame coding policy: the class byte
// stamped on the frame is the decision, so a sparse write of a BULK flow is
// coded and the dispatch that chose its reliability agrees with the write.
func (f *multipathFlow) frameCoding(class protocol.Class) bool {
	f.refreshClass()
	return f.codingForClass(class)
}

// laneRetransmitsSparse is laneRetransmits for a sparse chunk, which is coded
// wherever the lane's coded path is coding.
func (f *multipathFlow) laneRetransmitsSparse(laneID uint64) bool {
	f.lanesMu.RLock()
	lane := f.lanes[laneID]
	f.lanesMu.RUnlock()
	if lane == nil || lane.fc == nil {
		return true
	}
	return !lane.fc.codingBulk()
}

// noteAckedCoverage publishes what the peer holds of this endpoint's stream,
// cumulative point and selective ranges together, so the classifier's send
// backlog does not count bytes acknowledged beyond a gap. Called under
// replayMu after an acknowledgement advanced.
func (f *multipathFlow) noteAckedCoverage() {
	covered := f.acked
	if f.ackTrack != nil {
		snapshot := f.ackTrack.Snapshot()
		covered = max(covered, snapshot.cumulative)
		for _, r := range snapshot.ranges {
			if r[1] > r[0] && r[0] >= snapshot.cumulative {
				covered += r[1] - r[0]
			}
		}
	}
	threshold := f.classifierBacklogBytes()
	now := f.clockNow()
	f.ackMu.Lock()
	defer f.ackMu.Unlock()
	f.ackCovered = covered
	if saturatingSub(f.bytesUp.Load(), covered) > threshold {
		return
	}
	if !f.refillMode {
		f.ackDrained = true
		return
	}
	// The room this opened is a drain only if no full read takes it at
	// once; countAndSample decides.
	if !f.drainPending {
		f.drainPending, f.drainPendingAt = true, now
	}
}

// countAndSample is the classifier's view of one observation, at now, of n
// bytes in direction up: the flow's byte totals including them (added to the
// totals when publish is set), the send backlog against those totals, and
// whether a drain was published since the last sample, all under ackMu,
// which the acknowledgement path also holds when it records a drain.
//
// In refill mode (a threshold too small to separate a transfer from a
// conversation by bytes outstanding) the backlog counts only while the
// latest read filled its buffer, and room an acknowledgement opened is a
// drain unless a full read takes it within refillWindow.
//
// bufLen is the buffer the read had room for, so a read that filled it is a
// full read whatever its size; zero means unknown, and fullReadBytes stands
// in for it.
func (f *multipathFlow) countAndSample(now time.Time, n int, up, publish bool, bufLen int) (upBytes, downBytes, backlog uint64, drained bool) {
	f.ackMu.Lock()
	defer f.ackMu.Unlock()
	if publish && n > 0 {
		if up {
			f.bytesUp.Add(countBytes(n))
		} else {
			f.bytesDown.Add(countBytes(n))
		}
	}
	upBytes, downBytes = f.bytesUp.Load(), f.bytesDown.Load()
	if !publish {
		if up {
			upBytes += countBytes(n)
		} else {
			downBytes += countBytes(n)
		}
	}
	backlog = saturatingSub(upBytes, f.ackCovered)
	if f.refillMode {
		if up && n > 0 {
			full := n >= f.fullReadBytes
			if bufLen > 0 {
				full = n >= bufLen
			}
			if f.drainPending && !(full && now.Sub(f.drainPendingAt) <= refillWindow) {
				f.ackDrained = true
			}
			f.drainPending = false
			f.lastReadFull = full
		} else if f.drainPending && now.Sub(f.drainPendingAt) > refillWindow {
			f.ackDrained, f.drainPending = true, false
		}
		switch {
		case !f.lastReadFull:
			backlog = 0
		case f.drainPending || backlog > f.refillThreshold:
			// Still behind, or a refill is due within the window: an
			// observation of something else -- receive-side bytes, a
			// re-examination -- does not end the stretch before the
			// refill has had its chance.
			backlog = max(backlog, f.refillThreshold+1)
		}
	}
	drained = f.ackDrained
	f.ackDrained = false
	return upBytes, downBytes, backlog, drained
}

// ackSample is countAndSample for no new bytes.
func (f *multipathFlow) ackSample() (backlog uint64, drained bool) {
	_, _, backlog, drained = f.countAndSample(f.clockNow(), 0, true, false, 0)
	return backlog, drained
}

// setAckCoverage publishes acknowledged coverage without a drain, for tests
// that model the peer.
func (f *multipathFlow) setAckCoverage(covered uint64) {
	f.ackMu.Lock()
	f.ackCovered = covered
	f.ackMu.Unlock()
}

func (f *multipathFlow) classifierBacklogBytes() uint64 {
	if f.classifier == nil {
		return classifier.DefaultConfig().BulkBacklogBytes
	}
	return f.classifier.BacklogBytes()
}

// publishClass stores what the classifier now says: the current class in
// f.class, and whether the flow has ever been BULK in f.everBulk.
func (f *multipathFlow) publishClass(current classifier.Class) {
	f.class.Store(uint32(protocol.Class(current)))
	if current == classifier.ClassBulk || f.classifier.Class() == classifier.ClassBulk {
		f.everBulk.Store(true)
	}
}

// classByteIndex maps a class byte to the telemetry index.
func classByteIndex(class protocol.Class) int {
	switch classifier.Class(class) {
	case classifier.ClassInteractive:
		return 1
	case classifier.ClassBulk:
		return 2
	}
	return 0
}

// noteDataFrame is the frame connection's DATA observer, for the class
// telemetry.
func (f *multipathFlow) noteDataFrame(frame protocol.Frame, coded bool) {
	f.noteDataBytes(frame.Header.Class, coded, len(frame.Payload))
}

// codedDataOutstanding reports whether a chunk this endpoint sent on the
// coded substrate is still unacknowledged. Under the phased classifier a BULK
// flow codes only its sparse writes, so whether the lane could code says
// nothing about whether a stalled flow has coded work pending. The scheduler
// is the authority: a chunk acknowledged selectively has left it, and each
// attempt records the substrate its write actually used.
func (f *multipathFlow) codedDataOutstanding() bool {
	sched := f.scheduler.Load()
	return sched != nil && sched.UnreliableOutstanding()
}

// noteDataBytes records one DATA frame's payload by its class byte and the
// substrate that took it (R3).
func (f *multipathFlow) noteDataBytes(class protocol.Class, coded bool, n int) {
	if n <= 0 {
		return
	}
	substrate := 0
	if coded {
		substrate = 1
	}
	f.classBytes[classByteIndex(class)][substrate].Add(countBytes(n))
	if f.metrics != nil {
		f.metrics.DataClassBytes(classByteIndex(class), coded, countBytes(n))
	}
}

// classLogFields renders the per-flow class telemetry for the flow's
// completion record: DATA payload bytes this endpoint sent, by class byte and
// substrate, and the classifier's phase history.
func (f *multipathFlow) classLogFields() []any {
	exits, reentries := f.classifier.PhaseTransitions()
	return []any{
		"classifier_phased", f.phased(),
		"class_ever_bulk", f.everBulk.Load(),
		"class_exits", exits, "class_reentries", reentries,
		"data_bytes_new_coded", f.classBytes[0][1].Load(),
		"data_bytes_new_stream", f.classBytes[0][0].Load(),
		"data_bytes_interactive_coded", f.classBytes[1][1].Load(),
		"data_bytes_interactive_stream", f.classBytes[1][0].Load(),
		"data_bytes_bulk_coded", f.classBytes[2][1].Load(),
		"data_bytes_bulk_stream", f.classBytes[2][0].Load(),
	}
}

// countBytes converts a byte count held as an int; a negative one is zero.
func countBytes(n int) uint64 {
	if n <= 0 {
		return 0
	}
	return uint64(n) // #nosec G115 -- n is positive, and every int fits in uint64.
}
