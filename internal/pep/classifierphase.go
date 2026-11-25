package pep

import (
	"fmt"

	"github.com/bojieli/queqiao/internal/classifier"
	"github.com/bojieli/queqiao/internal/profile"
	"github.com/bojieli/queqiao/internal/protocol"
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
	if f.sparseCurrent+f.sparsePrior+uint64(n) > sparseAllowanceBytes {
		return false
	}
	f.sparseCurrent += uint64(n)
	return true
}

// sendClass is the class byte a DATA frame carries. Under the legacy bundle
// it is the flow's class. Under the phased bundle it is the current class,
// except that a sparse write is INTERACTIVE.
func (f *multipathFlow) sendClass(sparse bool) protocol.Class {
	if sparse && f.phased() {
		return protocol.ClassInteractive
	}
	return protocol.Class(f.class.Load())
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
	f.classBytes[classByteIndex(class)][substrate].Add(uint64(n))
	if f.metrics != nil {
		f.metrics.DataClassBytes(classByteIndex(class), coded, uint64(n))
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
