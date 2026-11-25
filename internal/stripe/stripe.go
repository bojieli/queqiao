// Package stripe schedules one byte stream across several independent lanes by
// self-pacing: a lane is handed its next chunk when it finishes the last one.
//
// The design it replaces predicted. It estimated each lane's rate, computed
// which lane would deliver a frame soonest, and committed frames to that lane's
// queue ahead of time -- up to 56 frames, 1.75 MiB, on a queue the scheduler
// could not take back. Every byte of that commitment was a bet on an estimate.
// When the bet was wrong, the bytes were already numbered and sitting behind a
// lane that had slowed down, and the receiver could not deliver past them; the
// remedies for that (reinjecting a stalled head on a timer, a replay window
// sized to the whole reorder span) were all corrections for mis-prediction.
//
// Self-pacing needs no estimate. A lane asks for work when it has capacity, so
// a lane that is fast asks often and a lane that is throttled asks rarely; a
// lane that has stopped entirely asks never and is thereby excluded without
// anyone deciding to exclude it. The rate each lane gets is whatever it can
// actually carry, measured by the only instrument that cannot be wrong about
// it: the lane itself.
//
// Two properties follow, and both are the point:
//
//   - A lane's commitment is bounded by what its transport has not yet taken,
//     not by a queue depth chosen in advance. The transport's own congestion
//     control is therefore the clock: a stream write returns when the packer
//     has consumed the bytes, which it does only when the congestion window and
//     the pacer allow. Losing a lane costs that commitment and nothing more,
//     and the loss is recoverable because any chunk may be re-issued anywhere.
//   - Lanes are time-independent. Nothing about chunk N's placement constrains
//     chunk N+1, so there is no ordering to violate and no schedule to fall
//     behind. Order is reconstructed at the receiver from byte offsets, which
//     is where it belongs.
package stripe

import (
	"context"
	"errors"
	"io"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/bojieli/queqiao/internal/memlimit"
)

// ErrClosed reports that the scheduler has been closed.
var ErrClosed = errors.New("stripe: scheduler closed")

// Windows supplies the admission limits a lane must satisfy to be handed more
// work.
//
// Keeping this an interface is what stops congestion control leaking into the
// scheduler. The scheduler's job is to hand the oldest ready chunk to a lane
// that has room; deciding how much room a lane has is a different question,
// answered by the lane's transport.
type Windows interface {
	// Lane is how many bytes this lane may hold that its transport has not yet
	// taken, given how many it holds now. It is a write-ahead bound, not a
	// congestion window: the transport already has one of those, and what this
	// governs is how far ahead of it the sender commits bytes that no other
	// lane can then take back.
	Lane(laneID uint64, queued uint64) int
	// Total is how much the flow may hold unacknowledged across every lane. It
	// is what stops N lanes claiming N times a single connection's share.
	Total() int
}

// OutstandingWindows optionally bounds bytes assigned to a lane until the
// peer acknowledges them. Stream writes normally supply the scheduler's clock,
// but a kernel TCP write returns after copying into the socket buffer rather
// than after congestion control sends the bytes. A TCP bundle therefore uses
// this second bound to keep one socket from pre-claiming the whole stream.
type OutstandingWindows interface {
	Outstanding(laneID uint64, outstanding uint64) int
}

// Config bounds the scheduler.
type Config struct {
	// ChunkSize is the largest chunk handed to a lane. It trades balance
	// granularity against per-chunk overhead: with C bytes per chunk, the most
	// any single lane can hold up is C bytes of the receiver's contiguous
	// point, so this is the head-of-line exposure of one stalled lane.
	ChunkSize int
	// LaneWindow caps how many chunks a lane may hold unacknowledged, as a
	// backstop on bookkeeping. The binding limits are the byte bounds: Windows
	// for what a lane may commit ahead of its transport, and
	// MaxOutstandingBytes for what the flow may retain.
	LaneWindow int
	// MaxOutstanding bounds chunks held across all lanes.
	MaxOutstanding int
	// Retention, when set, reports the current bound instead. It is consulted
	// rather than fixed because what a flow must retain is a property of the
	// path: the lanes' combined congestion windows, plus what the peer has not
	// yet acknowledged. A constant either starves a fast path or lets a slow
	// one hold memory it will never use.
	Retention func() int
	// MaxOutstandingBytes bounds the same set by size, and is the ceiling on
	// Retention. Counting chunks cannot bound memory when a chunk is whatever one
	// read returned: 2048 chunks is 64 MiB if they are full and a fraction of
	// that if they are not, so a count generous enough to keep a fast path busy
	// is also generous enough to let a flow retain tens of megabytes. Measured
	// with a count-only bound, throughput fell across successive transfers in
	// one process as the heap grew.
	MaxOutstandingBytes int
	// Memory is shared by every flow of an endpoint. The producer acquires it
	// only for bytes it already holds, sized to the chunk's backing
	// allocation, and keeps that reservation until the logical chunk is
	// acknowledged. This turns aggregate read-ahead into a hard endpoint limit
	// instead of MaxOutstandingBytes multiplied by flows. Nil preserves the
	// unbounded server/default profile.
	//
	// A producer waiting on the application holds none of it. It blocks
	// reading into a private staging buffer of StagingBytes, and only once
	// that read returns bytes does it reserve a chunk, copy them in, and try
	// to fill the rest from what the source already holds (AvailableReader).
	// Reserving first let enough idle application sockets -- each blocked in
	// Read -- hold the whole endpoint budget forever, so a flow with a request
	// in hand could not read it (issue #169).
	Memory *memlimit.Budget
	// StagingBytes sizes the producer's private read buffer when Memory is
	// set. It is part of the per-session static footprint, so it must be
	// small: with 1,024 sessions, 1 KiB is 1 MiB in the worst case. Zero
	// selects DefaultStagingBytes, capped at ChunkSize.
	StagingBytes int
	// Autotune, when set, replaces Retention with the flow-layer bound of the
	// owned-memory bundle; see Autotune.
	Autotune *Autotune
	// RetransmitAfter is asked how long a chunk may be outstanding before it is
	// presumed lost and offered again. Re-issuing duplicates bytes the receiver
	// discards, so this trades a little bandwidth for not waiting on a lane
	// that has gone quiet. Nil, or a zero answer, disables it.
	//
	// It is asked rather than fixed because the answer is a property of the
	// path. A chunk with data behind it is proved lost by that data within a
	// round trip, and this timer only governs the case where there is nothing
	// behind it -- a small exchange, or the last chunks of a transfer. Fixed at
	// a second and a half it was six round trips on the path this targets, and
	// measured live, one small flow in five cost 1.79 s instead of 0.29
	// because it spent that constant waiting for a chunk the code had failed
	// to repair.
	RetransmitAfter func() time.Duration
	// ReliableReissueBurst bounds how many chunks already carried by reliable
	// lanes a single timer sweep may copy. Zero keeps the unbounded legacy
	// behavior. TCP striping sets one: the oldest missing byte is what blocks
	// delivery, while copying every chunk behind it only multiplies traffic.
	ReliableReissueBurst int
	// Reliable reports whether a lane retransmits for itself. When nil every
	// lane is taken to be reliable, which is what a lane on a QUIC stream is.
	//
	// It decides the lifecycle of an attempt. A reliable one remains active
	// until delivery or lane failure, and may have one speculative copy on a
	// different lane. A written unreliable one is terminal after its recovery
	// deadline or receiver evidence; it is retired before the logical chunk is
	// offered again, including to the same lane. Without that retirement a
	// single coded lane accumulates one admission charge per erasure and
	// eventually cannot retry the bytes which are missing.
	Reliable func(laneID uint64) bool
	// ForceReliable pins selected byte ranges to a reliable carrier even when
	// the lane normally uses datagrams. The decision is snapshotted at dispatch
	// just like Reliable, so an in-flight recovery cannot change substrates
	// when the policy later clears. Nil leaves the lane's decision unchanged.
	ForceReliable func(offset uint64) bool
	// CodedRetry selects the coded-retry bundle (design §8.1, §14 stage 4):
	//
	//   - an attempt the lane carries unreliably has no recovery deadline
	//     until its transport reports, through Registered, that the bytes
	//     actually left in a packet; a frame waiting in a queue below this
	//     scheduler has not started to be lost. Abandoned retires it at once;
	//   - every attempt after a chunk's first is reliable: once the code has
	//     had its one chance at a chunk, the chunk is sticky to the stream;
	//   - receiver gap evidence retires only an unreliable attempt that has
	//     been registered, so an attempt still queued below is never copied.
	//
	// Together these give each chunk one repair owner at a time: the code
	// until its deadline, then the stream. ForceReliable still applies.
	CodedRetry bool
	// Windows supplies admission limits. When nil, only LaneWindow and
	// MaxOutstanding apply, which is the behaviour of a flow with no
	// congestion coupling.
	Windows Windows
	// Now is the clock, for tests.
	Now func() time.Time
}

// DefaultStagingBytes is the producer's private read buffer when a shared
// Memory budget is configured.
const DefaultStagingBytes = 1024

// ErrUnsupported is returned by an AvailableReader that cannot read without
// waiting for the application.
var ErrUnsupported = errors.New("stripe: source cannot read without blocking")

// AvailableReader is a source that can return what it already holds without
// waiting for the application to write more.
//
// The producer calls it while holding a shared reservation, so it must not
// block on the application: it fills p with bytes available now, waiting at
// most a short fixed bound, and returns (n, nil) once it has none, however
// small n is. An error other than ErrUnsupported ends the stream exactly as
// the same error from Read would. A source that is not an AvailableReader,
// or returns ErrUnsupported, has its chunks published at the size its
// ordinary Read returned: a second ordinary Read is never issued while a
// reservation is held.
type AvailableReader interface {
	ReadAvailable(p []byte) (int, error)
}

// Autotune is the flow-layer retention bound of the owned-memory bundle.
//
// The legacy bound sums the congestion windows of a flow's lanes, which on a
// path with one shared window would let every flow claim the path twice. This
// one follows two gauges the scheduler owns: U, dispatched-and-unacknowledged
// unique bytes, and Q, READY bytes read but not yet dispatched. Once per
// credit round trip it doubles (up to MaxOutstandingBytes) if newly validated
// unique coverage arrived in that round, and the producer was blocked on the
// bound at some point in it, and a lane had room to take more at that point.
// Otherwise it moves toward 2 x max U over the last four rounds, never below
// Startup, and never upward in a round without newly validated coverage. Q is
// limited separately by ReadyLimit, so the bound cannot grow without credit.
type Autotune struct {
	// Round is the credit round trip: the path's smoothed RTT plus the peer's
	// ACK delay. Non-positive answers select DefaultAutotuneRound.
	Round func() time.Duration
	// Startup is the bound before any credit has arrived, and its floor.
	// Zero selects DefaultAutotuneStartupChunks chunks.
	Startup int
	// ReadyLimit bounds READY bytes; nil or a non-positive answer leaves them
	// bounded only by the retention bound.
	ReadyLimit func() int
}

const (
	// DefaultAutotuneRound is the credit round used before the path has an
	// RTT sample.
	DefaultAutotuneRound = 100 * time.Millisecond
	// DefaultAutotuneStartupChunks is the startup allowance, in chunks.
	DefaultAutotuneStartupChunks = 4
	// autotuneHistory is how many rounds of max U the bound decays toward.
	autotuneHistory = 4
	// autotuneMaxCatchUp bounds how many elapsed rounds one lazy update
	// replays. The history fills in autotuneHistory rounds and each later
	// round halves the remaining distance, so this reaches the floor from any
	// int-sized bound.
	autotuneMaxCatchUp = autotuneHistory + 64
)

// DefaultConfig returns bounds suitable for a long-haul path.
func DefaultConfig() Config {
	return Config{
		ChunkSize:           64 * 1024,
		LaneWindow:          4,
		MaxOutstanding:      64,
		MaxOutstandingBytes: 16 * 1024 * 1024,
		RetransmitAfter:     func() time.Duration { return 2 * time.Second },
	}
}

func (c *Config) applyDefaults() {
	d := DefaultConfig()
	if c.ChunkSize <= 0 {
		c.ChunkSize = d.ChunkSize
	}
	if c.LaneWindow <= 0 {
		c.LaneWindow = d.LaneWindow
	}
	if c.MaxOutstanding <= 0 {
		c.MaxOutstanding = d.MaxOutstanding
	}
	if c.MaxOutstanding < c.LaneWindow {
		c.MaxOutstanding = c.LaneWindow
	}
	if c.MaxOutstandingBytes <= 0 {
		c.MaxOutstandingBytes = d.MaxOutstandingBytes
	}
	if c.Now == nil {
		c.Now = time.Now
	}
	if c.StagingBytes <= 0 {
		c.StagingBytes = DefaultStagingBytes
	}
	if c.StagingBytes > c.ChunkSize {
		c.StagingBytes = c.ChunkSize
	}
}

// Chunk is a contiguous run of the source stream, addressed by byte offset.
// Offsets are what makes lanes independent: a chunk carries where it belongs,
// so it can travel any lane and arrive in any order.
type Chunk struct {
	// Reliable pins the dispatch to a stream even if coding policy changes.
	Reliable bool
	Offset   uint64
	Data     []byte
	// Final marks the chunk that ends the stream. It is delivered like any
	// other, so the end of the stream is ordered with respect to the data
	// rather than being a separate event that can overtake it.
	Final bool

	// urgent marks a chunk that came back from a lane which stalled or died.
	// Such a chunk is admitted to a lane even when that lane is at its window,
	// because it is almost certainly the one holding up the receiver's
	// contiguous point: every other lane is then full of chunks that cannot be
	// acknowledged until this one lands, so respecting the window here would
	// deadlock the flow with every lane waiting on a chunk none of them is
	// allowed to carry.
	urgent bool
	// attempt identifies one dispatch of this logical byte range. It is not
	// transmitted: byte offsets identify data to the peer, while this identity
	// lets local completion callbacks distinguish a retired unreliable attempt
	// from the replacement carrying the same bytes on the same lane.
	attempt uint64
	// reservation is charged once for the backing allocation, not once per
	// retransmission. Copies handed to lanes clear it; only the scheduler's
	// canonical chunk releases it.
	reservation int
}

// End returns the offset one past this chunk's last byte.
func (c *Chunk) End() uint64 { return c.Offset + uint64(len(c.Data)) }

type attempt struct {
	id       uint64
	lane     uint64
	deadline time.Time
	// reliable records whether the lane retransmitted for itself at the moment
	// this chunk was handed to it.
	//
	// It belongs to the attempt and not to the lane, because a lane's answer
	// changes: one that carried a chunk over a coded datagram path may be
	// carrying the next over its stream. Asking the lane as it is now, rather
	// than as it was when it took these bytes, strands exactly the chunks that
	// were sent unreliably -- nothing will re-issue them, because the lane now
	// says it did not need to. Measured live, that took a download from 6.2
	// Mbit/s to 0.25.
	reliable bool
	// written records that the lane's transport has taken these bytes. Until
	// then they are the lane's queued commitment and bound its admission;
	// afterwards they are the transport's problem, and the scheduler retains
	// them only so they can be re-issued if the lane dies.
	written bool
	// registered records, under CodedRetry, that an unreliable attempt's
	// bytes were registered in a packet; its deadline counts from then.
	// abandoned records that the coded path gave the attempt up before its
	// transport reported the write, which settles it when that report comes.
	registered bool
	abandoned  bool
	// gapFloor is the receiver gap evidence already seen when the attempt
	// was registered. Evidence no newer than that was about bytes that left
	// before this attempt did, and cannot show this attempt lost.
	gapFloor uint64
}

// gapAdvance is receiver gap evidence reaching offset, seen at a time.
type gapAdvance struct {
	at     time.Time
	offset uint64
}

// gapHistory bounds how many advances of the gap evidence are remembered for
// placing a late registration report. One that predates all of them counts
// the oldest as already seen, which errs towards the deadline.
const gapHistory = 16

type outstanding struct {
	chunk    *Chunk
	attempts []attempt
}

// Stats reports what the scheduler did, for tests and metrics.
type Stats struct {
	ChunksIssued     uint64
	ChunksRetransmit uint64
	ChunksCompleted  uint64
	BytesIssued      uint64
	BytesRetransmit  uint64
	LaneFailures     uint64
	SourceBytes      uint64
	PeakOutstanding  int
	// CodedAbandoned counts unreliable attempts the coded path abandoned,
	// under CodedRetry.
	CodedAbandoned    uint64
	CompletedByLaneID map[uint64]uint64
}

// Scheduler hands chunks of a source stream to lanes that ask for them.
//
// It is safe for concurrent use: one goroutine per lane calls Next, and the
// completion path may be called from anywhere.
type Scheduler struct {
	cfg Config

	mu       sync.Mutex
	ready    sync.Cond // a lane may have work, or space freed
	produced sync.Cond // the producer may read more

	src         io.Reader
	nextOffset  uint64
	pending     []*Chunk
	live        map[uint64]*outstanding
	laneLoad    map[uint64]int
	laneBytes   map[uint64]uint64
	laneQueued  map[uint64]uint64
	totalBytes  uint64
	nextAttempt uint64
	eof         bool
	srcErr      error
	closed      bool
	finished    chan struct{}
	producerCtx context.Context
	cancel      context.CancelFunc
	stats       Stats

	// uniqueBytes is U: bytes of logical chunks dispatched at least once and
	// not yet acknowledged, each counted once however many attempts it has.
	uniqueBytes uint64
	// readyBytes is Q: bytes in the ready set that have never been
	// dispatched. A retry waiting there is already in uniqueBytes.
	readyBytes uint64
	// idleLanes counts lanes waiting in Next with room for work and nothing
	// ready to take.
	idleLanes int
	// gapHigh is the highest offset ReissueUnacknowledgedBelow has been
	// given, and gapNow the one it is acting on (CodedRetry only). gapSeen
	// records recent advances of gapHigh with when they were seen, so a
	// registration reported late can be placed among them.
	gapHigh uint64
	gapNow  uint64
	gapSeen [gapHistory]gapAdvance
	gapNext int
	tune    autotuneState
}

// autotuneState is the Autotune bound and the facts of its current round.
type autotuneState struct {
	bound      int
	roundStart time.Time
	covered    bool // newly validated unique coverage arrived this round
	blocked    bool // the producer waited on the bound this round
	room       bool // ...while a lane had room to take more
	waiting    bool // the producer is waiting on the bound now
	roundMaxU  uint64
	history    [autotuneHistory]uint64
	rounds     int
}

// New returns a scheduler that reads src and hands it out in chunks. It starts
// one goroutine to read ahead; Close stops it.
func New(src io.Reader, cfg Config) *Scheduler {
	cfg.applyDefaults()
	producerCtx, cancel := context.WithCancel(context.Background())
	s := &Scheduler{
		cfg:         cfg,
		src:         src,
		live:        make(map[uint64]*outstanding),
		laneLoad:    make(map[uint64]int),
		laneBytes:   make(map[uint64]uint64),
		laneQueued:  make(map[uint64]uint64),
		finished:    make(chan struct{}),
		producerCtx: producerCtx,
		cancel:      cancel,
	}
	s.stats.CompletedByLaneID = make(map[uint64]uint64)
	s.ready.L = &s.mu
	s.produced.L = &s.mu
	if cfg.Autotune != nil {
		s.tune.bound = s.autotuneStartup()
		s.tune.roundStart = cfg.Now()
	}
	go s.produce()
	return s
}

// produce reads the source into ready chunks, staying within MaxOutstanding so
// a stalled transfer cannot buffer the whole stream in memory.
func (s *Scheduler) produce() {
	var staging []byte
	if s.cfg.Memory != nil {
		staging = make([]byte, s.cfg.StagingBytes)
	}
	for {
		allowance, ok := s.waitForRoom()
		if !ok {
			return
		}
		var (
			buf      []byte
			n        int
			reserved int
			err      error
		)
		if staging == nil {
			buf = make([]byte, allowance)
			// One Read rather than ReadFull: a chunk is at most ChunkSize,
			// not exactly it. Waiting for a full buffer would hold a small
			// write -- a request, an interactive turn -- until enough later
			// bytes arrived to fill the chunk, which is latency invented by
			// the scheduler.
			n, err = s.src.Read(buf)
		} else {
			var ok bool
			buf, n, reserved, ok, err = s.readStaged(staging[:min(len(staging), allowance)], allowance)
			if !ok {
				return
			}
		}
		if !s.publish(buf, n, reserved, err) {
			return
		}
	}
}

// waitForRoom blocks until the flow's bounds admit another read, and returns
// the most that read may take: ChunkSize, or under Autotune what is left of
// the retention and READY allowances. It reports false once the scheduler is
// closed.
func (s *Scheduler) waitForRoom() (int, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for {
		if s.closed {
			return 0, false
		}
		if len(s.pending)+len(s.live) >= s.cfg.MaxOutstanding {
			s.produced.Wait()
			continue
		}
		allowance := s.cfg.ChunkSize
		var retained uint64
		if s.cfg.Autotune == nil {
			retained = s.retainedBytes()
		} else {
			s.autotuneLocked(s.cfg.Now())
			ready := s.readyBytesLocked()
			if limit := s.readyLimit(); limit > 0 {
				if ready >= byteCount(limit) {
					s.tune.waiting = false
					s.produced.Wait()
					continue
				}
				allowance = min(allowance, limit-intCount(ready))
			}
			// Each logical byte is charged once, however many attempts
			// carry it or whether a retry is also waiting in the ready set:
			// what the bound limits is retained source bytes.
			retained = s.uniqueBytes + ready
			if retained < byteCount(s.tune.bound) {
				allowance = min(allowance, s.tune.bound-intCount(retained))
			}
		}
		if retained >= uint64(s.readAheadLimit()) {
			if s.cfg.Autotune != nil {
				s.tune.waiting = true
				s.tune.blocked = true
				if s.idleLanes > 0 {
					s.tune.room = true
				}
			}
			s.produced.Wait()
			continue
		}
		s.tune.waiting = false
		return allowance, true
	}
}

// readStaged blocks on the application holding no shared memory, then
// reserves exactly the allocation the bytes in hand need. ok is false when
// the scheduler closed while it waited for memory.
func (s *Scheduler) readStaged(staging []byte, allowance int) (buf []byte, n, reserved int, ok bool, err error) {
	n, err = s.src.Read(staging)
	if n <= 0 {
		return nil, 0, 0, true, err
	}
	size := n
	filler, canFill := s.src.(AvailableReader)
	// A read that filled the staging buffer suggests more is waiting; one
	// that did not has drained the source and the chunk is what it is. The
	// larger allocation is only speculative, so it is taken only if it is
	// free now: bytes in hand never wait -- nor hold up the fair queue --
	// for room they may not need.
	fill := canFill && err == nil && n == len(staging) && allowance > n &&
		s.cfg.Memory.TryAcquire(allowance)
	if fill {
		size = allowance
	} else if acquireErr := s.cfg.Memory.Acquire(s.producerCtx, size); acquireErr != nil {
		s.mu.Lock()
		if !s.closed {
			s.srcErr = acquireErr
			s.eof = true
			s.ready.Broadcast()
		}
		s.mu.Unlock()
		return nil, 0, 0, false, nil
	}
	buf = make([]byte, size)
	copy(buf, staging[:n])
	reserved = size
	if fill {
		m, fillErr := filler.ReadAvailable(buf[n:])
		if m > 0 {
			n += m
		}
		if fillErr != nil && !errors.Is(fillErr, ErrUnsupported) {
			err = fillErr
		}
		if n <= size/2 && s.cfg.Memory.TryAcquire(n) {
			// Do not keep a mostly-empty allocation. The copy needs both
			// buffers live at once, so the smaller one is reserved before it
			// exists; if that does not fit now, the original -- already
			// charged at its allocation -- is kept instead.
			short := make([]byte, n)
			copy(short, buf[:n])
			s.cfg.Memory.Release(size)
			buf, reserved = short, n
		}
	}
	return buf, n, reserved, true, err
}

// publish files what one read produced. It reports false when the producer
// must stop: the scheduler closed, or the source ended.
func (s *Scheduler) publish(buf []byte, n, reserved int, err error) bool {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		s.cfg.Memory.Release(reserved)
		return false
	}
	if n > 0 {
		chunk := &Chunk{Offset: s.nextOffset, Data: buf[:n], reservation: reserved}
		s.nextOffset += uint64(n)
		s.stats.SourceBytes += uint64(n)
		s.pending = append(s.pending, chunk)
		s.readyBytes += byteCount(n)
	} else {
		s.cfg.Memory.Release(reserved)
	}
	if err != nil {
		if errors.Is(err, io.EOF) {
			s.markEOFLocked()
		} else {
			s.srcErr = err
			s.eof = true
		}
		s.signalIfDoneLocked()
		s.ready.Broadcast()
		s.mu.Unlock()
		return false
	}
	s.ready.Broadcast()
	s.mu.Unlock()
	return true
}

// readAheadLimit is how much the flow may retain: chunks read and not yet
// acknowledged, because a lane that dies may not have delivered what its
// transport accepted. It is a memory bound and nothing else -- what a lane may
// commit is bounded separately, by Windows.
func (s *Scheduler) readAheadLimit() int {
	if s.cfg.Autotune != nil {
		return s.tune.bound
	}
	if s.cfg.Retention == nil {
		return s.cfg.MaxOutstandingBytes
	}
	want := s.cfg.Retention()
	if want <= 0 || want > s.cfg.MaxOutstandingBytes {
		return s.cfg.MaxOutstandingBytes
	}
	return want
}

// retainedBytes is what the flow is holding: chunks read but not yet
// acknowledged. Must be called with the lock held.
func (s *Scheduler) retainedBytes() uint64 {
	total := s.totalBytes
	for _, chunk := range s.pending {
		total += uint64(len(chunk.Data))
	}
	return total
}

// byteCount converts a byte count held as an int; a negative one is zero.
func byteCount(n int) uint64 {
	if n <= 0 {
		return 0
	}
	return uint64(n) // #nosec G115 -- n is positive, and every int fits in uint64.
}

// intCount converts a byte count to int, saturating.
func intCount(n uint64) int {
	if n > math.MaxInt {
		return math.MaxInt
	}
	return int(n) // #nosec G115 -- bounded by math.MaxInt just above.
}

// readyBytesLocked is Q: bytes read and not yet dispatched. A chunk waiting
// for a retry is already counted in U and is not counted again.
func (s *Scheduler) readyBytesLocked() uint64 { return s.readyBytes }

func (s *Scheduler) readyLimit() int {
	if s.cfg.Autotune == nil || s.cfg.Autotune.ReadyLimit == nil {
		return 0
	}
	return s.cfg.Autotune.ReadyLimit()
}

func (s *Scheduler) autotuneStartup() int {
	startup := s.cfg.Autotune.Startup
	if startup <= 0 {
		startup = DefaultAutotuneStartupChunks * s.cfg.ChunkSize
	}
	if startup < s.cfg.ChunkSize {
		startup = s.cfg.ChunkSize
	}
	if startup > s.cfg.MaxOutstandingBytes {
		startup = s.cfg.MaxOutstandingBytes
	}
	return startup
}

func (s *Scheduler) autotuneRound() time.Duration {
	if round := s.cfg.Autotune.Round; round != nil {
		if d := round(); d > 0 {
			return d
		}
	}
	return DefaultAutotuneRound
}

// sampleUniqueLocked records U for the current round's maximum.
func (s *Scheduler) sampleUniqueLocked() {
	if s.uniqueBytes > s.tune.roundMaxU {
		s.tune.roundMaxU = s.uniqueBytes
	}
}

// autotuneLocked closes every credit round that has ended by now. It is
// evaluated lazily, at the events that can change the answer: a round with
// no event in it has no coverage, so it can only move the bound down, and a
// producer blocked on the bound is woken by the completion that ends it.
func (s *Scheduler) autotuneLocked(now time.Time) {
	if s.cfg.Autotune == nil {
		return
	}
	round := s.autotuneRound()
	for i := 0; now.Sub(s.tune.roundStart) >= round; i++ {
		if i >= autotuneMaxCatchUp {
			// Every further empty round is the same as the last, and by
			// now the bound has reached where they would take it.
			s.tune.roundStart = now
			break
		}
		s.closeRoundLocked()
		s.tune.roundStart = s.tune.roundStart.Add(round)
	}
}

func (s *Scheduler) closeRoundLocked() {
	t := &s.tune
	s.sampleUniqueLocked()
	copy(t.history[1:], t.history[:autotuneHistory-1])
	t.history[0] = t.roundMaxU
	if t.rounds < autotuneHistory {
		t.rounds++
	}
	ceiling := s.cfg.MaxOutstandingBytes
	floor := s.autotuneStartup()
	if t.covered && t.blocked && t.room {
		t.bound = min(2*t.bound, ceiling)
	} else {
		var peak uint64
		for _, u := range t.history[:t.rounds] {
			peak = max(peak, u)
		}
		target := intCount(min(2*peak, byteCount(ceiling)))
		target = max(target, floor)
		switch {
		case target < t.bound:
			// Move toward it rather than to it: one quiet round is not
			// proof the flow no longer needs what it was using.
			t.bound -= (t.bound - target + 1) / 2
		case target > t.bound && t.covered:
			t.bound = target
		}
	}
	t.bound = min(max(t.bound, floor), ceiling)
	t.covered = false
	// A producer still waiting on the bound is blocked in the new round
	// too, and so is a lane still idle beside it.
	t.blocked = t.waiting
	t.room = t.waiting && s.idleLanes > 0
	t.roundMaxU = s.uniqueBytes
}

// RetentionBound reports the current Autotune bound, or zero without one.
func (s *Scheduler) RetentionBound() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cfg.Autotune == nil {
		return 0
	}
	s.autotuneLocked(s.cfg.Now())
	return s.tune.bound
}

// HasReadyWork reports whether anything read from the source is waiting for
// its first dispatch, including an empty final marker: work the flow owes
// the peer although none of it has been sent.
func (s *Scheduler) HasReadyWork() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, chunk := range s.pending {
		if _, live := s.live[chunk.Offset]; !live || chunk.Final {
			return true
		}
	}
	return false
}

// ReadyBytes reports Q: bytes read from the source and not yet dispatched to
// any lane. They are pending work although nothing has been sent.
func (s *Scheduler) ReadyBytes() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.readyBytesLocked()
}

// markEOFLocked records the end of the stream. The final marker rides on the
// last chunk when there is one, so a receiver cannot see the end before the
// bytes that precede it.
func (s *Scheduler) markEOFLocked() {
	s.eof = true
	if len(s.pending) > 0 {
		s.pending[len(s.pending)-1].Final = true
		return
	}
	for _, out := range s.live {
		if out.chunk.End() == s.nextOffset {
			// The last data chunk is already in flight; it cannot be amended,
			// so the stream is ended by an empty final chunk at the tail.
			break
		}
	}
	s.pending = append(s.pending, &Chunk{Offset: s.nextOffset, Final: true})
}

// Next blocks until this lane should carry a chunk, and returns it. It returns
// nil with a nil error when the stream is finished, and ErrClosed after Close.
//
// This is the whole of the pacing policy: a lane calls it when it is ready for
// more work, so the lane's own speed decides its share. There is no rate
// estimate here, and no decision about which lane deserves the chunk -- the
// lane that asks first gets it, and a lane asks when it is free.
func (s *Scheduler) Next(ctx context.Context, laneID uint64, windowBytes int) (*Chunk, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	stop := context.AfterFunc(ctx, func() { s.mu.Lock(); defer s.mu.Unlock(); s.ready.Broadcast() })
	defer stop()

	for {
		if s.closed {
			return nil, ErrClosed
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		idle := false
		if s.laneLoad[laneID] < s.cfg.LaneWindow {
			if chunk := s.takeReadyLocked(laneID, windowBytes); chunk != nil {
				return chunk, nil
			}
			if err := s.finishedLocked(); err != nil {
				return nil, err
			} else if s.doneLocked() {
				return nil, nil
			}
			// Idle means the lane could carry another chunk if one were
			// ready: below its count limit and with byte room for one. Only
			// the autotune reads it, so the legacy path skips the probe.
			idle = s.cfg.Autotune != nil && len(s.pending) == 0 &&
				s.hasRoomLocked(laneID, windowBytes, byteCount(s.cfg.ChunkSize))
		}
		if idle {
			// This lane could carry more and has nothing to carry. If the
			// producer is held by the retention bound at the same time, the
			// bound -- not the path -- is what limits the flow.
			s.idleLanes++
			if s.tune.waiting {
				s.tune.room = true
			}
		}
		s.ready.Wait()
		if idle {
			s.idleLanes--
		}
	}
}

// takeReadyLocked assigns the oldest ready chunk to a lane. Oldest first
// matters: the receiver delivers contiguously, so the chunk nearest its
// contiguous point is the one whose absence is holding up delivery.
func (s *Scheduler) takeReadyLocked(laneID uint64, windowBytes int) *Chunk {
	for i, chunk := range s.pending {
		// Room is charged against the chunk's actual size. Charging a nominal
		// ChunkSize instead looks harmless and is not: a read from a TCP socket
		// usually returns far less than the maximum, so the windows fill with
		// chunks that are mostly accounting. Measured on a 100 Mbit/s path,
		// that under-filled a 2 MB window to about 224 KB of real data and cost
		// a quarter of single-lane throughput.
		if !s.hasRoomLocked(laneID, windowBytes, uint64(len(chunk.Data))) && !chunk.urgent {
			// The lane is full. Only recovery work jumps the window, and it is
			// always at the head of the ready set, so stopping here keeps the
			// scan cheap.
			return nil
		}
		if out, ok := s.live[chunk.Offset]; ok && laneHasReliableAttempt(out, laneID) {
			// This lane already took this chunk in a way that will deliver it
			// or die, so a second copy is spent on the one outcome it cannot
			// help. Had it taken it unreliably the opposite would hold: the
			// chunk may simply be gone, and on a single-lane flow this is the
			// only way it comes back.
			//
			continue
		}
		s.pending = append(s.pending[:i], s.pending[i+1:]...)
		out, exists := s.live[chunk.Offset]
		if !exists {
			out = &outstanding{chunk: chunk}
			s.live[chunk.Offset] = out
			// Close the rounds that ended before this dispatch with the U
			// they had: bytes that sat READY were not in flight then.
			s.autotuneLocked(s.cfg.Now())
			s.readyBytes -= min(s.readyBytes, uint64(len(chunk.Data)))
			s.uniqueBytes += uint64(len(chunk.Data))
			s.sampleUniqueLocked()
			s.stats.ChunksIssued++
			s.stats.BytesIssued += uint64(len(chunk.Data))
		} else {
			s.stats.ChunksRetransmit++
			s.stats.BytesRetransmit += uint64(len(chunk.Data))
		}
		var deadline time.Time
		if after := s.retransmitAfter(); after > 0 {
			deadline = s.cfg.Now().Add(after)
		}
		s.nextAttempt++
		if s.nextAttempt == 0 {
			// Zero belongs to source chunks which have not been dispatched. A
			// wrap is not realistic in one flow, but preserving the invariant is
			// free and keeps stale callbacks unambiguous even there.
			s.nextAttempt++
		}
		id := s.nextAttempt
		reliable := s.laneRetransmits(laneID)
		if s.cfg.ForceReliable != nil && s.cfg.ForceReliable(chunk.Offset) {
			reliable = true
		}
		if s.cfg.CodedRetry {
			if exists {
				// The sticky reliable bit: a chunk dispatched before had its
				// one coded attempt, and every later one is on the stream.
				reliable = true
			}
			if !reliable {
				// The deadline starts at registration, not here: from now
				// until then the bytes wait in queues they cannot be lost in.
				deadline = time.Time{}
			}
		}
		out.attempts = append(out.attempts, attempt{
			id: id, lane: laneID, deadline: deadline, reliable: reliable,
		})
		chunk.urgent = false
		s.laneLoad[laneID]++
		s.laneBytes[laneID] += uint64(len(chunk.Data))
		s.laneQueued[laneID] += uint64(len(chunk.Data))
		s.totalBytes += uint64(len(chunk.Data))
		if len(s.live) > s.stats.PeakOutstanding {
			s.stats.PeakOutstanding = len(s.live)
		}
		s.produced.Signal()
		issued := *chunk
		issued.Reliable = reliable
		issued.reservation = 0
		issued.attempt = id
		return &issued
	}
	return nil
}

// laneRetransmits reports whether a lane recovers its own losses.
func (s *Scheduler) laneRetransmits(laneID uint64) bool {
	if s.cfg.Reliable == nil {
		return true
	}
	return s.cfg.Reliable(laneID)
}

// laneHasAttempt reports whether this lane is carrying this chunk at all,
// however it took it.
func laneHasAttempt(out *outstanding, laneID uint64) bool {
	for _, a := range out.attempts {
		if a.lane == laneID {
			return true
		}
	}
	return false
}

// laneHasReliableAttempt reports whether this lane already holds this chunk in
// a form that will arrive or fail loudly.
func laneHasReliableAttempt(out *outstanding, laneID uint64) bool {
	for _, a := range out.attempts {
		if a.lane == laneID && a.reliable {
			return true
		}
	}
	return false
}

func (s *Scheduler) finishedLocked() error {
	if s.srcErr != nil && len(s.pending) == 0 && len(s.live) == 0 {
		return s.srcErr
	}
	return nil
}

func (s *Scheduler) doneLocked() bool {
	return s.eof && len(s.pending) == 0 && len(s.live) == 0
}

// Complete reports that a chunk arrived. It is idempotent and indifferent to
// which lane carried it: a chunk re-issued on a second lane is complete when
// either copy lands, and the other lane's attempt is released with it.
func (s *Scheduler) Complete(laneID uint64, chunk *Chunk) {
	if chunk == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	live, ok := s.live[chunk.Offset]
	if ok {
		for _, a := range live.attempts {
			s.releaseLaneLocked(a.lane, uint64(len(live.chunk.Data)), !a.written)
		}
		delete(s.live, chunk.Offset)
		if s.cfg.Autotune != nil {
			// Validated coverage is the only thing that may grow the bound,
			// and only for bytes newly covered: a chunk is removed from live
			// once, whether cumulative or range coverage reached it first.
			s.autotuneLocked(s.cfg.Now())
			s.tune.covered = true
		}
		s.uniqueBytes -= min(s.uniqueBytes, uint64(len(live.chunk.Data)))
		s.stats.ChunksCompleted++
		s.stats.CompletedByLaneID[laneID]++
	}
	// A chunk can be in the ready set and in flight at the same time: that is
	// what re-offering a slow lane's chunk to another lane means. If it then
	// arrives by the first route, the copy waiting in the ready set is bytes
	// the peer already has, and handing it to a lane sends them again. Left
	// unremoved, this measured a 50% overhead on a 20% loss path -- 481 chunks
	// issued for a 320-chunk object -- and it was invisible in the retransmit
	// counter, because a chunk taken after its completion is no longer live and
	// counts as a new issue rather than a re-issue.
	dropped := s.removePendingLocked(chunk.Offset)
	if !ok && dropped == nil {
		return
	}
	if ok {
		s.releaseChunkLocked(live.chunk)
	} else {
		// Acknowledged before its first dispatch: it leaves Q.
		s.readyBytes -= min(s.readyBytes, uint64(len(dropped.Data)))
		s.releaseChunkLocked(dropped)
	}
	s.signalIfDoneLocked()
	s.produced.Signal()
	s.ready.Broadcast()
}

// Fail reports that a lane could not carry a chunk. The chunk returns to the
// ready set for any other lane to pick up, so a lane that breaks mid-transfer
// costs its window and nothing more.
func (s *Scheduler) Fail(laneID uint64, chunk *Chunk) {
	if chunk == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out, ok := s.live[chunk.Offset]
	if !ok {
		return
	}
	if !s.dropAttemptLocked(out, laneID, chunk.attempt) {
		// This callback belongs to an unreliable attempt already retired by
		// timeout or receiver evidence. It must not fail its replacement merely
		// because both used the same lane and byte range.
		return
	}
	s.stats.LaneFailures++
	if len(out.attempts) == 0 {
		// The logical chunk remains outstanding even though this physical
		// attempt ended. Keeping that distinction makes a replacement a retry,
		// preserves late logical completion, and avoids briefly treating the
		// same retained bytes as new source work.
		s.requeueLocked(out.chunk)
	}
	s.ready.Broadcast()
}

// RetireLane releases every chunk a lane was carrying, for a lane that has
// died rather than a single chunk that failed.
func (s *Scheduler) RetireLane(laneID uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, out := range s.live {
		if !laneHasAttempt(out, laneID) {
			continue
		}
		s.stats.LaneFailures++
		s.dropLaneAttemptsLocked(out, laneID)
		if len(out.attempts) == 0 {
			s.requeueLocked(out.chunk)
		}
	}
	delete(s.laneLoad, laneID)
	delete(s.laneBytes, laneID)
	delete(s.laneQueued, laneID)
	s.ready.Broadcast()
}

func (s *Scheduler) dropAttemptLocked(out *outstanding, laneID, attemptID uint64) bool {
	for i, a := range out.attempts {
		if a.id != attemptID || a.lane != laneID {
			continue
		}
		queued := !a.written
		out.attempts = append(out.attempts[:i], out.attempts[i+1:]...)
		s.releaseLaneLocked(a.lane, uint64(len(out.chunk.Data)), queued)
		return true
	}
	return false
}

func (s *Scheduler) dropLaneAttemptsLocked(out *outstanding, laneID uint64) {
	kept := out.attempts[:0]
	for _, a := range out.attempts {
		if a.lane != laneID {
			kept = append(kept, a)
			continue
		}
		s.releaseLaneLocked(a.lane, uint64(len(out.chunk.Data)), !a.written)
	}
	out.attempts = kept
}

func (s *Scheduler) releaseLaneLocked(laneID uint64, size uint64, queued bool) {
	if s.laneLoad[laneID] > 0 {
		s.laneLoad[laneID]--
	}
	if queued {
		s.releaseQueuedLocked(laneID, size)
	}
	if s.laneBytes[laneID] >= size {
		s.laneBytes[laneID] -= size
	} else {
		size = s.laneBytes[laneID]
		s.laneBytes[laneID] = 0
	}
	if s.totalBytes >= size {
		s.totalBytes -= size
	} else {
		s.totalBytes = 0
	}
}

// hasRoomLocked reports whether a lane may be handed another chunk: it must fit
// both the lane's own window and the flow's.
//
// A lane holding nothing is always allowed one chunk. A window smaller than a
// chunk is otherwise able to stall a lane permanently, and a flow window that
// has collapsed must still make progress rather than deadlock.
func (s *Scheduler) hasRoomLocked(laneID uint64, windowBytes int, chunk uint64) bool {
	// The escape hatch is deliberately keyed on the flow holding nothing, not
	// on this lane holding nothing. Per-lane, it would hand every lane a free
	// chunk and let the flow exceed its window by one chunk per lane -- which
	// is exactly the over-claiming the flow window exists to prevent.
	if s.totalBytes == 0 {
		return true
	}
	if chunk == 0 {
		chunk = 1
	}
	// The flow's own window binds first and has no per-lane escape: it is what
	// stops N lanes claiming N times a single connection's share, so a lane
	// with nothing queued must not be able to step over it.
	if s.cfg.Windows != nil {
		if total := s.cfg.Windows.Total(); total > 0 && s.totalBytes+chunk > uint64(total) {
			return false
		}
		if windows, ok := s.cfg.Windows.(OutstandingWindows); ok {
			outstanding := s.laneBytes[laneID]
			if lane := windows.Outstanding(laneID, outstanding); lane > 0 && outstanding+chunk > uint64(lane) {
				return false
			}
		}
	}
	queued := s.laneQueued[laneID]
	if queued == 0 {
		// A lane whose transport has taken everything it was given always gets
		// one more chunk. Otherwise a bound smaller than a chunk stalls that
		// lane permanently, and the flow can deadlock with every lane waiting
		// for room none of them will get.
		return true
	}
	if windowBytes > 0 && queued+chunk > uint64(windowBytes) {
		return false
	}
	if s.cfg.Windows != nil {
		if lane := s.cfg.Windows.Lane(laneID, queued); lane > 0 && queued+chunk > uint64(lane) {
			return false
		}
	}
	return true
}

// retransmitAfter is how long the caller currently wants a chunk to wait
// before it is presumed lost.
func (s *Scheduler) retransmitAfter() time.Duration {
	if s.cfg.RetransmitAfter == nil {
		return 0
	}
	return s.cfg.RetransmitAfter()
}

// requeueLocked returns a chunk to the ready set in offset order, so the
// receiver's contiguous point is always what the next free lane works on.
func (s *Scheduler) requeueLocked(chunk *Chunk) {
	if s.pendingHas(chunk.Offset) {
		return
	}
	// A chunk only returns to the ready set because a lane stalled or failed,
	// which makes it the work most likely to be blocking delivery.
	chunk.urgent = true
	i := 0
	for i < len(s.pending) && s.pending[i].Offset < chunk.Offset {
		i++
	}
	s.pending = append(s.pending, nil)
	copy(s.pending[i+1:], s.pending[i:])
	s.pending[i] = chunk
}

// ReissueUnacknowledgedBelow re-offers chunks the peer has proved it did not
// receive, and reports how many.
//
// The proof is data acknowledged beyond them. A peer that reports holding
// bytes above a chunk received those bytes over the same path, so a chunk
// still outstanding well below that point is not late, it is gone -- the same
// inference a fast retransmit makes, at the layer where an unrepairable coded
// symbol actually goes missing.
//
// Only attempts that cannot recover themselves are re-offered. A chunk on a
// reliable lane will be retransmitted by that lane, and a second copy is spent
// on the one outcome it cannot help.
//
// Without this the only recovery was a timer, and the timer is a second and a
// half: measured live, a single chunk erased from the coded prefix of a
// download held the receiver's contiguous point -- and therefore every
// acknowledgement, and therefore the sender's entire clock -- for five
// seconds, on a transfer that took nine.
func (s *Scheduler) ReissueUnacknowledgedBelow(offset uint64) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gapNow = offset
	defer func() {
		if offset > s.gapHigh && s.cfg.CodedRetry {
			s.gapSeen[s.gapNext%gapHistory] = gapAdvance{at: s.cfg.Now(), offset: offset}
			s.gapNext++
		}
		s.gapHigh = max(s.gapHigh, offset)
	}()
	reissued := 0
	for _, out := range s.live {
		if out.chunk.End() > offset || len(out.attempts) == 0 {
			continue
		}
		if s.pendingHas(out.chunk.Offset) {
			continue
		}
		// The peer has proved that written unreliable copies are gone. Retire
		// their admission charges before making the replacement; retaining every
		// lost datagram attempt grows the lane window forever and eventually
		// makes a single missing tail chunk impossible to retry.
		s.retireUnreliableAttemptsLocked(out, time.Time{})
		reliable := false
		for _, a := range out.attempts {
			reliable = reliable || a.reliable
		}
		if reliable {
			// A reliable original is still capable of delivering. Retiring a
			// proven-missing speculative datagram is necessary accounting, but
			// receiver evidence alone does not justify another copy while that
			// original has not reached its recovery deadline.
			continue
		}
		if len(out.attempts) != 0 {
			// A copy not yet taken by its transport is still real work. Its Wrote
			// callback will make it eligible for the normal timed retry.
			continue
		}
		s.requeueLocked(out.chunk)
		reissued++
	}
	if reissued > 0 {
		s.ready.Broadcast()
	}
	return reissued
}

// ReissueExpired advances attempts according to their transport semantics.
//
// A reliable attempt remains live: its transport will deliver it or fail
// loudly, so recovery may add one copy on another lane but never replaces it.
// An unreliable attempt is different. Once its transport accepted the bytes
// and its recovery deadline passed, that attempt is terminally lost. It is
// retired before the logical chunk is dispatched again, keeping one active
// unreliable attempt per chunk no matter how many consecutive copies the path
// erases. Each dispatch has its own identity, so a late callback from the
// retired attempt cannot mutate the replacement.
func (s *Scheduler) ReissueExpired() int {
	after := s.retransmitAfter()
	if after <= 0 {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.cfg.Now()
	candidates := make([]*outstanding, 0)
	for _, out := range s.live {
		if len(out.attempts) == 0 || s.pendingHas(out.chunk.Offset) {
			continue
		}
		// A reliable original can have one speculative copy on an unreliable
		// lane. That copy has its own terminal lifecycle: once written and
		// expired it must release its accounting before the original is
		// considered for another speculation. Treating the mixed set as simply
		// "reliable" left the lost copy in the set forever.
		retired := s.retireUnreliableAttemptsLocked(out, now)
		if len(out.attempts) == 0 && retired {
			candidates = append(candidates, out)
			continue
		}
		reliable, expired := false, false
		for _, a := range out.attempts {
			reliable = reliable || a.reliable
			expired = expired || (!a.deadline.IsZero() && !now.Before(a.deadline))
		}
		if !expired {
			continue
		}
		// Reliable attempts can have one speculative copy on another lane.
		// Unreliable attempts are terminal after their deadline and are retired
		// before the same logical bytes are dispatched again.
		if reliable {
			if len(out.attempts) > 1 {
				continue
			}
		} else {
			if len(out.attempts) != 0 {
				continue
			}
		}
		candidates = append(candidates, out)
	}
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].chunk.Offset < candidates[j].chunk.Offset
	})
	reissued := 0
	reliableReissued := 0
	for _, out := range candidates {
		reliable := len(out.attempts) > 0 && out.attempts[0].reliable
		if reliable && s.cfg.ReliableReissueBurst > 0 {
			if reliableReissued >= s.cfg.ReliableReissueBurst {
				continue
			}
			reliableReissued++
		}
		// Retained reliable attempts get a new deadline so their pending copy
		// cannot be offered on every supervisor tick. Unreliable replacements
		// receive a fresh deadline when Next dispatches them.
		if len(out.attempts) > 0 {
			out.attempts[0].deadline = now.Add(after)
		}
		s.requeueLocked(out.chunk)
		reissued++
	}
	if reissued > 0 {
		s.ready.Broadcast()
	}
	return reissued
}

// retireUnreliableAttemptsLocked removes written unreliable attempts. With a
// non-zero deadline, only attempts expired at that instant are terminal; a
// zero deadline means receiver range evidence already proved them missing.
func (s *Scheduler) retireUnreliableAttemptsLocked(out *outstanding, deadline time.Time) bool {
	kept := out.attempts[:0]
	retired := false
	for _, a := range out.attempts {
		retire := !a.reliable && a.written
		if retire && !deadline.IsZero() {
			retire = !a.deadline.IsZero() && !deadline.Before(a.deadline)
		} else if retire && s.cfg.CodedRetry {
			// Coverage beyond an attempt still waiting to leave says nothing
			// about it; copying it now would give its bytes a second owner.
			// Nor does coverage the receiver already reported before the
			// attempt left: only evidence that has advanced since then
			// can show the attempt lost.
			retire = a.registered && s.gapNow > a.gapFloor
		}
		if !retire {
			kept = append(kept, a)
			continue
		}
		s.releaseLaneLocked(a.lane, uint64(len(out.chunk.Data)), false)
		retired = true
	}
	out.attempts = kept
	return retired
}

// removePendingLocked drops a chunk from the ready set, for a chunk that has
// arrived by another route.
func (s *Scheduler) removePendingLocked(offset uint64) *Chunk {
	for i, chunk := range s.pending {
		if chunk.Offset != offset {
			continue
		}
		s.pending = append(s.pending[:i], s.pending[i+1:]...)
		return chunk
	}
	return nil
}

func (s *Scheduler) releaseChunkLocked(chunk *Chunk) {
	if chunk == nil || chunk.reservation == 0 {
		return
	}
	reservation := chunk.reservation
	chunk.reservation = 0
	s.cfg.Memory.Release(reservation)
}

func (s *Scheduler) pendingHas(offset uint64) bool {
	for _, c := range s.pending {
		if c.Offset == offset {
			return true
		}
	}
	return false
}

// Done is closed once every byte of the source has been delivered. A caller
// that must act at the end of the stream -- sending a FIN, for instance --
// waits on this rather than on the lanes, because which lane carried the last
// chunk is not knowable in advance and does not matter.
func (s *Scheduler) Done() <-chan struct{} { return s.finished }

// Err returns the source's read error, if the stream ended in one.
func (s *Scheduler) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.srcErr
}

func (s *Scheduler) signalIfDoneLocked() {
	if !s.doneLocked() {
		return
	}
	select {
	case <-s.finished:
	default:
		close(s.finished)
	}
}

// releaseQueuedLocked drops bytes from a lane's queued commitment.
func (s *Scheduler) releaseQueuedLocked(laneID uint64, size uint64) {
	if s.laneQueued[laneID] >= size {
		s.laneQueued[laneID] -= size
		return
	}
	s.laneQueued[laneID] = 0
}

// Wrote reports that a lane's transport has taken a chunk's bytes.
//
// This is what paces a lane, and it is deliberately not the same event as
// acknowledgement. A QUIC stream write returns once the transport has packed
// the bytes into packets, which it does only when its congestion window and
// pacer allow -- so a lane that cannot move data stops accepting it, at the
// transport's own clock rather than a round trip later. Waiting for the peer's
// application-level acknowledgement instead adds a whole return trip to that
// loop, and a sender that must cover it commits several times as much to one
// lane, where no other lane can take it back.
//
// The chunk stays retained until Complete, because a lane that dies may not
// have delivered what its transport accepted.
func (s *Scheduler) Wrote(laneID uint64, chunk *Chunk) { s.wrote(laneID, chunk, nil) }

// WroteAs is Wrote for a transport that knows which substrate it used, and it
// is the one a lane writer should call.
//
// The attempt's reliability was recorded when the lane took the chunk, from
// what the lane said it would do. A lane's answer can change between then and
// the write -- the coding plan is re-chosen and the flow re-classified while
// the chunk waits in the lane's queue -- and when it changes towards the coded
// path the attempt is left believing it is on a stream. Such an attempt is
// never re-offered to its own lane, so on a single-lane flow the erased chunk
// waits for a second lane or the whole progress budget. What the wire was given
// is the only answer that cannot be stale.
func (s *Scheduler) WroteAs(laneID uint64, chunk *Chunk, reliable bool) {
	s.wrote(laneID, chunk, &reliable)
}

func (s *Scheduler) wrote(laneID uint64, chunk *Chunk, reliable *bool) {
	if chunk == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out, ok := s.live[chunk.Offset]
	if !ok {
		return
	}
	for i := range out.attempts {
		if out.attempts[i].id != chunk.attempt || out.attempts[i].lane != laneID || out.attempts[i].written {
			continue
		}
		a := &out.attempts[i]
		a.written = true
		if reliable != nil {
			if s.cfg.CodedRetry && a.reliable != *reliable {
				s.substrateChangedLocked(a, *reliable)
			}
			a.reliable = *reliable
		}
		s.releaseQueuedLocked(laneID, uint64(len(out.chunk.Data)))
		if s.cfg.CodedRetry && a.abandoned && !a.reliable {
			// The coded path gave these bytes up before the write was
			// reported, and they went nowhere else.
			s.retireAttemptLocked(out, a.id, true)
		} else if s.cfg.CodedRetry && s.provenLostLocked(out, a) {
			s.retireAttemptLocked(out, a.id, false)
		}
		s.ready.Broadcast()
		return
	}
}

// substrateChangedLocked keeps a CodedRetry attempt's deadline right when its
// write used a different substrate than its dispatch expected. Bytes that went
// to the stream after all are the stream's, on the deadline any reliable
// attempt has; bytes that went to the code wait for their registration,
// unless it has already been reported.
func (s *Scheduler) substrateChangedLocked(a *attempt, reliable bool) {
	switch {
	case reliable:
		// Whatever deadline the code had, the stream's own starts now.
		a.deadline = time.Time{}
		if after := s.retransmitAfter(); after > 0 {
			a.deadline = s.cfg.Now().Add(after)
		}
	case !a.registered:
		a.deadline = time.Time{}
	}
}

// Registered reports, under CodedRetry, that every packet carrying an
// unreliable attempt's bytes has been registered for sending. The attempt's
// recovery deadline starts now. A report for an attempt that has been retired
// is ignored: the attempt identity is not reused.
func (s *Scheduler) Registered(laneID uint64, chunk *Chunk) { s.RegisteredAgo(laneID, chunk, 0) }

// RegisteredAgo is Registered for a registration that happened age ago: the
// report can reach the scheduler a little after the packet was built, and the
// deadline counts from the packet, not from the report.
func (s *Scheduler) RegisteredAgo(laneID uint64, chunk *Chunk, age time.Duration) {
	if chunk == nil || !s.cfg.CodedRetry {
		return
	}
	// Taken before the lock, so time spent waiting for it is not added to
	// the code's ownership either.
	registeredAt := s.cfg.Now().Add(-max(age, 0))
	s.mu.Lock()
	defer s.mu.Unlock()
	out, a := s.attemptLocked(laneID, chunk)
	if a == nil || a.registered {
		return
	}
	a.registered = true
	a.gapFloor = s.gapFloorAtLocked(registeredAt)
	if a.reliable {
		// A stream copy (the pre-OPEN_OK safety copy) owns these bytes.
		return
	}
	a.deadline = time.Time{}
	if after := s.retransmitAfter(); after > 0 {
		a.deadline = registeredAt.Add(after)
	}
	// Gap evidence that arrived after the packet left, but before this
	// report did, was skipped while the attempt looked unregistered. It
	// shows the attempt lost now.
	if s.provenLostLocked(out, a) {
		s.retireAttemptLocked(out, a.id, false)
		s.ready.Broadcast()
	}
}

// provenLostLocked reports whether gap evidence newer than a registered,
// written coded attempt already covers beyond its chunk.
func (s *Scheduler) provenLostLocked(out *outstanding, a *attempt) bool {
	return a.registered && a.written && !a.reliable &&
		s.gapHigh > a.gapFloor && out.chunk.End() <= s.gapHigh
}

// gapFloorAtLocked is the gap evidence already seen at an instant.
func (s *Scheduler) gapFloorAtLocked(at time.Time) uint64 {
	floor := uint64(0)
	oldest := s.gapHigh
	for i := 0; i < min(s.gapNext, gapHistory); i++ {
		seen := s.gapSeen[i]
		oldest = min(oldest, seen.offset)
		if !seen.at.After(at) {
			floor = max(floor, seen.offset)
		}
	}
	if s.gapNext > gapHistory {
		// Advances older than the history happened before any it holds.
		floor = max(floor, oldest)
	}
	if floor == 0 && s.gapNext == 0 {
		return s.gapHigh
	}
	return floor
}

// Abandoned reports, under CodedRetry, that the coded path gave up an
// unreliable attempt before all of its bytes were registered: they will
// never be sent. The attempt is retired and the chunk returns to the ready
// set, where the sticky bit puts it on the stream. A write the transport has
// not reported yet is settled when it is, because it may have gone to the
// stream as well. A report for a retired attempt is ignored.
func (s *Scheduler) Abandoned(laneID uint64, chunk *Chunk) {
	if chunk == nil || !s.cfg.CodedRetry {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out, a := s.attemptLocked(laneID, chunk)
	if a == nil {
		return
	}
	if !a.written {
		a.abandoned = true
		return
	}
	if a.reliable {
		return
	}
	s.retireAttemptLocked(out, a.id, true)
	s.ready.Broadcast()
}

func (s *Scheduler) attemptLocked(laneID uint64, chunk *Chunk) (*outstanding, *attempt) {
	out, ok := s.live[chunk.Offset]
	if !ok {
		return nil, nil
	}
	for i := range out.attempts {
		if out.attempts[i].id == chunk.attempt && out.attempts[i].lane == laneID {
			return out, &out.attempts[i]
		}
	}
	return nil, nil
}

// retireAttemptLocked ends one unreliable attempt and, if it was the chunk's
// last, returns the chunk to the ready set.
func (s *Scheduler) retireAttemptLocked(out *outstanding, id uint64, abandoned bool) {
	for i, a := range out.attempts {
		if a.id != id {
			continue
		}
		out.attempts = append(out.attempts[:i], out.attempts[i+1:]...)
		s.releaseLaneLocked(a.lane, uint64(len(out.chunk.Data)), !a.written)
		if abandoned {
			s.stats.CodedAbandoned++
		}
		break
	}
	if len(out.attempts) == 0 {
		s.requeueLocked(out.chunk)
	}
}

// LaneOutstanding reports what one lane is holding: chunks handed to it and
// not yet acknowledged, their total size, and how much of that its transport
// has not yet taken. It answers the question a throughput number cannot --
// whether a lane is short of work or full of it -- and the last of the three is
// what admission is compared against.
func (s *Scheduler) LaneOutstanding(laneID uint64) (chunks int, retained, queued uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.laneLoad[laneID], s.laneBytes[laneID], s.laneQueued[laneID]
}

// Pending reports chunks read from the source and not yet handed to any lane,
// and the flow's total unacknowledged bytes.
func (s *Scheduler) Pending() (ready int, outstandingBytes uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.pending), s.totalBytes
}

// Stats returns a snapshot.
func (s *Scheduler) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := s.stats
	out.CompletedByLaneID = make(map[uint64]uint64, len(s.stats.CompletedByLaneID))
	for k, v := range s.stats.CompletedByLaneID {
		out.CompletedByLaneID[k] = v
	}
	return out
}

// Close releases every waiter. Chunks in flight are abandoned.
func (s *Scheduler) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	s.cancel()
	seen := make(map[*Chunk]struct{}, len(s.pending)+len(s.live))
	for _, chunk := range s.pending {
		seen[chunk] = struct{}{}
	}
	for _, out := range s.live {
		seen[out.chunk] = struct{}{}
	}
	for chunk := range seen {
		s.releaseChunkLocked(chunk)
	}
	s.pending = nil
	s.live = make(map[uint64]*outstanding)
	s.uniqueBytes = 0
	s.readyBytes = 0
	s.ready.Broadcast()
	s.produced.Broadcast()
}
