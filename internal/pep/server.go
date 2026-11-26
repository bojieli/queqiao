package pep

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apernet/quic-go"
	"github.com/bojieli/queqiao/internal/classifier"
	"github.com/bojieli/queqiao/internal/identity"
	"github.com/bojieli/queqiao/internal/limiter"
	"github.com/bojieli/queqiao/internal/metrics"
	"github.com/bojieli/queqiao/internal/portmux"
	"github.com/bojieli/queqiao/internal/profile"
	"github.com/bojieli/queqiao/internal/protocol"
	"github.com/bojieli/queqiao/internal/session"
)

// Must exceed the client's bounded lane-replacement wait so a final-ACK loss
// can still be repaired after QUIC dead-path detection and scheduler backoff.
// Tombstones retain only final sequence metadata and remain bounded by the
// configured session admission limit.
const completedSessionLinger = 90 * time.Second

type ServerConfig struct {
	// Profile names the deployment this gateway serves; see internal/profile.
	// The zero value is the supported access-link profile.
	Profile           profile.Profile
	ListenAddr        string
	Credentials       identity.ServerCredentials
	Enrollment        *identity.EnrollmentService
	ChunkSize         int
	HandshakeTimeout  time.Duration
	FlowIdleTimeout   time.Duration
	FlowMaxLifetime   time.Duration
	MaxSessions       int
	DestinationPolicy DestinationPolicy
	EnableTCP         bool
	EnableQUIC        bool
	// TCPFallbackLanes is the admission ceiling for one negotiated TCP-only
	// flow. The client chooses the active target; keeping the server ceiling at
	// 16 lets operators compare 8 and 16 without changing the gateway.
	TCPFallbackLanes int
	// TCPCongestion selects the Linux kernel congestion controller inherited by
	// accepted fallback sockets. "system" leaves the host default untouched.
	TCPCongestion string
	// Memory selects the memory-ownership bundle; empty is legacy. A gateway
	// has no shared send budget, so only the retention autotune and the
	// no-progress accounting of READY bytes apply to it.
	Memory     MemoryMode
	Congestion CongestionControlKind
	// CCAPI selects how QUIC congestion controllers are driven; see
	// CCAPIVersion. Empty is v1.
	CCAPI CCAPIVersion
	// Sched selects how a QUIC connection orders the classes of work it
	// sends; see SchedMode. Empty is legacy. Class requires CCAPI v2.
	Sched SchedMode
	// CongestionScope selects what one congestion controller governs; see
	// CongestionScope. Empty is connection. Path requires CCAPI v2 and
	// Sched class.
	CongestionScope CongestionScope
	// CodedRetry selects the coded-retry bundle; see CodedRetryMode. Empty
	// is legacy.
	CodedRetry CodedRetryMode
	// Classifier selects the flow-classifier bundle; see ClassifierMode.
	// Empty is legacy. The gateway classifies the direction it sends, so
	// downstream DATA follows this setting, not the client's.
	Classifier                    ClassifierMode
	BrutalBytesPerSec             uint64
	AdaptiveMinBytesSec           uint64
	AdaptiveMaxBytesSec           uint64
	AggregateBytesPerSec          uint64
	InteractiveReserveBytesPerSec uint64
	// StreamReceiveWindow and ConnectionReceiveWindow override the QUIC
	// receive windows. Zero selects the defaults, which match TUIC.
	StreamReceiveWindow     uint64
	ConnectionReceiveWindow uint64
	Metrics                 *metrics.Registry
	Logger                  *slog.Logger
	// UDPOnStream keeps SOCKS UDP packets on the lane's control stream even
	// where the QUIC connection negotiated datagrams. See the client's field:
	// it is a measurement control, and both endpoints must agree for the
	// comparison to mean anything.
	UDPOnStream bool
	// HopPortCount enables the server to accept QUIC connections on multiple
	// UDP ports simultaneously. It must match the client's hop_port_count in
	// the profile. 0 and 1 both disable port hopping. Values ≥ 2 cause the
	// server to listen on that many ports derived from the provider ID and
	// route responses back through whichever port each client most recently
	// used.
	HopPortCount int
	// AcceptWire lists the data-plane wire versions this gateway accepts. The
	// order does not matter: the gateway always negotiates the newest version
	// the client also offers. Empty accepts every version this build speaks,
	// which is what lets gateways be upgraded ahead of their clients; a
	// single version emulates a gateway that speaks only that one, and is how
	// a deployment retires a version once its clients have moved.
	AcceptWire []byte
	// GatewayAdmission selects what the session limit counts; see
	// GatewayAdmission. Empty is legacy.
	GatewayAdmission GatewayAdmission
	// Recovery selects the recovery bundle; see RecoveryMode. Empty is flow.
	// On a gateway carrier disables lane demotion, activates the adaptive
	// keep-alive schedule and grants PROBE replies; it requires
	// CongestionScope path and CodedRetry sticky.
	Recovery RecoveryMode
	// testReleasedAdmission emulates a released gateway's admission exactly:
	// a stream that finds no session slot is closed before its first frame
	// is read, and no standby connection is admitted past a full bound. It
	// exists so tests can show the degraded path against released gateways
	// (design §10.2) and is false in production.
	testReleasedAdmission bool
	// testLaneWriteHook is intentionally unexported and nil in production. It
	// lets package integration tests reproduce loss of a specific logical
	// frame without depending on encrypted QUIC packet layout.
	testLaneWriteHook func(protocol.Frame) error

	// CarrierShadow runs the carrier-liveness observer beside today's
	// recovery; see CarrierShadowConfig. Off by default; it never changes a
	// decision.
	CarrierShadow CarrierShadowConfig
}

type Server struct {
	cfg         ServerConfig
	semaphore   chan struct{}
	connections chan struct{}
	enrollments chan struct{}
	sessionsMu  sync.RWMutex
	sessions    map[[16]byte]*serverFlow
	// pending holds sessions whose OPEN has arrived but whose destination is
	// still being dialled. A JOIN for one of them waits for the OPEN to be
	// decided instead of being told the session does not exist (issue #159).
	pending map[[16]byte]*pendingSession
	// ended remembers, for a while, the sessions this gateway has held, so
	// a JOIN for one is told it ended rather than that it was never seen.
	ended            endedSessions
	accountMu        sync.Mutex
	accountUsage     map[string]*accountUsage
	maxObservedLanes atomic.Int64
	// These keep three kinds of record readable during a storm: lane-join
	// refusals, account admission refusals, and enrollment or renewal
	// attempts. All three are a stranger's to generate.
	//
	// They are three limiters rather than one because a storm in any of them
	// must not suppress the others. The record an operator wants when a user
	// says some sites load and others do not is the account one, and a peer
	// hammering lane joins would otherwise bury it.
	refusals        recordLimiter
	accountRefusals recordLimiter
	enrollLog       recordLimiter
	budget          *limiter.Budget
	metrics         *metrics.Registry
	// udpRelays holds the relay sockets of UDP associations whose lane died,
	// so the replacement association keeps the source address the destination
	// has been talking to.
	udpRelays *udpRelayStore

	// handshakes bounds TLS/TCP connections between accept and admission:
	// in owned mode every one, in legacy mode those accepted past a full
	// session limit to be read as a JOIN or PROBE.
	handshakes chan struct{}
	// principalConns counts each principal's admitted QUIC data connections,
	// so one standby can be admitted beside them when the bound is full.
	connMu         sync.Mutex
	principalConns map[string]*principalConnections
	// firstFrames bounds, gateway-wide, the QUIC streams being read before
	// they hold a session slot: in owned mode every stream until admission,
	// in legacy mode those read past a full session limit. Each holds a
	// frame reader, so the per-connection bound alone would let the total
	// grow with the connection count.
	firstFrames chan struct{}
	// tombstones counts completed sessions kept in sessions for final-ACK
	// replay (owned admission). They hold no session slot, so the owned
	// count of live sessions leaves them out, and they have a bound of their
	// own. Guarded by sessionsMu.
	tombstones int

	// carrierShadow is the carrier-liveness observer; nil when it is off.
	carrierShadow *carrierShadow
}

type serverFlow struct {
	flow      *multipathFlow
	principal identity.Principal
	// wire is the version the flow's OPEN negotiated. A flow keeps it for
	// life: a JOIN arriving on a connection of another version is refused
	// rather than mixed into the flow. Zero means the preferred version.
	wire        byte
	maxLanes    int
	tcpMaxLanes int
	tcpMode     bool
	completed   atomic.Bool
	tombstone   sync.Once
	mu          sync.Mutex

	// retireControlOnJoin: a RESERVE_CONTROL JOIN retires the lane that held
	// the control role, rather than adding beside it (owned admission;
	// design §10.3 step 1).
	retireControlOnJoin bool
}

// quicAuthState carries the immutable device principal established by the
// mutual TLS handshake. flows counts sessions multiplexed on the connection.
type quicAuthState struct {
	principal identity.Principal
	flows     atomic.Int64
	// probeGranted says this connection's one verification grant for a
	// PROBE reply has been issued (recovery=carrier).
	probeGranted atomic.Bool
}

// shared reports whether more than one flow is using this connection.
func (a *quicAuthState) shared() bool {
	if a == nil {
		return false
	}
	return a.flows.Load() > 1
}

// serverLaneBudget is the total lanes this endpoint will admit for one flow.
// It must match the client's split in bulkLaneBudget: counting the reserved
// control lane against the bulk maximum makes the server reject and close
// every joined bulk lane, which the peer sees as an immediate EOF and retries,
// churning through lanes instead of transferring.
func serverLaneBudget(reserveControl bool) int {
	bulk, control := bulkLaneBudget(reserveControl)
	return bulk + control
}

// laneJoinAdmissionTimeout bounds the OPEN_OK write of an admitted JOIN. The
// connection handshake deadline is already cleared by then, so without its
// own bound a blocked write holds a staged lane -- counted against the
// admission ceiling, never ready, never schedulable -- for as long as the
// peer stays half-open. One path-detection budget matches the rescue-race
// window a young staged lane is protected from eviction for.
const laneJoinAdmissionTimeout = laneDeadPathDetection

// Lane admission refusals are typed so the join handler can answer with the
// reset code matching how permanent the refusal is. The client may retry the
// ceiling answer or hand off to TCP, so only the genuine ceiling may carry
// ResetFlowLimit; answers that cannot change during the flow's life must not
// wear it.
var (
	// errLaneFlowTCPMode refuses a non-TCP lane: the flow committed to TCP
	// fallback and stays there for the rest of its life.
	errLaneFlowTCPMode = errors.New("flow has switched to TCP fallback")
	// errLaneFlowClosed refuses a lane for a flow that is already gone.
	errLaneFlowClosed = errors.New("flow is closed")
	// errLaneDuplicateID refuses a lane id the flow already carries.
	errLaneDuplicateID = errors.New("duplicate lane id")
	// errLaneLimitReached is the genuine admission ceiling, the one refusal
	// a peer may legitimately retry: lanes come and go, so the answer can
	// change during the flow's life.
	errLaneLimitReached = errors.New("flow lane limit reached")
)

func (s *serverFlow) addLane(lane *mpLane) error {
	return s.addLaneReplacing(lane, 0, false)
}

func (s *serverFlow) addLaneReplacing(lane *mpLane, replacedLaneID uint64, replaceLane bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	f := s.flow
	f.lanesMu.Lock()
	retired := make(map[uint64]*mpLane)
	defer func() {
		f.lanesMu.Unlock()
		for _, old := range retired {
			if sched := f.scheduler.Load(); sched != nil {
				sched.RetireLane(old.id)
			}
			if old.fc != nil {
				_ = old.fc.Close()
			}
		}
	}()
	if err := f.validateLaneLocked(lane); err != nil {
		return err
	}
	if s.tcpMode && lane.kind != TransportTCP {
		return errLaneFlowTCPMode
	}
	handoff := lane.kind == TransportTCP && !s.tcpMode
	limit := s.maxLanes
	if handoff {
		limit = s.tcpMaxLanes
	}
	// Plan the entire admission before retiring anything. Duplicate IDs,
	// shutdown, or a capacity refusal must leave the old transport intact.
	remaining := 0
	for id, old := range f.lanes {
		if old.closed.Load() {
			continue
		}
		if (handoff && old.kind != TransportTCP) || (replaceLane && replacedLaneID != lane.id && id == replacedLaneID) {
			retired[id] = old
		} else {
			remaining++
		}
	}
	if remaining >= limit {
		victim := f.oldestLaneLocked(lane.control, laneDeadPathDetection, retired)
		if victim == nil || remaining-1 >= limit {
			// The deferred cleanup owns committed retirements only.
			clear(retired)
			return errLaneLimitReached
		}
		retired[victim.id] = victim
	}
	if s.retireControlOnJoin && lane.control && lane.staged {
		// A control transfer displaces lanes only on commit -- the victim at
		// the ceiling and any lane REPLACE_LANE names -- so an OPEN_OK that
		// cannot be delivered leaves the table as it was. The staged lane
		// may be over the ceiling until then.
		for _, old := range retired {
			lane.evictOnCommit = append(lane.evictOnCommit, old)
		}
		clear(retired)
	}
	for id, old := range retired {
		delete(f.lanes, id)
		old.closed.Store(true)
	}
	if handoff {
		s.tcpMode = true
		s.maxLanes = limit
	}
	f.addLaneLocked(lane)
	if s.tcpMode && s.maxLanes > 1 {
		f.tcpStriping.Store(true)
	}
	return nil
}

// commitControlTransfer completes a RESERVE_CONTROL JOIN in owned admission
// (design §10.3 step 1): the lane that held the control role is retired, but
// only now, once the replacement's OPEN_OK has been written and the lane is
// active. Admission merely stages the replacement beside it, so a JOIN whose
// OPEN_OK cannot be delivered is rolled back by removing the staged lane and
// leaves the old holder exactly as it was. A holder younger than the eviction
// protection is a rescue that just won and is kept; the client closes the old
// stream itself in any case.
func (s *serverFlow) commitControlTransfer(replacement *mpLane) {
	if !s.retireControlOnJoin || !replacement.control {
		return
	}
	// Also called for the eviction staged at the lane ceiling.
	f := s.flow
	s.mu.Lock()
	f.lanesMu.Lock()
	var retired []*mpLane
	if f.lanes[replacement.id] == replacement && !replacement.closed.Load() {
		for _, victim := range replacement.evictOnCommit {
			if f.lanes[victim.id] == victim && !victim.closed.Load() {
				delete(f.lanes, victim.id)
				victim.closed.Store(true)
				retired = append(retired, victim)
			}
		}
		replacement.evictOnCommit = nil
		for id, old := range f.lanes {
			if id == replacement.id || old.closed.Load() || !f.laneIsControl(old) {
				continue
			}
			if old.suspected.Load() || time.Since(old.admitted) >= laneDeadPathDetection {
				delete(f.lanes, id)
				old.closed.Store(true)
				retired = append(retired, old)
			}
		}
	}
	f.lanesMu.Unlock()
	s.mu.Unlock()
	for _, old := range retired {
		if sched := f.scheduler.Load(); sched != nil {
			sched.RetireLane(old.id)
		}
		if old.fc != nil {
			_ = old.fc.Close()
		}
	}
}

// wireVersion is the version the flow is pinned to.
func (s *serverFlow) wireVersion() byte {
	if s.wire == 0 {
		return defaultFrameWire()
	}
	return s.wire
}

func newServerFlow(flow *multipathFlow, principal identity.Principal, initialKind TransportKind, tcpMaxLanes int) *serverFlow {
	serverSession := &serverFlow{
		flow: flow, principal: principal, maxLanes: serverLaneBudget(flow.reserveControlLane),
		tcpMaxLanes: tcpMaxLanes,
	}
	if initialKind == TransportTCP {
		serverSession.tcpMode = true
		serverSession.maxLanes = tcpMaxLanes
		flow.tcpStriping.Store(tcpMaxLanes > 1)
	}
	return serverSession
}

func NewServer(cfg ServerConfig) (*Server, error) {
	if cfg.ListenAddr == "" {
		return nil, errors.New("server listen address is required")
	}
	if err := cfg.Credentials.Validate(); err != nil {
		return nil, fmt.Errorf("server identity: %w", err)
	}
	// ChunkSize is a sending policy: how much of a byte stream one DATA frame
	// carries. It is not a receive limit -- protocol.MaxPayload is, and it is
	// fixed by the wire version -- so an out-of-range value is corrected to the
	// default rather than allowed to produce frames the peer must reject.
	memoryMode, err := ParseMemoryMode(string(cfg.Memory))
	if err != nil {
		return nil, err
	}
	cfg.Memory = memoryMode
	if cfg.ChunkSize <= 0 || cfg.ChunkSize > protocol.MaxPayload {
		cfg.ChunkSize = defaultChunkSize
	}
	if cfg.HandshakeTimeout <= 0 {
		cfg.HandshakeTimeout = 10 * time.Second
	}
	if cfg.FlowIdleTimeout <= 0 {
		cfg.FlowIdleTimeout = defaultFlowIdleTimeout
	}
	if cfg.FlowMaxLifetime <= 0 {
		cfg.FlowMaxLifetime = defaultFlowMaxLifetime
	}
	if cfg.FlowIdleTimeout > cfg.FlowMaxLifetime {
		return nil, errors.New("flow idle timeout cannot exceed maximum lifetime")
	}
	if cfg.MaxSessions <= 0 {
		cfg.MaxSessions = 4096
	}
	if cfg.MaxSessions > maxConfiguredSessions {
		return nil, fmt.Errorf("maximum sessions must not exceed %d", maxConfiguredSessions)
	}
	admission, err := ParseGatewayAdmission(string(cfg.GatewayAdmission))
	if err != nil {
		return nil, err
	}
	cfg.GatewayAdmission = admission
	acceptWire, err := resolveWireVersions(cfg.AcceptWire)
	if err != nil {
		return nil, fmt.Errorf("accepted wire versions: %w", err)
	}
	// The gateway's preference is what negotiates, so its list is kept newest
	// first whatever order it was configured in.
	slices.SortFunc(acceptWire, func(a, b byte) int { return int(b) - int(a) })
	cfg.AcceptWire = acceptWire
	if cfg.TCPFallbackLanes == 0 {
		cfg.TCPFallbackLanes = maxTCPFallbackLanes
	}
	if cfg.TCPFallbackLanes < 1 || cfg.TCPFallbackLanes > maxTCPFallbackLanes {
		return nil, fmt.Errorf("TCP fallback lanes must be between 1 and %d", maxTCPFallbackLanes)
	}
	cfg.TCPCongestion, err = normalizeTCPCongestion(cfg.TCPCongestion)
	if err != nil {
		return nil, err
	}
	if cfg.Congestion == "" {
		cfg.Congestion = defaultCongestion()
	}
	if cfg.Congestion != CongestionReno && cfg.Congestion != CongestionBBR && cfg.Congestion != CongestionBBRTUIC && cfg.Congestion != CongestionErasure && cfg.Congestion != CongestionAdaptive && cfg.Congestion != CongestionBrutal {
		return nil, fmt.Errorf("unsupported QUIC congestion controller %q", cfg.Congestion)
	}
	if cfg.Congestion == CongestionBrutal && cfg.BrutalBytesPerSec == 0 {
		return nil, errors.New("brutal congestion requires a positive per-lane byte rate")
	}
	if err := validateBundle(&cfg.CCAPI, cfg.Congestion, cfg.Profile); err != nil {
		return nil, err
	}
	if err := validateSched(&cfg.Sched, cfg.CCAPI); err != nil {
		return nil, err
	}
	if err := validateCongestionScope(&cfg.CongestionScope, cfg.CCAPI, cfg.Sched); err != nil {
		return nil, err
	}
	if err := resolveCodedRetry(&cfg.CodedRetry, cfg.CCAPI); err != nil {
		return nil, err
	}
	if err := resolveClassifier(&cfg.Classifier, cfg.Profile); err != nil {
		return nil, err
	}
	if err := validateRecovery(&cfg.Recovery, cfg.CongestionScope, cfg.CodedRetry, "", 0, true); err != nil {
		return nil, err
	}
	if err := validateProfileBundle(cfg.Profile, cfg.CCAPI, cfg.Sched, cfg.CongestionScope, cfg.CodedRetry, "", cfg.Recovery, cfg.Classifier, true); err != nil {
		return nil, err
	}
	if err := cfg.CarrierShadow.resolve(); err != nil {
		return nil, err
	}
	if cfg.AdaptiveMinBytesSec == 0 {
		cfg.AdaptiveMinBytesSec = defaultAdaptiveMinBytesPerSec
	}
	if cfg.AdaptiveMaxBytesSec == 0 {
		cfg.AdaptiveMaxBytesSec = defaultAdaptiveMaxBytesPerSec
	}
	if cfg.AdaptiveMaxBytesSec < cfg.AdaptiveMinBytesSec {
		return nil, errors.New("adaptive maximum byte rate cannot be below its minimum")
	}
	if cfg.AggregateBytesPerSec == 0 && cfg.InteractiveReserveBytesPerSec != 0 {
		return nil, errors.New("interactive reserve requires an aggregate byte budget")
	}
	// The reserve is withheld from bulk traffic, so a reserve equal to the
	// whole budget leaves bulk not merely slow but unable to send a byte: the
	// budget has no bulk capacity to admit against and refuses every bulk
	// request outright. Require the reserve to leave something behind.
	if cfg.AggregateBytesPerSec != 0 && cfg.InteractiveReserveBytesPerSec >= cfg.AggregateBytesPerSec {
		return nil, errors.New("interactive reserve must leave bulk capacity below the aggregate byte budget")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Metrics == nil {
		cfg.Metrics = metrics.New()
	}
	if !cfg.EnableTCP && !cfg.EnableQUIC {
		cfg.EnableTCP = true
	}
	budget := limiter.New(limiter.Config{TotalBytesPerSec: cfg.AggregateBytesPerSec, ReserveBytesPerSec: cfg.InteractiveReserveBytesPerSec})
	cfg.ChunkSize = chunkSizeForBudget(cfg.ChunkSize, budget)
	handshakes := recoveryReads
	if cfg.GatewayAdmission == GatewayAdmissionOwned {
		handshakes = max(cfg.MaxSessions, recoveryReads)
	}
	server := &Server{
		cfg:          cfg,
		semaphore:    make(chan struct{}, cfg.MaxSessions),
		connections:  make(chan struct{}, cfg.MaxSessions),
		enrollments:  make(chan struct{}, min(cfg.MaxSessions, 64)),
		sessions:     make(map[[16]byte]*serverFlow),
		pending:      make(map[[16]byte]*pendingSession),
		accountUsage: make(map[string]*accountUsage),
		budget:       budget,
		metrics:      cfg.Metrics,
		udpRelays:    newUDPRelayStore(),

		handshakes:     make(chan struct{}, handshakes),
		principalConns: make(map[string]*principalConnections),
		firstFrames:    make(chan struct{}, handshakes),
	}
	server.carrierShadow = newCarrierShadow(cfg.CarrierShadow, carrierShadowGateway, cfg.Logger, cfg.Metrics)
	return server, nil
}

// Metrics exposes aggregate counters for an optional operator endpoint.
func (s *Server) Metrics() *metrics.Registry { return s.metrics }

func (s *Server) Serve(ctx context.Context) error {
	serveCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go s.watchAuthorizationStore(serveCtx)
	errCh := make(chan error, 2)
	count := 0
	if s.cfg.EnableTCP {
		count++
		go func() { errCh <- s.serveTCP(serveCtx) }()
	}
	if s.cfg.EnableQUIC {
		count++
		go func() { errCh <- s.serveQUIC(serveCtx) }()
	}
	var firstErr error
	for range count {
		err := <-errCh
		if err != nil && firstErr == nil {
			firstErr = err
			cancel()
		}
	}
	return firstErr
}

// watchAuthorizationStore adopts authorization changes written by provider CLI
// processes, and reports the state of that adoption.
//
// A failed refresh leaves the previous snapshot in force. That is the right
// behaviour - a malformed or briefly unreadable file must not disarm a running
// gateway - but it is also silent by construction: Authorize keeps admitting
// established devices from the cached snapshot while every enrollment, which
// re-reads from disk, fails. The gateway looks healthy from the outside and
// the users who cannot enroll are told their invitations are bad.
//
// So the transitions are what get reported. The first failure is always
// written, because it is the only record that says when this started;
// continuing failures are restated at a bounded rate carrying how long it has
// been going and how old the rules still in force are; and recovery is always
// written, because a store that silently starts working again leaves an
// operator reading an error with no ending.
func (s *Server) watchAuthorizationStore(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	var watch authorizationWatch
	for {
		select {
		case <-ticker.C:
			changed, err := s.cfg.Credentials.Store.Refresh()
			now := time.Now()
			if err != nil {
				write, consecutive, suppressed, failingFor := watch.failed(now)
				s.metrics.AuthorizationRefreshFailed(consecutive)
				if !write {
					continue
				}
				s.cfg.Logger.LogAttrs(ctx, slog.LevelError,
					"authorization refresh failed; retaining last known-good state",
					slog.String("error", err.Error()),
					slog.Uint64("consecutive", consecutive),
					slog.Uint64("suppressed", suppressed),
					slog.Int64("failing_for_seconds", int64(failingFor/time.Second)),
					slog.Int64("enforcing_snapshot_age_seconds", s.authorizationSnapshotAge(now)))
				continue
			}
			if recovered, attempts, unreadableFor := watch.succeeded(now); recovered {
				s.cfg.Logger.LogAttrs(ctx, slog.LevelWarn,
					"authorization refresh recovered",
					slog.Uint64("failed_attempts", attempts),
					slog.Int64("unreadable_for_seconds", int64(unreadableFor/time.Second)))
			}
			s.metrics.AuthorizationRefreshed(s.cfg.Credentials.Store.LastGoodAt(), changed)
			if changed {
				s.cfg.Logger.Info("authorization state reloaded")
			}
		case <-ctx.Done():
			return
		}
	}
}

// authorizationSnapshotAge reports how old the authorization state currently
// being enforced is, in seconds, or -1 if it has never been read. Enrollment
// refreshes the same snapshot under its own lock, so the store is asked rather
// than tracked here.
func (s *Server) authorizationSnapshotAge(now time.Time) int64 {
	lastGood := s.cfg.Credentials.Store.LastGoodAt()
	if lastGood.IsZero() {
		return -1
	}
	return int64(now.Sub(lastGood) / time.Second)
}

func (s *Server) serveTCP(ctx context.Context) error {
	lc := net.ListenConfig{KeepAlive: 30 * time.Second}
	listener, err := lc.Listen(ctx, "tcp", s.cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("listen on remote TLS/TCP address: %w", err)
	}
	if err := setTCPListenerCongestion(listener, s.cfg.TCPCongestion); err != nil {
		_ = listener.Close()
		return fmt.Errorf("configure remote TLS/TCP congestion control: %w", err)
	}
	return s.ServeListener(ctx, listener)
}

// ServeListener runs the authenticated server on an already-bound listener.
// This also supports socket activation and deterministic integration tests.
func (s *Server) ServeListener(ctx context.Context, listener net.Listener) error {
	defer listener.Close()
	go func() {
		<-ctx.Done()
		_ = listener.Close()
	}()

	tlsConfig, err := identity.ServerTLSConfigProtocols(s.cfg.Credentials, wireALPNs(s.acceptWire()), s.cfg.Enrollment != nil)
	if err != nil {
		return fmt.Errorf("configure server TLS identity: %w", err)
	}
	var wg sync.WaitGroup
	s.cfg.Logger.Info("remote TLS/TCP listener ready", "address", listener.Addr().String())
	for {
		raw, acceptErr := listener.Accept()
		if acceptErr != nil {
			if ctx.Err() != nil || errors.Is(acceptErr, net.ErrClosed) {
				wg.Wait()
				return nil
			}
			return fmt.Errorf("accept remote lane: %w", acceptErr)
		}
		if err := setTCPConnCongestion(raw, s.cfg.TCPCongestion); err != nil {
			_ = raw.Close()
			return fmt.Errorf("configure accepted TLS/TCP congestion control: %w", err)
		}
		// A TLS/TCP connection carries exactly one stream, so its admission
		// is its stream's. In owned mode the bound taken here covers only the
		// handshake and the first frame; the session limit is charged at OPEN.
		if s.admissionOwned() {
			if !tryAcquire(s.handshakes) {
				_ = raw.Close()
				s.cfg.Logger.Warn("remote TLS/TCP handshake limit reached")
				continue
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				s.handleTCP(ctx, tls.Server(raw, tlsConfig), streamAdmission{release: func() { <-s.handshakes }})
			}()
			continue
		}
		select {
		case s.semaphore <- struct{}{}:
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() { <-s.semaphore }()
				s.handleTCP(ctx, tls.Server(raw, tlsConfig), streamAdmission{charged: true})
			}()
		default:
			// Every slot is held. The JOIN that would recover one of those
			// flows, or a PROBE, needs none, so the connection is read under a
			// small separate bound before anything else is refused (#170).
			if !s.cfg.testReleasedAdmission && tryAcquire(s.handshakes) {
				wg.Add(1)
				go func() {
					defer wg.Done()
					s.handleTCP(ctx, tls.Server(raw, tlsConfig), streamAdmission{recoveryOnly: true, release: func() { <-s.handshakes }})
				}()
				continue
			}
			_ = raw.Close()
			s.cfg.Logger.Warn("remote session limit reached")
		}
	}
}

func (s *Server) handleTCP(ctx context.Context, conn *tls.Conn, admission streamAdmission) {
	defer conn.Close()
	if admission.release != nil {
		release := sync.OnceFunc(admission.release)
		defer release()
		admission.release = release
	}
	_ = conn.SetDeadline(time.Now().Add(handshakeBound(conn, s.cfg.HandshakeTimeout)))
	if err := conn.HandshakeContext(ctx); err != nil {
		s.cfg.Logger.Debug("remote TLS handshake failed", "error", err)
		return
	}
	state := conn.ConnectionState()
	if admission.recoveryOnly && (state.NegotiatedProtocol == identity.EnrollmentALPN || state.NegotiatedProtocol == identity.RenewalALPN) {
		// Read past a full session limit only to find a JOIN or a PROBE.
		return
	}
	if state.NegotiatedProtocol == identity.EnrollmentALPN {
		if s.cfg.Enrollment != nil {
			if !s.admitEnrollment() {
				s.recordEnrollmentAdmission("enrollment")
				return
			}
			defer s.releaseEnrollment()
			result, err := s.cfg.Enrollment.Serve(conn)
			s.recordEnrollment("enrollment", result, err)
		}
		return
	}
	if state.NegotiatedProtocol == identity.RenewalALPN {
		if s.cfg.Enrollment != nil {
			if !s.admitEnrollment() {
				s.recordEnrollmentAdmission("renewal")
				return
			}
			defer s.releaseEnrollment()
			if principal, err := identity.PrincipalFromTLS(state); err == nil {
				result, err := s.cfg.Enrollment.Renew(conn, principal)
				s.recordEnrollment("renewal", result, err)
			}
		}
		return
	}
	wire, ok := checkNegotiatedWire(state.NegotiatedProtocol, s.acceptWire())
	if !ok {
		return
	}
	principal, err := identity.PrincipalFromTLS(state)
	if err != nil {
		return
	}
	s.metrics.DataConnectionAccepted(wire)
	s.handleSession(ctx, conn, principal, nil, admission)
}

func (s *Server) serveQUIC(ctx context.Context) error {
	if s.cfg.HopPortCount < 2 {
		// Fast path: single port, no mux overhead.
		packetConn, err := net.ListenPacket("udp", s.cfg.ListenAddr)
		if err != nil {
			return fmt.Errorf("listen on remote QUIC address: %w", err)
		}
		return s.ServePacketConn(ctx, packetConn)
	}

	// Port-hopping path: bind the primary socket, derive the port list, then
	// open secondary sockets and hand everything to a ServerPortMux.
	listenAddr, err := net.ResolveUDPAddr("udp", s.cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("resolve QUIC listen address: %w", err)
	}
	primaryConn, err := net.ListenUDP("udp", listenAddr)
	if err != nil {
		return fmt.Errorf("listen on remote QUIC address: %w", err)
	}
	primaryPort := primaryConn.LocalAddr().(*net.UDPAddr).Port
	ports := portmux.HopPorts(s.cfg.Credentials.ProviderID, primaryPort, s.cfg.HopPortCount)
	mux, err := portmux.NewServerPortMux(primaryConn, ports)
	if err != nil {
		_ = primaryConn.Close()
		return fmt.Errorf("create server port mux: %w", err)
	}
	// A pool narrower than configured is reported rather than inferred: the
	// deployment still works, but it hops across fewer ports than the operator
	// asked for, and that is a fact about this host worth having in the log.
	s.cfg.Logger.Info("port hop listener ready",
		"primary_addr", primaryConn.LocalAddr(),
		"hop_port_count", len(ports)-len(mux.SkippedPorts()),
		"hop_ports_configured", len(ports),
		"hop_ports_unavailable", mux.SkippedPorts())
	return s.ServePacketConn(ctx, mux)
}

// ServePacketConn runs the QUIC listener on an already-bound UDP socket.
func (s *Server) ServePacketConn(ctx context.Context, packetConn net.PacketConn) error {
	tlsConfig, err := identity.ServerTLSConfigProtocols(s.cfg.Credentials, wireALPNs(s.acceptWire()), s.cfg.Enrollment != nil)
	if err != nil {
		_ = packetConn.Close()
		return fmt.Errorf("configure server TLS identity: %w", err)
	}
	listener, err := quic.Listen(packetConn, tlsConfig, quicServerConfig(
		flowWindows{stream: s.cfg.StreamReceiveWindow, connection: s.cfg.ConnectionReceiveWindow}))
	if err != nil {
		_ = packetConn.Close()
		return fmt.Errorf("create QUIC listener: %w", err)
	}
	defer listener.Close()
	defer packetConn.Close()
	go func() {
		<-ctx.Done()
		_ = listener.Close()
	}()
	s.cfg.Logger.Info("remote QUIC listener ready", "address", listener.Addr().String())
	var wg sync.WaitGroup
	for {
		conn, acceptErr := listener.Accept(ctx)
		if acceptErr != nil {
			if ctx.Err() != nil {
				wg.Wait()
				return nil
			}
			return fmt.Errorf("accept QUIC lane: %w", acceptErr)
		}
		// The listener returns connections whose mutual TLS handshake is
		// complete, so the bound is charged to an authorised principal.
		ticket, admitted := s.admitQUICConnection(conn.ConnectionState().TLS)
		if !admitted {
			_ = conn.CloseWithError(0x100, "server connection limit reached")
			s.cfg.Logger.Warn("remote QUIC connection limit reached")
			continue
		}
		if s.isStandby(ticket) {
			s.cfg.Logger.Debug("remote QUIC standby connection admitted past the connection limit")
		}
		// Session admission is performed per QUIC stream in handleQUIC. Holding
		// one slot from that session semaphore for the lifetime of a multiplexed
		// connection would incorrectly reduce MaxSessions and prevent the
		// connection from carrying the configured number of independent flows.
		// The separate connection semaphore above bounds idle/untrusted peers.
		wg.Add(1)
		go func() {
			defer wg.Done()
			// The ticket follows the connection, not the flows it carried:
			// handleQUIC waits for every stream handler, and the handler of
			// an OPEN runs for its flow's life, which continues on another
			// connection once this one dies.
			release := sync.OnceFunc(func() { s.releaseQUICConnection(ticket) })
			defer release()
			go func() {
				<-conn.Context().Done()
				release()
			}()
			s.handleQUIC(ctx, conn)
		}()
	}
}

func (s *Server) admitConnection() bool {
	select {
	case s.connections <- struct{}{}:
		return true
	default:
		return false
	}
}

func (s *Server) releaseConnection() { <-s.connections }

func (s *Server) admitEnrollment() bool {
	select {
	case s.enrollments <- struct{}{}:
		return true
	default:
		return false
	}
}

func (s *Server) releaseEnrollment() { <-s.enrollments }

func (s *Server) handleQUIC(ctx context.Context, conn *quic.Conn) {
	var wg sync.WaitGroup
	// Close the shared connection before waiting for stream handlers. This
	// ordering is important during shutdown: a handler blocked in Read must be
	// released before Wait can complete.
	defer wg.Wait()
	defer conn.CloseWithError(0, "queqiao session complete")
	controller := configureQUICController(conn, congestionConfig{
		kind: s.cfg.Congestion, brutalBytesPerSecond: s.cfg.BrutalBytesPerSec,
		adaptiveMinBytesPerSec: s.cfg.AdaptiveMinBytesSec, adaptiveMaxBytesPerSec: s.cfg.AdaptiveMaxBytesSec,
		ccAPI: s.cfg.CCAPI, sched: s.cfg.Sched, scope: s.cfg.CongestionScope,
		// The ALPN and the principal are checked below: only an authorised
		// data connection attaches to its path's controller.
		provisional: true,
	})
	state := conn.ConnectionState().TLS
	if state.NegotiatedProtocol == identity.EnrollmentALPN {
		if s.cfg.Enrollment == nil {
			return
		}
		if !s.admitEnrollment() {
			s.recordEnrollmentAdmission("enrollment")
			return
		}
		defer s.releaseEnrollment()
		stream, err := acceptQUICStream(ctx, conn, controller)
		if err == nil {
			_ = stream.SetDeadline(time.Now().Add(s.cfg.HandshakeTimeout))
			result, serveErr := s.cfg.Enrollment.Serve(stream)
			s.recordEnrollment("enrollment", result, serveErr)
			_ = stream.Close()
		}
		return
	}
	if state.NegotiatedProtocol == identity.RenewalALPN {
		if s.cfg.Enrollment == nil {
			return
		}
		if !s.admitEnrollment() {
			s.recordEnrollmentAdmission("renewal")
			return
		}
		defer s.releaseEnrollment()
		principal, err := identity.PrincipalFromTLS(state)
		if err != nil {
			return
		}
		stream, err := acceptQUICStream(ctx, conn, controller)
		if err == nil {
			_ = stream.SetDeadline(time.Now().Add(s.cfg.HandshakeTimeout))
			result, renewErr := s.cfg.Enrollment.Renew(stream, principal)
			s.recordEnrollment("renewal", result, renewErr)
			_ = stream.Close()
		}
		return
	}
	wire, ok := checkNegotiatedWire(state.NegotiatedProtocol, s.acceptWire())
	if !ok {
		return
	}
	principal, err := identity.PrincipalFromTLS(state)
	if err != nil {
		return
	}
	if s.cfg.CCAPI == CCAPIV2 && s.cfg.Congestion == CongestionErasure && s.cfg.CongestionScope == CongestionScopePath {
		// An authorised data connection leaves its provisional controller
		// for the authority of its path: the principal's, over this uplink,
		// to this remote host (§4.1). Every connection of one device -- an
		// old client's isolated and rescue lanes too -- attaches to the same
		// one.
		if err := authorizePathPrincipal(s.cfg.Credentials.Store, principal, time.Now()); err != nil {
			// Not authorised now: the connection carries no data, rather
			// than carrying it later outside the device's path budget.
			return
		}
		controller = attachPathController(conn, congestionConfig{
			kind: s.cfg.Congestion, ccAPI: s.cfg.CCAPI, sched: s.cfg.Sched, scope: s.cfg.CongestionScope,
			// No hierarchicalPath: the tree is a client's (§11.3). The
			// gateway's authorities take no tree view, as the legacy gateway
			// had no tree; no gate judges one (#235).
		}, func() string { return gatewayPathPrincipal(principal) })
	}
	s.metrics.DataConnectionAccepted(wire)
	auth := &quicAuthState{principal: principal}
	// openStreams counts this connection's stream handlers: under
	// recovery=carrier the keep-alive floor applies while it has any.
	var openStreams atomic.Int64
	if s.cfg.Recovery == RecoveryCarrier {
		go runKeepAliveSchedule(conn, func() bool { return openStreams.Load() > 0 }, time.Second)
	}
	// connFirstFrames and probes are this connection's own bounds (§10.5): the
	// streams whose first frame is being read or whose JOIN is being
	// admitted, and the PROBEs that hold no session slot.
	connFirstFrames := make(chan struct{}, firstFrameStreams)
	releaseFirstFrame := func() { <-s.firstFrames; <-connFirstFrames }
	// tryFirstFrame takes a slot of both first-frame bounds, or neither.
	tryFirstFrame := func() bool {
		if !tryAcquire(connFirstFrames) {
			return false
		}
		if !tryAcquire(s.firstFrames) {
			<-connFirstFrames
			return false
		}
		return true
	}
	probes := make(chan struct{}, probeStreams)
	owned := s.admissionOwned()
	// Mutual TLS authenticates the whole QUIC connection before any stream is
	// accepted. Every stream therefore begins directly with OPEN or JOIN.
	dispatch := func(lane streamConn) bool {
		if owned {
			// The accept loop has already taken this stream's first-frame
			// slot; the session limit is charged at OPEN.
			wg.Add(1)
			go func(lane streamConn) {
				defer wg.Done()
				openStreams.Add(1)
				defer openStreams.Add(-1)
				s.handleSession(ctx, lane, principal, auth, streamAdmission{release: releaseFirstFrame, probes: probes})
			}(lane)
			return true
		}
		select {
		case s.semaphore <- struct{}{}:
			wg.Add(1)
			go func(lane streamConn) {
				defer wg.Done()
				openStreams.Add(1)
				defer openStreams.Add(-1)
				defer func() { <-s.semaphore }()
				s.handleSession(ctx, lane, principal, auth, streamAdmission{charged: true})
			}(lane)
			return true
		default:
			// Every slot is held, typically by the flows this stream may be
			// trying to recover. A JOIN or a PROBE needs no slot, so the first
			// frame is read under the connection's own bound before anything
			// is refused (#170).
			if !s.cfg.testReleasedAdmission && tryFirstFrame() {
				wg.Add(1)
				go func(lane streamConn) {
					defer wg.Done()
					openStreams.Add(1)
					defer openStreams.Add(-1)
					s.handleSession(ctx, lane, principal, auth, streamAdmission{recoveryOnly: true, release: releaseFirstFrame, probes: probes})
				}(lane)
				return true
			}
			_ = lane.Close()
			s.cfg.Logger.Warn("remote session limit reached")
			return false
		}
	}
	for {
		if owned {
			// Wait for room rather than refuse: streams beyond the bound stay
			// in QUIC's accept queue, and only this connection waits.
			if !waitForSlot(ctx, conn, connFirstFrames) {
				return
			}
		}
		// Waiting for another stream is not a handshake operation. Applying the
		// per-stream authentication timeout here used to close the entire QUIC
		// connection after ten seconds without a *new* stream, even while an
		// existing long download was actively transferring. Each accepted stream
		// still gets the bounded authentication deadline in handleSession; the
		// outer connection is bounded by QUIC's idle timeout and server shutdown.
		stream, err := acceptQUICStream(ctx, conn, controller)
		if err != nil {
			if owned {
				<-connFirstFrames
			}
			if ctx.Err() == nil {
				s.cfg.Logger.Debug("accept QUIC stream failed", "error", err)
			}
			return
		}
		// The gateway-wide bound is taken only for a stream that exists, so
		// an idle connection waiting for its next stream holds none of it.
		if owned && !waitForSlot(ctx, conn, s.firstFrames) {
			_ = stream.Close()
			<-connFirstFrames
			return
		}
		dispatch(stream)
	}
}

func (s *Server) handleSession(ctx context.Context, conn streamConn, principal identity.Principal, auth *quicAuthState, admission streamAdmission) {
	defer conn.Close()
	// The first-frame bound is released once this stream is admitted or
	// refused, and at the latest when it ends.
	releaseFirstFrame := func() {}
	if admission.release != nil {
		releaseFirstFrame = sync.OnceFunc(admission.release)
		defer releaseFirstFrame()
	}
	if auth != nil {
		auth.flows.Add(1)
		defer auth.flows.Add(-1)
	}
	sessionStarted := time.Now()
	_ = conn.SetDeadline(time.Now().Add(handshakeBound(conn, s.cfg.HandshakeTimeout)))
	fc := newFrameConn(conn)
	fc.setPacketsOnStream(s.cfg.UDPOnStream)
	open, err := fc.Read()
	if err != nil {
		s.cfg.Logger.Debug("read authenticated stream open", "error", err)
		return
	}
	// A QUIC connection may outlive a revocation. Re-authorize every new
	// stream so a disabled device cannot open flows, probes, or replacement
	// lanes merely because its original TLS handshake predates the change.
	if _, err := s.cfg.Credentials.Store.Authorize(principal, time.Now()); err != nil {
		return
	}
	if open.Header.Type == protocol.TypeProbe {
		if !admission.charged && admission.probes != nil {
			// A PROBE holds no session slot. It is charged to its
			// connection's probe allowance instead; its work is fixed by
			// PROTOCOL.md §14 and its time by the handshake deadline.
			if !tryAcquire(admission.probes) {
				s.cfg.Logger.Debug("remote probe allowance reached")
				return
			}
			defer func() { <-admission.probes }()
			releaseFirstFrame()
		}
		s.handlePathProbeFor(fc, open, auth)
		return
	}
	if open.Header.Type == protocol.TypeJoin {
		validJoinFlags := protocol.FlagReserveControl
		if protocol.HasReplaceLane(fc.wire) {
			// A version-1 connection cannot carry the flag at all; its decoder
			// has already refused the frame. Restating it here keeps the rule
			// next to the payload lengths it decides.
			validJoinFlags |= protocol.FlagReplaceLane
		}
		if session.IsZeroSessionID(open.Header.SessionID) || open.Header.FlowID == 0 || open.Header.Sequence != 0 || open.Header.Flags&^validJoinFlags != 0 || len(open.Payload) != 8 && len(open.Payload) != 16 || len(open.Payload) == 16 && open.Header.Flags&protocol.FlagReplaceLane == 0 || len(open.Payload) == 8 && open.Header.Flags&protocol.FlagReplaceLane != 0 {
			_ = fc.Write(protocol.Frame{Header: protocol.Header{Version: protocol.Version, Type: protocol.TypeReset, SessionID: open.Header.SessionID, FlowID: open.Header.FlowID, Class: protocol.ClassBulk}, Payload: session.ResetPayload(session.ResetProtocol, "invalid lane join")})
			return
		}
		laneID := binary.BigEndian.Uint64(open.Payload)
		replaceLane := open.Header.Flags&protocol.FlagReplaceLane != 0
		replacedLaneID := uint64(0)
		if replaceLane {
			replacedLaneID = binary.BigEndian.Uint64(open.Payload[8:])
		}
		if laneID == 0 {
			_ = fc.Write(protocol.Frame{Header: protocol.Header{Version: protocol.Version, Type: protocol.TypeReset, SessionID: open.Header.SessionID, FlowID: open.Header.FlowID, Class: protocol.ClassBulk}, Payload: session.ResetPayload(session.ResetProtocol, "invalid lane join")})
			return
		}
		// A JOIN is charged to the flow it joins, within that flow's lane
		// ceiling, and never to the session limit. It stays under the
		// first-frame bound until it has been admitted or refused.
		s.admitLaneJoin(ctx, conn, fc, principal, open.Header.SessionID, laneID, replacedLaneID, replaceLane, open, releaseFirstFrame)
		return
	}
	if admission.recoveryOnly {
		// Read past a full session limit only to find a JOIN or a PROBE. An
		// OPEN is refused exactly as it was before it was read.
		s.cfg.Logger.Warn("remote session limit reached")
		return
	}
	sessionID := open.Header.SessionID
	laneID := uint64(0)
	if open.Header.Type != protocol.TypeOpen || session.IsZeroSessionID(sessionID) || open.Header.FlowID == 0 || open.Header.Sequence != 0 {
		_ = fc.Write(protocol.Frame{Header: protocol.Header{Version: protocol.Version, Type: protocol.TypeReset, SessionID: sessionID, FlowID: open.Header.FlowID, Class: protocol.ClassNew}, Payload: session.ResetPayload(session.ResetProtocol, "invalid flow open")})
		return
	}
	// lease is what this logical flow holds against the gateway's limits.
	lease := &flowLease{}
	defer func() { lease.release() }()
	udpResume, udpResumable := session.DecodeUDPResumeOpen(open.Payload)
	if s.admissionOwned() && udpResumable && len(udpResume) > 0 {
		// A replacement naming a relay that a live association of this
		// principal still holds takes the relay over, with the session and
		// account slots that association was charged (§11.2).
		if relay, moved := s.udpRelays.transfer(ctx, udpResume, principal, s.udpTransferWait()); relay != nil {
			releaseFirstFrame()
			lease = moved
			s.handleUDPAssociation(ctx, conn, fc, principal, sessionID, open.Header.FlowID, udpResume, true, lease, relay)
			return
		}
	}
	if !admission.charged {
		// Owned admission: the session limit counts logical flows, charged
		// here at OPEN. The refusal is FLOW_LIMIT, a code every wire version
		// knows and the answer a peer already gets for an account's limit.
		acquired := tryAcquire(s.semaphore)
		if !acquired {
			// A parked relay's slots are due back at its expiry, which the
			// sweeper may not have reached yet; take them back before
			// refusing anyone.
			s.udpRelays.expire()
			acquired = tryAcquire(s.semaphore)
		}
		if !acquired {
			s.cfg.Logger.Warn("remote session limit reached")
			_ = fc.Write(protocol.Frame{Header: protocol.Header{Version: protocol.Version, Type: protocol.TypeReset, SessionID: sessionID, FlowID: open.Header.FlowID, Class: protocol.ClassNew}, Payload: session.ResetPayload(session.ResetFlowLimit, "remote session limit reached")})
			return
		}
		lease.add(func() { <-s.semaphore })
	}
	releaseFirstFrame()
	if refusal := s.admitAccountFlowReclaiming(principal); refusal != nil {
		s.refuseAccountFlow(fc, sessionID, open.Header.FlowID, principal, refusal)
		return
	}
	lease.add(func() { s.releaseAccountFlow(principal) })
	if session.IsUDPAssociation(open.Payload) {
		s.handleUDPAssociation(ctx, conn, fc, principal, sessionID, open.Header.FlowID, nil, false, lease, nil)
		return
	}
	if udpResumable {
		s.handleUDPAssociation(ctx, conn, fc, principal, sessionID, open.Header.FlowID, udpResume, true, lease, nil)
		return
	}
	destination, err := session.DecodeDestination(open.Payload)
	if err != nil {
		_ = fc.Write(protocol.Frame{Header: protocol.Header{Version: protocol.Version, Type: protocol.TypeReset, SessionID: sessionID, FlowID: open.Header.FlowID, Class: protocol.ClassNew}, Payload: session.ResetPayload(session.ResetDestination, "destination unavailable")})
		return
	}
	// The session is announced before the dial, not after it. A client sends
	// DATA, and may need a replacement lane, as soon as its OPEN is written;
	// a JOIN that overtook a slow dial used to find no session and get the
	// permanent "unknown session" answer for a flow that was about to exist.
	pending, ok := s.beginPendingSession(sessionID, open.Header.FlowID, principal, fc.wire)
	if !ok {
		_ = fc.Write(protocol.Frame{Header: protocol.Header{Version: protocol.Version, Type: protocol.TypeReset, SessionID: sessionID, FlowID: open.Header.FlowID, Class: protocol.ClassNew}, Payload: session.ResetPayload(session.ResetFlowLimit, "session already exists")})
		return
	}
	settlePending := sync.OnceFunc(func() { s.endPendingSession(sessionID, pending) })
	defer settlePending()
	destinationDialStarted := time.Now()
	destinationConn, err := s.cfg.DestinationPolicy.DialContext(ctx, destination)
	if err != nil {
		// Waiting JOINs learn of the failure first: the RESET below goes to
		// the OPEN's own stream, which may be the one that is blocked.
		settlePending()
		_ = fc.Write(protocol.Frame{Header: protocol.Header{Version: protocol.Version, Type: protocol.TypeReset, SessionID: sessionID, FlowID: open.Header.FlowID, Class: protocol.ClassNew}, Payload: session.ResetPayload(session.ResetDestination, "destination unavailable")})
		s.cfg.Logger.Debug("destination dial failed", "error", err)
		return
	}
	s.cfg.Logger.Debug("remote flow opened", "transport", transportKindForConn(conn), "wire", fc.wire, "account", principal.AccountID, "device", principal.DeviceID, "open_duration", destinationDialStarted.Sub(sessionStarted), "destination_dial_duration", time.Since(destinationDialStarted), "total_duration", time.Since(sessionStarted))
	defer destinationConn.Close()
	flow := newMultipathFlow(ctx, destinationConn, sessionID, open.Header.FlowID, s.cfg.ChunkSize, protocol.FlagAckDown, protocol.FlagAckUp, s.budget, s.metrics, s.cfg.Logger)
	flow.classifier = classifier.New(s.classifierConfig())
	flow.memOwned = s.cfg.Memory == MemoryOwned
	flow.codedRetrySticky = s.cfg.CodedRetry == CodedRetrySticky
	flow.carrierShadow = s.carrierShadow
	flow.carrierRecovery = s.cfg.Recovery == RecoveryCarrier
	// Wire version 1 requires range acknowledgements on both endpoints.
	flow.ackRanges.Store(true)
	flow.idleTimeout = s.cfg.FlowIdleTimeout
	flow.maxLifetime = s.cfg.FlowMaxLifetime
	flow.reserveControlLane = open.Header.Flags&protocol.FlagReserveControl != 0
	flow.controlLaneShared = func(*mpLane) bool { return auth.shared() }
	// A flow that fails here is gone for good: tell the client on every lane
	// so it does not spend a JOIN discovering that (issue #159).
	flow.resetOnFailure = true
	initialKind := transportKindForConn(conn)
	serverSession := newServerFlow(flow, principal, initialKind, s.cfg.TCPFallbackLanes)
	serverSession.wire = fc.wire
	serverSession.retireControlOnJoin = s.admissionOwned()
	flow.wire = fc.wire
	openLane := &mpLane{
		id: laneID, kind: initialKind, fc: fc, writeHook: s.cfg.testLaneWriteHook,
		control: flow.reserveControlLane && initialKind == TransportQUIC,
	}
	if err := serverSession.addLane(openLane); err != nil {
		flow.closeAll()
		return
	}
	s.observeLanes(serverSession.flow.laneCount())
	registeredOK := s.registerSession(sessionID, serverSession)
	if !registeredOK {
		settlePending()
		_ = fc.Write(protocol.Frame{Header: protocol.Header{Version: protocol.Version, Type: protocol.TypeReset, SessionID: sessionID, FlowID: open.Header.FlowID, Class: protocol.ClassNew}, Payload: session.ResetPayload(session.ResetFlowLimit, "session already exists")})
		flow.closeAll()
		return
	}
	registered := true
	go s.watchFlowCompletion(ctx, sessionID, serverSession)
	go s.watchAuthorization(ctx, serverSession)
	defer func() {
		if registered {
			s.cfg.Logger.Debug("session released with its flow", "lanes", serverSession.flow.laneCount())
			s.unregisterSession(sessionID, serverSession)
		}
	}()
	if err := fc.Write(protocol.Frame{Header: protocol.Header{Version: protocol.Version, Type: protocol.TypeOpenOK, SessionID: sessionID, FlowID: open.Header.FlowID, Class: protocol.ClassNew}}); err != nil {
		// The stream that carried OPEN died while the destination was being
		// dialled. If a JOIN has asked to rescue it -- waiting now, or told
		// "session pending" and due to retry -- that is a lost lane, not a
		// failed flow: the flow runs for its replacement grace and the JOIN,
		// whose own OPEN_OK tells the client the session exists, replaces the
		// lane. If no JOIN ever named it, it is more likely a transport
		// candidate the client raced and abandoned, so it is closed.
		if !s.pendingRescueRequested(pending) {
			flow.closeAll()
			return
		}
		flow.failLane(openLane, fmt.Errorf("lane %d: write flow open acknowledgement: %w", laneID, err))
	}
	// JOINs waiting on this OPEN are released only now: earlier, one could
	// retire the OPEN's lane before its OPEN_OK went out.
	settlePending()
	_ = conn.SetDeadline(time.Time{})
	s.metrics.FlowStarted()
	s.metrics.WireFlowStarted(fc.wire)
	stats, err := flow.run(ctx)
	s.metrics.WireFlowFinished(fc.wire)
	// A peer may close the transport immediately after receiving the final
	// bytes, racing the server's final-ACK bookkeeping. If both directions
	// have observed FIN sequences, the logical flow is complete even when the
	// runner reports the late socket EOF as an error; retain the same bounded
	// tombstone so a replacement lane can replay the final ACK. Do not retain
	// one-sided or context-canceled flows.
	flowComplete := err == nil || (ctx.Err() == nil && serverSession.flow.finSent.Load() && serverSession.flow.remoteFinSeen.Load())
	if !flowComplete && ctx.Err() == nil && serverSession.flow.remoteFinSeen.Load() && expectedDestinationCloseError(err) {
		flowComplete = true
	}
	s.metrics.FlowFinished(stats.BytesRead, stats.BytesSent, !flowComplete && err != nil && !errors.Is(err, context.Canceled))
	if flowComplete {
		// Keep a bounded tombstone long enough for a client that lost the
		// final cumulative ACK to authenticate a replacement lane and finish
		// its local close handshake. No destination connection or payload is
		// retained; only the flow's final sequence metadata remains.
		s.retainCompletedSession(sessionID, serverSession)
		registered = false
	}
	codedFrames, streamFrames := flow.dataSubstrates()
	substrate, hasCoded := flow.codedSubstrate()
	logFields := []any{"session_id", fmt.Sprintf("%x", sessionID), "flow_id", flow.flowID,
		"transport", transportKindForConn(conn), "wire", fc.wire, "bytes_from_client", stats.BytesRead, "bytes_to_client", stats.BytesSent,
		"duration", stats.Ended.Sub(stats.Started), "lane_bytes", stats.LaneBytes,
		// Where the payload went. The server is the sender for a download, so
		// this is the split that decides what a download costs.
		"data_coded", codedFrames, "data_stream", streamFrames,
		"coded_substrate", codedSubstrateFields(substrate, hasCoded),
		"class", classifier.Class(flow.class.Load())}
	logFields = append(logFields, flow.classLogFields()...)
	logFields = append(logFields, codedSubstrateLogFields(substrate, hasCoded)...)
	logFields = append(logFields, flow.replacementLogFields()...)
	logFields = append(logFields, pathTraceFields(conn)...)
	if !flowComplete && err != nil && !errors.Is(err, context.Canceled) {
		s.cfg.Logger.Warn("remote flow ended with error", append(logFields, "error", err)...)
		return
	}
	s.cfg.Logger.Info("remote flow complete", logFields...)
}

const (
	maxPathProbeFrames = protocol.MaxProbeFrames
	maxPathProbeBytes  = protocol.MaxProbeBytes
)

// handlePathProbe accepts only a small, destination-free sequence and reflects
// each validated frame exactly once. The equal-size authenticated echo cannot
// amplify traffic or name a destination, and it gives this endpoint's own
// congestion controller outbound packets to measure. That is the only sound
// source for its sending direction; reverse-direction arrivals cannot reveal
// which losses the peer caused by its offered rate.
func (s *Server) handlePathProbe(fc *frameConn, first protocol.Frame) {
	s.handlePathProbeFor(fc, first, nil)
}

// handlePathProbeFor is handlePathProbe on a connection. Under
// recovery=carrier the reply to the first PROBE a connection carries before
// any flow (its own stream is the one counted) is under a verification
// grant: that is a client verifying a new
// standby (§10.2), and the grant lets the reply go out while a silent
// sibling holds the path's window. Routine probes on a carrier with flows
// stay under ordinary admission, and a connection gets at most one grant.
func (s *Server) handlePathProbeFor(fc *frameConn, first protocol.Frame, auth *quicAuthState) {
	frames, bytes := 0, 0
	frame := first
	sessionID := first.Header.SessionID
	for {
		if frame.Header.Type != protocol.TypeProbe || session.IsZeroSessionID(frame.Header.SessionID) ||
			frame.Header.SessionID != sessionID ||
			frame.Header.FlowID != 0 || frame.Header.Sequence != uint64(frames) || frame.Header.Flags != 0 ||
			frame.Header.Class != protocol.ClassNew || len(frame.Payload) == 0 || len(frame.Payload) > protocol.MaxProbePayload ||
			bytes+len(frame.Payload) > maxPathProbeBytes {
			return
		}
		if frames == 0 && s.cfg.Recovery == RecoveryCarrier && auth != nil && auth.flows.Load() <= 1 && auth.probeGranted.CompareAndSwap(false, true) {
			// The reply verifies a client's standby in this direction while
			// a silent sibling holds the path's window (§10.2).
			if qs, ok := fc.transport().(*quicStreamConn); ok {
				bindProbeGrant(qs.conn, qs.stream, 0, s.cfg.HandshakeTimeout)
			}
		}
		frames++
		bytes += len(frame.Payload)
		if err := fc.Write(frame); err != nil {
			return
		}
		if frames >= maxPathProbeFrames || bytes >= maxPathProbeBytes {
			return
		}
		next, err := fc.Read()
		if err != nil {
			return
		}
		frame = next
	}
}

// watchFlowCompletion closes a correctness gap between the application FIN
// exchange and the flow runner's final goroutine return. Both direction FIN
// sequences prove that no additional payload can be delivered, so retaining
// a tombstone at that point lets a replacement lane replay the final ACK even
// if the peer has already closed its socket and the runner is waiting for a
// late duplicate ACK.
func (s *Server) watchFlowCompletion(ctx context.Context, sessionID [16]byte, serverSession *serverFlow) {
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if ctx.Err() == nil && serverSession.flow.finSent.Load() && serverSession.flow.remoteFinSeen.Load() {
				s.retainCompletedSession(sessionID, serverSession)
				// Both FINs are an application-level proof that no additional
				// payload can be delivered in either direction. Tear down the
				// physical lanes now; otherwise sendInner can wait forever for a
				// final ACK lost in the last-lane close race and receiveInner can
				// wait forever for another frame. The tombstone above preserves
				// final-ACK metadata for bounded replay.
				serverSession.flow.closeAll()
				return
			}
		case <-serverSession.flow.doneChan():
			return
		case <-ctx.Done():
			return
		}
	}
}

func (s *Server) retainCompletedSession(sessionID [16]byte, serverSession *serverFlow) {
	serverSession.tombstone.Do(func() {
		serverSession.completed.Store(true)
		counted := false
		if s.admissionOwned() {
			// A completed flow gives its slot back at once; its replay
			// record is bounded separately. Past that bound it is not kept.
			s.sessionsMu.Lock()
			if s.tombstones < s.cfg.MaxSessions {
				s.tombstones++
				counted = true
			}
			s.sessionsMu.Unlock()
			if !counted {
				// Over the record bound: not kept at all, and removed now,
				// before this flow's slot is released, so the next OPEN is
				// not refused by a record counted as live.
				s.unregisterSession(sessionID, serverSession)
				return
			}
		}
		time.AfterFunc(completedSessionLinger, func() {
			s.cfg.Logger.Debug("session tombstone expired")
			s.unregisterSession(sessionID, serverSession)
			if counted {
				s.sessionsMu.Lock()
				s.tombstones--
				s.sessionsMu.Unlock()
			}
		})
	})
}

// refuseLaneJoin answers a lane join with a reset, and says so where an
// operator will see it.
//
// This used to be a Debug record, which meant a gateway refusing hundreds of
// session resumes an hour looked completely healthy at the default level: the
// refusals were only diagnosable from the peer, which is the one place the
// answer is least useful. A refused join is a flow that is about to fail, so
// it belongs at the level a failing flow is reported at.
// wireMismatchAnswer is the RESET message for a JOIN whose connection
// negotiated a different wire version from the flow it names. The code is
// ResetProtocol, which every version already knows.
const wireMismatchAnswer = "wire version mismatch"

// acceptWire is the versions this gateway negotiates, newest first.
func (s *Server) acceptWire() []byte {
	if len(s.cfg.AcceptWire) == 0 {
		return protocol.SupportedVersions()
	}
	return s.cfg.AcceptWire
}

func (s *Server) refuseLaneJoin(fc *frameConn, sessionID [16]byte, flowID, laneID uint64, reason metrics.LaneJoinRefusal, code session.ResetCode, message string) {
	s.metrics.LaneJoinRefused(reason)
	if write, suppressed, total := s.refusals.due(reason, time.Now()); write {
		level := slog.LevelInfo
		if reason == metrics.LaneJoinPrincipalMismatch || reason == metrics.LaneJoinFlowMismatch || reason == metrics.LaneJoinWireMismatch {
			// Not a lost session: a peer that authenticated is naming a live
			// session that is not the one it holds. That is a build
			// disagreeing about the wire or a peer reaching for identifiers
			// that route rather than authorize, and neither is routine.
			level = slog.LevelWarn
		}
		s.cfg.Logger.LogAttrs(context.Background(), level, "lane join refused",
			slog.String("reason", reason.String()),
			slog.Int("wire", int(fc.wire)),
			slog.Uint64("lane", laneID),
			slog.Uint64("flow_id", flowID),
			slog.Uint64("suppressed", suppressed),
			slog.Uint64("total", total))
	}
	_ = fc.Write(protocol.Frame{Header: protocol.Header{
		Version: protocol.Version, Type: protocol.TypeReset, SessionID: sessionID,
		FlowID: flowID, Class: protocol.ClassBulk,
	}, Payload: session.ResetPayload(code, message)})
}

// handleLaneJoinOpen runs only after mutual TLS and exact JOIN validation. It
// additionally binds the new lane to the principal that created the session;
// session and flow IDs are routing identifiers, never bearer credentials.
func (s *Server) handleLaneJoinOpen(ctx context.Context, conn streamConn, fc *frameConn, principal identity.Principal, sessionID [16]byte, laneID uint64, open protocol.Frame) {
	s.handleLaneJoinOpenWithReplacement(ctx, conn, fc, principal, sessionID, laneID, 0, false, open)
}

func (s *Server) handleLaneJoinOpenWithReplacement(ctx context.Context, conn streamConn, fc *frameConn, principal identity.Principal, sessionID [16]byte, laneID, replacedLaneID uint64, replaceLane bool, open protocol.Frame) {
	s.admitLaneJoin(ctx, conn, fc, principal, sessionID, laneID, replacedLaneID, replaceLane, open, func() {})
}

// admitLaneJoin is the JOIN handler. admitted is called once the lane has
// been admitted and acknowledged, before the handler settles into holding
// it, so a bound that covers only admission is released then; every refusal
// returns, and the caller releases it on return.
func (s *Server) admitLaneJoin(ctx context.Context, conn streamConn, fc *frameConn, principal identity.Principal, sessionID [16]byte, laneID, replacedLaneID uint64, replaceLane bool, open protocol.Frame, admitted func()) {
	if session.IsZeroSessionID(sessionID) || laneID == 0 || open.Header.FlowID == 0 {
		s.refuseLaneJoin(fc, sessionID, open.Header.FlowID, laneID, metrics.LaneJoinInvalidIdentity, session.ResetProtocol, "invalid lane join identity")
		return
	}
	serverSession, pending, wrongWire := s.lookupLiveOrPending(sessionID, open.Header.FlowID, principal, fc.wire)
	if wrongWire {
		s.refuseLaneJoin(fc, sessionID, open.Header.FlowID, laneID, metrics.LaneJoinWireMismatch, session.ResetProtocol, wireMismatchAnswer)
		return
	}
	// A matching pending OPEN takes precedence even once its session is in
	// the table: until its OPEN_OK is written, a JOIN admitted now could
	// retire the OPEN's lane under it.
	if serverSession == nil || pending != nil {
		switch outcome := s.awaitPendingSession(ctx, pending); outcome {
		case pendingAdmitted:
			serverSession = s.lookupSession(sessionID)
		case pendingStillDialling:
			// Transient, and said with the code the peer already retries on:
			// the session exists and is only slow to finish opening.
			s.refuseLaneJoin(fc, sessionID, open.Header.FlowID, laneID, metrics.LaneJoinSessionPending, session.ResetFlowLimit, sessionPendingAnswer)
			return
		case pendingFailed:
			// The OPEN this JOIN waited on failed, and the stream that carried
			// it has said why. The destination code tells the peer the same:
			// whatever it does next, it must not resume or reopen this flow.
			s.refuseLaneJoin(fc, sessionID, open.Header.FlowID, laneID, metrics.LaneJoinOpenFailed, session.ResetDestination, "pending open failed")
			return
		}
	}
	switch {
	case serverSession == nil:
		// A rescue arriving after the session is gone is the failure mode that
		// matters most on a lossy path: the peer will spend its whole
		// replacement grace discovering this answer and then fail the flow.
		// The refusals below are kept apart because they send the operator
		// to different places.
		if held, openFailed := s.ended.heldBy(sessionID, open.Header.FlowID, principal, time.Now()); held && openFailed {
			// The OPEN failed before this JOIN arrived; say so, as a JOIN
			// that had waited on it would have been told.
			s.refuseLaneJoin(fc, sessionID, open.Header.FlowID, laneID, metrics.LaneJoinOpenFailed, session.ResetDestination, "pending open failed")
			return
		} else if held {
			// A rescue that reached the gateway after the session's grace
			// ended it: the late tail of the recovery deadline.
			s.carrierShadow.noteJoinAfterEnd(sessionID, open.Header.FlowID, laneID)
			s.refuseLaneJoin(fc, sessionID, open.Header.FlowID, laneID, metrics.LaneJoinSessionEnded, session.ResetProtocol, "unknown session: "+sessionEndedAnswer)
			return
		}
		s.refuseLaneJoin(fc, sessionID, open.Header.FlowID, laneID, metrics.LaneJoinUnknownSession, session.ResetProtocol, "unknown session")
		return
	case serverSession.flow.flowID != open.Header.FlowID:
		s.refuseLaneJoin(fc, sessionID, open.Header.FlowID, laneID, metrics.LaneJoinFlowMismatch, session.ResetProtocol, "unknown session")
		return
	case !samePrincipal(serverSession.principal, principal):
		s.refuseLaneJoin(fc, sessionID, open.Header.FlowID, laneID, metrics.LaneJoinPrincipalMismatch, session.ResetProtocol, "unknown session")
		return
	case serverSession.wireVersion() != fc.wire:
		// A flow speaks the version its OPEN negotiated, on every lane. The
		// versions frame the same bytes differently only at the JOIN, but a
		// flow whose lanes disagree about the wire is one nobody has tested,
		// and a client that pins its flows never asks for it. The flow itself
		// is left alone: this lane is refused, not the session.
		s.refuseLaneJoin(fc, sessionID, open.Header.FlowID, laneID, metrics.LaneJoinWireMismatch, session.ResetProtocol, wireMismatchAnswer)
		return
	}
	// A JOIN that gets this far is the peer's rescue, observable while it is
	// still only a handshake. If the flow is waiting out a lane-replacement
	// outage, that grace exists to cover exactly this arrival, so it restarts
	// here rather than expiring underneath the handshake it was waiting for.
	// The grace is per outage, so a rescue that dials several JOINs in
	// parallel extends it once per arriving JOIN, each one fresh evidence.
	// The shadow record needs the deadline in force when the JOIN arrived;
	// it is written after the extension so logging cannot delay it.
	var shadowDeadline time.Time
	if s.carrierShadow != nil {
		shadowDeadline = serverSession.flow.replacementDeadlineTime()
	}
	shadowArrival := time.Now()
	if !serverSession.completed.Load() && serverSession.flow.extendReplacementOutage(shadowArrival, laneReplacementWait) {
		s.metrics.LaneGraceExtended()
		s.cfg.Logger.Debug("lane replacement grace extended by rescue join", "lane", laneID, "flow_id", open.Header.FlowID)
	}
	s.carrierShadow.noteJoinArrival(serverSession.flow, laneID, shadowArrival, shadowDeadline)
	kind := transportKindForConn(conn)
	controlReplacement := open.Header.Flags&protocol.FlagReserveControl != 0
	if controlReplacement && (!serverSession.flow.reserveControlLane || kind != TransportQUIC) {
		s.refuseLaneJoin(fc, sessionID, open.Header.FlowID, laneID, metrics.LaneJoinInvalidControlReplacement, session.ResetProtocol, "invalid control lane replacement")
		return
	}
	_ = conn.SetDeadline(time.Time{})
	// One bounded write phase covers admission and replay, including completed
	// sessions which have no live flow watchdog left to interrupt a stuck peer.
	writeCtx, cancelWrite := context.WithTimeout(ctx, laneJoinAdmissionTimeout)
	defer cancelWrite()
	if serverSession.completed.Load() {
		s.cfg.Logger.Debug("lane join reached a completed session", "lane", laneID)
		if err := fc.WriteContext(writeCtx, protocol.Frame{Header: protocol.Header{Version: protocol.Version, Type: protocol.TypeOpenOK, SessionID: sessionID, FlowID: open.Header.FlowID, Class: protocol.ClassBulk}}); err != nil {
			return
		}
		// The completed server flow has already acknowledged the peer's FIN;
		// repeat that ACK on this authenticated replacement lane.
		_ = fc.WriteContext(writeCtx, protocol.Frame{Header: protocol.Header{
			Version: protocol.Version, Type: protocol.TypeAck, Flags: protocol.FlagAckFinal | serverSession.flow.recvAckFlag,
			SessionID: sessionID, FlowID: open.Header.FlowID, Sequence: serverSession.flow.remoteFinSequence.Load(),
			Class: protocol.ClassBulk,
		}})
		// The final ACK above acknowledges the peer's FIN.  If the server's
		// own FIN was lost while the last physical lane was closing, replay it
		// as well; otherwise the client can receive the complete application
		// body yet remain stuck waiting for the remote half-close until its
		// replacement timeout.  Keep the ACK first for compatibility with
		// clients that begin consuming the tombstone immediately after
		// OpenOK, then let the normal flow reader process this FIN.
		if serverSession.flow.finSent.Load() {
			flags := uint16(protocol.FlagFin)
			if serverSession.flow.localAbortSent.Load() {
				flags |= protocol.FlagCloseAbort
			}
			_ = fc.WriteContext(writeCtx, protocol.Frame{Header: protocol.Header{
				Version: protocol.Version, Type: protocol.TypeClose, Flags: flags,
				SessionID: sessionID, FlowID: open.Header.FlowID, Sequence: serverSession.flow.finSequence.Load(),
				Class: protocol.ClassBulk,
			}})
		}
		return
	}
	replacement := &mpLane{
		id: laneID, kind: kind, fc: fc, writeHook: s.cfg.testLaneWriteHook,
		control: controlReplacement, staged: true,
	}
	if s.cfg.Recovery == RecoveryCarrier && kind == TransportQUIC {
		// Replay towards the client waits for its ACK state on the new
		// lane, or one SRTT, as the client's own does (§10.3 step 5).
		srtt := 100 * time.Millisecond
		if qs, ok := fc.transport().(*quicStreamConn); ok && qs.conn != nil {
			if measured := qs.conn.ConnectionStats().SmoothedRTT; measured > 0 {
				srtt = measured
			}
		}
		replacement.replayGate = &replayGate{wait: srtt}
	}
	if err := serverSession.addLaneReplacing(replacement, replacedLaneID, replaceLane); err != nil {
		s.cfg.Logger.Debug("lane join admission refused", "lane", laneID, "error", err)
		// The handshake deadline was cleared above; the refusal is bounded
		// like the acknowledgement would have been, so a stalled RESET
		// cannot hold the admission bound this JOIN is under.
		_ = conn.SetDeadline(time.Now().Add(laneJoinAdmissionTimeout))
		switch {
		case errors.Is(err, errLaneFlowTCPMode):
			// The flow committed to TCP fallback for the rest of its life, so
			// a QUIC join can never be admitted. Saying so with the transport
			// code -- rather than the transient ceiling answer -- lets a new
			// peer commit to TCP at once instead of retrying an answer that
			// cannot change.
			s.refuseLaneJoin(fc, sessionID, open.Header.FlowID, laneID, metrics.LaneJoinLaneUnavailable, session.ResetTransport, "flow switched to TCP fallback")
		case errors.Is(err, errLaneFlowClosed), errors.Is(err, errLaneDuplicateID):
			// Both mean this endpoint cannot make the named lane part of the
			// flow the peer thinks it is joining, and neither heals with a
			// retry: the same permanent answer "unknown session" already
			// carries for the refusals above.
			s.refuseLaneJoin(fc, sessionID, open.Header.FlowID, laneID, metrics.LaneJoinUnknownSession, session.ResetProtocol, "unknown session")
		default:
			s.refuseLaneJoin(fc, sessionID, open.Header.FlowID, laneID, metrics.LaneJoinLaneUnavailable, session.ResetFlowLimit, "lane unavailable")
		}
		return
	}
	// The handshake deadline was cleared above, so the OPEN_OK write needs its
	// own bound: without one a peer that stops reading wedges the staged lane
	// in the lane map, where it counts against the admission ceiling yet can
	// never carry traffic. One path-detection budget matches the rescue-race
	// window a young staged lane is protected from eviction for. WriteContext
	// holds the lane's write mutex while it arms the deadline, so a concurrent
	// control write neither misses nor keeps it.
	openOK := protocol.Frame{Header: protocol.Header{Version: protocol.Version, Type: protocol.TypeOpenOK, SessionID: sessionID, FlowID: open.Header.FlowID, Class: protocol.ClassBulk}}
	writeErr := fc.WriteContext(writeCtx, openOK)
	if writeErr != nil {
		serverSession.flow.removeLane(replacement)
		return
	}
	if err := serverSession.flow.activateLane(replacement); err != nil {
		serverSession.flow.removeLane(replacement)
		return
	}
	serverSession.commitControlTransfer(replacement)
	s.cfg.Logger.Debug("lane joined", "lane", laneID, "transport", kind, "control", controlReplacement, "lanes", serverSession.flow.laneCount())
	s.observeLanes(serverSession.flow.laneCount())
	// A replacement can arrive after the destination has already reached EOF
	// but before the original lane carried the logical FIN. Replay any known
	// close state on this active lane immediately. Without this, the first
	// rescue only lets the peer's FIN reach the server; the server then marks a
	// tombstone and the client needs a second rescue merely to learn the FIN it
	// had already received as application bytes. FIN/ACK frames are
	// idempotent at the reassembler and cumulative-ACK state, so replaying them
	// is safe even when the original frame was merely delayed.
	if serverSession.flow.remoteFinSeen.Load() {
		_ = fc.WriteContext(writeCtx, protocol.Frame{Header: protocol.Header{
			Version: protocol.Version, Type: protocol.TypeAck,
			Flags:     protocol.FlagAckFinal | serverSession.flow.recvAckFlag,
			SessionID: sessionID, FlowID: open.Header.FlowID,
			Sequence: serverSession.flow.remoteFinSequence.Load(), Class: protocol.ClassBulk,
		}})
	}
	if serverSession.flow.finSent.Load() {
		flags := uint16(protocol.FlagFin)
		if serverSession.flow.localAbortSent.Load() {
			flags |= protocol.FlagCloseAbort
		}
		_ = fc.WriteContext(writeCtx, protocol.Frame{Header: protocol.Header{
			Version: protocol.Version, Type: protocol.TypeClose, Flags: flags,
			SessionID: sessionID, FlowID: open.Header.FlowID,
			Sequence: serverSession.flow.finSequence.Load(), Class: protocol.ClassBulk,
		}})
	}
	cancelWrite()
	admitted()
	// A retired lane must release its admission slot while the logical flow
	// continues on its replacement. Waiting only for the flow leaks slots.
	select {
	case <-fc.done:
	case <-serverSession.flow.doneChan():
	case <-ctx.Done():
	}
}

func samePrincipal(a, b identity.Principal) bool {
	return a.ProviderID == b.ProviderID && a.AccountID == b.AccountID && a.DeviceID == b.DeviceID
}

// accountUsage is one account's live admission state: how many flows it holds
// and which of its devices are holding them. The device counts are what make
// a client limit a limit on devices rather than on flows -- a device that
// holds two hundred flows is still one client.
type accountUsage struct {
	flows   int
	devices map[string]int
}

// accountRefusal is a flow open refused by the opening account's own policy
// rather than by the gateway's capacity. It carries the reason for the counter
// and the log, and the code and message for the RESET.
//
// The message names the limit that was hit. That matters more than it looks:
// the peer is where this refusal is first seen, frequently by someone who
// cannot read the gateway's logs, and a message that does not distinguish
// "this account is out of flows" from "this account is out of device slots"
// sends them looking in the wrong place.
type accountRefusal struct {
	reason  metrics.AccountRefusal
	code    session.ResetCode
	message string
}

func (r *accountRefusal) Error() string { return r.message }

var (
	errAccountFlowLimit = &accountRefusal{
		reason: metrics.AccountRefusalFlowLimit, code: session.ResetFlowLimit,
		message: "account flow limit reached",
	}
	errAccountClientLimit = &accountRefusal{
		reason: metrics.AccountRefusalClientLimit, code: session.ResetFlowLimit,
		message: "account device limit reached",
	}
	errAccountUnauthorized = &accountRefusal{
		reason: metrics.AccountRefusalUnauthorized, code: session.ResetAuthentication,
		message: "device is not authorized",
	}
)

// admitAccountFlow reserves one flow against the opening account's policy, and
// counts the opening device against the account's client limit. A device
// already holding a flow is already counted, so an existing client is never
// refused by the client limit however many flows it opens.
func (s *Server) admitAccountFlow(principal identity.Principal) *accountRefusal {
	authorization, err := s.cfg.Credentials.Store.Authorize(principal, time.Now())
	if err != nil {
		return errAccountUnauthorized
	}
	limits := authorization.Account.Limits()
	s.accountMu.Lock()
	defer s.accountMu.Unlock()
	usage := s.accountUsage[principal.AccountID]
	flows, clients, known := 0, 0, false
	if usage != nil {
		flows, clients = usage.flows, len(usage.devices)
		_, known = usage.devices[principal.DeviceID]
	}
	if limits.MaxFlows > 0 && flows >= limits.MaxFlows {
		return errAccountFlowLimit
	}
	if limits.MaxClients > 0 && !known && clients >= limits.MaxClients {
		return errAccountClientLimit
	}
	// Nothing is recorded until the open is admitted, so a refused open
	// leaves no entry behind for an account that holds no flows.
	if usage == nil {
		usage = &accountUsage{devices: make(map[string]int)}
		s.accountUsage[principal.AccountID] = usage
	}
	usage.flows++
	usage.devices[principal.DeviceID]++
	return nil
}

func (s *Server) releaseAccountFlow(principal identity.Principal) {
	s.accountMu.Lock()
	defer s.accountMu.Unlock()
	usage := s.accountUsage[principal.AccountID]
	if usage == nil {
		return
	}
	if usage.flows <= 1 {
		delete(s.accountUsage, principal.AccountID)
		return
	}
	usage.flows--
	if usage.devices[principal.DeviceID] <= 1 {
		delete(usage.devices, principal.DeviceID)
		return
	}
	usage.devices[principal.DeviceID]--
}

// refuseAccountFlow answers a flow open with a reset, and says so where an
// operator will see it.
//
// This path used to be silent at every log level and carried no counter, so a
// gateway refusing every second open an account made looked completely
// healthy. The account whose limit was hit is named because that is the only
// thing an operator can act on, and it is not a secret from the operator who
// set the limit.
func (s *Server) refuseAccountFlow(fc *frameConn, sessionID [16]byte, flowID uint64, principal identity.Principal, refusal *accountRefusal) {
	s.metrics.AccountAdmissionRefused(refusal.reason)
	if write, suppressed, total := s.accountRefusals.due(refusal.reason, time.Now()); write {
		s.cfg.Logger.LogAttrs(context.Background(), slog.LevelWarn, "account flow open refused",
			slog.String("reason", refusal.reason.String()),
			slog.String("account", principal.AccountID),
			slog.String("device", principal.DeviceID),
			slog.Uint64("suppressed", suppressed),
			slog.Uint64("total", total))
	}
	_ = fc.Write(protocol.Frame{Header: protocol.Header{
		Version: protocol.Version, Type: protocol.TypeReset, SessionID: sessionID,
		FlowID: flowID, Class: protocol.ClassNew,
	}, Payload: session.ResetPayload(refusal.code, refusal.message)})
}

// watchAuthorization applies revocation and account expiry to already-open
// flows. TLS prevents new use immediately; this watcher bounds an existing
// connection's remaining lifetime without a CRL or server restart.
func (s *Server) watchAuthorization(ctx context.Context, flow *serverFlow) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if _, err := s.cfg.Credentials.Store.Authorize(flow.principal, time.Now()); err != nil {
				// No RESET here: the broadcast's bounded wait would leave
				// data the scheduler already holds moving to a revoked
				// device. Its replacement JOIN is refused at authorization.
				flow.flow.closeAll()
				return
			}
		case <-flow.flow.doneChan():
			return
		case <-ctx.Done():
			return
		}
	}
}

func (s *Server) registerSession(id [16]byte, flow *serverFlow) bool {
	s.sessionsMu.Lock()
	defer s.sessionsMu.Unlock()
	live := len(s.sessions)
	if s.admissionOwned() {
		live -= s.tombstones
	}
	if live >= s.cfg.MaxSessions {
		return false
	}
	if _, exists := s.sessions[id]; exists {
		return false
	}
	s.sessions[id] = flow
	return true
}

func (s *Server) lookupSession(id [16]byte) *serverFlow {
	s.sessionsMu.RLock()
	defer s.sessionsMu.RUnlock()
	return s.sessions[id]
}

func (s *Server) unregisterSession(id [16]byte, flow *serverFlow) {
	s.sessionsMu.Lock()
	if s.sessions[id] == flow {
		delete(s.sessions, id)
		s.ended.add(id, flow.flow.flowID, flow.principal, time.Now())
	}
	s.sessionsMu.Unlock()
}

// pendingJoinWait bounds how long a JOIN waits for its session's OPEN to
// finish dialling. It is shorter than a client's join handshake, so the peer
// hears the transient "session pending" rather than timing out on its own.
var pendingJoinWait = 5 * time.Second

type pendingSession struct {
	flowID    uint64
	principal identity.Principal
	// wire is the version the OPEN negotiated, so a JOIN of another version
	// is refused before it can wait on, or ask to rescue, this OPEN.
	wire byte
	// done is closed once the OPEN has either registered its session or
	// failed; admitted, written before, says which.
	done     chan struct{}
	admitted bool
	// waiters counts JOINs waiting on this OPEN; tests synchronize on it.
	waiters atomic.Int32
	// rescueRequested records that a matching JOIN found this OPEN, even if
	// its own wait has since ended with "session pending" and its client is
	// backing off before asking again.
	rescueRequested atomic.Bool
}

type pendingOutcome int

const (
	pendingNone pendingOutcome = iota
	pendingAdmitted
	pendingStillDialling
	pendingFailed
)

// beginPendingSession announces a session whose OPEN is being dialled. It
// refuses an identifier that is already pending or live, which is what
// registerSession would have answered after the dial.
func (s *Server) beginPendingSession(id [16]byte, flowID uint64, principal identity.Principal, wire byte) (*pendingSession, bool) {
	s.sessionsMu.Lock()
	defer s.sessionsMu.Unlock()
	if _, exists := s.sessions[id]; exists {
		return nil, false
	}
	if _, exists := s.pending[id]; exists {
		return nil, false
	}
	if s.ended.contains(id, time.Now()) {
		// A late copy of an OPEN whose session already ended must not reach
		// the destination a second time.
		return nil, false
	}
	pending := &pendingSession{flowID: flowID, principal: principal, wire: wire, done: make(chan struct{})}
	if s.pending == nil {
		s.pending = make(map[[16]byte]*pendingSession)
	}
	s.pending[id] = pending
	return pending, true
}

func (s *Server) endPendingSession(id [16]byte, pending *pendingSession) {
	s.sessionsMu.Lock()
	if s.pending[id] == pending {
		delete(s.pending, id)
	}
	// The outcome is recorded before any waiter wakes: a flow admitted and
	// already gone by the time a waiter looks is an ended session, not a
	// failed OPEN.
	pending.admitted = s.sessions[id] != nil
	if !pending.admitted {
		// An OPEN that failed is a session this gateway did hold, however
		// briefly: it must not later be answered as never seen.
		s.ended.addFailedOpen(id, pending.flowID, pending.principal, time.Now())
	}
	s.sessionsMu.Unlock()
	close(pending.done)
}

// lookupLiveOrPending reads the session table and the pending OPENs in one
// snapshot, so an OPEN registering in between cannot make a JOIN miss both.
//
// A pending OPEN is returned only to the JOIN of the device and flow that sent
// it, and that JOIN is counted as its waiter here, under the same lock the
// OPEN takes to decide whether a waiter is there to rescue it. The caller
// must then call awaitPendingSession, which uncounts it.
//
// wrongWire reports a pending OPEN of this device and flow that negotiated a
// different wire version: the JOIN is neither counted nor allowed to mark the
// OPEN as one a client wants rescued.
func (s *Server) lookupLiveOrPending(id [16]byte, flowID uint64, principal identity.Principal, wire byte) (live *serverFlow, pending *pendingSession, wrongWire bool) {
	s.sessionsMu.RLock()
	defer s.sessionsMu.RUnlock()
	pending = s.pending[id]
	if pending == nil || pending.flowID != flowID || !samePrincipal(pending.principal, principal) {
		return s.sessions[id], nil, false
	}
	if pending.wire != 0 && pending.wire != wire {
		return s.sessions[id], nil, true
	}
	pending.waiters.Add(1)
	pending.rescueRequested.Store(true)
	return s.sessions[id], pending, false
}

// pendingRescueRequested decides, under the lock JOINs register under,
// whether a client has asked to rescue this OPEN: a JOIN is waiting on it now,
// or one found it, was told "session pending", and will retry. The flow is
// then kept for one replacement grace, the same bounded window any lost lane
// gets; an OPEN no JOIN ever named is not kept.
func (s *Server) pendingRescueRequested(pending *pendingSession) bool {
	s.sessionsMu.Lock()
	defer s.sessionsMu.Unlock()
	return pending.waiters.Load() > 0 || pending.rescueRequested.Load()
}

// awaitPendingSession waits, bounded, for a pending OPEN to be decided. Only a
// JOIN naming the same flow from the same device waits; anything else is
// answered from the session table as before.
func (s *Server) awaitPendingSession(ctx context.Context, pending *pendingSession) pendingOutcome {
	if pending == nil {
		return pendingNone
	}
	defer pending.waiters.Add(-1)
	// The JOIN's stream carries the server's handshake deadline, and the
	// refusal must still be written inside it.
	wait := pendingJoinWait
	if bound := s.cfg.HandshakeTimeout / 2; bound > 0 && bound < wait {
		wait = bound
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-pending.done:
	case <-timer.C:
		return pendingStillDialling
	case <-ctx.Done():
		return pendingStillDialling
	}
	if !pending.admitted {
		return pendingFailed
	}
	return pendingAdmitted
}

func (s *Server) observeLanes(count int) {
	for {
		old := s.maxObservedLanes.Load()
		if int64(count) <= old || s.maxObservedLanes.CompareAndSwap(old, int64(count)) {
			return
		}
	}
}

// MaxObservedLanes reports the largest number of lanes attached to any flow
// since this server instance started. It is safe for benchmark instrumentation
// and does not expose session IDs or destination metadata.
func (s *Server) MaxObservedLanes() int { return int(s.maxObservedLanes.Load()) }

// pathTraceFields names the path-tree node a flow's QUIC carrier was keyed
// under, as resolvePath derives it: the egress (the tree's root and the
// ancestor every leaf on this gateway shares) and the egress->peer leaf key.
// A live validation campaign uses it to confirm that the leaves it meant to
// load are distinct at the gateway (two clients behind one NAT are one leaf)
// and that the flows ran on QUIC at all. Only with QUEQIAO_LANE_TRACE=1, the
// campaign trace; it changes nothing but the log record.
func pathTraceFields(conn streamConn) []any {
	if !laneTrace.Load() {
		return nil
	}
	q, ok := conn.(*quicStreamConn)
	if !ok || q.conn == nil {
		return nil
	}
	egress, key := resolvePath(q.conn.LocalAddr(), q.conn.RemoteAddr())
	return []any{"path_egress", egress, "path_key", key}
}

func transportKindForConn(conn streamConn) TransportKind {
	if _, ok := conn.(*quicStreamConn); ok {
		return TransportQUIC
	}
	return TransportTCP
}

// classifierConfig is the gateway's flow-classification policy. A gateway
// serving a datacenter leg has the same reason as its client to stop calling a
// large request a bulk transfer, and the two ends classify independently, so
// setting it on only one of them would leave the other demoting flows the first
// had decided to protect.
func (s *Server) classifierConfig() classifier.Config {
	cfg := s.cfg.Profile.Classifier
	if cfg.BulkBytes == 0 {
		cfg = profile.Default().Classifier
	}
	return phasedClassifierConfig(cfg, s.cfg.Classifier)
}
