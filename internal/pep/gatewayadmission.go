package pep

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/apernet/quic-go"
	"github.com/bojieli/queqiao/internal/identity"
)

// GatewayAdmission selects what a gateway's session limit counts (design
// §10.5, stage 5 of the path-authority migration).
//
// A gateway used to take one slot of its session semaphore for every stream,
// before reading the stream's first frame, and to hold it for the stream's
// life. A JOIN's handler and a PROBE took slots exactly like an OPEN, so a
// gateway whose slots were all held by established flows refused the PROBE
// and the JOINs that would have let those very flows recover (issue #170).
//
// Two modes exist:
//
//   - legacy (the default) keeps that charge for every stream that finds a
//     free slot. Only a stream that finds none behaves differently: its first
//     frame is read under a small per-connection bound, and a JOIN or PROBE
//     for an existing binding is admitted without a slot; anything else is
//     closed as before. Below saturation nothing changes.
//   - owned charges the session limit to logical flows at OPEN, on the QUIC
//     and the TLS/TCP entry points alike. Every stream's first frame is parsed
//     under a per-connection bound, a JOIN is charged to the flow it joins
//     (its lane ceiling), a PROBE to a per-connection allowance, and a refused
//     OPEN is answered with FLOW_LIMIT. A UDP association whose replacement
//     names its live relay hands the relay, its session slot and its account
//     slot to the replacement (§11.2).
//
// Both modes admit one standby connection beside a principal's existing data
// connections when the connection bound is full.
type GatewayAdmission string

const (
	GatewayAdmissionLegacy GatewayAdmission = "legacy"
	GatewayAdmissionOwned  GatewayAdmission = "owned"
)

// ParseGatewayAdmission accepts the names a configuration may use. The empty
// string is the default, legacy.
func ParseGatewayAdmission(value string) (GatewayAdmission, error) {
	switch GatewayAdmission(strings.ToLower(strings.TrimSpace(value))) {
	case "", GatewayAdmissionLegacy:
		return GatewayAdmissionLegacy, nil
	case GatewayAdmissionOwned:
		return GatewayAdmissionOwned, nil
	}
	return "", fmt.Errorf("unsupported gateway admission %q (want %s or %s)", value, GatewayAdmissionLegacy, GatewayAdmissionOwned)
}

const (
	// firstFrameStreams bounds, per QUIC connection, the streams whose first
	// frame is being read or whose JOIN is being admitted. It is not a limit
	// on flows: an OPEN leaves it as soon as its frame is read. In owned mode
	// the accept loop waits for room, so a peer that stalls its own first
	// frames delays only its own connection; in legacy mode it bounds the
	// streams read past a full session limit.
	firstFrameStreams = 32
	// probeStreams is the per-connection allowance for path PROBEs that do
	// not hold a session slot. A client verifies one connection with one
	// probe at a time; the margin covers a probe overlapping its retry. The
	// work of each is fixed by PROTOCOL.md §14.
	probeStreams = 4
	// recoveryReads bounds, gateway-wide in legacy mode, the streams (QUIC)
	// and connections (TLS/TCP) read past a full session limit so that a JOIN
	// or PROBE on them can be found. Each is bounded by the handshake
	// deadline until it is admitted. In owned mode the gateway-wide bounds
	// are MaxSessions (at least this), the bound every stream used to be
	// read under.
	recoveryReads = 64
)

func (s *Server) admissionOwned() bool {
	return s.cfg.GatewayAdmission == GatewayAdmissionOwned
}

// streamAdmission says what a stream was charged before its first frame was
// read, and what it still has to be charged once the frame says what it is.
type streamAdmission struct {
	// charged: the stream already holds a session slot, taken before its
	// first frame (legacy). Its caller releases it.
	charged bool
	// recoveryOnly: the stream found no session slot (legacy). Only a JOIN
	// or a PROBE may proceed; anything else is closed, as it always was.
	recoveryOnly bool
	// release frees the bound the first frame was read under. It is called
	// once the stream has been admitted or refused, and may be nil.
	release func()
	// probes is the connection's PROBE allowance. Nil keeps a PROBE under
	// release's bound for its whole exchange, which the handshake deadline
	// bounds (TLS/TCP, where a connection carries one stream).
	probes chan struct{}
}

func tryAcquire(slots chan struct{}) bool {
	select {
	case slots <- struct{}{}:
		return true
	default:
		return false
	}
}

// flowLease is what a logical flow holds against the gateway's limits: its
// session slot and its account slot. A UDP association that hands its relay
// to its replacement hands the lease with it, so the transfer charges nothing
// new and releases nothing early.
type flowLease struct {
	mu       sync.Mutex
	releases []func()
}

func (l *flowLease) add(release func()) {
	l.mu.Lock()
	l.releases = append(l.releases, release)
	l.mu.Unlock()
}

// release frees everything still held, newest first. It is idempotent.
func (l *flowLease) release() {
	if l == nil {
		return
	}
	l.mu.Lock()
	releases := l.releases
	l.releases = nil
	l.mu.Unlock()
	for i := len(releases) - 1; i >= 0; i-- {
		releases[i]()
	}
}

// detach moves everything held to a new lease and leaves this one empty.
func (l *flowLease) detach() *flowLease {
	if l == nil {
		return &flowLease{}
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	moved := &flowLease{releases: l.releases}
	l.releases = nil
	return moved
}

// principalConnections counts one principal's admitted data connections that
// hold a slot of the general bound, and names the standby admitted past it.
type principalConnections struct {
	general int
	standby *connectionTicket
}

// connectionTicket is what an admitted QUIC connection gives back on close.
// standby is read and changed only under the server's connMu: a standby
// whose principal's last general connection closes inherits that slot.
type connectionTicket struct {
	principal string
	standby   bool
}

func principalKey(p identity.Principal) string {
	return p.ProviderID + "\x00" + p.AccountID + "\x00" + p.DeviceID
}

// dataPrincipal names the authorised principal of a data connection, or
// reports that the connection is not one (enrollment, renewal, an unaccepted
// wire, or no usable certificate).
func (s *Server) dataPrincipal(state tls.ConnectionState) (identity.Principal, bool) {
	if _, ok := checkNegotiatedWire(state.NegotiatedProtocol, s.acceptWire()); !ok {
		return identity.Principal{}, false
	}
	principal, err := identity.PrincipalFromTLS(state)
	if err != nil {
		return identity.Principal{}, false
	}
	return principal, true
}

// admitQUICConnection charges an authenticated QUIC connection. The general
// bound is tried first. When it is full, a principal that already holds a
// data connection may have one more: that is the standby its client dials to
// verify a path and move its flows, and refusing it is refusing the recovery
// of connections this gateway already carries. The overflow is at most one
// per principal holding a general slot, so it is bounded by the general
// bound, and a standby takes over the general slot of the first of its
// principal's connections to close rather than letting it go.
func (s *Server) admitQUICConnection(state tls.ConnectionState) (*connectionTicket, bool) {
	principal, data := s.dataPrincipal(state)
	key := ""
	if data {
		key = principalKey(principal)
	}
	if s.admitConnection() {
		ticket := &connectionTicket{principal: key}
		if key != "" {
			s.connMu.Lock()
			if s.principalConns == nil {
				s.principalConns = make(map[string]*principalConnections)
			}
			p := s.principalConns[key]
			if p == nil {
				p = &principalConnections{}
				s.principalConns[key] = p
			}
			p.general++
			s.connMu.Unlock()
		}
		return ticket, true
	}
	if key == "" || s.cfg.testReleasedAdmission {
		return nil, false
	}
	if _, err := s.cfg.Credentials.Store.Authorize(principal, time.Now()); err != nil {
		return nil, false
	}
	s.connMu.Lock()
	defer s.connMu.Unlock()
	p := s.principalConns[key]
	if p == nil || p.general == 0 || p.standby != nil {
		return nil, false
	}
	ticket := &connectionTicket{principal: key, standby: true}
	p.standby = ticket
	return ticket, true
}

// isStandby reports whether a ticket is, at this moment, the overflow
// standby rather than a holder of a general slot.
func (s *Server) isStandby(ticket *connectionTicket) bool {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	return ticket.standby
}

func (s *Server) releaseQUICConnection(ticket *connectionTicket) {
	if ticket == nil {
		return
	}
	releaseGeneral := true
	if ticket.principal != "" {
		s.connMu.Lock()
		if p := s.principalConns[ticket.principal]; p != nil {
			switch {
			case ticket.standby:
				p.standby = nil
				releaseGeneral = false
			case p.standby != nil:
				// A connection the principal held closes while its standby
				// lives: the standby is where its flows went, so it takes
				// over the general slot this one gives back, and the
				// principal can have a standby again for its next recovery.
				p.standby.standby = false
				p.standby = nil
				releaseGeneral = false
			default:
				p.general--
			}
			if p.general <= 0 && p.standby == nil {
				delete(s.principalConns, ticket.principal)
			}
		}
		s.connMu.Unlock()
	}
	if releaseGeneral {
		s.releaseConnection()
	}
}

// udpTransferWait bounds how long a UDP replacement waits for the live
// association it replaces to stop and hand its relay over. It leaves the
// replacement half of its handshake deadline to answer.
func (s *Server) udpTransferWait() time.Duration {
	wait := s.cfg.HandshakeTimeout / 2
	if wait <= 0 || wait > 5*time.Second {
		wait = 5 * time.Second
	}
	return wait
}

// waitForSlot waits for a slot of slots, giving up when the connection or
// the server ends.
func waitForSlot(ctx context.Context, conn *quic.Conn, slots chan struct{}) bool {
	select {
	case slots <- struct{}{}:
		return true
	case <-ctx.Done():
	case <-conn.Context().Done():
	}
	return false
}
