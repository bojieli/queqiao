package pep

import (
	"context"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apernet/quic-go"
	quiccongestion "github.com/apernet/quic-go/congestion"

	wancongestion "github.com/bojieli/queqiao/internal/congestion"
	"github.com/bojieli/queqiao/internal/identity"
	"github.com/bojieli/queqiao/internal/pathmodel"
)

// pathAuthorities holds the path controllers of congestion=path (design
// docs/DESIGN-PATH-AUTHORITY.md §4.1), one per path and sending direction.
//
// The key is (principal, local uplink, remote host). The remote port is
// excluded, so port hopping and a NAT rebinding stay on one path. A gateway
// keys by the client's authorised principal, so several clients behind one
// address are separate paths; a client is one principal.
//
// A controller whose last attachment detached is kept for
// pathKnowledgeLifetime, so a connection made a minute after the last one
// starts from what the path has measured rather than from nothing. Those are
// the law's own samples, aging inside it; nothing is seeded.
var pathAuthorities = &pathAuthorityRegistry{controllers: make(map[string]*wancongestion.PathController)}

// pathKnowledgeLifetime matches the path model's knowledge lifetime.
const pathKnowledgeLifetime = 10 * time.Minute

// pathAuthoritySweepEvery bounds how often the registry looks for idle
// controllers to forget.
const pathAuthoritySweepEvery = time.Minute

// clientPathPrincipal is the prefix of a client's own principal in a path
// key. Each Client gets its own (see newClientPathPrincipal): one daemon can
// run several credential profiles, and two of them reaching the same gateway
// over the same uplink are different devices whose budgets must not mix. A
// gateway's keys always carry "principal:", so the two never collide in one
// process.
const clientPathPrincipal = "client"

var clientPathPrincipals atomic.Uint64

// newClientPathPrincipal names one Client's identity in path keys.
func newClientPathPrincipal() string {
	return clientPathPrincipal + ":" + strconv.FormatUint(clientPathPrincipals.Add(1), 10)
}

// pathRekey returns the migration callback of an attachment whose key is
// now current: it resolves the principal and the addresses again, compares
// the whole key, and on a change returns the new key's controller after
// pointing the connection's readers (follow) at its model. It is called
// only on the connection's run loop.
func pathRekey(principal func() string, current string, initial quiccongestion.ByteCount, follow func(next string)) func(_, newLocal, _, newRemote net.Addr) *wancongestion.PathController {
	return pathRekeyIn(principal, current, initial, false, nil, func(next, _, _ string) { follow(next) })
}

// pathRekeyIn is pathRekey for an authority that may sit in a hierarchical
// profile's tree (§11.3): the new key's controller is created with its
// knowledge view, and follow also learns the new key's egress and address
// part, from which the readers' view is rebuilt.
func pathRekeyIn(principal func() string, current string, initial quiccongestion.ByteCount, hierarchical bool, egressPolicy *EgressCapacity, follow func(next, egress, addrs string)) func(_, newLocal, _, newRemote net.Addr) *wancongestion.PathController {
	return func(_, newLocal, _, newRemote net.Addr) *wancongestion.PathController {
		egress, nextAddrs := resolvePath(newLocal, newRemote)
		nextWho := principal()
		next := nextWho + "|" + nextAddrs
		if next == current {
			return nil
		}
		current = next
		// Resolve the controller first: a lookup may sweep an expired entry
		// and its model, and the readers must follow the model the
		// controller reports to.
		c := pathAuthorities.controllerIn(nextWho, nextAddrs, initial, treePosition(hierarchical, egress, nextAddrs))
		joinEgress(c, nextWho, egress, egressPolicy)
		follow(next, egress, nextAddrs)
		return c
	}
}

// treePosition is where an authority's path sits in a hierarchical
// profile's tree (§11.3): under its uplink, as the endpoint pair. Nil when
// the profile has no tree.
func treePosition(hierarchical bool, egress, addrs string) *pathmodel.Key {
	if !hierarchical {
		return nil
	}
	return &pathmodel.Key{Egress: egress, Dest: addrs}
}

// pathModelIn is an authority's model: the path's own model, viewed through
// the tree when the profile has one. The tree supplies priors only -- the
// erasure, burst factor and round trip its confident nodes know while the
// path's own model has not measured enough -- and never a share or seed
// (pathmodel.Knowledge).
func pathModelIn(name string, position *pathmodel.Key) pathmodel.Model {
	leaf := pathmodel.Shared(name)
	if position == nil {
		return leaf
	}
	return pathmodel.SharedKnowledge(*position, leaf)
}

// pathPrincipalNow is the client's identity in path keys for a connection
// dialled now: its principal and the generation of its uplink. The key's
// address part distinguishes uplinks by source address; the generation
// distinguishes the networks behind an uplink change that kept the address.
// A port-only rebinding changes neither.
func (c *Client) pathPrincipalNow() string {
	if c.pathPrincipal == "" {
		return clientPathPrincipal
	}
	return c.pathPrincipal + "@" + strconv.FormatUint(c.uplinkGeneration.Load(), 10)
}

// authorizePathPrincipal checks, before a gateway data connection attaches
// to its principal's controller, that the principal is still authorised. A
// variable so a test can withdraw authorisation between the handshake and
// this check.
var authorizePathPrincipal = func(store *identity.Store, p identity.Principal, now time.Time) error {
	_, err := store.Authorize(p, now)
	return err
}

// gatewayPathPrincipal names an authorised principal in a path key.
func gatewayPathPrincipal(p identity.Principal) string {
	return "principal:" + p.ProviderID + "/" + p.AccountID + "/" + p.DeviceID
}

type pathAuthorityRegistry struct {
	mu          sync.Mutex
	controllers map[string]*wancongestion.PathController
	swept       time.Time
}

// controller returns the path's controller, creating it on first use.
func (r *pathAuthorityRegistry) controller(principal, key string, initialPacketSize quiccongestion.ByteCount) *wancongestion.PathController {
	return r.controllerIn(principal, key, initialPacketSize, nil)
}

// controllerIn is controller for a path at a position in a hierarchical
// profile's tree, or at none (nil). A controller created at a position
// reports to its own model through the tree's knowledge view and takes its
// erasure prior from it; one that already exists keeps the view it was
// created with.
func (r *pathAuthorityRegistry) controllerIn(principal, key string, initialPacketSize quiccongestion.ByteCount, position *pathmodel.Key) *wancongestion.PathController {
	name := principal + "|" + key
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	if now.Sub(r.swept) >= pathAuthoritySweepEvery {
		r.swept = now
		for k, c := range r.controllers {
			if idle, ok := c.IdleSince(); ok && now.Sub(idle) > pathKnowledgeLifetime {
				c.JoinEgress(nil)
				delete(r.controllers, k)
				// The principal's model goes with its controller: a
				// principal that does not return would otherwise keep it
				// for the life of the process.
				pathmodel.Forget(k)
			}
		}
	}
	if c, ok := r.controllers[name]; ok {
		if idle, ok := c.IdleSince(); !ok || now.Sub(idle) <= pathKnowledgeLifetime {
			// Handed out for an installation that attaches on the
			// connection's run loop: a sweep in that gap must not evict it.
			c.Touch()
			return c
		}
		// Expired, whenever the last sweep ran: the path starts cold.
		c.JoinEgress(nil)
		delete(r.controllers, name)
		pathmodel.Forget(name)
	}
	c := wancongestion.NewPathController(initialPacketSize, pathModelIn(name, position), wancongestion.DefaultClassWeights)
	r.controllers[name] = c
	return c
}

// forget drops every controller and its model, for tests that need a cold
// path.
func (r *pathAuthorityRegistry) forget() {
	r.mu.Lock()
	for k, c := range r.controllers {
		c.JoinEgress(nil)
		pathmodel.Forget(k)
	}
	clear(r.controllers)
	egressGroups.forget()
	r.mu.Unlock()
}

// attachPathController attaches conn to its path's controller and installs
// the attachment as the connection's PathSender. A migration that changes
// the path key -- a new remote host or a new local uplink -- moves the
// attachment, with the connection's flight, to that path's controller; a
// port-only rebinding keeps it.
//
// principal is read at installation and at every migration, not once when
// the dial began: a client's uplink generation can move while a dial is in
// flight, and every connection on one physical path must resolve the same
// key. A migration compares the whole key, principal included.
func attachPathController(conn *quic.Conn, cfg congestionConfig, principal func() string) wancongestion.TelemetryProvider {
	egress, addrs := resolvePath(conn.LocalAddr(), conn.RemoteAddr())
	who := principal()
	current := who + "|" + addrs
	initial := conn.InitialPacketSize()
	position := treePosition(cfg.hierarchicalPath, egress, addrs)
	controller := pathAuthorities.controllerIn(who, addrs, initial, position)
	joinEgress(controller, who, egress, cfg.egress)
	// The connection's FEC and probe readers read the model the law reports
	// to: the principal's, through the tree's knowledge under a hierarchical
	// profile.
	readers := pathModelOfKey(conn, current)
	readers.setKnowledge(knowledgeAt(current, position))
	rekey := pathRekeyIn(principal, current, initial, cfg.hierarchicalPath, cfg.egress, func(next, egress, addrs string) {
		rekeyPathModel(conn, next)
		pathModelOf(conn).setKnowledge(knowledgeAt(next, treePosition(cfg.hierarchicalPath, egress, addrs)))
	})
	attachment := controller.Attach(rekey, func() (net.Addr, net.Addr) { return conn.LocalAddr(), conn.RemoteAddr() })
	if cfg.generation != nil {
		// An uplink change that keeps this connection's addresses is no
		// migration; the attachment follows the generation itself.
		attachment.SetGeneration(cfg.generation)
	}
	// PROBING follows the transport's own progress export (§4.4, §10.2).
	attachment.SetTransportProgress(func() wancongestion.TransportProgress {
		st := conn.ConnectionStats()
		return wancongestion.TransportProgress{Epoch: st.ProgressEpoch, PTOExpiries: st.PTOExpiriesSinceProgress}
	})
	// The coded path reads the datagrams' own burst statistic, from
	// whichever controller the connection is attached to now, and sizes its
	// window from its own source rate.
	readers.setDatagramBurst(func() (float64, bool) { return attachment.Controller().DatagramBurstFactor() })
	markPathScoped(conn)
	markClassScheduled(conn)
	conn.SetCongestionControl(attachment)
	return attachment
}

// joinEgress seats a gateway's controller in the joint limit of the interface
// it sends out of (egress.go). A client's controllers, whose principal is not
// a gateway's, never join, and neither does any when the gateway has no
// --egress-capacity policy (a client).
func joinEgress(c *wancongestion.PathController, principal, egressHost string, policy *EgressCapacity) {
	if policy == nil || !strings.HasPrefix(principal, "principal:") {
		return
	}
	c.JoinEgress(egressGroups.group(egressHost, *policy))
}

// knowledgeAt is the readers' knowledge view of the path named name at a
// tree position, or nil without one.
func knowledgeAt(name string, position *pathmodel.Key) *pathmodel.Knowledge {
	if position == nil {
		return nil
	}
	return pathmodel.SharedKnowledge(*position, pathmodel.Shared(name))
}

// provisionalPathController is a gateway connection's controller until it is
// known to be an authorised data connection: a standalone, bounded erasure
// sender that belongs to no path and reports to no path model (§4.1).
// Enrollment and renewal connections keep it for their whole life.
func provisionalPathController(conn *quic.Conn) wancongestion.TelemetryProvider {
	sender := wancongestion.NewErasurePathSender(wancongestion.NewErasureSender(conn.InitialPacketSize()), nil)
	sender.EnableClassScheduling(wancongestion.DefaultClassWeights)
	markClassScheduled(conn)
	conn.SetCongestionControl(sender)
	return sender
}

// pathScopedConns holds the connections attached to a path controller. A
// coded path on one sizes its window from its own source rate (§9).
var pathScopedConns sync.Map // *quic.Conn -> struct{}

func markPathScoped(conn *quic.Conn) {
	if _, loaded := pathScopedConns.LoadOrStore(conn, struct{}{}); !loaded {
		context.AfterFunc(conn.Context(), func() { pathScopedConns.Delete(conn) })
	}
}

func isPathScoped(conn *quic.Conn) bool {
	_, ok := pathScopedConns.Load(conn)
	return ok
}
