package pep

import (
	"net"
	"strconv"
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
	name := principal + "|" + key
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	if now.Sub(r.swept) >= pathAuthoritySweepEvery {
		r.swept = now
		for k, c := range r.controllers {
			if idle, ok := c.IdleSince(); ok && now.Sub(idle) > pathKnowledgeLifetime {
				delete(r.controllers, k)
				// The principal's model goes with its controller: a
				// principal that does not return would otherwise keep it
				// for the life of the process.
				pathmodel.Forget(k)
			}
		}
	}
	if c, ok := r.controllers[name]; ok {
		// Handed out for an installation that attaches on the
		// connection's run loop: a sweep in that gap must not evict it.
		c.Touch()
		return c
	}
	// The model is the principal's too: its pooled erasure sets the law's
	// compensation, and one device's loss must not move another's rate.
	//
	// It is flat: the hierarchical model is dc-long-haul's, and that profile
	// stays on the legacy bundle (validateBundle), so the connection's
	// readers, pointed at pathmodel.Shared(name) by rekeyPathModel, read
	// exactly the model the law reports to.
	c := wancongestion.NewPathController(initialPacketSize, pathmodel.Shared(name), wancongestion.DefaultClassWeights)
	r.controllers[name] = c
	return c
}

// forget drops every controller and its model, for tests that need a cold
// path.
func (r *pathAuthorityRegistry) forget() {
	r.mu.Lock()
	for k := range r.controllers {
		pathmodel.Forget(k)
	}
	clear(r.controllers)
	r.mu.Unlock()
}

// attachPathController attaches conn to its path's controller and installs
// the attachment as the connection's PathSender. A migration that changes
// the path key -- a new remote host or a new local uplink -- moves the
// attachment, with the connection's flight, to that path's controller; a
// port-only rebinding keeps it.
func attachPathController(conn *quic.Conn, cfg congestionConfig, principal string) wancongestion.TelemetryProvider {
	_, current := resolvePath(conn.LocalAddr(), conn.RemoteAddr())
	initial := conn.InitialPacketSize()
	controller := pathAuthorities.controller(principal, current, initial)
	// The connection's FEC and probe readers read the model the law reports
	// to: the principal's.
	rekeyPathModel(conn, principal+"|"+current)
	// rekey is called only on the connection's run loop.
	rekey := func(_, newLocal, _, newRemote net.Addr) *wancongestion.PathController {
		_, next := resolvePath(newLocal, newRemote)
		if next == current {
			return nil
		}
		current = next
		// The connection's other readers of the path follow it too.
		rekeyPathModel(conn, principal+"|"+next)
		return pathAuthorities.controller(principal, next, initial)
	}
	attachment := controller.Attach(rekey, func() (net.Addr, net.Addr) { return conn.LocalAddr(), conn.RemoteAddr() })
	markClassScheduled(conn)
	conn.SetCongestionControl(attachment)
	return attachment
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
