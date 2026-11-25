package pep

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/apernet/quic-go"

	"github.com/bojieli/queqiao/internal/pathmodel"
)

// connPathModel is a connection's view of its path model, shared by the
// readers that size their work from it -- the coded path's FEC and the control
// generation's erasure test. It starts as the model of the path the
// connection was opened on and follows it to another path only when the
// connection migrates to a different path key (cc-api v2 keeps the controller
// across migration and rekeys it then). A port-only rebinding keeps the key
// and so the model.
type connPathModel struct {
	model atomic.Pointer[pathmodel.PathModel]
	// datagramBurst, if set, supplies the burst factor of the datagram
	// substrate's own loss (congestion=path; design §4.2, §9), which the
	// coded path sizes parity from in place of the path-wide figure.
	datagramBurst atomic.Pointer[func() (float64, bool)]
}

var _ pathmodel.Model = (*connPathModel)(nil)

func (m *connPathModel) Report(member pathmodel.Member, o pathmodel.Observation) pathmodel.State {
	return m.model.Load().Report(member, o)
}

func (m *connPathModel) Current() pathmodel.State {
	st := m.model.Load().Current()
	if f := m.datagramBurst.Load(); f != nil {
		if b, ok := (*f)(); ok {
			st.BurstFactor = b
		}
	}
	return st
}

// setDatagramBurst installs the datagram burst factor's source.
func (m *connPathModel) setDatagramBurst(f func() (float64, bool)) { m.datagramBurst.Store(&f) }

var connPathModels sync.Map // *quic.Conn -> *connPathModel

// pathModelOf returns the connection's path model reference, creating it on
// first use with the model of the connection's current path.
func pathModelOf(conn *quic.Conn) *connPathModel {
	if m, ok := connPathModels.Load(conn); ok {
		return m.(*connPathModel)
	}
	created := &connPathModel{}
	created.model.Store(pathmodel.Shared(peerKey(conn)))
	actual, loaded := connPathModels.LoadOrStore(conn, created)
	if !loaded {
		context.AfterFunc(conn.Context(), func() { connPathModels.Delete(conn) })
	}
	return actual.(*connPathModel)
}

// rekeyPathModel points the connection's readers at the model for key.
func rekeyPathModel(conn *quic.Conn, key string) {
	pathModelOf(conn).model.Store(pathmodel.Shared(key))
}
