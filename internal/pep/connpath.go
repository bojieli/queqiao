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
}

var _ pathmodel.Model = (*connPathModel)(nil)

func (m *connPathModel) Report(member pathmodel.Member, o pathmodel.Observation) pathmodel.State {
	return m.model.Load().Report(member, o)
}

func (m *connPathModel) Current() pathmodel.State { return m.model.Load().Current() }

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
