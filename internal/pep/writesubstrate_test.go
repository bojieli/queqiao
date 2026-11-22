package pep

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/bojieli/queqiao/internal/coded"
	"github.com/bojieli/queqiao/internal/pathmodel"
	"github.com/bojieli/queqiao/internal/protocol"
)

type bufferConn struct{ bytes.Buffer }

func (*bufferConn) Close() error { return nil }

// The write is where a data frame's substrate is chosen, so it is the write
// that has to say which one it used. The scheduler recorded an answer when the
// lane took the chunk; if the coding plan or the flow's class changed while
// the chunk waited in the lane's queue, that answer is stale, and a chunk
// believed to be on the stream is never re-offered to its own lane after its
// datagram is erased.
func TestAWriteReportsTheSubstrateItActuallyUsed(t *testing.T) {
	model := pathmodel.NewPathModel()
	model.Report(1, pathmodel.Observation{
		Erasure: 0.14, BurstFactor: 1, ObservedSamples: 5000,
		Delivered: 1 << 20, RoundTrip: 200 * time.Millisecond,
	})
	path := coded.New(newPipeCarrier(), coded.Config{Path: model})
	defer path.Close()
	if !path.Coding() {
		t.Fatal("a 14% erasure path is not coding; this test is not testing the coded substrate")
	}
	wantsCoding := true
	fc := &frameConn{control: &bufferConn{}, bulk: path, wantsCoding: func() bool { return wantsCoding }}
	frame := protocol.Frame{Header: protocol.Header{Version: protocol.Version, Type: protocol.TypeData, FlowID: 1}, Payload: []byte("payload")}

	reliable, err := fc.writeContextVia(context.Background(), frame)
	if err != nil {
		t.Fatal(err)
	}
	if reliable {
		t.Fatal("a frame written to the coded datagram path was reported reliable")
	}

	wantsCoding = false
	reliable, err = fc.writeContextVia(context.Background(), frame)
	if err != nil {
		t.Fatal(err)
	}
	if !reliable {
		t.Fatal("a frame written to the stream was reported unreliable")
	}

	// A frame with an open safety copy goes both ways, and the stream copy is
	// what makes it deliverable.
	wantsCoding = true
	fc.openUnconfirmed = func() bool { return true }
	fc.openSafetyCopy.Store(true)
	reliable, err = fc.writeContextVia(context.Background(), frame)
	if err != nil {
		t.Fatal(err)
	}
	if !reliable {
		t.Fatal("a coded frame with a stream safety copy was reported unreliable")
	}
}
