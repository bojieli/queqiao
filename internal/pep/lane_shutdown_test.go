package pep

import (
	"context"
	"errors"
	"github.com/bojieli/queqiao/internal/protocol"
	"net"
	"sync"
	"testing"
)

type blockedCloseConn struct {
	net.Conn
	entered, release chan struct{}
	once             sync.Once
}

func (c *blockedCloseConn) Close() error {
	c.once.Do(func() { close(c.entered) })
	<-c.release
	return c.Conn.Close()
}

func TestLaneAdmissionRejectsFlowAlreadyClosing(t *testing.T) {
	for _, staged := range []bool{false, true} {
		t.Run(map[bool]string{false: "add", true: "activate"}[staged], func(t *testing.T) {
			a, b := net.Pipe()
			defer b.Close()
			inner := &blockedCloseConn{Conn: a, entered: make(chan struct{}), release: make(chan struct{})}
			f := newMultipathFlow(context.Background(), inner, [16]byte{1}, 1, 1024, protocol.FlagAckUp, protocol.FlagAckDown, nil, nil)
			capture := newAckCaptureConn(0, nil)
			defer capture.Close()
			lane := &mpLane{id: 1, fc: newFrameConn(capture), staged: staged}
			if staged {
				if err := f.addLane(lane); err != nil {
					t.Fatal(err)
				}
			}
			done := make(chan struct{})
			go func() { f.closeAll(); close(done) }()
			<-inner.entered
			var err error
			if staged {
				err = f.activateLane(lane)
			} else {
				err = f.addLane(lane)
			}
			close(inner.release)
			<-done
			if err == nil {
				t.Fatal("closing flow admitted or activated a lane")
			}
			if !staged && !errors.Is(err, errLaneFlowClosed) {
				t.Fatalf("unexpected error: %v", err)
			}
			if staged && lane.ready.Load() {
				t.Fatal("closing flow published staged lane")
			}
		})
	}
}
