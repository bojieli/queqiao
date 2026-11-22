package pep

import (
	"context"
	"errors"
	"github.com/bojieli/queqiao/internal/protocol"
	"net"
	"testing"
)

func TestACKValidationIsAtomicAndProgressRequiresNewCoverage(t *testing.T) {
	for _, name := range []string{"malformed", "beyond-sent", "duplicate", "empty"} {
		t.Run(name, func(t *testing.T) {
			inner, peer := net.Pipe()
			defer inner.Close()
			defer peer.Close()
			f := newMultipathFlow(context.Background(), inner, [16]byte{1}, 7, 1024, protocol.FlagAckUp, protocol.FlagAckDown, nil, nil)
			defer f.closeAll()
			f.noteSent(0, 8)
			lane := &mpLane{id: 9}
			lane.suspected.Store(true)
			f.setRescueReplacement(lane.id)
			frame := protocol.Frame{Header: protocol.Header{Version: protocol.Version, Type: protocol.TypeAck, SessionID: f.sessionID, FlowID: f.flowID, Flags: protocol.FlagAckUp | protocol.FlagAckRanges}}
			switch name {
			case "malformed":
				frame.Header.Sequence = 3
				frame.Payload = []byte{1}
			case "beyond-sent":
				frame.Payload, _ = protocol.EncodeAckRanges([][2]uint64{{4, 9}})
			case "duplicate":
				frame.Payload, _ = protocol.EncodeAckRanges([][2]uint64{{4, 8}})
				if err := f.receiveACK(inboundEvent{frame: frame}); err != nil {
					t.Fatal(err)
				}
			case "empty":
				frame.Payload, _ = protocol.EncodeAckRanges(nil)
			}
			f.lastAckProgressNS.Store(1)
			err := f.receiveACK(inboundEvent{frame: frame, lane: lane})
			if name == "malformed" || name == "beyond-sent" {
				if err == nil {
					t.Fatal("invalid ACK accepted")
				}
				if f.acked != 0 {
					t.Fatal("invalid ACK advanced cumulative point")
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if f.lastAckProgressNS.Load() != 1 || !lane.suspected.Load() {
				t.Fatal("non-progress ACK changed health")
			}
			if id, ok := f.rescueReplacement(); !ok || id != lane.id {
				t.Fatal("non-progress ACK cleared recovery hint")
			}
			if name == "empty" {
				frame.Payload, _ = protocol.EncodeAckRanges([][2]uint64{{4, 8}})
				if err := f.receiveACK(inboundEvent{frame: frame, lane: lane}); err != nil {
					t.Fatal(err)
				}
				if f.lastAckProgressNS.Load() == 1 || lane.suspected.Load() {
					t.Fatal("new coverage did not publish progress")
				}
			}
		})
	}
}

// The receive worker may abort a full-closed socket before a pending source
// read returns. That later EOF must not change the sequence already on wire.
func TestAbortACKSurvivesLateSourceEOF(t *testing.T) {
	for _, sent := range []int{32, 64} {
		inner, peer := net.Pipe()
		f := newMultipathFlow(context.Background(), inner, [16]byte{1}, 7, 1024,
			protocol.FlagAckUp, protocol.FlagAckDown, nil, nil)
		f.noteSent(0, sent)
		f.noteLocalClose(64)
		f.localAbortSent.Store(true)
		f.noteLocalClose(128)
		frame := protocol.Frame{Header: protocol.Header{Version: protocol.Version,
			Type: protocol.TypeAck, SessionID: f.sessionID, FlowID: f.flowID,
			Flags: protocol.FlagAckUp | protocol.FlagAckFinal, Sequence: 64}}
		if err := f.receiveACK(inboundEvent{frame: frame}); !errors.Is(err, errLocalApplicationClose) {
			t.Errorf("sent=%d: abort ACK after late EOF: %v", sent, err)
		}
		if f.acked != 0 {
			t.Errorf("abort ACK falsely credited delivery: %d", f.acked)
		}
		frame.Header.Sequence = 65
		if err := f.receiveACK(inboundEvent{frame: frame}); err == nil || errors.Is(err, errLocalApplicationClose) {
			t.Errorf("mismatched abort ACK accepted: %v", err)
		}
		f.closeAll()
		peer.Close()
	}
}
