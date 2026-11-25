package pep

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	quiccongestion "github.com/apernet/quic-go/congestion"

	"github.com/bojieli/queqiao/internal/protocol"
)

// fakeClassStream is a lane transport that records, for every write, its
// class and whether the stream was boosted when the bytes were accepted --
// which is what the fork's persistent boost property serves at CONTROL. A
// non-CONTROL write can be held before its bytes reach the stream.
type fakeClassStream struct {
	mu       sync.Mutex
	boost    bool
	boostLog []bool
	writes   []fakeWrite

	hold    chan struct{} // non-nil: a non-CONTROL write waits on it
	holding chan struct{} // signalled when a write starts waiting

	// pause, if set, holds setBoost(true) until it is closed; boosting is
	// signalled when it starts waiting.
	pause    chan struct{}
	boosting chan struct{}
}

type fakeWrite struct {
	class   quiccongestion.SendClass
	boosted bool
	n       int
}

func newFakeClassStream() *fakeClassStream {
	return &fakeClassStream{holding: make(chan struct{}, 16)}
}

func (s *fakeClassStream) writeClass(p []byte, class quiccongestion.SendClass) (int, error) {
	s.mu.Lock()
	hold := s.hold
	s.mu.Unlock()
	if hold != nil && class != quiccongestion.ClassControl {
		s.holding <- struct{}{}
		<-hold
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.writes = append(s.writes, fakeWrite{class: class, boosted: s.boost, n: len(p)})
	return len(p), nil
}

func (s *fakeClassStream) setBoost(on bool) bool {
	s.mu.Lock()
	pause := s.pause
	s.mu.Unlock()
	if on && pause != nil {
		s.boosting <- struct{}{}
		<-pause
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.boost == on {
		panic("a boost transition repeated: transitions are not serialised")
	}
	s.boost = on
	s.boostLog = append(s.boostLog, on)
	return true
}

func (s *fakeClassStream) classed() bool { return true }

func (s *fakeClassStream) boosted() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.boost
}

func (s *fakeClassStream) log() ([]bool, []fakeWrite) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]bool(nil), s.boostLog...), append([]fakeWrite(nil), s.writes...)
}

func (s *fakeClassStream) Read([]byte) (int, error) { return 0, io.EOF }
func (s *fakeClassStream) Write(p []byte) (int, error) {
	return s.writeClass(p, quiccongestion.ClassBulk)
}
func (s *fakeClassStream) Close() error { return nil }

func dataFrame(class protocol.Class, n int) protocol.Frame {
	return protocol.Frame{Header: protocol.Header{Version: protocol.Version, Type: protocol.TypeData, FlowID: 1, Class: class}, Payload: make([]byte, n)}
}

func ackFrame() protocol.Frame {
	return protocol.Frame{Header: protocol.Header{Version: protocol.Version, Type: protocol.TypeAck, FlowID: 1}, Payload: make([]byte, 8)}
}

func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// The sending endpoint's class of each frame type.
func TestFrameSendClass(t *testing.T) {
	for _, c := range []struct {
		f    protocol.Frame
		want quiccongestion.SendClass
	}{
		{dataFrame(protocol.ClassBulk, 1), quiccongestion.ClassBulk},
		{dataFrame(protocol.ClassInteractive, 1), quiccongestion.ClassInteractive},
		{dataFrame(protocol.ClassNew, 1), quiccongestion.ClassInteractive},
		{protocol.Frame{Header: protocol.Header{Type: protocol.TypePacket, Class: protocol.ClassBulk}}, quiccongestion.ClassInteractive},
		{ackFrame(), quiccongestion.ClassControl},
		{protocol.Frame{Header: protocol.Header{Type: protocol.TypeOpen}}, quiccongestion.ClassControl},
		{protocol.Frame{Header: protocol.Header{Type: protocol.TypeJoin, Class: protocol.ClassBulk}}, quiccongestion.ClassControl},
		{protocol.Frame{Header: protocol.Header{Type: protocol.TypeClose, Class: protocol.ClassBulk}}, quiccongestion.ClassControl},
		{protocol.Frame{Header: protocol.Header{Type: protocol.TypeReset}}, quiccongestion.ClassControl},
		{protocol.Frame{Header: protocol.Header{Type: protocol.TypeProbe}}, quiccongestion.ClassControl},
	} {
		if got := frameSendClass(c.f); got != c.want {
			t.Errorf("frame type %d class %d: send class %d, want %d", c.f.Header.Type, c.f.Header.Class, got, c.want)
		}
	}
}

// Test 6 of §15.1: a control frame that starts waiting while its flow's DATA
// writer holds writeMu -- here after the writer's last check and before its
// bytes reach the stream -- boosts the stream, so the DATA bytes ahead of it
// are served at CONTROL; the boost ends once the control frame itself has
// been accepted.
func TestControlFrameWaitingBehindDataBoostsTheStream(t *testing.T) {
	s := newFakeClassStream()
	fc := newFrameConn(s)
	s.hold = make(chan struct{})

	dataDone := make(chan error, 1)
	go func() { dataDone <- fc.WriteContext(context.Background(), dataFrame(protocol.ClassBulk, 4096)) }()
	<-s.holding // the DATA writer holds writeMu; its bytes have not reached the stream

	ackDone := make(chan error, 1)
	go func() { ackDone <- fc.WriteContext(context.Background(), ackFrame()) }()
	waitUntil(t, "the waiting ACK to boost the stream", s.boosted)
	if n := fc.boost.count(); n != 1 {
		t.Fatalf("%d waiters, want 1", n)
	}

	close(s.hold)
	if err := <-dataDone; err != nil {
		t.Fatal(err)
	}
	if err := <-ackDone; err != nil {
		t.Fatal(err)
	}
	boostLog, writes := s.log()
	if len(boostLog) != 2 || !boostLog[0] || boostLog[1] {
		t.Fatalf("boost transitions %v, want exactly on then off", boostLog)
	}
	if len(writes) != 2 || writes[0].class != quiccongestion.ClassBulk || !writes[0].boosted {
		t.Fatalf("writes %+v: the DATA ahead of the waiting ACK was not boosted", writes)
	}
	if writes[1].class != quiccongestion.ClassControl {
		t.Fatalf("the ACK was written as class %d", writes[1].class)
	}
	if s.boosted() || fc.boost.count() != 0 {
		t.Fatal("the boost outlived the last waiting control frame")
	}

	// A control frame that does not have to wait does not boost.
	if err := fc.WriteContext(context.Background(), ackFrame()); err != nil {
		t.Fatal(err)
	}
	if boostLog, _ := s.log(); len(boostLog) != 2 {
		t.Fatalf("an uncontended control frame toggled the boost: %v", boostLog)
	}
}

// Two waiters, one of which cancels: the boost stays on for the other, is
// turned on and off once each, and the cancelled waiter leaves no count.
func TestCancelledControlWaiterLeavesTheBoostToTheOther(t *testing.T) {
	s := newFakeClassStream()
	fc := newFrameConn(s)
	s.hold = make(chan struct{})
	dataDone := make(chan error, 1)
	go func() { dataDone <- fc.Write(dataFrame(protocol.ClassBulk, 4096)) }()
	<-s.holding

	cancelCtx, cancel := context.WithCancel(context.Background())
	cancelled := make(chan error, 1)
	go func() { cancelled <- fc.WriteContext(cancelCtx, ackFrame()) }()
	kept := make(chan error, 1)
	go func() { kept <- fc.Write(ackFrame()) }()
	waitUntil(t, "two waiters", func() bool { return fc.boost.count() == 2 })

	cancel()
	if err := <-cancelled; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled waiter returned %v", err)
	}
	if !s.boosted() || fc.boost.count() != 1 {
		t.Fatalf("after one waiter cancelled: boosted=%v waiters=%d, want the boost kept for the other", s.boosted(), fc.boost.count())
	}

	close(s.hold)
	if err := <-dataDone; err != nil {
		t.Fatal(err)
	}
	if err := <-kept; err != nil {
		t.Fatal(err)
	}
	boostLog, writes := s.log()
	if len(boostLog) != 2 || !boostLog[0] || boostLog[1] {
		t.Fatalf("boost transitions %v, want exactly on then off", boostLog)
	}
	controls := 0
	for _, w := range writes {
		if w.class == quiccongestion.ClassControl {
			controls++
		}
	}
	if controls != 1 {
		t.Fatalf("%d control frames written, want only the one that kept waiting", controls)
	}
}

// Zero-to-one and one-to-zero transitions are serialised with the count,
// including cancellations: under concurrent waiters that come and go the
// boost alternates strictly and ends off.
func TestBoostTransitionsAreSerialised(t *testing.T) {
	s := newFakeClassStream() // setBoost panics on a repeated transition
	var b controlBoost
	var wg sync.WaitGroup
	for g := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 500 {
				if !b.enter(s) {
					t.Error("enter refused")
					return
				}
				if (g+i)%7 == 0 {
					time.Sleep(time.Microsecond)
				}
				b.leave(s)
			}
		}()
	}
	wg.Wait()
	boostLog, _ := s.log()
	for i, on := range boostLog {
		if on != (i%2 == 0) {
			t.Fatalf("transition %d is %v: transitions are not alternating", i, on)
		}
	}
	if s.boosted() || b.count() != 0 {
		t.Fatal("the boost did not end with the last waiter")
	}
}

// A control frame that has announced it is waiting keeps its place: while
// its boost call is still in progress the DATA write ahead of it finishes,
// and a later DATA write must not take writeMu before the control frame.
func TestAnnouncedControlFrameKeepsItsPlaceAheadOfLaterData(t *testing.T) {
	s := newFakeClassStream()
	fc := newFrameConn(s)
	s.hold = make(chan struct{})
	s.pause = make(chan struct{})
	s.boosting = make(chan struct{}, 1)

	first := make(chan error, 1)
	go func() { first <- fc.WriteContext(context.Background(), dataFrame(protocol.ClassBulk, 1000)) }()
	<-s.holding // the first DATA write holds writeMu

	ack := make(chan error, 1)
	go func() { ack <- fc.WriteContext(context.Background(), ackFrame()) }()
	<-s.boosting // the ACK has announced itself and is inside setBoost

	// The first DATA write finishes while the boost call is paused.
	s.mu.Lock()
	hold := s.hold
	s.hold = nil
	s.mu.Unlock()
	close(hold)
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	second := make(chan error, 1)
	go func() { second <- fc.WriteContext(context.Background(), dataFrame(protocol.ClassBulk, 2000)) }()
	time.Sleep(50 * time.Millisecond)
	if _, writes := s.log(); len(writes) != 1 {
		t.Fatalf("a later DATA write went ahead of the waiting ACK: %+v", writes)
	}
	close(s.pause)
	if err := <-ack; err != nil {
		t.Fatal(err)
	}
	if err := <-second; err != nil {
		t.Fatal(err)
	}
	_, writes := s.log()
	if len(writes) != 3 || writes[1].class != quiccongestion.ClassControl || writes[2].n <= writes[0].n {
		t.Fatalf("writes %+v, want first DATA, the ACK, then the later DATA", writes)
	}
}

// A control write whose context is already cancelled does not boost the
// stream while another write holds writeMu: it could never proceed, and a
// boost would promote the buffered DATA ahead of it for nothing.
func TestCancelledControlWriteDoesNotBoost(t *testing.T) {
	s := newFakeClassStream()
	fc := newFrameConn(s)
	s.hold = make(chan struct{})
	data := make(chan error, 1)
	go func() { data <- fc.WriteContext(context.Background(), dataFrame(protocol.ClassBulk, 1000)) }()
	<-s.holding

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := fc.WriteContext(ctx, ackFrame()); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled control write returned %v", err)
	}
	if boostLog, _ := s.log(); len(boostLog) != 0 {
		t.Fatalf("a cancelled control write toggled the boost: %v", boostLog)
	}
	close(s.hold)
	if err := <-data; err != nil {
		t.Fatal(err)
	}
}
