package coded

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// taggedCarrier holds every tagged datagram until the test says what became
// of it, as a connection's datagram queue does until the packet is built.
type taggedCarrier struct {
	mu       sync.Mutex
	observer func(uint64, bool)
	tags     []uint64
	untagged int
	refuse   bool
	closed   chan struct{}
	once     sync.Once
}

func newTaggedCarrier() *taggedCarrier { return &taggedCarrier{closed: make(chan struct{})} }

func (c *taggedCarrier) Send([]byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.untagged++
	return nil
}

func (c *taggedCarrier) SendTagged(_ []byte, tag uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.refuse {
		return ErrDatagramTooLarge
	}
	c.tags = append(c.tags, tag)
	return nil
}

func (c *taggedCarrier) ObserveTags(f func(uint64, bool)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.observer = f
}

func (c *taggedCarrier) Receive() ([]byte, error) {
	<-c.closed
	return nil, errors.New("closed")
}

func (c *taggedCarrier) Close() error {
	c.once.Do(func() { close(c.closed) })
	return nil
}

// settle reports the oldest n held datagrams, as the run loop would.
func (c *taggedCarrier) settle(n int, registered bool) {
	c.mu.Lock()
	n = min(n, len(c.tags))
	tags := append([]uint64(nil), c.tags[:n]...)
	c.tags = c.tags[n:]
	observer := c.observer
	c.mu.Unlock()
	for _, tag := range tags {
		observer(tag, registered)
	}
}

func (c *taggedCarrier) held() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.tags)
}

func (c *taggedCarrier) waitHeld(t *testing.T, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for c.held() < n {
		if time.Now().After(deadline) {
			t.Fatalf("carrier holds %d tagged datagrams, want %d", c.held(), n)
		}
		time.Sleep(100 * time.Microsecond)
	}
}

type outcomes chan Outcome

func (o outcomes) notify(outcome Outcome) { o <- outcome }

func (o outcomes) none(t *testing.T) {
	t.Helper()
	select {
	case got := <-o:
		t.Fatalf("premature outcome %d", got)
	case <-time.After(20 * time.Millisecond):
	}
}

func (o outcomes) want(t *testing.T, want Outcome) {
	t.Helper()
	select {
	case got := <-o:
		if got != want {
			t.Fatalf("outcome = %d, want %d", got, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("no outcome, want %d", want)
	}
}

// A frame spread over several symbols is registered when the last of them is,
// and not before: the deadline it starts must not begin while part of the
// frame is still queued below the path.
func TestObservedFrameIsRegisteredWithItsLastSymbol(t *testing.T) {
	carrier := newTaggedCarrier()
	path := New(carrier, Config{SymbolBytes: 200})
	defer path.Close()
	got := make(outcomes, 4)
	frame := bytes.Repeat([]byte("f"), 1000)
	if err := path.SendObserved(context.Background(), frame, got.notify); err != nil {
		t.Fatal(err)
	}
	symbols := (len(frame) + path.symbolPayload() - 1) / path.symbolPayload()
	if symbols < 3 {
		t.Fatalf("fixture needs a fragmented frame, got %d symbols", symbols)
	}
	carrier.waitHeld(t, symbols)
	carrier.settle(symbols-1, true)
	got.none(t)
	carrier.settle(1, true)
	got.want(t, Registered)
	got.none(t)
}

// Small frames packed into symbols are each told when theirs is registered.
// Frames sent without an observer never make a tagged datagram.
func TestPackedObservedFramesAreEachRegistered(t *testing.T) {
	carrier := newTaggedCarrier()
	path := New(carrier, Config{})
	defer path.Close()
	first, second := make(outcomes, 1), make(outcomes, 1)
	if err := path.Send([]byte("plain")); err != nil {
		t.Fatal(err)
	}
	waitDrained(t, path)
	carrier.mu.Lock()
	plain := carrier.untagged
	carrier.mu.Unlock()
	if plain == 0 || carrier.held() != 0 {
		t.Fatal("an unobserved frame was tagged")
	}
	// Queue both before the send loop can drain, so they pack together.
	path.pending <- queuedFrame{data: []byte("one"), track: &frameTrack{notify: first.notify}}
	path.pending <- queuedFrame{data: []byte("two"), track: &frameTrack{notify: second.notify}}
	carrier.waitHeld(t, 1)
	time.Sleep(5 * time.Millisecond)
	carrier.settle(carrier.held(), true)
	first.want(t, Registered)
	second.want(t, Registered)
}

// A discarded datagram abandons every frame it carries part of, at once and
// once; the frame's other symbols registering later changes nothing.
func TestDiscardedSymbolAbandonsItsFrame(t *testing.T) {
	carrier := newTaggedCarrier()
	path := New(carrier, Config{SymbolBytes: 200})
	defer path.Close()
	got := make(outcomes, 4)
	if err := path.SendObserved(context.Background(), bytes.Repeat([]byte("d"), 600), got.notify); err != nil {
		t.Fatal(err)
	}
	carrier.waitHeld(t, 2)
	carrier.settle(1, false)
	got.want(t, Abandoned)
	carrier.settle(carrier.held(), true)
	got.none(t)

	// A carrier that refuses the datagram outright abandons it too.
	carrier.mu.Lock()
	carrier.refuse = true
	carrier.mu.Unlock()
	if err := path.SendObserved(context.Background(), []byte("refused"), got.notify); err != nil {
		t.Fatal(err)
	}
	got.want(t, Abandoned)
}

// Closing the path abandons every observed frame it still holds, whether it
// was waiting in the path's queue or its datagram was waiting in the
// carrier's, and a report arriving afterwards is ignored.
func TestClosedPathAbandonsWhatItHolds(t *testing.T) {
	carrier := newTaggedCarrier()
	path := New(carrier, Config{})
	inCarrier := make(outcomes, 2)
	if err := path.SendObserved(context.Background(), []byte("queued below"), inCarrier.notify); err != nil {
		t.Fatal(err)
	}
	carrier.waitHeld(t, 1)
	carrier.mu.Lock()
	late := append([]uint64(nil), carrier.tags...)
	carrier.mu.Unlock()
	_ = path.Close()
	inCarrier.want(t, Abandoned)
	for _, tag := range late {
		path.onTag(tag, true)
	}
	inCarrier.none(t)

	// A frame that was never taken off the queue is abandoned by whoever
	// sees the path closed: here the stopped path itself, directly.
	stopped := New(newTaggedCarrier(), Config{})
	_ = stopped.Close()
	queued := make(outcomes, 1)
	stopped.pending <- queuedFrame{data: []byte("never sent"), track: &frameTrack{notify: queued.notify}}
	stopped.abandonQueued()
	queued.want(t, Abandoned)
	if err := stopped.SendObserved(context.Background(), []byte("x"), queued.notify); err == nil {
		t.Fatal("a closed path accepted an observed frame")
	}
	queued.none(t)
}

// Over a carrier that cannot say when a packet is built, acceptance is the
// registration.
func TestUntaggedCarrierRegistersOnAcceptance(t *testing.T) {
	path := New(newLossyCarrier(), Config{})
	defer path.Close()
	got := make(outcomes, 1)
	if err := path.SendObserved(context.Background(), []byte("accepted"), got.notify); err != nil {
		t.Fatal(err)
	}
	got.want(t, Registered)
}

// lossyCarrier accepts everything and delivers nothing.
type lossyCarrier struct {
	once   sync.Once
	closed chan struct{}
}

func newLossyCarrier() *lossyCarrier    { return &lossyCarrier{closed: make(chan struct{})} }
func (*lossyCarrier) Send([]byte) error { return nil }
func (c *lossyCarrier) Receive() ([]byte, error) {
	<-c.closed
	return nil, errors.New("closed")
}
func (c *lossyCarrier) Close() error {
	c.once.Do(func() { close(c.closed) })
	return nil
}

// The QUIC carrier reports registration from the connection's run loop: an
// observed frame over a real connection is registered once its packets are
// built.
func TestQUICCarrierReportsRegistration(t *testing.T) {
	client, server := quicPair(t, liveChannel())
	clientCarrier, err := NewQUICCarrier(client)
	if err != nil {
		t.Fatal(err)
	}
	serverCarrier, err := NewQUICCarrier(server)
	if err != nil {
		t.Fatal(err)
	}
	sender := New(clientCarrier, Config{SymbolBytes: clientCarrier.MaxDatagramBytes()})
	defer sender.Close()
	receiver := New(serverCarrier, Config{})
	defer receiver.Close()
	got := make(outcomes, 1)
	frame := bytes.Repeat([]byte("q"), 3000)
	if err := sender.SendObserved(context.Background(), frame, got.notify); err != nil {
		t.Fatal(err)
	}
	got.want(t, Registered)
}
