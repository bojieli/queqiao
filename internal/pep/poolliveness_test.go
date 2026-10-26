package pep

import (
	"testing"
	"time"
)

func TestAPooledConnectionThatStopsAnsweringIsNoticedWithinRoundTrips(t *testing.T) {
	var l poolLiveness
	now := time.Now()
	const srtt = 200 * time.Millisecond
	l.observe(now, 10, 10, srtt)
	// Idle: nothing sent, nothing received, for a minute. Not silent -- it has
	// not been asked anything.
	now = now.Add(time.Minute)
	l.observe(now, 10, 10, srtt)
	if l.silent(now) {
		t.Fatal("an idle connection that sent nothing was judged silent")
	}
	// A flow opens: packets leave and nothing comes back.
	l.observe(now, 14, 10, srtt)
	now = now.Add(silenceBound(srtt) / 2)
	l.observe(now, 18, 10, srtt)
	if l.silent(now) {
		t.Fatal("a connection unanswered for two round trips was judged dead")
	}
	now = now.Add(silenceBound(srtt))
	l.observe(now, 20, 10, srtt)
	if !l.silent(now) {
		t.Fatalf("a connection unanswered for %v was still presumed alive", silenceBound(srtt)*3/2)
	}
	if l.dead(now) {
		t.Fatal("a connection merely silent was declared dead")
	}
	now = now.Add(deadSilenceFactor * silenceBound(srtt))
	l.observe(now, 22, 10, srtt)
	if !l.dead(now) {
		t.Fatalf("a connection silent for over %d bounds was not declared dead", deadSilenceFactor)
	}
	// Anything arriving clears it.
	l.observe(now, 20, 11, srtt)
	if l.silent(now) {
		t.Fatal("a connection that answered is still judged silent")
	}
}

func TestTheSilenceBoundIsRoundTripsWithAFloor(t *testing.T) {
	if got := silenceBound(200 * time.Millisecond); got != 1*time.Second {
		t.Fatalf("bound for 200ms = %v, want the 1s floor (4 RTT = 800ms)", got)
	}
	if got := silenceBound(400 * time.Millisecond); got != 1600*time.Millisecond {
		t.Fatalf("bound for 400ms = %v, want 4 round trips", got)
	}
	if got := silenceBound(0); got != minPoolSilence {
		t.Fatalf("bound with no RTT = %v, want the floor", got)
	}
}

// The case that made the first version of this wrong on every healthy
// connection. Having received the last packet of a response, a client sends an
// acknowledgement for it -- a packet the counter counts and the peer never
// answers -- and then hears nothing until the next keepalive.
func TestAnAcknowledgementIsNotAnUnansweredSend(t *testing.T) {
	var l poolLiveness
	now := time.Now()
	const srtt = 200 * time.Millisecond
	l.observe(now, 100, 80, srtt)
	// The response's last packets arrive, and the client acknowledges them.
	l.observe(now, 100, 85, srtt)
	l.observe(now.Add(10*time.Millisecond), 101, 85, srtt)
	l.observe(now.Add(30*time.Millisecond), 102, 85, srtt)
	// Then a keepalive interval of quiet.
	now = now.Add(5 * time.Second)
	l.observe(now, 102, 85, srtt)
	if l.silent(now) {
		t.Fatal("a connection that acknowledged what it received and then went quiet was judged silent")
	}
	// A request that goes out and is not answered still is.
	l.observe(now, 106, 85, srtt)
	now = now.Add(2 * silenceBound(srtt))
	l.observe(now, 109, 85, srtt)
	if !l.silent(now) {
		t.Fatal("a request unanswered past the bound was not noticed")
	}
}
