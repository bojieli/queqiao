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
		t.Fatal("a connection silent for less than twice the bound was declared dead")
	}
	now = now.Add(silenceBound(srtt))
	l.observe(now, 22, 10, srtt)
	if !l.dead(now) {
		t.Fatal("a connection silent for over twice the bound was not declared dead")
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
