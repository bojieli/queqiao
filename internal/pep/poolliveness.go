package pep

import (
	"sync"
	"time"
)

// poolLiveness notices a pooled connection that has stopped hearing back.
//
// A pooled QUIC connection is reused for every new flow until QUIC itself
// declares it dead, which takes the whole idle timeout -- fifteen seconds of
// silence. On a path that has just gone away, every request the browser makes
// in that window is opened on the dead connection, answered locally with SOCKS
// success, and then waits: its OPEN has not been confirmed, so no lane rescue
// can be attempted for it either, and the page hangs until the timeout fires.
//
// A live connection is never silent for long while it sends. Keepalives go out
// every few seconds and every ack-eliciting packet is answered within a round
// trip, so "packets went out and nothing at all came back for several round
// trips" is evidence about the connection rather than about one flow. It is
// measured from the first unanswered send, never from the last receive: an
// idle connection that is sending nothing has not been asked anything.
type poolLiveness struct {
	mu sync.Mutex
	// received and sent are the connection's packet counters at the last
	// observation; unansweredSince is when packets were first seen leaving
	// after the last one arrived, zero while nothing is waiting for an answer.
	received, sent  uint64
	unansweredSince time.Time
	srtt            time.Duration
}

// observe records the connection's counters as of now.
func (l *poolLiveness) observe(now time.Time, sent, received uint64, srtt time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if srtt > 0 {
		l.srtt = srtt
	}
	if received != l.received {
		l.received, l.sent = received, sent
		l.unansweredSince = time.Time{}
		return
	}
	if sent != l.sent && l.unansweredSince.IsZero() {
		l.unansweredSince = now
	}
}

// silent reports whether the connection has been sending without hearing
// anything back for longer than a live one could.
func (l *poolLiveness) silent(now time.Time) bool {
	return l.unanswered(now) > l.bound()
}

// dead reports whether that silence has lasted twice as long. By then QUIC has
// sent several probes into it, and a connection still hearing nothing is not
// worth the flows that are waiting on it.
func (l *poolLiveness) dead(now time.Time) bool {
	return l.unanswered(now) > 2*l.bound()
}

func (l *poolLiveness) unanswered(now time.Time) time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.unansweredSince.IsZero() {
		return 0
	}
	return now.Sub(l.unansweredSince)
}

func (l *poolLiveness) bound() time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()
	return silenceBound(l.srtt)
}

// silenceBound is how long a connection may go unanswered and still be
// presumed alive: four round trips, which survives an acknowledgement or two
// lost on a lossy path and a probe timeout, and never less than a second, so
// a fast path is not judged by a scheduling hiccup.
func silenceBound(srtt time.Duration) time.Duration {
	if bound := 4 * srtt; bound > minPoolSilence {
		return bound
	}
	return minPoolSilence
}

const (
	minPoolSilence = time.Second
	// poolLivenessInterval is how often a pooled connection's counters are
	// read. It bounds how late silence is noticed, not how soon it is
	// declared, so it only has to be small against silenceBound.
	poolLivenessInterval = 100 * time.Millisecond
)
