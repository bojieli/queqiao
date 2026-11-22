package pep

import (
	"sync"
	"testing"
)

func TestBulkReleaseAfterResetCannotRearmTimer(t *testing.T) {
	c := &Client{}
	e := &bulkConn{busy: true}
	c.bulkConns = []*bulkConn{e}
	c.closeBulkQUICPool("test reset")
	c.releaseBulkConn(e, false)
	c.bulkMu.Lock()
	armed := e.idleTimer != nil
	if e.idleTimer != nil {
		e.idleTimer.Stop()
	}
	c.bulkMu.Unlock()
	if armed {
		t.Fatal("detached entry rearmed idle timer")
	}
}

func TestBulkReleaseAndResetOwnTimerUnderOneLock(t *testing.T) {
	for i := 0; i < 300; i++ {
		c := &Client{}
		e := &bulkConn{busy: true}
		c.bulkConns = []*bulkConn{e}
		var wg sync.WaitGroup
		wg.Add(2)
		start := make(chan struct{})
		go func() { defer wg.Done(); <-start; c.releaseBulkConn(e, false) }()
		go func() { defer wg.Done(); <-start; c.closeBulkQUICPool("test reset") }()
		close(start)
		wg.Wait()
		c.bulkMu.Lock()
		armed := e.idleTimer != nil
		if e.idleTimer != nil {
			e.idleTimer.Stop()
		}
		c.bulkMu.Unlock()
		if armed {
			t.Fatal("reset left an idle timer behind")
		}
	}
}

func TestBulkOldIdleCallbackCannotExpireNewIdlePeriod(t *testing.T) {
	c := &Client{}
	e := &bulkConn{busy: true}
	c.bulkConns = []*bulkConn{e}
	defer c.closeBulkQUICPool("cleanup")
	c.releaseBulkConn(e, false)
	c.bulkMu.Lock()
	old := e.idleEpoch
	e.stopIdleLocked()
	e.busy = true
	c.bulkMu.Unlock()
	c.releaseBulkConn(e, false)
	c.bulkMu.Lock()
	current := e.idleEpoch
	c.bulkMu.Unlock()
	c.expireBulkConn(e, old)
	c.bulkMu.Lock()
	retained := len(c.bulkConns) == 1 && c.bulkConns[0] == e && e.idleTimer != nil
	c.bulkMu.Unlock()
	if !retained {
		t.Fatal("stale callback expired new idle period")
	}
	c.expireBulkConn(e, current)
	c.bulkMu.Lock()
	removed := len(c.bulkConns) == 0 && e.idleTimer == nil
	c.bulkMu.Unlock()
	if !removed {
		t.Fatal("current callback did not retire idle entry")
	}
}
