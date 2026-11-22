package pep

import (
	"errors"
	"sync"
)

var errReceiveBacklog = errors.New("receive backlog exceeds memory or frame budget")

// inboundBacklog keeps a slow application from blocking a lane reader before
// it reaches reverse-direction ACKs. The ordinary event channel remains the
// fast path. Only overflow needs a pump, and it is bounded by the receive
// profile and shared memory budget. ACKs are still issued after application
// delivery: buffering here does not release the sender's retention window.
type inboundBacklog struct {
	mu      sync.Mutex
	queue   []inboundEvent
	bytes   uint64
	pumping bool
}

func (f *multipathFlow) enqueueInbound(event inboundEvent) bool {
	q := &f.inboundBacklog
	q.mu.Lock()
	defer q.mu.Unlock()
	if f.doneChanClosed() || f.ctx.Err() != nil {
		return false
	}
	if !q.pumping {
		select {
		case f.events <- event:
			return true
		default:
		}
	}
	n := len(event.frame.Payload)
	if q.bytes+uint64(n) > f.memoryLimits.maxReceiveBytes || len(q.queue) >= f.memoryLimits.maxReceiveFrames || !f.receiveMemory.TryAcquire(n) {
		select {
		case f.ackErr <- errReceiveBacklog:
		default:
		}
		return false
	}
	q.queue = append(q.queue, event)
	q.bytes += uint64(n)
	if !q.pumping {
		q.pumping = true
		go f.pumpInbound()
	}
	return true
}

func (f *multipathFlow) pumpInbound() {
	q := &f.inboundBacklog
	cleanup := func() {
		q.mu.Lock()
		defer q.mu.Unlock()
		for _, event := range q.queue {
			f.receiveMemory.Release(len(event.frame.Payload))
		}
		q.queue = nil
		q.bytes = 0
		q.pumping = false
	}
	for {
		q.mu.Lock()
		if len(q.queue) == 0 {
			// Clear pumping under the same lock as enqueue. No producer can append
			// between observing the empty queue and retiring this pump.
			q.queue = nil
			q.pumping = false
			q.mu.Unlock()
			return
		}
		event := q.queue[0]
		q.queue[0] = inboundEvent{}
		q.queue = q.queue[1:]
		q.bytes -= uint64(len(event.frame.Payload))
		// Transfer ownership out of the backlog before publishing to events;
		// reassembly may immediately reserve these same bytes on another worker.
		f.receiveMemory.Release(len(event.frame.Payload))
		q.mu.Unlock()
		select {
		case f.events <- event:
		case <-f.done:
			cleanup()
			return
		case <-f.ctx.Done():
			cleanup()
			return
		}
	}
}
