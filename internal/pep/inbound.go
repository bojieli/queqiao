package pep

import (
	"errors"

	"github.com/bojieli/queqiao/internal/protocol"
)

// Queue data within the same byte/frame ceilings used for reassembly, while
// the lane reader continues servicing reverse-direction ACKs. Delivery still
// acknowledges bytes only after the application accepts them.
func (f *multipathFlow) queueInbound(event inboundEvent) bool {
	f.inboundMu.Lock()
	if f.doneChanClosed() || f.ctx.Err() != nil {
		f.inboundMu.Unlock()
		return false
	}
	n := len(event.frame.Payload)
	if f.inboundCount >= f.memoryLimits.maxReceiveFrames || f.inboundBytes+uint64(n) > f.memoryLimits.maxReceiveBytes || !f.receiveMemory.TryAcquire(n) {
		f.inboundMu.Unlock()
		select {
		case f.ackErr <- errors.New("application receive window exhausted"):
		default:
		}
		return false
	}
	event.charged = n
	event.queued = true
	f.inboundBytes += uint64(n)
	f.inboundCount++
	f.inboundQueue = append(f.inboundQueue, event)
	if !f.inboundRunning {
		f.inboundRunning = true
		go f.forwardInbound()
	}
	f.inboundMu.Unlock()
	notifyActivity(f.inboundWake)
	return true
}

func (f *multipathFlow) releaseInbound(event inboundEvent) {
	if !event.queued || event.charged < 0 {
		return
	}
	f.receiveMemory.Release(event.charged)
	f.inboundMu.Lock()
	f.inboundBytes -= uint64(event.charged)
	f.inboundCount--
	f.inboundMu.Unlock()
}

func (f *multipathFlow) forwardInbound() {
	defer func() {
		f.inboundMu.Lock()
		pending := f.inboundQueue
		f.inboundQueue = nil
		f.inboundMu.Unlock()
		for _, event := range pending {
			f.releaseInbound(event)
		}
		for {
			select {
			case event := <-f.events:
				f.releaseInbound(event)
			default:
				return
			}
		}
	}()
	for {
		f.inboundMu.Lock()
		var event inboundEvent
		available := len(f.inboundQueue) > 0
		if available {
			event = f.inboundQueue[0]
			f.inboundQueue[0] = inboundEvent{}
			f.inboundQueue = f.inboundQueue[1:]
			if len(f.inboundQueue) == 0 {
				f.inboundQueue = nil
			}
		}
		f.inboundMu.Unlock()
		if !available {
			select {
			case <-f.inboundWake:
				continue
			case <-f.done:
				return
			case <-f.ctx.Done():
				return
			}
		}
		select {
		case f.events <- event:
		case <-f.done:
			f.releaseInbound(event)
			return
		case <-f.ctx.Done():
			f.releaseInbound(event)
			return
		}
	}
}

func (f *multipathFlow) acceptOpenConfirmation(frame protocol.Frame) error {
	if frame.Header.SessionID != f.sessionID || frame.Header.FlowID != f.flowID || len(frame.Payload) != 0 || !f.openConfirmationRequired.CompareAndSwap(true, false) {
		return errors.New("unexpected flow open acknowledgement")
	}
	return nil
}
