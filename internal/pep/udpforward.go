package pep

import (
	"context"
	"net"
	"sync"
	"time"
)

// One bounded queue per active destination coalesces DNS work and preserves
// packet order without letting one slow name block another destination or replies.
const udpForwardTargets = 16
const udpForwardQueue = 8
const udpForwardLifetime = 30 * time.Second
const udpForwardPacketAge = 10 * time.Second

type udpForwardPacket struct {
	payload []byte
	queued  time.Time
}
type udpForwarder struct {
	ctx       context.Context
	conn      *net.UDPConn
	writeGate chan struct{}
	resolve   func(context.Context, string) ([]*net.UDPAddr, error)
	sent      func(int)
	mu        sync.Mutex
	targets   map[string]chan udpForwardPacket
	wg        sync.WaitGroup
}

func newUDPForwarder(ctx context.Context, conn *net.UDPConn, resolve func(context.Context, string) ([]*net.UDPAddr, error), sent func(int)) *udpForwarder {
	return &udpForwarder{ctx: ctx, conn: conn, writeGate: make(chan struct{}, 1), resolve: resolve, sent: sent, targets: make(map[string]chan udpForwardPacket)}
}

func (f *udpForwarder) enqueue(destination string, payload []byte) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.ctx.Err() != nil {
		return false
	}
	queue := f.targets[destination]
	if queue == nil {
		if len(f.targets) >= udpForwardTargets {
			return false
		}
		queue = make(chan udpForwardPacket, udpForwardQueue)
		f.targets[destination] = queue
		f.wg.Add(1)
		go f.run(destination, queue)
	}
	select {
	case queue <- udpForwardPacket{payload, time.Now()}:
		return true
	default:
		return false
	}
}

func (f *udpForwarder) run(destination string, queue chan udpForwardPacket) {
	defer f.wg.Done()
	defer func() {
		f.mu.Lock()
		if f.targets[destination] == queue {
			delete(f.targets, destination)
		}
		f.mu.Unlock()
	}()
	idle := time.NewTimer(udpForwardLifetime)
	defer idle.Stop()
	var addresses []*net.UDPAddr
	var expires time.Time
	for {
		select {
		case <-f.ctx.Done():
			return
		case <-idle.C:
			f.mu.Lock()
			if len(queue) == 0 {
				delete(f.targets, destination)
				f.mu.Unlock()
				return
			}
			f.mu.Unlock()
			idle.Reset(udpForwardLifetime)
		case packet := <-queue:
			if f.ctx.Err() != nil {
				return
			}
			if time.Since(packet.queued) > udpForwardPacketAge {
				continue
			}
			if !idle.Stop() {
				select {
				case <-idle.C:
				default:
				}
			}
			idle.Reset(udpForwardLifetime)
			if time.Now().After(expires) {
				ctx, cancel := context.WithDeadline(f.ctx, packet.queued.Add(udpForwardPacketAge))
				var err error
				addresses, err = f.resolve(ctx, destination)
				cancel()
				expires = time.Now().Add(udpForwardLifetime)
				if err != nil {
					addresses = nil
					expires = time.Now().Add(time.Second)
				}
			}
			if f.ctx.Err() != nil {
				return
			}
			if time.Since(packet.queued) > udpForwardPacketAge {
				continue
			}
			for _, address := range addresses {
				if err := f.writePacket(packet, address); err == nil {
					f.sent(len(packet.payload))
					break
				}
			}
		}
	}
}

// The association shares one socket across destination workers. Own both its
// deadline and write until cancellation cleanup finishes, so another worker
// cannot extend a blocked write or inherit an expired deadline.
func (f *udpForwarder) writePacket(packet udpForwardPacket, address *net.UDPAddr) error {
	deadline := time.Now().Add(5 * time.Second)
	if expires := packet.queued.Add(udpForwardPacketAge); expires.Before(deadline) {
		deadline = expires
	}
	ctx, cancel := context.WithDeadline(f.ctx, deadline)
	defer cancel()
	select {
	case f.writeGate <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-f.writeGate }()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := f.conn.SetWriteDeadline(deadline); err != nil {
		return err
	}
	finished := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		_ = f.conn.SetWriteDeadline(time.Now())
		close(finished)
	})
	defer func() {
		if !stop() {
			<-finished
		}
		_ = f.conn.SetWriteDeadline(time.Time{})
	}()
	_, err := f.conn.WriteToUDP(packet.payload, address)
	return err
}
