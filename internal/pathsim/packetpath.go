package pathsim

import "sync"

// PacketPath is a path across a Bottleneck for packets its caller carries
// itself: nothing listens and nothing is dialled. It exists so that traffic
// which is not a UDP flow between two sockets -- a kernel TCP connection
// read from a TUN device -- can contend for the same link, queue and loss
// process as the relays attached to it.
type PacketPath struct {
	relay *Relay
	once  sync.Once
}

// AttachPackets returns a packet path that contends for this bottleneck. cfg
// is the path's own delay, rate and queue, as for Attach.
func (b *Bottleneck) AttachPackets(cfg Config) *PacketPath {
	cfg = cfg.withDefaults()
	return &PacketPath{relay: &Relay{
		cfg: cfg, upstream: &b.up, downstream: &b.down, done: make(chan struct{}),
	}}
}

// Up sends payload in the client-to-server direction and calls deliver with
// it when, and if, the link delivers it. The path keeps payload until then.
func (p *PacketPath) Up(payload []byte, deliver func([]byte)) {
	p.relay.forward(p.relay.upstream, nil, payload, deliver)
}

// Down is Up for the server-to-client direction.
func (p *PacketPath) Down(payload []byte, deliver func([]byte)) {
	p.relay.forward(p.relay.downstream, nil, payload, deliver)
}

// Close drops whatever the path still holds. Nothing is delivered after it
// returns.
func (p *PacketPath) Close() {
	p.once.Do(func() {
		r := p.relay
		close(r.done)
		r.queueMu.Lock()
		queues := r.queues
		r.queues = nil
		r.queueMu.Unlock()
		for _, queue := range queues {
			queue.close()
		}
	})
}
