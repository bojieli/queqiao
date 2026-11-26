package pathsim

import (
	"sync/atomic"
	"testing"
	"time"
)

// A packet path shares its bottleneck's queue: packets it sends are delayed
// by the path's delay, counted in the link's statistics, and tail-dropped
// once the shared queue is full.
func TestPacketPathContendsForTheBottleneck(t *testing.T) {
	cfg := Config{OneWayDelay: 20 * time.Millisecond, RateBytesPerSec: 100_000, QueueBytes: 10_000, Seed: 1}
	link := NewBottleneck(cfg)
	path := link.AttachPackets(cfg)
	defer path.Close()
	var delivered atomic.Int64
	arrived := make(chan time.Duration, 64)
	start := time.Now()
	// Thirty 1000-byte packets at once into a 10 kB queue: about ten fit.
	for range 30 {
		path.Down(make([]byte, 1000), func(p []byte) {
			delivered.Add(int64(len(p)))
			arrived <- time.Since(start)
		})
	}
	first := <-arrived
	if first < cfg.OneWayDelay {
		t.Fatalf("the first packet arrived after %v on a %v path", first, cfg.OneWayDelay)
	}
	time.Sleep(400 * time.Millisecond)
	_, down := link.Stats()
	if down.PacketsDropped == 0 || down.PacketsDropped >= 30 {
		t.Fatalf("%d of 30 packets dropped at a queue that holds about ten", down.PacketsDropped)
	}
	if got := uint64(delivered.Load() / 1000); got != 30-down.PacketsDropped || down.PacketsOut != got {
		t.Fatalf("delivered %d, the link counts %d out and %d dropped of 30", got, down.PacketsOut, down.PacketsDropped)
	}
	up, _ := link.Stats()
	path.Up(make([]byte, 100), func([]byte) {})
	if after, _ := link.Stats(); after.PacketsIn != up.PacketsIn+1 {
		t.Fatal("an upstream packet was not counted on the shared link")
	}
}

func TestPacketPathDeliversNothingAfterClose(t *testing.T) {
	cfg := Config{OneWayDelay: 50 * time.Millisecond, Seed: 1}
	path := NewBottleneck(cfg).AttachPackets(cfg)
	var delivered atomic.Int64
	for range 5 {
		path.Down(make([]byte, 100), func([]byte) { delivered.Add(1) })
	}
	path.Close()
	path.Close()
	time.Sleep(120 * time.Millisecond)
	if n := delivered.Load(); n != 0 {
		t.Fatalf("%d packets delivered after Close", n)
	}
}
