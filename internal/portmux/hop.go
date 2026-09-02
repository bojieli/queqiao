package portmux

import (
	"context"
	"log/slog"
	"time"
)

// HopMetrics is the narrow interface the HopController needs from the metrics
// registry. Using an interface avoids a direct import of the metrics package
// from portmux and keeps the dependency graph clean.
type HopMetrics interface {
	PortHop()
}

// HopConfig controls the loss-detection and cooldown behaviour.
type HopConfig struct {
	// DetectWindow is how long a zero-receive window must persist before a
	// hop is triggered. Default 10 s. The GFW blocking window is measured at
	// 10+ minutes, so any value from 5–15 s balances false-positive risk
	// (congestion) against detection latency.
	DetectWindow time.Duration
	// MinSent is the minimum number of packets that must have been sent in the
	// detect window before loss is considered actionable. A quiet connection
	// should not hop just because it sent nothing and received nothing.
	MinSent uint64
	// Cooldown is the minimum interval between successive hops. Default 15 s.
	// This prevents thrashing when all ports are blocked.
	Cooldown time.Duration
	// PollInterval is how often the controller samples the counters.
	// Default 2 s. Lower values improve detection latency at the cost of
	// slightly more CPU; values below 1 s are clamped to 1 s.
	PollInterval time.Duration

	Metrics HopMetrics
	Logger  *slog.Logger
}

func (c *HopConfig) resolved() HopConfig {
	r := *c
	if r.DetectWindow <= 0 {
		r.DetectWindow = 10 * time.Second
	}
	if r.MinSent == 0 {
		r.MinSent = 5
	}
	if r.Cooldown <= 0 {
		r.Cooldown = 15 * time.Second
	}
	if r.PollInterval <= 0 {
		r.PollInterval = 2 * time.Second
	}
	if r.PollInterval < time.Second {
		r.PollInterval = time.Second
	}
	return r
}

// HopController monitors the send/receive ratio on a ClientPortMux and
// triggers a port hop when it detects the characteristic zero-receive pattern
// caused by per-port GFW blocking.
//
// Detection logic: sample (sendCount, recvCount) every PollInterval. Maintain
// a sliding window of duration DetectWindow. If totalSent > MinSent and
// totalReceived == 0 over the window, trigger a hop.
//
// RTT guard: GFW blocking produces total loss; congestion produces partial
// loss. The zero-receive threshold implicitly distinguishes them: even a path
// with 99% congestion loss receives something. A hop under congestion is
// harmless (the new port shares the same bottleneck) but the guard keeps churn
// low on paths that are merely slow.
//
// Cooldown: successive hops are separated by at least Cooldown. If all ports
// are exhausted the controller wraps around; the number of ports (typically
// 100) makes wrap-around negligible.
type HopController struct {
	mux  *ClientPortMux
	cfg  HopConfig
	// nextIdx is the next port index to try; wraps modulo len(ports).
	nextIdx int32
}

// NewHopController creates a controller for mux using cfg.
func NewHopController(mux *ClientPortMux, cfg HopConfig) *HopController {
	cfg = cfg.resolved()
	// Start at index 1 (skip the primary, which is index 0).
	var startIdx int32
	if len(mux.ports) > 1 {
		startIdx = 1
	}
	return &HopController{
		mux:     mux,
		cfg:     cfg,
		nextIdx: startIdx,
	}
}

// Run starts the monitoring loop. It returns when ctx is cancelled, which
// happens when the associated ClientPortMux is closed.
func (h *HopController) Run(ctx context.Context) {
	cfg := h.cfg

	// ringSize is the number of samples kept in the sliding window.
	// Each sample covers PollInterval; the window is DetectWindow.
	samplesPerWindow := int(cfg.DetectWindow / cfg.PollInterval)
	if samplesPerWindow < 2 {
		samplesPerWindow = 2
	}

	type sample struct {
		sent uint64
		recv uint64
	}
	ring := make([]sample, samplesPerWindow)
	ringPos := 0

	var (
		lastSend uint64
		lastRecv uint64
		lastHop  time.Time
	)

	ticker := time.NewTicker(cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		sent := h.mux.sendCount.Load()
		recv := h.mux.recvCount.Load()

		// Record the delta since the last sample.
		ring[ringPos] = sample{
			sent: sent - lastSend,
			recv: recv - lastRecv,
		}
		lastSend = sent
		lastRecv = recv
		ringPos = (ringPos + 1) % samplesPerWindow

		// Sum the entire window.
		var windowSent, windowRecv uint64
		for _, s := range ring {
			windowSent += s.sent
			windowRecv += s.recv
		}

		// Trigger condition: we are actively sending but receiving nothing.
		if windowSent < cfg.MinSent || windowRecv > 0 {
			continue
		}

		// Respect cooldown: don't hop again too soon.
		now := time.Now()
		if !lastHop.IsZero() && now.Sub(lastHop) < cfg.Cooldown {
			continue
		}

		// Choose the next port index, wrapping around the list.
		ports := h.mux.ports
		if len(ports) <= 1 {
			continue // no alternative ports
		}
		// Advance to the next index, skipping the current one.
		idx := h.nextIdx
		if idx >= int32(len(ports)) {
			idx = 1 // wrap, skipping primary at index 0
		}
		h.nextIdx = idx + 1

		fromPort, toPort := h.mux.Hop(idx)
		lastHop = now

		// Reset the window so the new port gets a fresh evaluation.
		for i := range ring {
			ring[i] = sample{}
		}

		if cfg.Metrics != nil {
			cfg.Metrics.PortHop()
		}
		if cfg.Logger != nil {
			cfg.Logger.Info("port hop", "from_port", fromPort, "to_port", toPort)
		}
	}
}
