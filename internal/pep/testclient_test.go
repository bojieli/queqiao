package pep

import (
	"io"
	"log/slog"
)

// attachTestCarriers gives a client built as a struct literal what NewClient
// always gives it: a carrier manager, the single owner of QUIC recovery. A
// client without one would reach a nil manager on its first pooled dial.
func attachTestCarriers(c *Client) *Client {
	if c.carriers != nil {
		return c
	}
	if c.cfg.Logger == nil {
		c.cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	cfg := c.cfg.CarrierRecovery
	if err := cfg.resolve(); err != nil {
		panic(err)
	}
	c.carriers = newCarrierManager(c, cfg)
	c.carriers.joins.rtt = c.carrierRTT
	return c
}
