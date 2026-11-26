package congestion

import "github.com/apernet/quic-go/monotime"

// The QUIC stack's clock reads the performance counter on Windows, where
// Go's clock advances once per clock interrupt (15.6 ms unless some process
// has asked for a shorter period). The path law measures the path in that
// clock: a round trip shorter than one step measured as zero or as a whole
// step, nearly every delivery-rate sample had a zero interval and was
// dropped, and the pacing budget refilled once per step. On loopback the
// path controller then settled at three packets per step (230 kB/s) and
// stayed there. Elsewhere the call does nothing.
var _ = monotime.UseHighResolutionClock()
