Fixed a data race between the flow telemetry goroutine and the congestion
controller's packet goroutine. The delay brake was recomputed each time
telemetry was collected, so that a trace taken while nothing was sending
would report a fresh figure rather than the last one, and recomputing it
read the inner sender's minimum round trip -- which the packet goroutine
writes. Every other value telemetry reports comes from an atomic for exactly
this reason. It is now read rather than recomputed, which costs a stale
figure on an idle connection and is the right trade against an
unsynchronised read of the controller's state.
