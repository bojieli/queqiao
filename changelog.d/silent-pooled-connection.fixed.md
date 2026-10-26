A pooled QUIC connection that sends and hears nothing back for four round
trips (at least one second) stops receiving new flows, and a replacement is
dialed at once; if it stays silent for four times that it is closed, so the
flows still on it fail in seconds and are retried on the replacement.
Acknowledgements, which a peer never answers, are not counted as unanswered
sends. A connection whose path had gone away -- a NAT rebinding, a blocked
port -- used to carry every new request until QUIC's 15 second idle timeout,
so a page load in that window hung for all of it.
