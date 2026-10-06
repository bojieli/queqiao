Avoid repeatedly replacing healthy lanes while an acknowledged request waits
for application data. The stall watchdog now requires unacknowledged outgoing
work; receive-only outages remain subject to the underlying transport's
failure detection. AUTO recovery also stops racing QUIC JOINs against an
irreversible TCP handoff, which could retire the apparent QUIC winner on the
server. QUIC-only mode retains parallel recovery.
