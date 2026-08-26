`pathmeasure -mode frames -udp-frames` measures the streaming frame workload
over UDP ASSOCIATE rather than a stream, which is how voice actually
travels. On a path erasing 3.6%, carrying frames over UDP through the tunnel
loses 34 of 3200 messages against 163 sent directly, and keeps the tail
flat; the same frames over TCP lose none and arrive with a p99 three and a
half times the median, because reliability turns each gap into delay for the
frames behind it.
