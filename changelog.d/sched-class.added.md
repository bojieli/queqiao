`--sched=class`, the second bundle of the path-authority migration, has each
QUIC connection's controller choose which class of work its next packet
serves, and the packet builder carry that choice out. Flow control frames
(OPEN, ACK, CLOSE and the rest) and QUIC's own window updates are CONTROL;
DATA of new and interactive flows and the datagram substrate are INTERACTIVE;
DATA of bulk flows is BULK. A weighted deficit round robin (4:2:1) serves
CONTROL first while its share is unused and gives every class at least its
weight under contention. When a flow's acknowledgement has to wait behind its
own DATA write, that flow's stream is boosted, so the bytes ahead of the
acknowledgement are served at CONTROL instead of queueing behind every other
bulk flow. It requires `--cc-api=v2`; the default stays `legacy`, which
behaves as before.
