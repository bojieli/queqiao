Under `--topology=single` with an explicit `--topology-fanout`, a bulk flow
moves to a fan-out connection only when another flow shares its own pooled
connection. A flow still draining on the previous connection after a handover
no longer makes a lone bulk flow on the replacement spend a handshake and the
fan-out slot.
