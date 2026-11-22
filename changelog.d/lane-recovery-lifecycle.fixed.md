Close JOIN lanes that complete after their manager exits, and drain failed
QUIC pool generations so new requests can reconnect without interrupting
existing sibling streams. Explicitly identify stalled lanes during rescue
replacement and refresh that identity across repeated stall episodes.
