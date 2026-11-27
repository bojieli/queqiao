A small write -- a keystroke, an SSE delta, a request -- is recovered in
half a round trip rather than two when a shaper drops it, whether or not
the send queue was empty when it was read. Its earlier form keyed on the
queue being empty, which is least true when loss is piling chunks up.
