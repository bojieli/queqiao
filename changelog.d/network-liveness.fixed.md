Bound optimistic OPEN completion and lane handshakes, verify stale pooled
paths without closing sibling streams, and try compatible gateway address
candidates. Restore single-lane TCP recovery, preserve actual payload
activity clocks, pin reliable dispatches, and make scheduler cancellation
reliable. Keep reverse ACK handling independent of bounded application
delivery, accept historical DATA copies after FIN, isolate UDP destination
resolution, and join relay workers before socket handoff. Detect physical
uplink identity and macOS route events without watching unrelated TUN
routes.
