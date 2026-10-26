The client starts accepting SOCKS connections after at most 3 seconds of
path measurement at startup, instead of after the measurement finishes. On a
gateway that was slow to answer the measurement ran to its own 20 second
timeout while browser connections queued unanswered; it now continues in the
background and the first flows share its handshake.
