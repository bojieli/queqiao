Wait for an optimistic OPEN to be confirmed before starting a speculative
stall-rescue JOIN while its original lane is still healthy. A JOIN that
overtakes OPEN can no longer turn a delayed handshake into an
"unknown session" failure. The existing OPEN deadline and recovery after real
lane loss remain in force.
