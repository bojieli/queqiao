Port hopping actually hops now. The client and server port muxes implemented
quic-go's OOB socket interface, so quic-go bypassed them with recvmmsg on the
raw file descriptor: the server never read hop-port sockets at all, and the
client never rewrote reply addresses nor counted receives, which also made
the loss detector hop on phantom evidence. Both muxes now use the plain
packet path they can control. The client also walks the hop pool in a
shuffled order that persists across dials — previously every dial restarted
on the primary port and could hop only once before the dial timeout, so 98
of the 100 configured ports were never tried — and the detector waits for a
full measurement window before firing instead of hopping seconds after
connect.
