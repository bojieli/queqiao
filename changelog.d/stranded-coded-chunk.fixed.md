A chunk that a lane took while its data was going over the stream, but whose
bytes reached the wire as a coded datagram because the coding plan or the
flow's class changed while it waited in the lane's queue, is now re-issued
when that datagram is lost. Its attempt used to remember the answer the lane
gave when it took the chunk, so the scheduler believed it was on a stream and
never offered it to its own lane again: a single-lane flow then waited for a
second lane or its whole 45 second progress budget, which looked like a page
that loaded part of a response and then hung.
