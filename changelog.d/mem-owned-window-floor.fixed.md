Under `--mem=owned`, a flow may send one path window unacknowledged from its
first byte, and again after an idle gap. Its retention bound used to start
at, and decay back to, the four-chunk startup allowance (128 KiB) and double
once per credit round, so every fresh or idle flow paid credit round trips to
send what its path could carry at once: an LLM agent's 132 KB prompt waited
one round trip, a 300 KB request two, and a 4 MiB upload spent most of its
life opening its own bound. The bound's floor is now the path's congestion
window (never more than the per-flow send limit); it reserves no memory,
since shared memory is taken only for bytes in hand. On an emulated
20 Mbit/s path with +150 ms of round trip a 132 KB prompt reached the server
in 0.05 s instead of 0.16 s; on an emulated 53 ms, 300 Mbit/s leg 4 MiB
uploads on fresh flows ran at 157 Mbit/s instead of 75, and a 355 KB request
beside a loaded upload took 0.13-0.19 s at the median instead of 0.32-0.36 s.
