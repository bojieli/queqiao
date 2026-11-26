Under `--mem=owned`, a flow back from an idle gap may again send one path
window unacknowledged at once. Its retention bound used to decay during the
gap to the four-chunk startup allowance (128 KiB), so any request just
larger -- an LLM agent's prompt of 132 KB on its fourth turn -- waited a
whole round trip for flow credit before its last bytes left. The bound's
floor is now the path's congestion window (never more than the per-flow
send limit); it reserves no memory, since shared memory is taken only for
bytes in hand. On an emulated 20 Mbit/s path with +150 ms of round trip a
132 KB prompt reached the server in 0.05 s instead of 0.16 s.
