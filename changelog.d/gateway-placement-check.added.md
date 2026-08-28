`queqiaod doctor` now answers the question that decides a deployment before
it exists: whether this gateway is on the useful side of this client's path
to a destination it actually calls. Given `--destination host:port`, it
establishes connections to that destination directly and through the local
SOCKS listener, alternating the arms every round, and reports the two
distributions beside the verdict. The gateway round trip is now sampled
rather than dialled once, and is subtracted from the tunnelled
establishment to leave the gateway's own hop onward, which separates a
placement decision the operator can revisit from transit that relocating
the client will not change. Where the drift measured between arms is larger
than the difference between them, the check reports that the comparison did
not resolve anything rather than offering a conclusion, which is the
discipline `pathmeasure -mode ab` already follows and for the same reason.
This matters most for a destination published behind an anycast edge, where
the session already terminates near the caller and routing it through a
distant gateway adds the whole gateway leg for nothing while every other
check on the host still passes. The command continues to say nothing about
what the path does, because no local check can; `pathprobe` and
`pathmeasure` remain the instruments for that.
