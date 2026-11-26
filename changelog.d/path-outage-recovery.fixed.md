Under `--congestion=path`, a path that was carrying downloads now comes back
from an outage promptly. Three things held it back. A connection whose
packets had gone unanswered could send ordinary packets only while little
was outstanding, so one cut off mid-transfer could send none, and waited for
its own backed-off probe timeout: after an 8 s outage a gateway sent nothing
of its own for another 5.5 s. It now keeps an allowance of two packets per
round trip, so the first packet the returning path delivers prompts one that
can be acknowledged. The bandwidth estimate, which stands for ten round
trips, expired during the outage and was replaced by the first trickle
acknowledged; the outage no longer ages it, nor the minimum round trip. And
rounds in which nothing at all was acknowledged no longer count as
congestion or as channel erasure for the loss bound, which also does not sit
out a probe wait after the path returns. On an emulated 20 Mbit/s, 360 ms
path with two downloads, they were moving 1.3-1.5 s after an 8 s drop, where
they had taken 5.5-8 s, and carried about 20 MB in the next ten seconds,
where they had carried 0.4-10 MB. The per-connection controllers are
unchanged.
