"""Two generic secondaries of design section 15.4, read from the endpoints'
/metrics scraped before and after each trial, and pooled over a block's
trials before any statistic is taken:

* sender-side credit delay p95: every endpoint's queqiao_credit_delay_seconds
  histogram (a chunk taken by the carrier to its covering flow ACK, minus the
  path's minimum round trip, on the sender's clock);
* the shadow false-promotion rate: of the carrier-shadow observer's resolved
  no-progress episodes that reached k_p probe timeouts -- the promotions it
  would have made -- the share that then saw progress again, so that the
  promotion would have been in vain. It is the carrier-shadow summary's
  calibration rate at n = k_p. Each episode is counted once, when it is
  resolved, in the numerator and the denominator together.

Standard library only.
"""
from __future__ import annotations

import math
import re
from typing import Dict, List, Optional, Sequence, Tuple

CREDIT_BUCKET = re.compile(r'^queqiao_credit_delay_seconds_bucket\{le="([^"]+)"\}$')
EPISODES = 'queqiao_carrier_shadow_pto_episodes_total{reached="%s"}'
RECOVERED = 'queqiao_carrier_shadow_pto_recovered_total{reached="%s"}'
SHADOW_BUCKETS = 16                      # metrics.CarrierShadowPTOBuckets
DEFAULT_KS, DEFAULT_KP = 2, 4            # pep's defaults, for a flag left at 0

Histogram = List[Tuple[float, float]]    # (upper bound s, cumulative count), in bound order
Scrape = Dict[str, float]


def _buckets(scrape: Scrape) -> Dict[float, float]:
    out = {}
    for k, v in scrape.items():
        m = CREDIT_BUCKET.match(k)
        if m:
            out[math.inf if m.group(1) == "+Inf" else float(m.group(1))] = v
    return out


def histogram_movement(pre: Scrape, post: Scrape) -> Optional[Histogram]:
    """One endpoint's credit delay histogram movement between two scrapes,
    or None unless both scrapes hold the same complete set of buckets
    (+Inf included) and the movement is a histogram: no bucket went back
    (a restart) and the cumulative counts do not decrease (a truncated or
    mixed scrape)."""
    a, b = _buckets(pre), _buckets(post)
    if not b or set(a) != set(b) or math.inf not in b:
        return None
    out: Histogram = []
    last = 0.0
    for le in sorted(b):
        d = b[le] - a[le]
        if d < 0 or d < last:
            return None
        out.append((le, d))
        last = d
    return out


def pool(hists: Sequence[Optional[Histogram]]) -> Optional[Histogram]:
    """The sum of several histograms with the same bounds; None if any is
    missing or their bounds differ (a pool of some is not the whole)."""
    if not hists or any(h is None for h in hists):
        return None
    bounds = [le for le, _ in hists[0]]
    if any([le for le, _ in h] != bounds for h in hists):
        return None
    return [(le, sum(h[i][1] for h in hists)) for i, le in enumerate(bounds)]


def credit_histogram(pairs: Sequence[Tuple[Scrape, Scrape]]) -> Optional[Histogram]:
    """A trial's credit delay histogram: the movement at every endpoint it
    used (each times the direction it sends), pooled."""
    return pool([histogram_movement(pre, post) for pre, post in pairs])


def quantile(q: float, h: Optional[Histogram]) -> float:
    """The q-quantile of a cumulative histogram, interpolated linearly inside
    its bucket (as Prometheus does); in the +Inf bucket, the last finite
    bound. NaN without samples."""
    if not h or h[-1][1] <= 0:
        return float("nan")
    rank = q * h[-1][1]
    lo_bound, lo_count = 0.0, 0.0
    for le, c in h:
        if c >= rank:
            if le == math.inf:
                return lo_bound
            if c == lo_count:
                return le
            return lo_bound + (le - lo_bound) * (rank - lo_count) / (c - lo_count)
        lo_bound, lo_count = le, c
    return lo_bound


def samples(h: Optional[Histogram]) -> List[float]:
    """The histogram as samples, each bucket's spread evenly inside it (the
    +Inf bucket's at the last finite bound), so pooled tails can be
    resampled by block (section 15.4)."""
    out: List[float] = []
    if not h:
        return out
    lo_bound, lo_count = 0.0, 0.0
    for le, c in h:
        n = int(round(c - lo_count))
        top = lo_bound if le == math.inf else le
        out += [lo_bound + (top - lo_bound) * (i + 0.5) / n for i in range(n)] if n > 0 else []
        lo_bound, lo_count = (top, c)
    return out


def effective_kp(record: dict) -> int:
    """k_p as an endpoint resolved it, from its runtime configuration record
    (0 there selects the default, which is never below k_s)."""
    ks = int(record.get("carrier_shadow_ks") or 0) or DEFAULT_KS
    kp = int(record.get("carrier_shadow_kp") or 0)
    return kp or max(DEFAULT_KP, ks)


def promotion_episodes(pre: Scrape, post: Scrape, kp: int) -> Optional[Tuple[float, float]]:
    """One endpoint's (false, resolved) promotions between two scrapes: the
    episodes resolved in between that reached k_p probe timeouts, and those
    of them that then progressed. None without the counters, or if they
    went back (a restart)."""
    label = f"{SHADOW_BUCKETS}+" if kp >= SHADOW_BUCKETS else str(kp)
    keys = (RECOVERED % label, EPISODES % label)
    if any(k not in pre or k not in post for k in keys):
        return None
    false_, resolved = (post[k] - pre[k] for k in keys)
    if false_ < 0 or resolved < false_:
        return None
    return false_, resolved


def promotions(pairs: Sequence[Tuple[Scrape, Scrape]], kps: Sequence[Optional[int]]) -> Optional[Tuple[float, float]]:
    """A trial's (false, resolved) promotions over every endpoint it used.
    `kps` is each endpoint's k_p, None when the observer was not on there:
    then the trial is unmeasured, never zero."""
    if not pairs or len(kps) != len(pairs) or any(k is None for k in kps):
        return None
    got = [promotion_episodes(pre, post, kp) for (pre, post), kp in zip(pairs, kps)]
    if any(g is None for g in got):
        return None
    return sum(g[0] for g in got), sum(g[1] for g in got)


def rate(counts: Optional[Tuple[float, float]]) -> float:
    """false / resolved; NaN when unmeasured or when nothing was promoted (a
    rate with a zero denominator is reported, not gated)."""
    if counts is None or counts[1] <= 0:
        return float("nan")
    return counts[0] / counts[1]


def pooled_promotions(counts: Sequence[Optional[Tuple[float, float]]]) -> Optional[Tuple[float, float]]:
    if not counts or any(c is None for c in counts):
        return None
    return sum(c[0] for c in counts), sum(c[1] for c in counts)


def outcomes(counts: Optional[Tuple[float, float]]) -> Optional[List[float]]:
    """The promotions as 0/1 samples (1: in vain), whose mean is the rate."""
    if counts is None:
        return None
    false_, resolved = int(round(counts[0])), int(round(counts[1]))
    return [1.0] * false_ + [0.0] * (resolved - false_)
