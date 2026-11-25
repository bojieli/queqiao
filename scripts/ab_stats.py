#!/usr/bin/env python3
"""Statistics for the live validation gates (docs/VALIDATING-A-BUNDLE.md).

Standard library only, so the analyzer runs anywhere the campaign ran. Every
function here is deterministic given its inputs and seed; the unit tests in
test_ab_stats.py pin each one against a known answer.

The primary gate is DESIGN-PATH-AUTHORITY.md section 15.4: for each block b,
the contrast d_b = candidate_b - ratio * main_b (ratio 0.90), and the gate
passes on a path when the lower bound of the one-sided 95% interval of the
mean contrast is above zero. The design names a one-sided t-interval as the
primary interval and the bootstrap as the check; this module computes both and
`gate` requires both to clear zero, so a disagreement fails rather than being
resolved in the candidate's favour.
"""
from __future__ import annotations

import math
import random
from dataclasses import dataclass
from typing import Any, Callable, List, Optional, Sequence

__all__ = [
    "mean", "stdev", "quantile", "median", "contrasts", "t_cdf", "t_quantile",
    "normal_quantile", "t_lower_bound", "bootstrap_bound", "bootstrap_lower_bound",
    "GateResult", "gate", "lower_bounds", "blocks_for_power", "jain", "paired_secondary",
]


def _finite(xs: Sequence[float]) -> List[float]:
    return [float(x) for x in xs if x is not None and x == x and not math.isinf(x)]


def mean(xs: Sequence[float]) -> float:
    xs = _finite(xs)
    return sum(xs) / len(xs) if xs else float("nan")


def stdev(xs: Sequence[float]) -> float:
    """Sample standard deviation (n - 1 denominator)."""
    xs = _finite(xs)
    if len(xs) < 2:
        return float("nan")
    m = sum(xs) / len(xs)
    return math.sqrt(sum((x - m) ** 2 for x in xs) / (len(xs) - 1))


def quantile(xs: Sequence[float], p: float) -> float:
    """Linear interpolation between order statistics (numpy's default,
    Hyndman-Fan type 7). NaN for an empty input."""
    xs = sorted(_finite(xs))
    if not xs:
        return float("nan")
    if not 0.0 <= p <= 1.0:
        raise ValueError("p must be in [0, 1]")
    h = (len(xs) - 1) * p
    lo = math.floor(h)
    hi = min(lo + 1, len(xs) - 1)
    return xs[lo] + (h - lo) * (xs[hi] - xs[lo])


def median(xs: Sequence[float]) -> float:
    return quantile(xs, 0.5)


def contrasts(candidate: Sequence[float], main: Sequence[float], ratio: float = 0.90) -> List[float]:
    """Per-block contrasts d_b = candidate_b - ratio * main_b.

    Defined for zero outcomes in either arm, and needs no ratio of a resampled
    denominator (section 15.4). The two sequences are paired by position.
    """
    if len(candidate) != len(main):
        raise ValueError("candidate and main must be paired block for block")
    return [float(c) - ratio * float(m) for c, m in zip(candidate, main)]


# --- Student t without scipy -------------------------------------------------

def _betacf(a: float, b: float, x: float) -> float:
    """Continued fraction for the regularized incomplete beta (Lentz)."""
    tiny = 1e-300
    qab, qap, qam = a + b, a + 1.0, a - 1.0
    c, d = 1.0, 1.0 - qab * x / qap
    d = 1.0 / (d if abs(d) > tiny else tiny)
    h = d
    for m in range(1, 400):
        m2 = 2 * m
        aa = m * (b - m) * x / ((qam + m2) * (a + m2))
        d = 1.0 + aa * d
        d = 1.0 / (d if abs(d) > tiny else tiny)
        c = 1.0 + aa / c
        c = c if abs(c) > tiny else tiny
        h *= d * c
        aa = -(a + m) * (qab + m) * x / ((a + m2) * (qap + m2))
        d = 1.0 + aa * d
        d = 1.0 / (d if abs(d) > tiny else tiny)
        c = 1.0 + aa / c
        c = c if abs(c) > tiny else tiny
        delta = d * c
        h *= delta
        if abs(delta - 1.0) < 1e-15:
            break
    return h


def _betainc(a: float, b: float, x: float) -> float:
    if x <= 0.0:
        return 0.0
    if x >= 1.0:
        return 1.0
    lbeta = math.lgamma(a + b) - math.lgamma(a) - math.lgamma(b)
    front = math.exp(lbeta + a * math.log(x) + b * math.log1p(-x))
    if x < (a + 1.0) / (a + b + 2.0):
        return front * _betacf(a, b, x) / a
    return 1.0 - front * _betacf(b, a, 1.0 - x) / b


def t_cdf(t: float, df: float) -> float:
    """CDF of Student's t with df degrees of freedom."""
    if df <= 0:
        raise ValueError("df must be positive")
    x = df / (df + t * t)
    tail = 0.5 * _betainc(df / 2.0, 0.5, x)
    return 1.0 - tail if t >= 0 else tail


def normal_quantile(p: float) -> float:
    """Inverse standard normal CDF (Acklam's rational approximation, refined
    by one Halley step; absolute error below 1e-12)."""
    if not 0.0 < p < 1.0:
        raise ValueError("p must be in (0, 1)")
    a = [-3.969683028665376e+01, 2.209460984245205e+02, -2.759285104469687e+02,
         1.383577518672690e+02, -3.066479806614716e+01, 2.506628277459239e+00]
    b = [-5.447609879822406e+01, 1.615858368580409e+02, -1.556989798598866e+02,
         6.680131188771972e+01, -1.328068155288572e+01]
    c = [-7.784894002430293e-03, -3.223964580411365e-01, -2.400758277161838e+00,
         -2.549732539343734e+00, 4.374664141464968e+00, 2.938163982698783e+00]
    d = [7.784695709041462e-03, 3.224671290700398e-01, 2.445134137142996e+00,
         3.754408661907416e+00]
    lo = 0.02425
    if p < lo:
        q = math.sqrt(-2 * math.log(p))
        x = (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / \
            ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1)
    elif p > 1 - lo:
        q = math.sqrt(-2 * math.log(1 - p))
        x = -(((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / \
            ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1)
    else:
        q = p - 0.5
        r = q * q
        x = (((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5]) * q / \
            (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1)
    e = 0.5 * math.erfc(-x / math.sqrt(2)) - p
    u = e * math.sqrt(2 * math.pi) * math.exp(x * x / 2)
    return x - u / (1 + x * u / 2)


def t_quantile(p: float, df: float) -> float:
    """Inverse CDF of Student's t, by bisection on t_cdf."""
    if not 0.0 < p < 1.0:
        raise ValueError("p must be in (0, 1)")
    if p == 0.5:
        return 0.0
    if p < 0.5:
        return -t_quantile(1.0 - p, df)
    lo, hi = 0.0, 1.0
    while t_cdf(hi, df) < p:
        hi *= 2.0
        if hi > 1e12:
            return float("inf")
    for _ in range(200):
        mid = (lo + hi) / 2.0
        if t_cdf(mid, df) < p:
            lo = mid
        else:
            hi = mid
        if hi - lo < 1e-12 * max(1.0, hi):
            break
    return (lo + hi) / 2.0


def t_lower_bound(xs: Sequence[float], alpha: float = 0.05) -> float:
    """Lower bound of the one-sided (1 - alpha) t-interval of the mean."""
    xs = _finite(xs)
    n = len(xs)
    if n < 2:
        return float("nan")
    return mean(xs) - t_quantile(1.0 - alpha, n - 1) * stdev(xs) / math.sqrt(n)


# --- bootstrap ---------------------------------------------------------------

def bootstrap_bound(xs: Sequence, alpha: float = 0.05, resamples: int = 20000, seed: int = 1,
                    statistic: Optional[Callable[[Sequence], float]] = None, upper: bool = False) -> float:
    """One-sided percentile-bootstrap bound of `statistic` (the mean by
    default) over resamples of `xs` with replacement.

    `xs` may hold tuples (paired rows) when a custom statistic is given. The
    lower bound is the alpha quantile of the resampled statistic and the upper
    bound the (1 - alpha) quantile, each taken as the order statistic at
    floor(alpha * R) so the bound is never optimistic by interpolation.
    Resamples whose statistic is NaN are dropped.
    """
    rows = list(xs) if statistic is not None else _finite(xs)
    if len(rows) < 2:
        return float("nan")
    stat = statistic or (lambda s: sum(s) / len(s))
    rng = random.Random(seed)
    n = len(rows)
    vals = []
    for _ in range(resamples):
        v = stat([rows[rng.randrange(n)] for _ in range(n)])
        if v == v:
            vals.append(v)
    if not vals:
        return float("nan")
    vals.sort()
    k = int(math.floor(alpha * len(vals)))
    return vals[len(vals) - 1 - k] if upper else vals[k]


def bootstrap_lower_bound(xs: Sequence[float], alpha: float = 0.05, resamples: int = 20000, seed: int = 1) -> float:
    return bootstrap_bound(xs, alpha=alpha, resamples=resamples, seed=seed)


# --- the section 15.4 gate ---------------------------------------------------

def lower_bounds(d: Sequence[float], alpha: float = 0.05, resamples: int = 20000, seed: int = 1):
    """(mean, t lower bound, bootstrap lower bound, passed) for contrasts
    that must be positive. Passes only when both bounds are above zero."""
    tl = t_lower_bound(d, alpha)
    bl = bootstrap_lower_bound(d, alpha, resamples, seed)
    return mean(d), tl, bl, bool(tl == tl and bl == bl and tl > 0 and bl > 0)


@dataclass
class GateResult:
    n: int
    mean_contrast: float
    t_lower: float
    boot_lower: float
    ratio_of_means: float
    zero_main_fraction: float
    void: bool
    passed: bool
    reason: str


def gate(candidate: Sequence[float], main: Sequence[float], ratio: float = 0.90, alpha: float = 0.05,
         resamples: int = 20000, seed: int = 1, max_zero_main_fraction: float = 0.10,
         min_blocks: int = 2) -> GateResult:
    """The section 15.4 primary test on paired block outcomes.

    Void (and to be rerun) when more than `max_zero_main_fraction` of main's
    blocks are zero. Passes only when both the t and the bootstrap one-sided
    lower bounds of the mean contrast are above zero.
    """
    d = contrasts(candidate, main, ratio)
    n = len(d)
    zeros = sum(1 for m in main if float(m) == 0.0)
    zf = zeros / n if n else float("nan")
    sm = sum(float(m) for m in main)
    rom = sum(float(c) for c in candidate) / sm if sm > 0 else float("nan")
    if n < min_blocks:
        return GateResult(n, mean(d), float("nan"), float("nan"), rom, zf, True, False,
                          f"only {n} paired blocks")
    tl = t_lower_bound(d, alpha)
    bl = bootstrap_lower_bound(d, alpha, resamples, seed)
    if zf > max_zero_main_fraction:
        return GateResult(n, mean(d), tl, bl, rom, zf, True, False,
                          f"void: {zeros}/{n} main blocks are zero (limit {max_zero_main_fraction:.0%})")
    if tl == tl and bl == bl and tl > 0 and bl > 0:
        return GateResult(n, mean(d), tl, bl, rom, zf, False, True, "pass")
    if (tl > 0) != (bl > 0):
        why = "t and bootstrap bounds disagree"
    else:
        why = "lower bound not above zero"
    return GateResult(n, mean(d), tl, bl, rom, zf, False, False, why)


def blocks_for_power(contrast_sd: float, main_mean: float, ratio: float = 0.90, alpha: float = 0.05,
                     power: float = 0.80, true_ratio: float = 1.0, max_blocks: int = 100000) -> int:
    """Blocks needed for the one-sided t-test of mean contrast > 0 to have
    `power` when the true candidate/main ratio is `true_ratio` (section 15.4:
    power is computed for "true ratio 1", whose expected contrast is
    (true_ratio - ratio) * E[main], not for the pilot's observed contrast).

    Uses the standard approximation n >= ((t_{1-a,n-1} + t_{power,n-1}) * sd / delta)^2
    and returns the smallest n >= 2 that satisfies it. Raises OverflowError
    when no n up to max_blocks does, rather than recommending a campaign
    known to miss the requested power.
    """
    delta = (true_ratio - ratio) * main_mean
    if not (delta > 0):
        raise ValueError("the alternative must have a positive expected contrast")
    if not (contrast_sd >= 0):
        raise ValueError("contrast_sd must be non-negative")
    if contrast_sd == 0:
        return 2
    for n in range(2, max_blocks + 1):
        need = (t_quantile(1 - alpha, n - 1) + t_quantile(power, n - 1)) * contrast_sd / delta
        if math.sqrt(n) >= need:
            return n
    raise OverflowError(f"more than {max_blocks} blocks would be needed")


def jain(xs: Sequence[float]) -> float:
    """Jain's fairness index: 1 when all equal, 1/n when one takes all."""
    xs = _finite(xs)
    if not xs:
        return float("nan")
    s2 = sum(x * x for x in xs)
    if s2 == 0:
        return float("nan")
    return sum(xs) ** 2 / (len(xs) * s2)


def paired_secondary(candidate: Sequence[float], main: Sequence[float], alpha: float = 0.05,
                     resamples: int = 20000, seed: int = 1, upper: bool = False):
    """Mean per-block difference candidate - main and its one-sided bootstrap
    bound (lower bound by default; upper when a larger value is worse).
    Blocks where either side is NaN are dropped."""
    diffs = [float(c) - float(m) for c, m in zip(candidate, main)
             if c is not None and m is not None and c == c and m == m]
    return mean(diffs), bootstrap_bound(diffs, alpha=alpha, resamples=resamples, seed=seed, upper=upper), len(diffs)


# --- sequential stopping ---------------------------------------------------------

def t_upper_bound(xs: Sequence[float], alpha: float = 0.05) -> float:
    """Upper bound of the one-sided (1 - alpha) t-interval of the mean."""
    xs = _finite(xs)
    n = len(xs)
    if n < 2:
        return float("nan")
    return mean(xs) + t_quantile(1.0 - alpha, n - 1) * stdev(xs) / math.sqrt(n)


def bounds(d: Sequence[float], alpha: float, resamples: int = 20000, seed: int = 1):
    """(t lower, t upper, bootstrap lower, bootstrap upper) one-sided bounds
    of the mean of d, each at level alpha."""
    return (t_lower_bound(d, alpha), t_upper_bound(d, alpha),
            bootstrap_bound(d, alpha=alpha, resamples=resamples, seed=seed),
            bootstrap_bound(d, alpha=alpha, resamples=resamples, seed=seed, upper=True))


def pooled_quantile_bounds(cand_blocks: Sequence[Sequence[float]], main_blocks: Sequence[Sequence[float]], q: float,
                           contrast: Callable[[float, float], float], alpha: float, resamples: int = 20000,
                           seed: int = 1, combine: Optional[Callable[[Sequence[Any], float], float]] = None):
    """A tail percentile over all blocks' pooled samples: the contrast of the
    arms' pooled q-quantiles, and its one-sided (lower, upper) bounds at
    level alpha from a bootstrap that resamples blocks (both arms' samples
    of a block together), so the blocks stay the unit of analysis. Returns
    (observed, lower, upper). With q None the statistic is the pooled mean
    (a proportion over events given as 0/1 samples). With `combine`, each
    block is one opaque item (a histogram) and combine(items, q) is the
    pooled statistic."""
    n = len(cand_blocks)
    if n == 0 or n != len(main_blocks):
        return float("nan"), float("nan"), float("nan")

    def stat(idx):
        if combine is not None:
            c_, m_ = [cand_blocks[i] for i in idx], [main_blocks[i] for i in idx]
            if not any(len(x) for x in c_) or not any(len(x) for x in m_):
                return float("nan")
            return contrast(combine(c_, q), combine(m_, q))
        c = [x for i in idx for x in cand_blocks[i]]
        m = [x for i in idx for x in main_blocks[i]]
        if not c or not m:
            return float("nan")
        return contrast(quantile(c, q) if q is not None else mean(c), quantile(m, q) if q is not None else mean(m))
    observed = stat(range(n))
    rng = random.Random(seed)
    reps = resamples
    ds = sorted(d for d in (stat([rng.randrange(n) for _ in range(n)]) for _ in range(reps)) if d == d)
    if not ds:
        return observed, float("nan"), float("nan")
    k = int(math.floor(alpha * len(ds)))
    return observed, ds[k], ds[min(len(ds) - 1, len(ds) - 1 - k)]


def _phi(x: float) -> float:
    return math.exp(-0.5 * x * x) / math.sqrt(2 * math.pi)


def _Phi_upper(x: float) -> float:
    """P(Z > x) for a standard normal Z."""
    return 0.5 * math.erfc(x / math.sqrt(2))


def pocock_spending(t: float, alpha: float) -> float:
    """Lan-DeMets Pocock-type alpha-spending function: alpha x ln(1 + (e - 1) t)."""
    return alpha * math.log(1 + (math.e - 1) * min(max(t, 0.0), 1.0))


_LEVELS_CACHE: dict = {}


def spending_levels(looks: Sequence[int], alpha: float, step: float = 0.05) -> List[float]:
    """Nominal one-sided levels for a group-sequential test that looks after
    each block count in `looks` (increasing), spending `alpha` by the Pocock-
    type function over information fraction n / looks[-1].

    Under the null the sum of n block contrasts (standardised) is a random
    walk, so the statistic at successive looks is correlated; the boundary
    at each look is solved so that the probability of a first crossing there
    is that look's share of alpha (Armitage, McPherson and Rowe's recursive
    numerical integration on a grid). The nominal level of look k is the
    tail probability of its boundary. Only the pass boundary spends alpha:
    stopping to fail never inflates the false-pass rate."""
    looks = [int(n) for n in looks]
    if not looks or looks[0] < 1 or any(b <= a for a, b in zip(looks, looks[1:])):
        raise ValueError("looks must be increasing positive block counts")
    key = (tuple(looks), float(alpha), step)
    if key in _LEVELS_CACHE:
        return list(_LEVELS_CACHE[key])
    cap = looks[-1]
    spent = [pocock_spending(n / cap, alpha) for n in looks]
    inc = [spent[0]] + [b - a for a, b in zip(spent, spent[1:])]
    levels: List[float] = []
    # Look 1: the statistic is N(0, n1).
    n1 = looks[0]
    b = normal_quantile(1 - inc[0]) * math.sqrt(n1)
    levels.append(inc[0])
    lo = -10.0 * math.sqrt(cap)
    xs = [lo + i * step for i in range(int((b - lo) / step) + 1)]
    f = [_phi(x / math.sqrt(n1)) / math.sqrt(n1) for x in xs]
    prev = n1
    for k in range(1, len(looks)):
        n = looks[k]
        sd = math.sqrt(n - prev)
        # Trapezoid weights for the density held on the grid below the last boundary.
        w = [step * fx for fx in f]
        w[0] *= 0.5
        w[-1] *= 0.5

        def cross(bk: float) -> float:
            return sum(wi * _Phi_upper((bk - x) / sd) for wi, x in zip(w, xs))
        lo_b, hi_b = -5.0 * math.sqrt(n), 12.0 * math.sqrt(n)
        for _ in range(80):
            mid = 0.5 * (lo_b + hi_b)
            if cross(mid) > inc[k]:
                lo_b = mid
            else:
                hi_b = mid
        bk = 0.5 * (lo_b + hi_b)
        levels.append(_Phi_upper(bk / math.sqrt(n)))
        # The density of the walk at look k, on the paths not yet crossed.
        nxs = [lo + i * step for i in range(int((bk - lo) / step) + 1)]
        f = [sum(wi * _phi((y - x) / sd) for wi, x in zip(w, xs)) / sd for y in nxs]
        xs, prev = nxs, n
    _LEVELS_CACHE[key] = list(levels)
    return levels


SPENDING = ("pocock", "bonferroni")


@dataclass
class Sequential:
    """A group-sequential design over blocks (DESIGN-PATH-AUTHORITY.md
    section 15.4).

    Looks happen after `first`, first + 1, ..., `cap` blocks. Each look tests
    at its own nominal one-sided level, from the Lan-DeMets Pocock-type
    alpha-spending function over the looks with total one-sided `alpha`
    (`spending = "pocock"`), or alpha / looks at every look (`"bonferroni"`,
    the conservative fallback). The levels are computed once, before the
    first block, and frozen in the campaign's plan (`levels`).

    At a look a contrast passes when its lower bound at the look's level is
    above zero (t and bootstrap both), fails when its upper bound is below
    zero (either), and otherwise waits for another block; at the cap a
    contrast that has not passed fails.
    """
    first: int = 6
    cap: int = 12
    alpha: float = 0.05
    spending: str = "pocock"
    levels: Optional[List[float]] = None

    def __post_init__(self):
        if not 2 <= self.first <= self.cap:
            raise ValueError("need 2 <= first <= cap")
        if not (isinstance(self.alpha, (int, float)) and math.isfinite(self.alpha) and 0 < self.alpha < 1):
            raise ValueError(f"alpha must be in (0, 1), not {self.alpha!r}")
        if self.spending not in SPENDING:
            raise ValueError(f"spending must be one of {', '.join(SPENDING)}, not {self.spending!r}")
        looks = self.cap - self.first + 1
        if self.levels is None:
            if self.spending == "bonferroni":
                self.levels = [self.alpha / looks] * looks
            else:
                self.levels = spending_levels(range(self.first, self.cap + 1), self.alpha)
        else:
            # Frozen levels read back from a plan: they must fit this design
            # and must not spend more than alpha in all.
            lv = [float(x) for x in self.levels]
            if len(lv) != looks or not all(math.isfinite(x) and 0 < x < 1 for x in lv) or sum(lv) > self.alpha * looks:
                raise ValueError(f"levels {self.levels!r} do not fit looks {self.first}..{self.cap}")
            self.levels = lv

    @property
    def looks(self) -> int:
        return self.cap - self.first + 1

    def alpha_at(self, blocks: int) -> float:
        """The nominal one-sided level of the look after `blocks` blocks."""
        k = min(max(blocks, self.first), self.cap) - self.first
        return self.levels[k]

    def decide(self, d: Sequence[float], resamples: int = 20000, seed: int = 1) -> str:
        """'pass', 'fail' or 'continue' for one contrast after len(d) blocks.
        At the cap it is pass or fail. Both the t and the bootstrap bound
        must agree for a pass, and either one suffices for a fail on a clear
        negative bound."""
        n = len(_finite(d))
        if n < self.first:
            return "continue"
        a = self.alpha_at(n)
        tl, tu, bl, bu = bounds(d, a, resamples, seed)
        if tl > 0 and bl > 0:
            return "pass"
        if tu < 0 or bu < 0:
            return "fail"
        if n >= self.cap:
            return "fail"
        return "continue"


def overall_stop(decisions: Sequence[str]) -> bool:
    """A gate of several contrasts stops once its verdict is determined:
    any contrast decided fail, or every contrast decided pass."""
    return any(x == "fail" for x in decisions) or (bool(decisions) and all(x == "pass" for x in decisions))
