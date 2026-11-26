"""Network impairment for live campaigns ([impair], opt-in).

The path's client host gets netem on the campaign's own traffic only, so
production that shares the host never matches:

* A `clsact` qdisc on the host's interface toward the relay (or gateway)
  adds classification hooks without replacing the interface's root qdisc,
  so production traffic keeps its queueing untouched.
* Flower filters on those hooks match only IPv4 to or from the address
  clients dial (the relay's), UDP and TCP, with the remote port in the
  campaign's arm ports. Egress matches redirect to one IFB device, ingress
  to another; each IFB carries the scenario's netem for that direction.
  A policer, when the scenario has one, is an action on the same filters.
* Two sets: `b`, the blocks' arms, applied just before each trial and removed
  right after it; `w`, the instances D runs beside the blocks (their own
  ports), for D's whole session.

Safety:

* Preflight refuses when a filter could match production: a filter port
  that any host declares as production, a filter port in either end's
  ephemeral port range (a production connection could use it), a clsact or
  ingress qdisc already on the interface, a device with the campaign's IFB
  names, an interface that does not carry the route to the remote address,
  or a remote address that is not an IPv4 literal.
* The tc state (every interface's qdiscs, classes and filters, and the list
  of interfaces) is snapshotted before the first trial and compared after
  every removal that leaves no set applied, and at teardown; any difference
  is reported as a teardown leftover, loudly, and the campaign fails.
* Each applied set has a watchdog on the host that removes it after its
  deadline even if the runner or its ssh connection dies; a trial whose
  watchdog fired is void (control plane). Teardown removes every set.
* Only `tc` and `ip link` on the campaign's own IFB devices and clsact
  filters; nothing else on the host is changed. Loading the ifb module
  (with no default devices) is the one host-wide effect, and it is left
  loaded.

Scenarios are frozen here (each trial type's name, as in the workload's
plan) and recorded in plan.json. Delay and jitter are split evenly between
the two directions, so a scenario's delay is what it adds to the round trip;
loss and rate apply to each direction.
"""
from __future__ import annotations

import ipaddress
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Tuple

# Filter priorities on the clsact hooks, per set: (udp, tcp).
PREFS = {"b": (49101, 49102), "w": (49111, 49112)}
SETS = tuple(PREFS)
IFB_PREFIX = "qqimp-"


class ImpairError(Exception):
    """The impairment could not be applied or removed as planned."""


@dataclass(frozen=True)
class Direction:
    delay_ms: float
    jitter_ms: float = 0.0
    rate_kbit: int = 0
    loss: str = ""                  # a netem loss clause ("gemodel 1% 30% 100% 0%", "random 2%")

    def limit(self) -> int:
        """netem's queue in packets: what is in the delay line plus about
        100 ms of queue at the rate (1500-byte packets); 1000 without one."""
        if not self.rate_kbit:
            return 1000
        per_s = self.rate_kbit * 1000 / 8 / 1500
        return int(per_s * (self.delay_ms + 3 * self.jitter_ms) / 1000 + per_s * 0.1) + 20

    def netem(self, drop: bool = False) -> str:
        parts = [f"delay {self.delay_ms:g}ms"]
        if self.jitter_ms:
            parts.append(f"{self.jitter_ms:g}ms distribution normal")
        if drop:
            parts.append("loss random 100%")
        elif self.loss:
            parts.append(f"loss {self.loss}")
        if self.rate_kbit:
            parts.append(f"rate {self.rate_kbit}kbit")
        parts.append(f"limit {self.limit()}")
        return " ".join(parts)


@dataclass(frozen=True)
class Scenario:
    name: str
    up: Direction                   # client host to the remote
    down: Direction                 # remote to the client host
    police_kbit: int = 0            # a token-bucket policer on each direction
    police_burst: int = 0           # its bucket, bytes
    # (start, seconds) after the trial's start: 100 % drop both ways.
    outages: Tuple[Tuple[float, float], ...] = ()
    summary: str = ""


def _same(d: Direction) -> Tuple[Direction, Direction]:
    return d, d


# Frozen scenarios. Round-trip figures; each direction gets half the delay.
SCENARIOS: Dict[str, Scenario] = {s.name: s for s in (
    # 5 Mbit/s, +250 ms RTT with 20 ms jitter (about 14 ms each way), and
    # Gilbert-Elliott bursty loss: 1 % chance to enter the bad state, 30 %
    # to leave it, every packet lost in it: 1/31 = 3.2 % average loss per
    # direction, bursts of 3.3 packets on average.
    Scenario("slow-lossy", *_same(Direction(125, 14, 5000, "gemodel 1% 30% 100% 0%")),
             summary="5 Mbit/s, +250 ms RTT, 20 ms jitter, bursty loss ~3 % (Gilbert-Elliott)"),
    # 20 Mbit/s, +150 ms RTT; everything dropped for 8 s at 30 s and for 25 s
    # at 70 s (both directions), so the trial's last 25 s are recovery.
    Scenario("blackhole", *_same(Direction(75, 0, 20000)), outages=((30, 8), (70, 25)),
             summary="20 Mbit/s, +150 ms RTT, 100 % drop 8 s at 30 s and 25 s at 70 s"),
    # A 4 Mbit/s policer with a 12 KB bucket (8 packets), +150 ms RTT.
    Scenario("policed", *_same(Direction(75)), police_kbit=4000, police_burst=12000,
             summary="4 Mbit/s token-bucket policer (12 KB burst), +150 ms RTT"),
    # D's idle-wake session: 2 % random loss per direction, +200 ms RTT.
    Scenario("idle-wake-lossy", *_same(Direction(100, 0, 0, "random 2%")),
             summary="2 % random loss, +200 ms RTT"),
)}


@dataclass
class ImpairConfig:
    host: str
    device: str = "auto"            # the interface toward the remote; auto: from `ip route get`
    watchdog_slack: float = 300.0   # seconds past a set's planned end before its watchdog removes it


def parse(cp: Any) -> Optional[ImpairConfig]:
    if not cp.has_section("impair"):
        return None
    s = cp["impair"]
    unknown = set(s) - {"host", "device", "watchdog_slack"} - set(cp.defaults())
    if unknown:
        raise ValueError(f"[impair] unknown setting(s): {', '.join(sorted(unknown))}")
    if not s.get("host"):
        raise ValueError("[impair] host is required (the impaired path's client host)")
    dev = s.get("device", "auto")
    if dev != "auto" and not re.match(r"^[A-Za-z0-9_.-]{1,15}$", dev):
        raise ValueError("[impair] device must be an interface name or auto")
    return ImpairConfig(host=s["host"], device=dev, watchdog_slack=float(s.get("watchdog_slack", "300")))


def port_range(ports: Sequence[int]) -> str:
    lo, hi = min(ports), max(ports)
    if sorted(ports) != list(range(lo, hi + 1)):
        raise ImpairError(f"filter ports must be contiguous: {sorted(ports)}")
    return str(lo) if lo == hi else f"{lo}-{hi}"


def production_port_reasons(ports: Sequence[int], production: Sequence[Any]) -> List[str]:
    """A filter port any host declares as production: refused."""
    out = []
    for p in production:
        hit = sorted(set(ports) & set(p.ports))
        if hit:
            out.append(f"impairment filter port(s) {hit} are declared production on {p.host} ([production:{p.name}])")
    return out


def ephemeral_reasons(ports: Sequence[int], host: str, text: str) -> List[str]:
    """A filter port in a host's ephemeral range: a production connection's
    own port could match the filter."""
    m = re.match(r"^\s*(\d+)\s+(\d+)\s*$", text or "")
    if not m:
        return [f"{host}: could not read the ephemeral port range ({(text or '').strip()[:100]!r})"]
    lo, hi = int(m.group(1)), int(m.group(2))
    hit = [p for p in ports if lo <= p <= hi]
    return [f"{host}: impairment filter port(s) {hit} are inside its ephemeral port range {lo}-{hi}"] if hit else []


# Every interface's tc state and the interface list: what the impairment
# must leave exactly as it found it. Read-only.
TC_STATE_SH = r'''
echo "links $(ls /sys/class/net | sort | tr '\n' ' ')"
for d in $(ls /sys/class/net | sort); do
  echo "== $d"
  tc qdisc show dev "$d" 2>&1
  tc class show dev "$d" 2>&1
  tc filter show dev "$d" 2>&1
  tc filter show dev "$d" ingress 2>&1
  tc filter show dev "$d" egress 2>&1
done
'''


def tc_diff(before: str, after: str) -> List[str]:
    b, a = before.strip().splitlines(), after.strip().splitlines()
    if b == a:
        return []
    import difflib
    return [l for l in difflib.unified_diff(b, a, "before", "after", lineterm="", n=0)
            if not l.startswith(("---", "+++", "@@"))][:40]


class Impairer:
    """Applies and removes the frozen scenarios on [impair] host for one
    campaign. `campaign` provides hosts, w(), cfg and log()."""

    def __init__(self, campaign: Any, icfg: ImpairConfig):
        self.c = campaign
        self.icfg = icfg
        cfg = campaign.cfg
        self.remote = cfg.endpoint_address
        self.ports = {"b": [cfg.gateway.port_base + a.index for a in cfg.arms],
                      "w": [cfg.gateway.port_base + a.index for a in cfg.beside_arms()]}
        self.dev = icfg.device if icfg.device != "auto" else ""
        self.before = ""            # the tc state before anything was applied
        self.active: Dict[str, str] = {}   # set -> scenario applied now
        self.leftovers: List[str] = []

    # -- what it is
    def remote_host(self) -> str:
        cfg = self.c.cfg
        return cfg.relay.host if cfg.relay else cfg.gateway.host

    def describe(self) -> Dict[str, Any]:
        return {"host": self.icfg.host, "device": self.dev or self.icfg.device, "remote": self.remote,
                "ports": {s: port_range(p) for s, p in self.ports.items() if p},
                "scenarios": {n: {"up": s.up.netem(), "down": s.down.netem(), "police_kbit": s.police_kbit,
                                  "police_burst": s.police_burst, "outages": s.outages, "summary": s.summary}
                              for n, s in SCENARIOS.items()}}

    @property
    def h(self):
        return self.c.hosts[self.icfg.host]

    def ifb(self, s: str) -> Tuple[str, str]:
        return f"{IFB_PREFIX}{s}u", f"{IFB_PREFIX}{s}d"

    def run_dir(self) -> str:
        return f"{self.c.w(self.icfg.host)}/run"

    # -- configuration checks (no host)
    def config_reasons(self) -> List[str]:
        cfg = self.c.cfg
        out = []
        try:
            ip = ipaddress.ip_address(self.remote)
            if ip.version != 4:
                out.append(f"[impair] the address clients dial ({self.remote}) must be IPv4")
        except ValueError:
            out.append(f"[impair] the address clients dial ({self.remote!r}) must be an IPv4 literal")
        paths = cfg.paths()
        bad = [p for p in paths if cfg.clients[p].host != self.icfg.host]
        if bad:
            out.append(f"[impair] host {self.icfg.host} is not the client host of path(s) {', '.join(bad)}: "
                       "the impairment and the measures need the path's own clock")
        for s, ports in self.ports.items():
            if ports:
                out += production_port_reasons(ports, cfg.production)
        return out

    # -- preflight (read-only on the hosts)
    def preflight_reasons(self) -> List[str]:
        out = self.config_reasons()
        allp = [p for ps in self.ports.values() for p in ps]
        rng = "cat /proc/sys/net/ipv4/ip_local_port_range"
        for host in dict.fromkeys([self.icfg.host, self.remote_host()]):
            rc, text = self.c.hosts[host].run(rng, what="read the ephemeral port range (read-only)")
            out += ephemeral_reasons(allp, host, text if rc == 0 else "")
        rc, text = self.h.run(rf'''
[ "$(id -u)" = 0 ] || echo "REFUSE not root: tc needs root on this host"
for t in tc ip modprobe; do command -v $t >/dev/null || echo "REFUSE no $t"; done
dev=$(ip -4 route get {self.remote} 2>/dev/null | sed -n 's/.* dev \([^ ]*\).*/\1/p' | head -1)
echo "route-dev $dev"
want={self.dev or '"$dev"'}
[ -n "$want" ] && [ "$dev" = "$want" ] || echo "REFUSE the route to the remote address leaves by '$dev', not '$want'"
[ "$(tc qdisc show dev "$want" 2>/dev/null | grep -cE '^qdisc (clsact|ingress) ')" = 0 ] || echo "REFUSE $want already has a clsact or ingress qdisc"
for x in /sys/class/net/{IFB_PREFIX}*; do [ -e "$x" ] && echo "REFUSE device $(basename $x) already exists"; done
for m in ifb sch_netem cls_flower act_mirred act_police; do modinfo $m >/dev/null 2>&1 || grep -qw "^$m" /proc/modules || echo "REFUSE kernel module $m is unavailable"; done
nd=""; for f in /usr/lib/tc/normal.dist /usr/lib/*/tc/normal.dist; do [ -f "$f" ] && nd=$f; done
[ -n "$nd" ] || echo "REFUSE netem's normal distribution table is missing"
''', what="check that the impairment can be applied safely (read-only)")
        if rc != 0:
            out.append(f"{self.icfg.host}: the impairment check failed ({text.strip()[:200]})")
        m = re.search(r"^route-dev (\S*)$", text, re.M)
        if not self.dev and m and m.group(1):
            self.dev = m.group(1)
        out += [f"{self.icfg.host}: {l[7:]}" for l in text.splitlines() if l.startswith("REFUSE ")]
        if not out:
            rc, self.before = self.h.run(TC_STATE_SH, what="snapshot the tc state (read-only)")
            if rc != 0 or "== " not in self.before:
                out.append(f"{self.icfg.host}: could not snapshot the tc state")
        return out

    def state(self) -> str:
        rc, text = self.h.run(TC_STATE_SH, what="read the tc state (read-only)")
        if rc != 0:
            raise ImpairError(f"{self.icfg.host}: could not read the tc state")
        return text

    # -- scripts
    def _filters(self, s: str, scen: Scenario) -> List[str]:
        u, d = self.ifb(s)
        rng = port_range(self.ports[s])
        police = (f" action police rate {scen.police_kbit}kbit burst {scen.police_burst} conform-exceed drop/pipe"
                  if scen.police_kbit else "")
        out = []
        for proto, pref in zip(("udp", "tcp"), PREFS[s]):
            out.append(f"tc filter add dev {self.dev} egress pref {pref} protocol ip flower ip_proto {proto} "
                       f"dst_ip {self.remote} dst_port {rng}{police} action mirred egress redirect dev {u}")
            out.append(f"tc filter add dev {self.dev} ingress pref {pref} protocol ip flower ip_proto {proto} "
                       f"src_ip {self.remote} src_port {rng}{police} action mirred egress redirect dev {d}")
        return out

    def remove_script(self, s: str) -> str:
        """Idempotent; touches only this set's filters (its priorities on the
        clsact this campaign added), its IFB devices, and the clsact once it
        carries no filter at all."""
        u, d = self.ifb(s)
        R = self.run_dir()
        dels = "".join(f"tc filter del dev {self.dev} {hook} pref {p} 2>/dev/null\n"
                       for p in PREFS[s] for hook in ("egress", "ingress"))
        pat = "|".join(str(p) for p in PREFS[s])
        return f'''{dels}ip link del {u} 2>/dev/null
ip link del {d} 2>/dev/null
if [ -f {R}/impair-clsact ] && [ -z "$(tc filter show dev {self.dev} ingress)$(tc filter show dev {self.dev} egress)" ]; then
  tc qdisc del dev {self.dev} clsact 2>/dev/null; tc qdisc show dev {self.dev} | grep -q '^qdisc clsact ' || rm -f {R}/impair-clsact
fi
left="$( (tc filter show dev {self.dev} egress; tc filter show dev {self.dev} ingress) 2>/dev/null | grep -E 'pref ({pat}) ' | head -3)"
for x in {u} {d}; do [ -e /sys/class/net/$x ] && left="$left device $x"; done
if [ -z "$left" ]; then echo IMPAIR-REMOVED; else echo "IMPAIR-LEFT $left"; exit 1; fi
'''

    def apply_script(self, s: str, scen: Scenario, watchdog_after: float) -> str:
        u, d = self.ifb(s)
        R = self.run_dir()
        steps = [f"ip link add {u} type ifb", f"ip link set {u} up", f"ip link add {d} type ifb", f"ip link set {d} up",
                 f"tc qdisc add dev {u} root netem {scen.up.netem()}",
                 f"tc qdisc add dev {d} root netem {scen.down.netem()}"]
        body = "".join(f"run {x}\n" for x in steps)
        filt = "".join(f"run {x}\n" for x in self._filters(s, scen))
        return f'''{self.c.owned(self.icfg.host)}R={R}
cat > $R/impair-remove-{s}.sh <<'QQ_IMPAIR_EOF'
{self.remove_script(s)}QQ_IMPAIR_EOF
# A watchdog removes the set after its deadline, even if the runner is gone.
cat > $R/impair-wd-{s}.sh <<'QQ_IMPAIR_EOF'
sleep {int(watchdog_after)}
echo fired > {R}/impair-wd-{s}.fired
bash {R}/impair-remove-{s}.sh
QQ_IMPAIR_EOF
rm -f $R/impair-wd-{s}.fired
run() {{ "$@" || {{ echo "IMPAIR-FAIL: $*"; bash $R/impair-remove-{s}.sh; exit 1; }}; }}
[ -e /sys/class/net/{u} ] && {{ echo "IMPAIR-FAIL: {u} exists"; exit 1; }}
grep -qw '^ifb' /proc/modules || run modprobe ifb numifbs=0
setsid nohup bash $R/impair-wd-{s}.sh > $R/impair-wd-{s}.out 2>&1 < /dev/null &
echo $! > $R/impair-wd-{s}.pid
{body}if [ "$(tc qdisc show dev {self.dev} | grep -c '^qdisc clsact ')" = 0 ]; then
  run tc qdisc add dev {self.dev} clsact
  touch $R/impair-clsact
fi
{filt}echo IMPAIR-APPLIED
'''

    def outage_script(self, scenario: str) -> str:
        """The blocks' set's outages, timed from the job's start: netem's loss
        to 100 % both ways and back, each change's time printed (this host's
        clock, as the probes'). A trap restores the base on any exit."""
        scen = SCENARIOS[scenario]
        u, d = self.ifb("b")
        base = f"tc qdisc change dev {u} root netem {scen.up.netem()} && tc qdisc change dev {d} root netem {scen.down.netem()}"
        drop = (f"tc qdisc change dev {u} root netem {scen.up.netem(drop=True)} && "
                f"tc qdisc change dev {d} root netem {scen.down.netem(drop=True)}")
        import ab_campaign as ab
        loop = ""
        for at, dur in scen.outages:
            loop += (f"sleep $(awk -v t0=$t0 -v n=$(now) 'BEGIN{{x = t0 + {at} - n; if (x < 0) x = 0; printf \"%.3f\", x}}')\n"
                     f"drop || {{ echo 'impairment: the outage could not be applied'; exit 1; }}\n"
                     f"s=$(now); sleep {dur}; base || exit 1; e=$(now); ev=\"$ev[$s,$e],\"\n")
        return (ab.NOW_SH + f"base() {{ {base}; }}\ndrop() {{ {drop}; }}\ntrap 'base' EXIT\n"
                f"t0=$(now); ev=''\n{loop}"
                f"printf 'J\\t{{\"kind\":\"impair\",\"scenario\":\"{scenario}\",\"fail\":\"\",\"outages\":[%s]}}\\n' \"${{ev%,}}\"\n")

    # -- actions
    def apply(self, s: str, scenario: str, seconds: float) -> None:
        """Apply `scenario` to set `s` for about `seconds`. Raises ImpairError
        (after removing whatever it applied) when it cannot."""
        scen = SCENARIOS[scenario]
        reasons = production_port_reasons(self.ports[s], self.c.cfg.production)
        if reasons:
            raise ImpairError("; ".join(reasons))
        if not self.dev and not self.c.dry:
            raise ImpairError("the impaired interface is unknown (no preflight)")
        if not self.dev:
            self.dev = "<dev>"
        if s in self.active:
            raise ImpairError(f"set {s} is already applied ({self.active[s]})")
        self.active[s] = scenario
        rc, out = self.h.run(self.apply_script(s, scen, seconds + self.icfg.watchdog_slack),
                             what=f"impair: apply {scenario} to the {'blocks' if s == 'b' else 'beside'} ports")
        if self.c.dry:
            return
        if rc != 0 or "IMPAIR-APPLIED" not in out:
            self.active.pop(s, None)
            raise ImpairError(f"{self.icfg.host}: applying {scenario} failed: {out.strip()[-300:]}")

    def remove(self, s: str) -> bool:
        """Remove set `s`; True if its watchdog had already removed it (the
        trial ran partly unimpaired). Raises ImpairError when anything of it
        is left, or when no set is applied and the tc state differs from the
        snapshot."""
        R = self.run_dir()
        rc, out = self.h.run(f'''{self.c.owned(self.icfg.host)}R={R}
if [ -f $R/impair-wd-{s}.pid ]; then
  p=$(cat $R/impair-wd-{s}.pid)
  if ps -o args= -p "$p" 2>/dev/null | grep -q -F "$R/impair-wd-{s}.sh"; then
    kill -STOP "$p" 2>/dev/null; pkill -P "$p" -x sleep 2>/dev/null; kill -KILL "$p" 2>/dev/null
  fi
  rm -f $R/impair-wd-{s}.pid
fi
[ -f $R/impair-wd-{s}.fired ] && echo WATCHDOG-FIRED
[ -f $R/impair-remove-{s}.sh ] || {{ echo IMPAIR-REMOVED; exit 0; }}
bash $R/impair-remove-{s}.sh
''', what=f"impair: remove the {'blocks' if s == 'b' else 'beside'} set")
        self.active.pop(s, None)
        if self.c.dry:
            return False
        if rc != 0 or "IMPAIR-REMOVED" not in out:
            raise ImpairError(f"{self.icfg.host}: removing the impairment left state behind: {out.strip()[-300:]}")
        if not self.active:
            diff = tc_diff(self.before, self.state())
            if diff:
                raise ImpairError(f"{self.icfg.host}: the tc state differs from the snapshot after removal:\n  "
                                  + "\n  ".join(diff))
        return "WATCHDOG-FIRED" in out

    def teardown(self) -> List[str]:
        """Remove every set (applied or not: idempotent) and compare the tc
        state with the snapshot. Returns what is left, for the teardown's
        leftovers."""
        if not self.dev or self.c.dry and self.dev == "<dev>":
            return []
        left: List[str] = []
        for s in SETS:
            try:
                self.active[s] = self.active.get(s, "teardown")
                self.remove(s)
            except Exception as e:
                left.append(f"impairment: {e}")
        self.active.clear()
        if not left and self.before and not self.c.dry:
            try:
                diff = tc_diff(self.before, self.state())
            except Exception as e:
                diff = [f"could not read the tc state: {e}"]
            left += [f"impairment: tc state differs from the snapshot: {x}" for x in diff]
        self.leftovers = left
        return left
