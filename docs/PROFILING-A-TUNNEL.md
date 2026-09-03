# Finding out which segment is at fault

A slow deployment raises one question that no measurement in this repository
answered until now: not how bad the path is, but *whose fault it is*.

There are three candidates and they need three different responses.

| Segment | What it is | What fixes it |
| --- | --- | --- |
| `client-to-internet` | the client's own access link and first mile | the client's ISP; no transport can repair it |
| `client-to-gateway` | the long haul this transport carries | this transport: coding, pacing, lane recovery |
| `gateway-to-internet` | the gateway's own transit onward | the gateway's provider, or moving the gateway |

`pathprobe` finds an erasure floor, `pathmeasure` says what a stack achieves,
and `queqiaod doctor` checks preconditions and placement — but all three
describe a path end to end. An operator losing a seventh of everything that
crosses the path learns from them only that they are losing a seventh.

```sh
queqiaod segments \
  --profile ~/.config/queqiao/provider.json \
  --metrics http://127.0.0.1:12090/metrics \
  --ssh operator@gateway.example.net \
  --remote-metrics http://127.0.0.1:19090/metrics \
  --destination www.wikipedia.org:443
```

## How the segments are told apart

The legs overlap in exactly one place each, so the pattern of which ones are
lossy names the segment they share.

- The client reaching an unfiltered destination **near itself** crosses its own
  access link and nothing else.
- The client reaching the gateway crosses its access link *and* the long haul.
- The gateway reaching an unfiltered destination **near itself** shares neither.

So a lossy leg to the gateway with a clean local anchor is the long haul; the
same loss with a lossy local anchor is the first mile, which the gateway leg
merely inherited. The report says which, and says so in those words.

For this to hold, the legs have to be measured in the same minutes. The path
this project was built for moves between roughly zero and seventeen percent
loss within minutes, so the far side runs while the near side does rather than
after it.

## The anchors, and why they are not shared

**A probe from a filtered network to a filtered destination measures the
filter.** Read as loss, that would convict a healthy access link of the worst
fault this report can report and send the operator to their ISP. A client in
China probing a blocked host sees near-total loss on a first mile in perfect
health.

So each vantage point is judged against a destination that is local and
unfiltered *from where it is probing*. Rather than guess where each end is,
both a Chinese and a global anchor are probed from both ends:

```sh
--client-reference baidu.com:443  --client-reference www.google.com:443
--gateway-reference baidu.com:443 --gateway-reference www.google.com:443
```

Each end's link is then judged by **whichever anchor answered cleanly**, never
by the average. One clean anchor proves the link carries traffic; an anchor
that did not is evidence about the route to *it*. That makes the defaults
correct at both ends without a geolocation table, and it turns the
disagreement between the two anchors into a finding of its own:

- an address that **answers echo but refuses a handshake** is reported as
  filtering, and charged to no segment;
- a leg that returns **nothing at all** is reported as unanswered, never as
  100% loss — from the client those two are indistinguishable, and only one of
  them is a finding.

Override the anchors whenever the defaults are wrong for your ends. Any
destination works as long as it is unfiltered and genuinely near the vantage
point that probes it.

## Why `--metrics` matters more than the probes

The running client already measures the client-to-gateway segment better than
anything this command can send at it: on the real four-tuple, at the real rate,
and **separately per direction**. An echo probe is a round trip and averages
the two halves of a path this project has measured differing by fourteen
percentage points.

Run the client with `--metrics-listen 127.0.0.1:12090` and pass that URL. The
report then reads the erasure the transport is acting on, the residual the code
could not repair, and the minimum round trip the delay bound is held against.
Without it the segment falls back to a round-trip probe, and the report says so.

Ratios drawn from an idle session are not quoted as measurements. Drive some
traffic through the tunnel before profiling it.

## Why `--ssh` matters

Without a vantage point on the gateway, the gateway's own transit is not
measured at all — `doctor` infers that leg by subtracting the gateway round
trip from a tunnelled establishment, which is a derivation, not a measurement.
A clean report from the client alone has not cleared that segment; it never
looked at it, and the report says that too.

`--ssh` runs the *same code* on the gateway over your existing ssh config,
agent and keys — no credentials pass through this process. It needs a
`queqiaod` there that has this command; point `--remote-binary` at it if it is
not on the path. A gateway running an older build falls back to parsing
`ping(8)`, which loses the per-packet order and therefore the loss pattern, and
the report labels every leg measured that way.

Because stdin carries the request, ssh runs with `BatchMode=yes`: key or agent
authentication only, and a password prompt fails fast instead of hanging.

## Reading the verdict

```
VERDICT  The fault is on the client-to-gateway segment.

  FAULT  client-to-gateway
         the tunnel measures 14.2% erasure downstream and 0.3% upstream, at 197ms
         minimum round trip
         Measured by the running transport itself on the live session, which is the
         only instrument here that separates the two directions. Of the downstream
         erasure, 1.1% was left unrepaired after coding and cost a further round
         trip. The client's local anchor is clean, so this is the long haul rather
         than the first mile, and it is the one segment this transport exists to
         repair.

  OK     client-to-internet
         clean: 0.0% to baidu.com:443 at 12.4ms
```

The `burst` column separates the two loss regimes, on the same statistic
`pathprobe -pattern` reports. A burst factor near 1 is an erasure channel and
sending slower will not make it drop less; a high one is a queue or a policer,
where backing off is exactly the response.

A run takes a few seconds per leg and minutes with `--ssh` and several
destinations, so it narrates itself on stderr as it goes:

```
  profiling 155.103.252.95:12540: 4 legs from the client at about 7s each
  gateway vantage: ssh rtx-pro, 2 legs running alongside these
  [1/4] client -> 155.103.252.95:12540         9.9% loss, 216ms         (7.1s)
  [2/4] client -> baidu.com:443                clean, 28ms              (7.0s)
  [3/4] client -> www.google.com:443           echo ok, handshake refused (7.2s)
  metrics: 9.8% receive erasure, 1.5% send, over 48219 coded symbols
```

That goes to stderr, so `--json` on stdout stays machine-readable with the
narration still visible; `--quiet` silences it for a log that should hold only
the result.

`--json` emits the whole run — every leg, both ends' metrics, and the findings
— which is the form to attach to a bug report. Check what it says about your
own infrastructure before sharing it; see
[contributing network evidence](CONTRIBUTING-NETWORK-EVIDENCE.md).

## What this command is not

It samples segments; it does not drive them. Throughput, capacity knees, and
completion time under load belong to [`pathprobe` and
`pathmeasure`](MEASURING-A-DC-PATH.md), which drive a path rather than sample
it. Gateway *placement* — whether a destination is served closer to the client
than the gateway is — belongs to `queqiaod doctor --destination`, which
alternates its arms properly for that comparison.
