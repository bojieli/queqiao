# Reproducing the datacenter path measurements

Everything in [PATH-CHARACTER-DC-20260826.md](PATH-CHARACTER-DC-20260826.md)
came from two binaries in this repository. Here's how to re-run it, so the
numbers can be checked on another path instead of taken on trust.

These two describe a path end to end. To find out *which segment* of a
deployment a loss belongs to — the client's access link, the long haul, or the
gateway's own transit — see [which segment is at
fault](PROFILING-A-TUNNEL.md), which measures both ends in the same minutes and
localises it.

## Why there are two tools

`pathprobe` is open-loop. It sends at a rate nothing is allowed to adjust and
counts what arrives, so it describes the path itself. **Its server does the
sending**, which means it measures the download direction. That detail led us
to a wrong conclusion once, so it's worth repeating.

`pathmeasure` is closed-loop. It measures what a stack achieves, and it reports
which constraint was binding by reading the kernel's TCP_INFO. Its client does
the sending by default; `-reverse` measures the download.

```sh
GOOS=linux GOARCH=amd64 go build -o pathprobe   ./cmd/pathprobe
GOOS=linux GOARCH=amd64 go build -o pathmeasure ./cmd/pathmeasure
```

## Characterizing a path

On the far host:

```sh
pathprobe   -mode server -listen :12599 &
pathmeasure -mode serve  -listen :12600 &
```

From the near host:

```sh
# Download loss and the capacity knee. Sweep until deliver/sent starts falling.
pathprobe -mode client -remote HOST:12599 -sweep 1,5,20,80,300,600 -duration 8 -pattern

# Upload loss. Measure both directions. We found a path that dropped 0.0% one
# way and 14% the other, minutes apart.
pathmeasure -mode udp -remote HOST:12600 -rate 50 -duration 8

# Flow completion time for request-sized payloads, cold and warm reported apart.
pathmeasure -mode fct -remote HOST:12600 -sizes 100KB,300KB,1MB -repeat 3
pathmeasure -mode fct -reverse -remote HOST:12600 -sizes 100KB,300KB -repeat 3

# What a long-lived connection keeps between bursts.
pathmeasure -mode burst -remote HOST:12600 -bursts 6 -bytes 307200 -idle 3 -cc cubic
pathmeasure -mode burst -remote HOST:12600 -bursts 6 -bytes 307200 -idle 3 -cc bbr

# Concurrent load, both workload shapes.
pathmeasure -mode load   -remote HOST:12600 -sizes 300KB -flows 16
pathmeasure -mode frames -remote HOST:12600 -flows 16 -frames 100
```

`-pattern` reports `burst_factor`. A memoryless channel has
`P(loss|prev ok) = p` and `P(ok|prev lost) = 1-p`. When those hold, backing off
won't help.

## Measure the loss in the same minutes

This path's loss rate moves between roughly zero and 17% within minutes. A
comparison quoted without a loss figure from the same window can't be lined up
against any other one, because the transport repairs erasure and its advantage
scales with how much there is. The same download comparison gave 13.5x at 14%
loss and 6.5x in the single digits, hours apart.

```sh
# Run this before and after every comparison, not once at the start of a session.
pathprobe -mode client -remote HOST:12599 -rate 20 -duration 6
```

## Comparing anything

**Use `-mode ab`. Don't hand-roll an A/B.** On this path, position in the test
sequence was worth 158ms while the policy under test was worth 2.4ms. Running
the baseline first produced a 53% win that reversed when we flipped the order.
`ab` alternates, pools the samples, prints the order effect next to the arm
effect, and says so when the order dominates.

```sh
# direct against a tunnel
pathmeasure -mode ab -reverse -remote HOST:12600 \
  -a direct -b socks5=127.0.0.1:12080 -sizes 300KB -repeat 3 -rounds 2

# this project against the TUIC-shaped reference on the same QUIC stack.
# Without this arm, a result against TCP overstates our contribution by ~7x.
pathmeasure -mode ab -reverse -remote HOST:12600 \
  -a socks5=127.0.0.1:12081 -b socks5=127.0.0.1:12080 -sizes 300KB -repeat 3 -rounds 2
```

## HTTP/2 windows

```sh
# on the far host: two servers differing only in the window they advertise
pathmeasure -mode h2serve -listen :12610                      # library default
pathmeasure -mode h2serve -listen :12612 -h2-window 65535     # the RFC default

# an ingress next to a receiver you can't change
pathmeasure -mode h2proxy -listen :12613 -remote 127.0.0.1:12612 -h2-window 8388608

# from the near host
pathmeasure -mode h2 -remote HOST:12612 -sizes 300KB,1MB -repeat 3
```

## Measuring a real inference endpoint

Synthetic transfers tell you what the path does. They don't tell you what a
request costs, because a request also contains a model. To measure the workload
itself, put both arms behind the same reverse proxy on the inference host so
that hop cancels, and drive the real API:

```sh
# Upload-dominated, the shape of a speech recognition call: a few hundred
# kilobytes of audio up, a sentence back.
pathmeasure -mode workload -rounds 20 \
  -url http://HOST:PORT/v1/audio/transcriptions \
  -a direct -b socks5=127.0.0.1:12080 \
  -post-file speech.wav -form-field file -form-value model=sensevoice

# Download-dominated, the shape of a synthesis call: a sentence up, a few
# hundred kilobytes of audio back.
pathmeasure -mode workload -rounds 20 \
  -url http://HOST:PORT/v1/audio/speech \
  -a direct -b socks5=127.0.0.1:12080 \
  -post-file request.json -content-type application/json

# The long-lived bursty session, rather than a cold one. Worth running as its
# own arm: it is what the client-side fix actually changes.
pathmeasure -mode workload -reuse ...
```

The mode alternates arms every round, pairs each round against itself, and
splits each request into connect, request-to-first-byte, and download.

Two things to get right, both of which will otherwise produce a wrong number:

**Response timing through a local proxy has the same problem, in reverse.** The
tunnel forwards into a loopback socket the client has not drained yet, so on a
warm connection the whole response can be sitting in that buffer before the
client's first read returns. We measured a 100KB synthesis response as a 3.6ms
"download" that way, against 67.9ms for the same response on a cold connection
where the tunnel was still filling. Neither figure is wrong about what the
client observed; only the second is about the path. Where the two arms differ by
three orders of magnitude, suspect the buffer rather than the transport.

**Client-side upload timing is meaningless through a local proxy.** The client
writes into a loopback socket that accepts the whole body into a buffer
immediately, so `WroteRequest` fires in under a millisecond while the bytes have
gone nowhere. We measured 0.4ms of "upload" for a 355KB file. The real send time
reappears as server time. That is why the mode reports connect,
request-to-first-byte, and download rather than a separate upload figure: there
is no honest one to report.

**Check that the model leg matches.** Request-to-first-byte on a synthesis call
is one round trip plus the model, and the model is the same model on the same
GPU in both arms. If those two numbers don't agree, something other than the
transport differs and the comparison is invalid. Ours came out 4479.3ms against
4457.7ms, which is the check passing. The mode runs this automatically and says
which way it went, and it skips the check on an upload-shaped request, where
the send is most of that leg and the arms are supposed to differ.

The corollary is that a total-latency ratio on a workload with heavy compute
understates the transport by however much compute dominates. TTS came out at
1.24x end to end and 12.2x on the bytes. Both are true; report both, and say
which one the transport is responsible for.

## Four traps that cost us real time

**A host running a TUN-mode proxy can't measure its own paths.** Capture below
the socket layer isn't escaped by binding a source address or an interface;
`curl --interface en0` still left through the tunnel. Where that tunnel
terminates at the server you're measuring, the result describes the proxy.
`-local-address` only helps when the redirect is routing-based.

Check before trusting any baseline, because the symptom is quiet: a direct arm
that reports a 1.9ms connect on a 193ms path is not connecting to what you
think. `route -n get <dest>` naming a `utun` interface, or a gateway inside
`198.18.0.0/15`, means the direct arm isn't direct. We threw out a whole
second-path control this way after the numbers came back at 1.03x, which is
what you get when both arms share an outer tunnel that was already warm.

**Completion has to be acknowledged by the peer.** Timing an upload by watching
the local socket drain measures how long bytes took to reach a proxy's buffer
on loopback. Our first tunnel comparison reported 17 Gbit/s across a 200ms
path. `pathmeasure` now waits for an application-level ack, which is the only
definition of "delivered" that survives an intermediary.

**A relative threshold isn't a fair threshold.** Counting late messages against
each arm's own floor gives the arm with the lower median more room under its
own bar, so it reports fewer late messages for that reason alone. Our first
frame comparison showed 30% versus 2% that way, and no difference at all
against fixed thresholds.

**Check what a metric is scoped to.** The aggregate metrics endpoint reports
one lane, and with connection pooling that's usually the control connection,
which is genuinely idle. Its bandwidth estimate says nothing about the lane
carrying data.

## Standing up the comparison arms

The reference proxy, for separating QUIC's win from ours:

```sh
queqiaoref -mode gencert -gencert-prefix ref            # writes cert, key, token
queqiaoref -mode server -listen :18444 -token-file ref-token \
  -tls-cert ref-cert.pem -tls-key ref-key.pem &
queqiaoref -mode client -listen 127.0.0.1:12081 -remote HOST:18444 \
  -token-file ref-token -root-ca ref-cert.pem &
```

Queqiao itself, kept away from any production deployment on the same host by
using its own state directory and port:

```sh
queqiaod provider init -state /tmp/qqstate -name Bench -endpoint HOST:18443
queqiaod provider add-user -state /tmp/qqstate -name bench
queqiaod server --state /tmp/qqstate --listen :18443 --path-profile dc-long-haul &
queqiaod provider invite -state /tmp/qqstate -user bench      # then on the client:
queqiaod enroll 'queqiao://enroll/...' --local-address if:eth0
queqiaod client --profile ~/.config/queqiao/*.json --path-profile dc-long-haul &
```

## Qualifying a second path

The `dc-long-haul` profile is experimental because everything known about it
comes from one link. A second path either confirms the constants or shows they
were that link's constants, and there is no way to tell which from here.

What a second path has to answer, in order. Each step decides whether the next
one is worth running:

1. **Is the loss rate-independent?** Sweep `pathprobe` from 1 Mbit/s to past the
   knee and look at loss against offered rate. Flat means erasure, which is
   what this transport repairs. Loss that appears only near the knee is
   congestion, and this profile is the wrong answer to it. Run both directions
   separately: on our path one direction lost nothing and the other lost 14%,
   and a single-direction reading would have described neither.
2. **Is it memoryless?** Compare `P(loss|prev ok)` against the overall rate, and
   `P(ok|prev lost)` against its complement. If they match, a code sized for the
   rate repairs it. If loss arrives in long runs, it needs a different size and
   possibly a different mechanism.
3. **Where is the knee, relative to what you'll offer?** The profile assumes
   your traffic is nowhere near capacity. If it isn't, use the access-link
   profile.
4. **What do the free client-side fixes recover?** Run `-mode workload` with and
   without `-reuse`, and with `tcp_slow_start_after_idle` at 1 and at 0. On our
   path this recovered the entire median on the clean direction and almost none
   of the erasing one. Whichever it does on yours is the thing to know before
   deploying anything.
5. **Then the transport comparison**, with the QUIC arm included, order
   alternated, and a loss reading in the same minutes.

The constants worth checking against ours are the erasure rate and its
directional asymmetry, the burst factor, the knee, and the ratio between what
step 4 recovers and what step 5 does. If the second path agrees, the profile
stops being experimental. If it disagrees, the profile needs a discovered
constant where it currently has a fixed one, and knowing which constant is the
useful outcome.
