# Reproducing the datacenter path measurements

Everything in [PATH-CHARACTER-DC-20260826.md](PATH-CHARACTER-DC-20260826.md)
came from two binaries in this repository. Here's how to re-run it, so the
numbers can be checked on another path instead of taken on trust.

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
# ASR: multipart upload, tiny response. Upload-dominated.
POST /v1/audio/transcriptions   # 150-400KB WAV in, ~150 bytes of transcript out

# TTS: tiny request, one burst back. Download-dominated.
POST /v1/audio/speech           # ~270 bytes in, ~100KB of MP3 out
```

Break each request down with `httptrace`: `GotConn`, `WroteRequest`,
`GotFirstResponseByte`, and last byte. Then alternate the arms every round and
compare each round against itself.

Two things to get right, both of which will otherwise produce a wrong number:

**Client-side upload timing is meaningless through a local proxy.** The client
writes into a loopback socket that accepts the whole body into a buffer
immediately, so `WroteRequest` fires in under a millisecond while the bytes have
gone nowhere. We measured 0.4ms of "upload" for a 355KB file. The real send time
reappears as server time. Compare `WroteRequest`-to-first-byte as one figure and
never quote the two separately across arms.

**Check that the model leg matches.** Request-to-first-byte on a TTS call is one
round trip plus synthesis, and synthesis is the same model on the same GPU in
both arms. If those two numbers don't agree to within a few percent, something
other than the transport differs between the arms and the comparison is invalid.
Ours came out 4479.3ms against 4457.7ms, which is the check passing.

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
