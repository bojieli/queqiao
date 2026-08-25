# Reproducing the datacenter path measurements

Everything in `PATH-CHARACTER-DC-20260826.md` was produced with two binaries in
this repository. This is the runbook, so the numbers can be re-taken on another
path rather than trusted.

## The two instruments, and why there are two

`pathprobe` is open-loop: it sends at a rate nobody may adjust and counts what
arrives, so it describes the path. **Its server is the sender**, so it measures
the download direction. That detail produced a wrong conclusion once and is
worth stating twice.

`pathmeasure` is closed-loop: it measures what a stack achieves, and names which
constraint was binding from the kernel's own TCP_INFO. Its client is the sender
by default and `-reverse` measures the download.

```sh
GOOS=linux GOARCH=amd64 go build -o pathprobe   ./cmd/pathprobe
GOOS=linux GOARCH=amd64 go build -o pathmeasure ./cmd/pathmeasure
```

## Characterising a path

On the far host:

```sh
pathprobe   -mode server -listen :12599 &
pathmeasure -mode serve  -listen :12600 &
```

From the near host:

```sh
# Download erasure and the capacity knee. Sweep until deliver/sent falls.
pathprobe -mode client -remote HOST:12599 -sweep 1,5,20,80,300,600 -duration 8 -pattern

# Upload erasure. Both directions are required: this project has measured a
# path erasing 0.0% one way and 14% the other, minutes apart.
pathmeasure -mode udp -remote HOST:12600 -rate 50 -duration 8

# Flow completion time for request-sized payloads, cold and warm separately.
pathmeasure -mode fct -remote HOST:12600 -sizes 100KB,300KB,1MB -repeat 3
pathmeasure -mode fct -reverse -remote HOST:12600 -sizes 100KB,300KB -repeat 3

# What a long-lived connection retains between bursts.
pathmeasure -mode burst -remote HOST:12600 -bursts 6 -bytes 307200 -idle 3 -cc cubic
pathmeasure -mode burst -remote HOST:12600 -bursts 6 -bytes 307200 -idle 3 -cc bbr
```

`-pattern` reports `burst_factor`. A memoryless channel has `P(loss|prev ok) = p`
and `P(ok|prev lost) = 1-p`; when those hold, backing off will not help.

## Comparing anything

**Use `-mode ab`. Do not hand-roll an A/B.** On the characterised path, position
in the measurement sequence was worth 158ms and the policy under test was worth
2.4ms; running the baseline first produced a 53% win that reversed when the
order reversed. `ab` alternates order, pools, and prints the order effect beside
the arm effect, and says so when the order dominates.

```sh
# direct against a tunnel
pathmeasure -mode ab -reverse -remote HOST:12600 \
  -a direct -b socks5=127.0.0.1:12080 -sizes 300KB -repeat 3 -rounds 2

# this project against the TUIC-shaped reference on the same QUIC stack --
# without this arm, a result against TCP overstates the contribution sevenfold
pathmeasure -mode ab -reverse -remote HOST:12600 \
  -a socks5=127.0.0.1:12081 -b socks5=127.0.0.1:12080 -sizes 300KB -repeat 3 -rounds 2
```

## Two traps that cost real time here

**A host running a TUN-mode proxy cannot measure its own paths.** Capture below
the socket layer is not escaped by binding a source address or an interface;
`curl --interface en0` still left through the tunnel. Where the tunnel
terminates at the very server being measured, the result describes the proxy.
`-local-address` helps only when the redirect is routing-based.

**Completion must be acknowledged by the peer.** Timing an upload by watching
the local socket drain measures how long bytes took to reach a proxy's buffer on
loopback. The first run of the tunnel comparison reported 17 Gbit/s across a
200ms path. `pathmeasure` now waits for an application-level ack, which is the
only definition of delivered that survives an intermediary.

## Standing up the comparison arms

The reference proxy, for the "is this QUIC's win or ours" question:

```sh
queqiaoref -mode gencert -gencert-prefix ref            # writes ref-cert, ref-key, ref-token
queqiaoref -mode server -listen :18444 -token-file ref-token \
  -tls-cert ref-cert.pem -tls-key ref-key.pem &
queqiaoref -mode client -listen 127.0.0.1:12081 -remote HOST:18444 \
  -token-file ref-token -root-ca ref-cert.pem &
```

Queqiao itself, isolated from any production deployment on the same host by
using its own state directory and port:

```sh
queqiaod provider init -state /tmp/qqstate -name Bench -endpoint HOST:18443
queqiaod provider add-user -state /tmp/qqstate -name bench
queqiaod server --state /tmp/qqstate --listen :18443 --path-profile dc-long-haul &
queqiaod provider invite -state /tmp/qqstate -user bench      # on the client:
queqiaod enroll 'queqiao://enroll/...' --local-address if:eth0
queqiaod client --profile ~/.config/queqiao/*.json --path-profile dc-long-haul &
```
