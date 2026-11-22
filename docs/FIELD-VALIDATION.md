# NAT and middlebox field validation

> [!NOTE]
> **Status:** Current qualification matrix for public protocol 2
> **Last reviewed:** 2026-08-20

This is the real-network qualification plan for the public preview. It is not
required to try Queqiao on your own path: for that, follow the [deployment
guide](DEPLOYING.md) and [benchmarking guide](BENCHMARKING.md). Use this matrix
when a result should become release evidence.

## Purpose

Deterministic emulation proves mechanisms; it cannot prove how unrelated
routers, carrier NATs, firewalls, traffic shapers, and access networks treat a
new UDP/TCP protocol. This campaign therefore uses independent real access
networks and records observed behavior without claiming a NAT type that was
not measured.

Do not publish subscriber addresses, device identifiers, private DNS names,
packet payloads, or credentials. Record provider/ASN and coarse geography only
when disclosure is acceptable; otherwise assign an opaque path identifier.

Each path exercises one unified protocol. Short-lived, interactive, and bulk
are workload views, not separate Queqiao modes:

| Workload view | Required observation on each applicable path |
| --- | --- |
| Short-lived | fresh and warm HTTPS/API requests, setup/first-byte/completion distribution |
| Interactive | SSH-like exchanges and UDP media-style packets, idle and during bulk contention |
| Bulk | download and upload completion, useful goodput, overhead, and resource plateau |

If one workload cannot be run, mark it missing rather than treating the cell as
a complete transport pass.

## Minimum matrix

Complete at least six independent access paths:

| Class | Minimum | Required diversity |
| --- | ---: | --- |
| Residential fixed broadband | 2 | Different ISP and edge router |
| Mobile 4G/5G or hotspot | 2 | Different carrier/CGNAT domain |
| Managed/restrictive Wi-Fi | 1 | Office, campus, hotel, or public network |
| Additional independent path | 1 | Different provider or access technology |

Use the established egress and a temporary egress in another provider/ASN.
Exercise port 443 and a high port across the campaign. Cover macOS with
Clash/mihomo, Linux with mihomo or a direct SOCKS client, and Windows with
Clash Verge or a direct SOCKS client. Do not imply that every access-path,
egress, port, and operating-system combination was tested; record the exact
cells that ran.

## Scenarios per path

1. Run the three workload views above plus persistent SOCKS5 UDP, with a
   same-window conventional proxy baseline.
2. Ten minutes idle followed by reuse, then an idle interval long enough to
   exercise the observed UDP mapping timeout where practical.
3. Client sleep/wake or interface down/up while a TCP flow and UDP association
   exist.
4. Wi-Fi/cellular or equivalent source-address handoff where the platform and
   access plan allow it.
5. Hard UDP block, intermittent UDP loss, restoration, and post-cooldown QUIC
   preference. Use a controlled endpoint firewall only when the access network
   itself cannot supply the condition; label the distinction.
6. A small-MTU/PMTU-blackhole case and a reordered/lossy case. These may be
   injected at an endpoint but must traverse the real path.
7. Service restart and binary rollback with live client configuration retained.

Two representative paths, including one mobile/CGNAT path and one residential
or managed path, run continuously for at least 24 hours; prefer 72 hours before
a production-ready claim. Mix persistent UDP, repeated HTTPS, idle periods,
and periodic bulk traffic rather than running a single synthetic loop.

## Acceptance criteria

- No crash, panic, data corruption, duplicate TCP application bytes, leaked
  plaintext/credentials in logs, or destination-policy bypass.
- QUIC is selected where UDP works. A blocked/failed UDP path reaches TLS/TCP
  within the documented bound, and a restored path is selected by a new
  association after cooldown.
- An in-session UDP rescue retains the same destination-observed relay address
  whenever a valid resume token is used. Packets lost during detection are
  counted and not described as corruption.
- TCP flows either resume without duplicate bytes or fail explicitly within a
  bounded lifetime; they never hang indefinitely.
- At the end of each active phase, session/flow counts return to zero and file
  descriptors return to baseline. RSS must plateau under a repeated workload;
  any persistent upward trend requires investigation.
- Every failure has timestamps, client/server logs, transport counters, and a
  reproducible classification. A path cell with missing evidence is not a pass.

## Record format

Create one redacted Markdown or JSON record per cell containing:

- candidate commit, binary version, wire version, OS/architecture, integration,
  egress identifier/provider, access-path identifier/class, and port;
- start/end time and duration, commands or script version, workload counts,
  success/loss/latency summaries, and baseline/final RSS and descriptor counts;
- QUIC lanes, TCP fallbacks, lane failures/replacements, UDP reconnects/rescue
  failures, and flow timeout counters;
- which conditions were naturally observed and which were endpoint-injected;
- pass/fail against each applicable criterion and links to redacted raw evidence.

`docs/field-results/README.md` is the index. A reviewer should be able to see
which matrix cells remain without reading prose claims elsewhere.
