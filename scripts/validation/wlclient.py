#!/usr/bin/env python3
"""Client side of the scenario workloads, run on a campaign's client hosts
through the arm's temporary SOCKS5 client. Stdlib only.

    python3 wlclient.py llm    --socks H:P --target 127.0.0.1:PORT [options]
    python3 wlclient.py term   --socks H:P --target 127.0.0.1:PORT [options]
    python3 wlclient.py bgdown --socks H:P --target 127.0.0.1:PORT --path /obj-N [options]
    python3 wlclient.py bgup   --socks H:P --target 127.0.0.1:PORT [options]

Output, on stdout:

* rows for the campaign's trials.tsv, tab-separated:
  t_start t_end flow http bytes seconds exit (wall-clock seconds, ms);
* one line per call or session, "J<TAB>{json}", with the raw timings the
  metrics are computed from (scripts/workloads/phase_a.py). No metric is
  computed here, so the analysis can be changed and re-run on old data.

Times inside a J record are relative to the record's own `t0` (wall clock):
`c` is the client's monotonic clock in seconds, `s` the server's monotonic
milliseconds, so lateness is computed per connection without synchronised
clocks (WORKLOADS.md 3.0).
"""
from __future__ import annotations

import argparse
import json
import os
import random
import socket
import ssl
import struct
import sys
import threading
import time

LOCK = threading.Lock()


def emit(line: str) -> None:
    with LOCK:
        sys.stdout.write(line + "\n")
        sys.stdout.flush()


def row(t0: float, t1: float, flow: str, http: int, size: int, secs: float, exit_code: int) -> None:
    emit(f"{t0:.3f}\t{t1:.3f}\t{flow}\t{http:03d}\t{size}\t{secs:.6f}\t{exit_code}")


def record(obj: dict) -> None:
    emit("J\t" + json.dumps(obj, separators=(",", ":")))


# --- SOCKS5 and TLS ---------------------------------------------------------------

def socks_connect(socks: str, target: str, timeout: float) -> socket.socket:
    """A TCP connection to target through a SOCKS5 proxy (no auth, CONNECT)."""
    sh, sp = socks.rsplit(":", 1)
    th, tp = target.rsplit(":", 1)
    s = socket.create_connection((sh, int(sp)), timeout=timeout)
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    s.sendall(b"\x05\x01\x00")
    if _exact(s, 2) != b"\x05\x00":
        raise OSError("socks: no acceptable auth method")
    addr = th.encode()
    s.sendall(b"\x05\x01\x00\x03" + bytes([len(addr)]) + addr + struct.pack(">H", int(tp)))
    head = _exact(s, 4)
    if head[1] != 0:
        raise OSError(f"socks: connect failed ({head[1]})")
    n = {1: 4, 4: 16}.get(head[3])
    if n is None:
        n = _exact(s, 1)[0]
    _exact(s, n + 2)
    return s


def _exact(s, n: int) -> bytes:
    buf = b""
    while len(buf) < n:
        b = s.recv(n - len(buf))
        if not b:
            raise EOFError("connection closed")
        buf += b
    return buf


def open_conn(args, timeout: float):
    """A connection to the far side: SOCKS CONNECT, then TLS 1.3 unless
    --no-tls. The certificate is the harness's own, so it is not verified."""
    s = socks_connect(args.socks, args.target, timeout)
    if args.no_tls:
        return s
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    try:
        ctx.minimum_version = ssl.TLSVersion.TLSv1_3
    except (ValueError, AttributeError):      # an ssl library without it as a minimum
        ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    return ctx.wrap_socket(s, server_hostname="wlserver")


def stopped(args) -> bool:
    return bool(args.stop_file) and os.path.exists(args.stop_file)


# --- W5: LLM streaming calls --------------------------------------------------------

class Reader:
    def __init__(self, sock):
        self.s = sock
        self.buf = b""

    def fill(self) -> None:
        b = self.s.recv(65536)
        if not b:
            raise EOFError("connection closed")
        self.buf += b

    def line(self) -> bytes:
        while b"\r\n" not in self.buf:
            self.fill()
        ln, _, self.buf = self.buf.partition(b"\r\n")
        return ln

    def exact(self, n: int) -> bytes:
        while len(self.buf) < n:
            self.fill()
        out, self.buf = self.buf[:n], self.buf[n:]
        return out


def llm_call(sock, reader: Reader, args, upload: int, seed: int) -> dict:
    """One call on an open connection: POST the prompt, read the SSE stream.
    Returns the raw timings; raises on a failed call."""
    body = os.urandom(upload)
    q = f"think_ms={args.think_ms}&n={args.deltas}&cadence_ms={args.cadence_ms}&seed={seed}"
    head = (f"POST /llm?{q} HTTP/1.1\r\nHost: wlserver\r\nContent-Type: application/json\r\n"
            f"Content-Length: {upload}\r\nAccept: text/event-stream\r\n\r\n").encode()
    sock.settimeout(args.timeout)            # SDK idle timeout: no byte for this long fails the call
    c_first = time.monotonic()
    sock.sendall(head)
    view = memoryview(body)
    for i in range(0, upload, 16384):
        sock.sendall(view[i:i + 16384])
    status = reader.line()
    if b" 200 " not in status + b" ":
        raise OSError(f"http status {status[:40]!r}")
    while reader.line():
        pass
    deltas, first_byte, R, size = [], None, None, 0
    while True:
        n = int(reader.line(), 16)
        if n == 0:
            reader.line()
            break
        if first_byte is None:
            # TTFT is the first byte of event 1, not its completion: a split
            # first chunk must not charge its tail to TTFT.
            if not reader.buf:
                reader.fill()
            first_byte = time.monotonic()
        data = reader.exact(n)
        reader.line()
        now = time.monotonic()        # the delta is complete: its receive time r_i
        size += n
        for ev in data.split(b"\n\n"):
            if not ev.startswith(b"data: ") or ev.startswith(b"data: [DONE]"):
                continue
            j = json.loads(ev[6:].decode())
            if "R" in j:
                R = j["R"]
            deltas.append([j["s"], round(now - c_first, 6)])
    done = time.monotonic()
    return {"ttft": round(first_byte - c_first, 6) if first_byte else None, "R": R, "bytes": size,
            "done": round(done - c_first, 6), "deltas": deltas}


def llm_agent(args, agent: int, t_origin: float) -> None:
    """single: `calls` calls, each on a new connection. loop: one
    connection, `turns` turns, turn k uploading upload + k x growth, with a
    gap between turns (tool execution)."""
    loop = args.sequence in ("loop", "swarm")
    n = args.turns if loop else args.calls
    sock = reader = None
    for k in range(n):
        upload = args.upload + (k * args.growth if loop else 0)
        t0 = time.time()
        rec = {"kind": "llm", "seq": args.sequence, "agent": agent, "turn": k, "U": upload, "t0": round(t0, 3),
               "T": args.think_ms / 1000.0, "N": args.deltas, "c": args.cadence_ms / 1000.0, "fail": ""}
        if stopped(args) or time.time() > args.deadline:
            # A call the schedule did not reach is a failed call, so an arm
            # that finishes one of five is not scored like one that finishes all.
            rec.update(fail="not run: the phase ended first", t1=round(t0, 3))
            record(rec)
            row(t0, t0, f"c{agent}t{k}", 0, 0, 0.0, 1)
            continue
        cold = sock is None
        try:
            if cold:
                c0 = time.monotonic()
                sock = open_conn(args, args.timeout)
                reader = Reader(sock)
                rec["connect"] = round(time.monotonic() - c0, 6)
                rec["tls"] = sock.version() if hasattr(sock, "version") else "none"
            rec["cold"] = cold
            rec.update(llm_call(sock, reader, args, upload, seed=args.seed * 1000 + agent * 10 + k))
            ok = len(rec["deltas"]) == args.deltas
            if not ok:
                rec["fail"] = f"short stream ({len(rec['deltas'])} of {args.deltas})"
        except (OSError, EOFError, ValueError, ssl.SSLError) as e:
            rec["fail"] = f"{type(e).__name__}: {e}"[:200]
            ok = False
        t1 = time.time()
        rec["t1"] = round(t1, 3)        # the call's real wall-clock end, for background coverage
        record(rec)
        row(t0, t1, f"c{agent}t{k}", 200 if ok else 0, rec.get("bytes", 0), t1 - t0, 0 if ok else 1)
        if not ok or not loop:
            if sock is not None:
                try:
                    sock.close()
                except OSError:
                    pass
            sock = reader = None
        if loop and k + 1 < n:
            time.sleep(args.turn_gap)
    if sock is not None:
        sock.close()


def cmd_llm(args) -> None:
    time.sleep(args.delay)
    agents = args.agents if args.sequence == "swarm" else 1
    t_origin = time.time()
    threads = []
    for a in range(agents):
        def run(a=a):
            time.sleep(args.stagger * a / max(1, agents - 1) if agents > 1 else 0)
            llm_agent(args, a, t_origin)
        threads.append(threading.Thread(target=run))
    for t in threads:
        t.start()
    for t in threads:
        t.join()


# --- W6: terminal keystrokes with output bursts ----------------------------------------

def cmd_term(args) -> None:
    """Keystrokes (36-byte records, exponential gaps with a floor) on one
    connection. Unless --canary, the far side sends a --burst-bytes output
    burst at --burst-at seconds and every --burst-every after it. Phase A is
    before the first burst; C is after it."""
    time.sleep(args.delay)
    rng = random.Random(args.seed)
    t0 = time.time()
    rec = {"kind": "term", "t0": round(t0, 3), "keys": [], "bursts": [], "fail": "", "canary": args.canary}
    try:
        sock = open_conn(args, args.timeout)
        sock.settimeout(args.timeout)
    except (OSError, EOFError, ssl.SSLError) as e:
        rec["fail"] = f"{type(e).__name__}: {e}"[:200]
        record(rec)
        row(t0, time.time(), "k0", 0, 0, 0.0, 1)
        return
    c0 = time.monotonic()
    pending = {}
    done = threading.Event()
    fail = []

    def reader():
        r = Reader(sock)
        try:
            while not done.is_set():
                t, n = struct.unpack(">cH", r.exact(3))
                payload = r.exact(n)
                if t == b"E":
                    seq = struct.unpack(">I", payload[:4])[0]
                    now = time.monotonic() - c0
                    with LOCK:
                        if seq in pending:
                            sent = pending.pop(seq)
                            rec["keys"].append([round(sent, 6), round(now - sent, 6)])
        except (OSError, EOFError, ssl.SSLError) as e:
            if not done.is_set():
                fail.append(f"{type(e).__name__}: {e}"[:200])

    th = threading.Thread(target=reader, daemon=True)
    th.start()
    seq = 0
    next_burst = args.burst_at
    wlock = threading.Lock()
    while not fail and not stopped(args):
        now = time.monotonic() - c0
        if now >= args.duration:
            break
        try:
            if not args.canary and now >= next_burst:
                with wlock:
                    sock.sendall(b"B" + struct.pack(">HI", 4, args.burst_bytes))
                rec["bursts"].append(round(now, 6))
                next_burst += args.burst_every
            seq += 1
            with LOCK:
                pending[seq] = time.monotonic() - c0
            with wlock:
                sock.sendall(b"K" + struct.pack(">H", 36) + struct.pack(">I", seq) + b"k" * 32)
        except (OSError, ssl.SSLError) as e:
            # A write that fails (reset, timeout) ends the session as a
            # failure, recorded like a read-side one: the arm's, not the harness's.
            fail.append(f"{type(e).__name__}: {e}"[:200])
            break
        time.sleep(max(args.floor, rng.expovariate(1.0 / args.mean_gap)))
    time.sleep(min(2.0, args.timeout))         # let the last echoes arrive
    done.set()
    # A keystroke whose echo never came is censored at the time waited, not
    # dropped: dropping it would leave the slowest keys out of p99.
    end = time.monotonic() - c0
    with LOCK:
        for sent in pending.values():
            rec["keys"].append([round(sent, 6), round(end - sent, 6)])
        rec["unanswered"] = len(pending)
    rec["fail"] = fail[0] if fail else ""
    try:
        sock.close()
    except OSError:
        pass
    t1 = time.time()
    rec["t1"] = round(t1, 3)
    record(rec)
    row(t0, t1, "k0", 0 if rec["fail"] else 200, len(rec["keys"]), t1 - t0, 1 if rec["fail"] else 0)


# --- backgrounds ----------------------------------------------------------------------

def cmd_bgdown(args) -> None:
    """`flows` downloads, each fetching object after object (plain HTTP from
    the origin) until the window closes or the stop file appears."""
    end = time.time() + args.duration

    def flow(i):
        k = 0
        while time.time() < end and not stopped(args):
            k += 1
            t0, got, code, err = time.time(), 0, 0, 0
            try:
                s = socks_connect(args.socks, args.target, 10)
                s.settimeout(5)
                s.sendall(f"GET {args.path} HTTP/1.0\r\nHost: origin\r\n\r\n".encode())
                head = b""
                while b"\r\n\r\n" not in head:
                    b = s.recv(65536)
                    if not b:
                        raise EOFError
                    head += b
                code = int(head.split(b" ", 2)[1])
                got = len(head.split(b"\r\n\r\n", 1)[1])
                while time.time() < end and not stopped(args):
                    try:
                        b = s.recv(1 << 16)
                    except socket.timeout:
                        continue
                    if not b:
                        break
                    got += len(b)
                else:
                    err = 28
                s.close()
            except (OSError, EOFError, ValueError):
                err = 7
            t1 = time.time()
            row(t0, t1, f"b{i}o{k}", code, got, t1 - t0, err)
            if err not in (0,):
                break

    ts = [threading.Thread(target=flow, args=(i,)) for i in range(1, args.flows + 1)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()


SINK_UNKNOWN = 99   # a cut upload whose received bytes could not be learned


def sink_report(args, tag: str, tries: int = 6) -> "int | None":
    """Bytes the far side's sink received for one tagged upload, once that
    request has ended there; None if it cannot be learned."""
    for _ in range(tries):
        time.sleep(0.5)
        try:
            s = open_conn(args, 10)
            s.settimeout(10)
            s.sendall(f"GET /sink-report?tag={tag} HTTP/1.1\r\nHost: wlserver\r\nContent-Length: 0\r\n\r\n".encode())
            r = Reader(s)
            status = r.line()
            length = 0
            while True:
                h = r.line()
                if not h:
                    break
                if h.lower().startswith(b"content-length:"):
                    length = int(h.split(b":", 1)[1])
            body = r.exact(length) if length else b""
            s.close()
            if b" 200 " not in status + b" ":
                continue
            rep = json.loads(body)
            if rep.get("done"):
                return int(rep["bytes"])
        except (OSError, EOFError, ValueError, ssl.SSLError):
            continue
    return None


def cmd_bgup(args) -> None:
    """`flows` uploads to the far side's sink, each posting object after
    object until the window closes or the stop file appears."""
    end = time.time() + args.duration
    block = os.urandom(1 << 16)
    run = os.urandom(6).hex()

    def flow(i):
        k = 0
        while time.time() < end and not stopped(args):
            k += 1
            t0, sent, code, err = time.time(), 0, 0, 0
            tag = f"{run}-{i}-{k}"
            try:
                s = open_conn(args, 10)
                s.settimeout(10)
                s.sendall(f"POST /sink HTTP/1.1\r\nHost: wlserver\r\nContent-Length: {args.object_bytes}\r\n"
                          f"X-QQ-Tag: {tag}\r\n\r\n".encode())
                while sent < args.object_bytes:
                    if time.time() >= end or stopped(args):
                        err = 28
                        break
                    n = min(len(block), args.object_bytes - sent)
                    s.sendall(block[:n])
                    sent += n
                if not err:
                    r = Reader(s)
                    code = int(r.line().split(b" ", 2)[1])
                s.close()
            except (OSError, EOFError, ValueError, ssl.SSLError):
                err = 7
            t1 = time.time()
            if err == 28:
                # Cut by the window: count what the sink received, not what
                # this side handed to its socket. Unknown is recorded as such.
                got = sink_report(args, tag)
                if got is None:
                    row(t0, t1, f"u{i}o{k}", 0, 0, t1 - t0, SINK_UNKNOWN)
                else:
                    row(t0, t1, f"u{i}o{k}", 200, got, t1 - t0, 28)
            else:
                row(t0, t1, f"u{i}o{k}", code, sent, t1 - t0, err)
            if err:
                break

    ts = [threading.Thread(target=flow, args=(i,)) for i in range(1, args.flows + 1)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()


# --- W7 call: UDP through SOCKS5 UDP ASSOCIATE ---------------------------------------

def socks_udp(socks: str, timeout: float):
    """(control TCP socket, UDP socket, relay address) for a SOCKS5 UDP
    ASSOCIATE. The control connection must stay open for the association."""
    sh, sp = socks.rsplit(":", 1)
    c = socket.create_connection((sh, int(sp)), timeout=timeout)
    c.sendall(b"\x05\x01\x00")
    if _exact(c, 2) != b"\x05\x00":
        raise OSError("socks: no acceptable auth method")
    c.sendall(b"\x05\x03\x00\x01" + b"\x00" * 6)
    head = _exact(c, 4)
    if head[1] != 0:
        raise OSError(f"socks: udp associate failed ({head[1]})")
    if head[3] == 1:
        addr = socket.inet_ntoa(_exact(c, 4))
    elif head[3] == 4:
        addr = socket.inet_ntop(socket.AF_INET6, _exact(c, 16))
    else:
        addr = _exact(c, _exact(c, 1)[0]).decode()
    port = struct.unpack(">H", _exact(c, 2))[0]
    if addr in ("0.0.0.0", "::"):
        addr = sh
    u = socket.socket(socket.AF_INET6 if ":" in addr else socket.AF_INET, socket.SOCK_DGRAM)
    u.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4 << 20)
    u.bind(("", 0))
    return c, u, (addr, port)


# Packet layout (both directions): magic "QC", stream (a|v), 0, seq u32,
# frame u32, index u16, count u16, client send time (double, monotonic ms)
# at 16; the reflector writes its receive time (double) and its running
# count (u32) at 24..35; padding follows.
CALL_HEAD = struct.Struct(">2sccIIHHd")


def cmd_call(args) -> None:
    """Open loop, both directions at once (the far side reflects): audio of
    160 B every 20 ms; video at 30 frames/s of 10 KB in 1,100 B packets,
    with a 40 KB key frame every 2 s. Records, per packet sent, its stream,
    frame, send time and -- when it came back -- its up and down transit
    deltas on the two unsynchronised clocks (milliseconds)."""
    time.sleep(args.delay)
    th, tp = args.target.rsplit(":", 1)
    dest = socket.inet_aton(th) + struct.pack(">H", int(tp))
    t0 = time.time()
    rec = {"kind": "call", "t0": round(t0, 3), "fail": "", "audio": [], "video": [], "count": 0,
           "seconds": args.duration}
    try:
        ctl, u, relay = socks_udp(args.socks, 10)
    except (OSError, EOFError, ValueError) as e:
        rec["fail"] = f"{type(e).__name__}: {e}"[:200]
        rec["t1"] = round(time.time(), 3)
        record(rec)
        row(t0, time.time(), "v0", 0, 0, 0.0, 1)
        return
    u.settimeout(0.2)
    c0 = time.monotonic()
    sent = {}          # (stream, seq) -> [frame, send ms]
    back = {}          # (stream, seq) -> (server ms, client receive ms)
    done = threading.Event()
    maxcount = [0]

    def receiver():
        while not done.is_set():
            try:
                data, _ = u.recvfrom(65535)
            except socket.timeout:
                continue
            except OSError:
                return
            now = (time.monotonic() - c0) * 1000.0
            payload = data[10:]   # RSV(2) FRAG(1) ATYP(1)=1 ADDR(4) PORT(2)
            if len(payload) < 36 or payload[:2] != b"QC":
                continue
            _, stream, _, seq, frame, _, _, _ = CALL_HEAD.unpack_from(payload)
            srv, cnt = struct.unpack_from(">dI", payload, 24)
            maxcount[0] = max(maxcount[0], cnt)
            back[(stream, seq)] = (srv, now)

    th_ = threading.Thread(target=receiver, daemon=True)
    th_.start()
    seq = {b"a": 0, b"v": 0}
    nxt_audio = 0.0
    nxt_video = 0.0
    frame_no = 0
    header = b"\x00\x00\x00\x01" + dest

    def send(stream: bytes, frame: int, idx: int, count: int, size: int):
        seq[stream] += 1
        t = (time.monotonic() - c0) * 1000.0
        body = CALL_HEAD.pack(b"QC", stream, b"\x00", seq[stream], frame, idx, count, t) + b"\x00" * 12
        body += b"p" * max(0, size - len(body))
        sent[(stream, seq[stream])] = [frame, t]
        u.sendto(header + body, relay)

    try:
        while not stopped(args):
            now = time.monotonic() - c0
            if now >= args.duration:
                break
            if now >= nxt_audio:
                send(b"a", int(nxt_audio / 0.02), 0, 1, 160)
                nxt_audio += 0.020
            if now >= nxt_video:
                frame_no += 1
                size = 40_000 if frame_no % 60 == 1 else 10_000
                parts = (size + 1099) // 1100
                for i in range(parts):
                    send(b"v", frame_no, i, parts, min(1100, size - i * 1100))
                nxt_video += 1.0 / 30
            time.sleep(max(0.0, min(nxt_audio, nxt_video) - (time.monotonic() - c0)))
    except OSError as e:
        rec["fail"] = f"{type(e).__name__}: {e}"[:200]
    time.sleep(1.0)                     # the last echoes
    done.set()
    for (stream, sq), (frame, t) in sorted(sent.items()):
        b = back.get((stream, sq))
        entry = [frame, round(t, 1)] + ([round(b[0] - t, 1), round(b[1] - b[0], 1)] if b else [])
        rec["audio" if stream == b"a" else "video"].append(entry)
    rec["count"] = maxcount[0]
    rec["t1"] = round(time.time(), 3)
    for s_ in (u, ctl):
        try:
            s_.close()
        except OSError:
            pass
    record(rec)
    row(t0, time.time(), "v0", 0 if rec["fail"] else 200, len(sent), time.time() - t0, 1 if rec["fail"] else 0)


# --- W3 browsing burst -----------------------------------------------------------------

def page_objects(seed: int, n: int = 40) -> list:
    """The page's object sizes: log-normal, median 15 KB, p95 300 KB (sigma
    from the ratio), capped at 2 MB; fixed by the seed."""
    import math as _m
    rng = random.Random(seed)
    sigma = _m.log(300_000 / 15_000) / 1.6449
    return [min(2_000_000, int(15_000 * _m.exp(rng.gauss(0, sigma)))) for _ in range(n)]


def http_get(sock, reader: "Reader", n: int) -> float:
    """GET /bytes/n on an open connection; returns the first-byte time
    (monotonic) and leaves the body read."""
    sock.sendall(f"GET /bytes/{n} HTTP/1.1\r\nHost: wlserver\r\n\r\n".encode())
    if not reader.buf:
        reader.fill()
    first = time.monotonic()
    status = reader.line()
    if b" 200 " not in status + b" ":
        raise OSError(f"http status {status[:40]!r}")
    length = 0
    while True:
        h = reader.line()
        if not h:
            break
        if h.lower().startswith(b"content-length:"):
            length = int(h.split(b":", 1)[1])
    left = length
    while left > 0:
        if not reader.buf:
            reader.fill()
        k = min(left, len(reader.buf))
        reader.buf = reader.buf[k:]
        left -= k
    return first


def cmd_browse(args) -> None:
    """One page: the HTML (30 KB), then its 40 objects queued on up to 6
    new connections (HTTP/1.1, as a browser opens per origin). Records every
    object's start, first byte and end (client monotonic, from the page start)."""
    time.sleep(args.delay)
    t0 = time.time()
    sizes = page_objects(args.seed)
    rec = {"kind": "browse", "t0": round(t0, 3), "fail": "", "objects": []}
    c0 = time.monotonic()
    queue = list(enumerate(sizes, start=1))
    lock = threading.Lock()

    def fetch_on(conn_reader, idx, n):
        sock, reader = conn_reader
        a = time.monotonic()
        try:
            fb = http_get(sock, reader, n)
            rec["objects"].append([idx, n, round(a - c0, 4), round(fb - c0, 4), round(time.monotonic() - c0, 4), ""])
            return True
        except (OSError, EOFError, ValueError, ssl.SSLError) as e:
            rec["objects"].append([idx, n, round(a - c0, 4), None, round(time.monotonic() - c0, 4),
                                   f"{type(e).__name__}"])
            return False

    try:
        s = open_conn(args, args.timeout)
        s.settimeout(args.timeout)
        conn = (s, Reader(s))
        fetch_on(conn, 0, 30_000)       # the HTML
    except (OSError, EOFError, ssl.SSLError) as e:
        rec["fail"] = f"{type(e).__name__}: {e}"[:200]
        conn = None

    def worker(first_conn):
        c = first_conn
        while True:
            with lock:
                if not queue:
                    break
                idx, n = queue.pop(0)
            if c is None:
                try:
                    so = open_conn(args, args.timeout)
                    so.settimeout(args.timeout)
                    c = (so, Reader(so))
                except (OSError, EOFError, ssl.SSLError) as e:
                    rec["objects"].append([idx, n, round(time.monotonic() - c0, 4), None,
                                           round(time.monotonic() - c0, 4), type(e).__name__])
                    continue
            if not fetch_on(c, idx, n):
                c = None
        if c:
            try:
                c[0].close()
            except OSError:
                pass

    if conn is not None:
        ws = [threading.Thread(target=worker, args=(conn if i == 0 else None,)) for i in range(6)]
        for w_ in ws:
            w_.start()
        for w_ in ws:
            w_.join()
    rec["t1"] = round(time.time(), 3)
    record(rec)
    bad = sum(1 for o in rec["objects"] if o[5])
    row(t0, time.time(), "g0", 0 if rec["fail"] else 200, len(rec["objects"]), time.time() - t0,
        1 if rec["fail"] or bad else 0)


# --- idle wake ----------------------------------------------------------------------

def cmd_wake(args) -> None:
    """`conns` connections, each: a sync burst, then silence; the far side
    pushes at each of --wakes (seconds after the connection's request) and
    --follow more at 1 s spacing; the client sends one message at each of
    --sends, then two warm ones 1 s apart. Records every push's server time
    and client receive time, every send's round trip, and how the
    connection ended."""
    wakes = args.wakes
    sends = [float(x) for x in args.sends.split(",") if x]
    t0 = time.time()

    def one(i):
        rec = {"kind": "wake", "conn": i, "of": args.conns, "t0": round(time.time(), 3), "fail": "", "pushes": [], "sends": [],
               "wakes": [float(x) for x in wakes.split(",") if x], "follow": args.follow, "end": ""}
        try:
            s = open_conn(args, 30)
            s.settimeout(None)
            c0 = time.monotonic()
            s.sendall(f"W {args.sync} {wakes} {args.follow}\n".encode())
        except (OSError, EOFError, ssl.SSLError) as e:
            rec["fail"] = f"{type(e).__name__}: {e}"[:200]
            rec["t1"] = round(time.time(), 3)
            record(rec)
            return
        pending = {}
        lock = threading.Lock()
        stop = threading.Event()

        def reader():
            r = Reader(s)
            try:
                while not stop.is_set():
                    t = r.exact(1)
                    if t in (b"Y",):
                        n = struct.unpack(">H", r.exact(2))[0]
                        r.exact(n)
                    elif t == b"P":
                        idx, srv = struct.unpack(">Id", r.exact(12))
                        rec["pushes"].append([idx, round(srv, 3), round(time.monotonic() - c0, 4)])
                    elif t == b"A":
                        n = struct.unpack(">I", r.exact(4))[0]
                        with lock:
                            sent_at = pending.pop(n, None)
                        if sent_at is not None:
                            rec["sends"].append([n, round(sent_at, 4), round(time.monotonic() - c0 - sent_at, 4)])
            except EOFError:
                if not stop.is_set():
                    rec["end"] = f"EOF at {time.monotonic() - c0:.1f}s"
            except (OSError, ssl.SSLError) as e:
                if not stop.is_set():
                    rec["end"] = f"{type(e).__name__} at {time.monotonic() - c0:.1f}s"

        th_ = threading.Thread(target=reader, daemon=True)
        th_.start()
        n = 0
        try:
            for at in sends:
                for k in range(3):        # the wake, then two warm sends
                    due = at + k
                    delay = due - (time.monotonic() - c0)
                    if delay > 0:
                        if stopped(args) or (time.monotonic() - c0) > args.duration:
                            break
                        time.sleep(delay)
                    n += 1
                    with lock:
                        pending[n] = time.monotonic() - c0
                    s.sendall(b"S" + struct.pack(">I", n))
            last = max(rec["wakes"] + sends + [0]) + args.follow + 6
            while time.monotonic() - c0 < last and not stopped(args):
                time.sleep(0.5)
        except (OSError, ssl.SSLError) as e:
            rec["end"] = rec["end"] or f"{type(e).__name__} at {time.monotonic() - c0:.1f}s"
        stop.set()
        rec["unanswered_sends"] = len(pending)
        try:
            s.close()
        except OSError:
            pass
        rec["t1"] = round(time.time(), 3)
        record(rec)

    ts = [threading.Thread(target=one, args=(i,)) for i in range(args.conns)]
    for t in ts:
        t.start()
        time.sleep(0.02)
    for t in ts:
        t.join()
    row(t0, time.time(), "w0", 200, args.conns, time.time() - t0, 0)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("cmd", choices=("llm", "term", "bgdown", "bgup", "call", "browse", "wake"))
    ap.add_argument("--socks", required=True)
    ap.add_argument("--target", required=True)
    ap.add_argument("--no-tls", action="store_true")
    ap.add_argument("--stop-file", default="")
    ap.add_argument("--duration", type=float, default=120)
    ap.add_argument("--delay", type=float, default=0, help="seconds to wait first (backgrounds start before)")
    ap.add_argument("--timeout", type=float, default=30, help="no bytes for this long fails a call")
    ap.add_argument("--seed", type=int, default=1)
    # llm
    ap.add_argument("--sequence", choices=("single", "loop", "swarm"), default="single")
    ap.add_argument("--upload", type=int, default=100_000)
    ap.add_argument("--growth", type=int, default=8_000)
    ap.add_argument("--calls", type=int, default=5)
    ap.add_argument("--turns", type=int, default=5)
    ap.add_argument("--turn-gap", type=float, default=2.0)
    ap.add_argument("--agents", type=int, default=8)
    ap.add_argument("--stagger", type=float, default=2.0)
    ap.add_argument("--think-ms", type=float, default=500)
    ap.add_argument("--deltas", type=int, default=400)
    ap.add_argument("--cadence-ms", type=float, default=25)
    # term
    ap.add_argument("--canary", action="store_true", help="keystrokes only (phase A), no bursts")
    ap.add_argument("--burst-at", type=float, default=40)
    ap.add_argument("--burst-every", type=float, default=20)
    ap.add_argument("--burst-bytes", type=int, default=300_000)
    ap.add_argument("--mean-gap", type=float, default=0.150)
    ap.add_argument("--floor", type=float, default=0.030)
    # wake
    ap.add_argument("--conns", type=int, default=50)
    ap.add_argument("--sync", type=int, default=200_000)
    ap.add_argument("--wakes", default="60,300,960", help="server push times, seconds after connecting")
    ap.add_argument("--sends", default="180,600", help="client send times, seconds after connecting")
    ap.add_argument("--follow", type=int, default=5)
    # backgrounds
    ap.add_argument("--flows", type=int, default=4)
    ap.add_argument("--path", default="/")
    ap.add_argument("--object-bytes", type=int, default=8 << 20)
    args = ap.parse_args(argv)
    args.deadline = time.time() + args.delay + args.duration
    {"llm": cmd_llm, "term": cmd_term, "bgdown": cmd_bgdown, "bgup": cmd_bgup, "call": cmd_call,
     "browse": cmd_browse, "wake": cmd_wake}[args.cmd](args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
