#!/usr/bin/env python3
"""Far side of the scenario workloads (docs/WORKLOADS, W5 and W6). Stdlib only.

The runner starts it on the gateway host, bound to loopback beside the HTTP
origin, so the last hop adds well under a millisecond:

    python3 wlserver.py --http PORT --term PORT --dir WORKDIR [--no-tls]

Both ports speak TLS by default (1.3 where the host's ssl library allows it), with a throwaway certificate made by
`openssl` at start-up, so handshakes and record sizes are real.

HTTP/1.1, keep-alive (port --http):

* `POST /llm?think_ms=500&n=400&cadence_ms=25&seed=S` -- the W5 emitter. It
  reads the request body (the prompt), measures R = time from its first to
  its last byte, waits think_ms, then sends n server-sent events, one every
  cadence_ms, each written and flushed on its own as an HTTP chunk. Event i
  is `data: {"i": i, "s": <server monotonic ms>, ["R": <s>,] "p": "<pad>"}`,
  180 +/- 60 bytes in all, sizes fixed by the seed. Then `data: [DONE]`.
* `POST /sink` -- reads and discards the body (the upload background). With
  an `X-QQ-Tag` header, the bytes received for that upload are kept, also
  when the client stops partway, and `GET /sink-report?tag=T` returns
  `{"bytes": n, "done": true|false}` (done once the request has ended), so
  a cut upload counts the bytes that actually arrived.

Terminal (port --term), binary records `type(1) length(2, big-endian) payload`:

* client `K` (a keystroke, 36 bytes): echoed at once as `E` with the same
  payload;
* client `B` (payload: 4-byte byte count): the server writes that many
  bytes of output as `O` records of up to 16 KiB -- a `cat` -- on the same
  connection, so later echoes queue behind it as they would in a terminal.
"""
from __future__ import annotations

import argparse
import json
import os
import random
import socket
import socketserver
import ssl
import struct
import subprocess
import threading
import time
from urllib.parse import parse_qs, urlsplit

PAD = "x" * 400
BLOCK = os.urandom(1 << 16)
# Bytes received per tagged upload, and whether its request has ended.
SINK: dict = {}
SINK_LOCK = threading.Lock()
# Per call id (4 bytes the client writes at 36..39): (stream, seq) -> the
# reflector's receive time, for the call's per-direction report; and when
# the call was last heard from, to forget old calls.
CALLS: dict = {}
CALLS_SEEN: dict = {}
CALLS_LOCK = threading.Lock()
MUX_FRAME = 16384


def mono_ms() -> float:
    return time.monotonic() * 1000.0


def event(i: int, rng: random.Random, R: float = None) -> bytes:
    """One SSE event of 180 +/- 60 bytes carrying its emission time."""
    size = 180 + rng.randint(-60, 60)
    body = {"i": i, "s": round(mono_ms(), 3)}
    if R is not None:
        body["R"] = round(R, 6)
    head = "data: " + json.dumps(body, separators=(",", ":"))[:-1] + ',"p":"'
    tail = '"}\n\n'
    pad = max(0, size - len(head) - len(tail))
    return (head + PAD[:pad] + tail).encode()


def chunk(b: bytes) -> bytes:
    return b"%x\r\n" % len(b) + b + b"\r\n"


def read_headers(f) -> tuple:
    line = f.readline(65536)
    if not line:
        return None, None, None
    method, target, _ = line.decode("latin-1").split(" ", 2)
    headers = {}
    while True:
        h = f.readline(65536)
        if h in (b"\r\n", b"\n", b""):
            break
        k, _, v = h.decode("latin-1").partition(":")
        headers[k.strip().lower()] = v.strip()
    return method, target, headers


def handshake(handler) -> None:
    """TLS in the connection's own thread, bounded: a slow handshake must not
    stall the accept loop and every connection behind it."""
    ctx = handler.server.context
    if ctx:
        handler.request.settimeout(15)
        handler.request = ctx.wrap_socket(handler.request, server_side=True)
        handler.request.settimeout(None)


class HTTP(socketserver.StreamRequestHandler):
    def setup(self):
        handshake(self)
        super().setup()

    def handle(self):
        self.request.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        while True:
            method, target, headers = read_headers(self.rfile)
            if method is None:
                return
            want = int(headers.get("content-length") or 0)
            got = 0
            first = last = None
            url = urlsplit(target)
            tag = headers.get("x-qq-tag", "") if url.path == "/sink" else ""
            if tag:
                with SINK_LOCK:
                    SINK[tag] = {"bytes": 0, "done": False}
            try:
                while got < want:
                    b = self.rfile.read1(min(65536, want - got)) if hasattr(self.rfile, "read1") else self.rfile.read(min(65536, want - got))
                    if not b:
                        return
                    now = time.monotonic()
                    first = first if first is not None else now
                    last = now
                    got += len(b)
                    if tag:
                        with SINK_LOCK:
                            SINK[tag]["bytes"] = got
            except (OSError, ssl.SSLError):
                return
            finally:
                if tag:
                    with SINK_LOCK:
                        SINK[tag]["done"] = True
            if method == "GET" and url.path == "/call-report":
                # The call's uplink as the reflector saw it: per stream, every
                # (seq, receive ms) it got, so the client can tell a packet
                # lost up from one lost down.
                q = {k: v[0] for k, v in parse_qs(url.query).items()}
                try:
                    cid = bytes.fromhex(q.get("id", ""))
                except ValueError:
                    cid = b""
                with CALLS_LOCK:
                    got = dict(CALLS.get(cid, {}))
                rep_ = {"a": sorted([s, round(t, 1)] for (st_, s), t in got.items() if st_ == b"a"),
                        "v": sorted([s, round(t, 1)] for (st_, s), t in got.items() if st_ == b"v")}
                body = json.dumps(rep_, separators=(",", ":")).encode() if got else b""
                self.wfile.write((b"HTTP/1.1 200 OK" if got else b"HTTP/1.1 404 Not Found") +
                                 b"\r\nContent-Length: %d\r\n\r\n" % len(body) + body)
                self.wfile.flush()
                continue
            if method == "GET" and url.path == "/mux":
                # An HTTP/2-shaped connection (W3 h2): every object requested on
                # it at once, the responses interleaved in frames.
                self.wfile.write(b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: qq-mux\r\n\r\n")
                self.wfile.flush()
                self.mux()
                return
            if method == "GET" and url.path == "/sink-report":
                q = {k: v[0] for k, v in parse_qs(url.query).items()}
                with SINK_LOCK:
                    rep = dict(SINK.get(q.get("tag", ""), {}))
                body = json.dumps(rep).encode() if rep else b""
                self.wfile.write((b"HTTP/1.1 200 OK" if rep else b"HTTP/1.1 404 Not Found") +
                                 b"\r\nContent-Length: %d\r\n\r\n" % len(body) + body)
                self.wfile.flush()
                continue
            if method == "POST" and url.path == "/llm":
                q = {k: v[0] for k, v in parse_qs(url.query).items()}
                R = (last - first) if first is not None else 0.0
                self.llm(R, float(q.get("think_ms", 500)), int(q.get("n", 400)), float(q.get("cadence_ms", 25)),
                         int(q.get("seed", 1)))
            elif method == "GET" and url.path.startswith("/bytes/"):
                # Browsing objects: N bytes, keep-alive.
                try:
                    n = int(url.path[len("/bytes/"):])
                except ValueError:
                    n = -1
                if not 0 <= n <= 1 << 30:
                    self.wfile.write(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n")
                    self.wfile.flush()
                    continue
                self.wfile.write(b"HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\n"
                                 b"Content-Length: %d\r\n\r\n" % n)
                left = n
                while left > 0:
                    k = min(left, len(BLOCK))
                    self.wfile.write(BLOCK[:k])
                    left -= k
                self.wfile.flush()
            elif method == "POST" and url.path == "/sink":
                # ?reply=N: an N-byte answer (W4's ASR reply), else "ok".
                q = {k: v[0] for k, v in parse_qs(url.query).items()}
                try:
                    n = max(0, min(int(q.get("reply", "0")), 1 << 20))
                except ValueError:
                    n = 0
                body = BLOCK[:n] if n else b"ok\n"
                self.wfile.write(b"HTTP/1.1 200 OK\r\nContent-Length: %d\r\n\r\n" % len(body) + body)
                self.wfile.flush()
            else:
                self.wfile.write(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n")
                self.wfile.flush()

    def mux(self) -> None:
        """Streams on one connection, as HTTP/2 carries a page's objects:
        requests are `R` + stream id (4) + size (4) + padding to 600 bytes;
        responses are frames `D` + stream id (4) + length (4) + payload, of
        up to 16 KiB, taken round-robin across the open streams, and `F` +
        stream id (4) when a stream's object is complete."""
        active: list = []
        cond = threading.Condition()
        closed = [False]

        def reader():
            try:
                while True:
                    head = self.rfile.read(600)
                    if len(head) < 600 or head[:1] != b"R":
                        break
                    sid, size = struct.unpack(">II", head[1:9])
                    with cond:
                        active.append([sid, min(size, 1 << 30)])
                        cond.notify()
            except (OSError, ssl.SSLError, ValueError):
                pass
            with cond:
                closed[0] = True
                cond.notify()
        threading.Thread(target=reader, daemon=True).start()
        try:
            while True:
                with cond:
                    while not active and not closed[0]:
                        cond.wait()
                    if not active:
                        return
                    st_ = active.pop(0)
                sid, left = st_
                k = min(left, MUX_FRAME)
                out = b"D" + struct.pack(">II", sid, k) + BLOCK[:k]
                left -= k
                if left <= 0:
                    out += b"F" + struct.pack(">I", sid)
                self.wfile.write(out)
                self.wfile.flush()
                if left > 0:
                    with cond:
                        active.append([sid, left])
        except (OSError, ssl.SSLError):
            return

    def llm(self, R: float, think_ms: float, n: int, cadence_ms: float, seed: int) -> None:
        time.sleep(think_ms / 1000.0)
        self.wfile.write(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\n"
                         b"Transfer-Encoding: chunked\r\n\r\n")
        self.wfile.flush()
        rng = random.Random(seed)
        start = time.monotonic()
        for i in range(n):
            due = start + i * cadence_ms / 1000.0
            delay = due - time.monotonic()
            if delay > 0:
                time.sleep(delay)
            self.wfile.write(chunk(event(i, rng, R if i == 0 else None)))
            self.wfile.flush()
        self.wfile.write(chunk(b"data: [DONE]\n\n") + b"0\r\n\r\n")
        self.wfile.flush()


def read_exact(sock, n: int) -> bytes:
    buf = b""
    while len(buf) < n:
        b = sock.recv(n - len(buf))
        if not b:
            raise EOFError
        buf += b
    return buf


class Term(socketserver.BaseRequestHandler):
    def setup(self):
        handshake(self)

    def handle(self):
        s = self.request
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        lock = threading.Lock()
        try:
            while True:
                t, n = struct.unpack(">cH", read_exact(s, 3))
                payload = read_exact(s, n)
                if t == b"K":
                    with lock:
                        s.sendall(b"E" + struct.pack(">H", len(payload)) + payload)
                elif t == b"B":
                    left = struct.unpack(">I", payload[:4])[0]
                    with lock:
                        while left > 0:
                            k = min(left, 16384)
                            s.sendall(b"O" + struct.pack(">H", k) + b"o" * k)
                            left -= k
        except (EOFError, OSError, ssl.SSLError):
            return


class Server(socketserver.ThreadingMixIn, socketserver.TCPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, addr, handler, context):
        self.context = context
        super().__init__(addr, handler)

    def handle_error(self, request, client_address):
        pass  # a failed handshake or a dropped client is the client's to record


def prefer_tls13(ctx: ssl.SSLContext) -> None:
    """TLS 1.3 where the local ssl library supports it as a minimum (it may
    not: LibreSSL builds), else TLS 1.2. The client records the version
    each connection negotiated."""
    try:
        ctx.minimum_version = ssl.TLSVersion.TLSv1_3
    except (ValueError, AttributeError):
        ctx.minimum_version = ssl.TLSVersion.TLSv1_2


def tls_context(directory: str) -> ssl.SSLContext:
    cert, key = os.path.join(directory, "wl-cert.pem"), os.path.join(directory, "wl-key.pem")
    if not (os.path.exists(cert) and os.path.exists(key)):
        subprocess.run(["openssl", "req", "-x509", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:prime256v1",
                        "-nodes", "-keyout", key, "-out", cert, "-days", "3", "-subj", "/CN=wlserver"],
                       check=True, capture_output=True)
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    prefer_tls13(ctx)
    ctx.load_cert_chain(cert, key)
    return ctx


def reflect(sock: socket.socket) -> None:
    """The call's far side: every datagram straight back to its sender, with
    the reflector's receive time (server monotonic ms, 8 bytes) and its
    running count of datagrams from that sender (4 bytes) written at bytes
    24..35 -- the layout wlclient's call generator reserves for them."""
    counts: dict = {}
    while True:
        try:
            data, peer = sock.recvfrom(65535)
        except OSError:
            continue
        now = mono_ms()
        counts[peer] = counts.get(peer, 0) + 1
        if len(data) >= 40 and data[:2] == b"QC":
            cid = data[36:40]
            stream, seq = data[2:3], struct.unpack_from(">I", data, 4)[0]
            with CALLS_LOCK:
                CALLS.setdefault(cid, {})[(stream, seq)] = now
                CALLS_SEEN[cid] = now
                if len(CALLS) > 64:
                    # Forget calls not heard from for ten minutes.
                    for old in [c for c, t in CALLS_SEEN.items() if now - t > 600_000]:
                        CALLS.pop(old, None)
                        CALLS_SEEN.pop(old, None)
        if len(data) >= 36:
            data = data[:24] + struct.pack(">dI", now, counts[peer]) + data[36:]
        try:
            sock.sendto(data, peer)
        except OSError:
            pass


class Wake(socketserver.BaseRequestHandler):
    """Idle-wake connections (W9a, trial D). The client opens one with a
    line `W <sync bytes> <wake seconds> <push|send> <followups>`; the server
    sends the sync burst at once (login history), then stays silent. For a
    push wake it pushes one message at the wake time (seconds after the
    request, on its own clock) and `followups` more at 1 s spacing; a push
    is `P<4-byte index><8-byte server monotonic ms>`. The client may send
    `S<4-byte n>` at any time, answered at once by `A<4-byte n>` (a send
    wake and its warm follow-ups)."""

    def setup(self):
        handshake(self)

    def handle(self):
        s = self.request
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        lock = threading.Lock()
        try:
            line = b""
            while not line.endswith(b"\n"):
                b = s.recv(1)
                if not b:
                    return
                line += b
            _, sync, at, mode, follow = line.decode().split()
            wakes = at if mode == "push" else ""
            t0 = time.monotonic()
            left = int(sync)
            with lock:
                while left > 0:
                    k = min(left, 16384)
                    s.sendall(b"Y" + struct.pack(">H", k) + b"y" * k)
                    left -= k

            def pushes():
                i = 0
                try:
                    for w in [float(x) for x in wakes.split(",") if x]:
                        for j in range(int(follow) + 1):
                            due = t0 + w + j
                            delay = due - time.monotonic()
                            if delay > 0:
                                time.sleep(delay)
                            with lock:
                                s.sendall(b"P" + struct.pack(">Id", i, mono_ms()))
                            i += 1
                except (OSError, ssl.SSLError):
                    return

            threading.Thread(target=pushes, daemon=True).start()
            while True:
                head = read_exact(s, 5)
                if head[:1] == b"S":
                    with lock:
                        s.sendall(b"A" + head[1:5])
        except (EOFError, OSError, ssl.SSLError, ValueError):
            return


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--bind", default="127.0.0.1")
    ap.add_argument("--http", type=int, required=True)
    ap.add_argument("--term", type=int, required=True)
    ap.add_argument("--udp", type=int, default=0, help="UDP reflector port for the call (0: none)")
    ap.add_argument("--wake", type=int, default=0, help="idle-wake port (0: none)")
    ap.add_argument("--dir", default=".")
    ap.add_argument("--no-tls", action="store_true")
    args = ap.parse_args(argv)
    ctx = None if args.no_tls else tls_context(args.dir)
    term = Server((args.bind, args.term), Term, ctx)
    threading.Thread(target=term.serve_forever, daemon=True).start()
    if args.wake:
        wake = Server((args.bind, args.wake), Wake, ctx)
        threading.Thread(target=wake.serve_forever, daemon=True).start()
    if args.udp:
        u = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        u.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4 << 20)
        u.bind((args.bind, args.udp))
        threading.Thread(target=reflect, args=(u,), daemon=True).start()
    Server((args.bind, args.http), HTTP, ctx).serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
