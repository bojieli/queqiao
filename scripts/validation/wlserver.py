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
* `POST /sink` -- reads and discards the body (the upload background).

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


class HTTP(socketserver.StreamRequestHandler):
    def handle(self):
        self.request.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        while True:
            method, target, headers = read_headers(self.rfile)
            if method is None:
                return
            want = int(headers.get("content-length") or 0)
            got = 0
            first = last = None
            while got < want:
                b = self.rfile.read1(min(65536, want - got)) if hasattr(self.rfile, "read1") else self.rfile.read(min(65536, want - got))
                if not b:
                    return
                now = time.monotonic()
                first = first if first is not None else now
                last = now
                got += len(b)
            url = urlsplit(target)
            if method == "POST" and url.path == "/llm":
                q = {k: v[0] for k, v in parse_qs(url.query).items()}
                R = (last - first) if first is not None else 0.0
                self.llm(R, float(q.get("think_ms", 500)), int(q.get("n", 400)), float(q.get("cadence_ms", 25)),
                         int(q.get("seed", 1)))
            elif method == "POST" and url.path == "/sink":
                self.wfile.write(b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\nok\n")
                self.wfile.flush()
            else:
                self.wfile.write(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n")
                self.wfile.flush()

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

    def get_request(self):
        sock, addr = super().get_request()
        if self.context:
            sock = self.context.wrap_socket(sock, server_side=True)
        return sock, addr


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


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--bind", default="127.0.0.1")
    ap.add_argument("--http", type=int, required=True)
    ap.add_argument("--term", type=int, required=True)
    ap.add_argument("--dir", default=".")
    ap.add_argument("--no-tls", action="store_true")
    args = ap.parse_args(argv)
    ctx = None if args.no_tls else tls_context(args.dir)
    term = Server((args.bind, args.term), Term, ctx)
    threading.Thread(target=term.serve_forever, daemon=True).start()
    Server((args.bind, args.http), HTTP, ctx).serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
