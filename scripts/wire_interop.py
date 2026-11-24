#!/usr/bin/env python3
"""Run released v0.12.x binaries against this tree's, over the real wire.

The in-process tests replay version 1 with this tree's own code on both ends.
They cannot show that a gateway built from this tree accepts what a deployed
v0.12.1 client actually sends, or that this tree's client reads what a
deployed v0.12.1 gateway actually answers. This does: it builds the old
release from its tag and the current tree, and runs each against the other on
loopback.

For every pairing -- old client to new gateway, new client to old gateway, new
to new as the control, and old to old as the baseline the old client is held
to -- and for each of TCP and QUIC, it runs a large
download, a DNS query through a SOCKS5 UDP association, and a forced stall: a
relay in front of the gateway black-holes every connection it carries while
letting new ones through, which is what a stalled path looks like to the lane
that has to be rescued. A stalled QUIC flow is a download; a stalled TCP flow
is an upload, because only a client with bytes it cannot send rescues a lone
TCP lane. Over TCP the relay also cuts every live connection mid-upload, the
lane failure a client replaces with a JOIN; and with QUIC it withdraws UDP
entirely mid-download from an `auto` client, which hands the flow off to TCP
with a JOIN.

A disruption the old client does not survive against its own v0.12.1 gateway
is reported as that client's limitation, not as a failure of the new gateway.

Everything listens on 127.0.0.1. Nothing here reaches another host.
"""

import argparse
import datetime
import hashlib
import json
import os
import pathlib
import re
import secrets
import selectors
import shutil
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request

DEFAULT_OLD_REF = "v0.12.1"
CHUNK = 64 * 1024


def chunk_at(index):
    """The test stream's 64 KiB chunk number index.

    Every chunk carries its own position, so a lost, duplicated, or reordered
    chunk changes the digest even when the length comes out right.
    """
    return (struct.pack("!Q", index) + b"queqiao!") * (CHUNK // 16)


def stream_chunks(size):
    index = 0
    while size > 0:
        piece = chunk_at(index)[: min(CHUNK, size)]
        yield piece
        size -= len(piece)
        index += 1
DNS_NAME = "interop.test"
DNS_ANSWER = "192.0.2.53"


def log(message):
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%H:%M:%S")
    print(f"[{stamp}] {message}", flush=True)


def free_port():
    """A port free on both TCP and UDP, which a gateway listens on together."""
    for _ in range(64):
        tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tcp.bind(("127.0.0.1", 0))
        port = tcp.getsockname()[1]
        udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            udp.bind(("127.0.0.1", port))
        except OSError:
            continue
        finally:
            udp.close()
            tcp.close()
        return port
    raise RuntimeError("no port free on both TCP and UDP")


def wait_for_tcp(port, timeout, process=None):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process is not None and process.poll() is not None:
            raise RuntimeError(f"process exited with {process.returncode} before listening on {port}")
        try:
            socket.create_connection(("127.0.0.1", port), timeout=0.5).close()
            return
        except OSError:
            time.sleep(0.1)
    raise RuntimeError(f"nothing listening on 127.0.0.1:{port} after {timeout}s")


# --- destinations -----------------------------------------------------------


class Destinations:
    """A download source, an upload sink, and a DNS responder, all loopback."""

    def __init__(self, download_bytes):
        self.download_bytes = download_bytes
        self.stopping = threading.Event()
        self.download = self._listen(self._serve_download)
        self.sink = self._listen(self._serve_sink)
        self.dns = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.dns.bind(("127.0.0.1", 0))
        self.dns.settimeout(0.5)
        threading.Thread(target=self._serve_dns, daemon=True).start()

    def _listen(self, handler):
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", 0))
        listener.listen(64)
        listener.settimeout(0.5)

        def accept():
            while not self.stopping.is_set():
                try:
                    conn, _ = listener.accept()
                except socket.timeout:
                    continue
                except OSError:
                    return
                threading.Thread(target=handler, args=(conn,), daemon=True).start()

        threading.Thread(target=accept, daemon=True).start()
        return listener

    def _serve_download(self, conn):
        # The first line names the size, so one source serves every test.
        with conn:
            try:
                header = b""
                while not header.endswith(b"\n") and len(header) < 32:
                    part = conn.recv(1)
                    if not part:
                        return
                    header += part
                for chunk in stream_chunks(int(header)):
                    conn.sendall(chunk)
                conn.shutdown(socket.SHUT_WR)
                conn.settimeout(10)
                while conn.recv(65536):
                    pass
            except OSError:
                return

    def _serve_sink(self, conn):
        # Answers with what it received: the count and its digest.
        with conn:
            total = 0
            digest = hashlib.sha256()
            try:
                while True:
                    part = conn.recv(1 << 16)
                    if not part:
                        break
                    total += len(part)
                    digest.update(part)
                conn.sendall(struct.pack("!Q", total) + digest.digest())
            except OSError:
                return

    def _serve_dns(self):
        while not self.stopping.is_set():
            try:
                query, peer = self.dns.recvfrom(4096)
            except socket.timeout:
                continue
            except OSError:
                return
            if len(query) < 12:
                continue
            answer = (
                query[:2]
                + b"\x81\x80"
                + query[4:6]
                + b"\x00\x01\x00\x00\x00\x00"
                + query[12:]
                + b"\xc0\x0c\x00\x01\x00\x01\x00\x00\x00\x3c\x00\x04"
                + socket.inet_aton(DNS_ANSWER)
            )
            self.dns.sendto(answer, peer)

    def close(self):
        self.stopping.set()
        for item in (self.download, self.sink, self.dns):
            try:
                item.close()
            except OSError:
                pass


def expected_digest(size):
    digest = hashlib.sha256()
    for chunk in stream_chunks(size):
        digest.update(chunk)
    return digest.hexdigest()


# --- a relay that can stall what it carries ---------------------------------


class StallRelay:
    """Forwards TCP and UDP from one loopback port to the gateway.

    freeze() black-holes every connection and UDP mapping the relay holds at
    that moment, in both directions, and keeps forwarding anything that starts
    afterwards: the lanes a flow has stop moving and a fresh dial gets through.
    """

    def __init__(self, front_port, upstream_port):
        self.upstream = ("127.0.0.1", upstream_port)
        self.generation = 0
        self.frozen_through = -1
        self.live = []
        self.udp_blocked = False
        self.lock = threading.Lock()
        self.stopping = threading.Event()
        self.tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.tcp.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.tcp.bind(("127.0.0.1", front_port))
        self.tcp.listen(128)
        self.tcp.settimeout(0.5)
        self.udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp.bind(("127.0.0.1", front_port))
        self.udp.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 8 << 20)
        self.udp.setblocking(False)
        threading.Thread(target=self._accept_tcp, daemon=True).start()
        threading.Thread(target=self._pump_udp, daemon=True).start()

    def _current(self):
        with self.lock:
            return self.generation

    def _dead(self, generation):
        with self.lock:
            return generation <= self.frozen_through

    def freeze(self):
        with self.lock:
            self.frozen_through = self.generation
            self.generation += 1

    def block_udp(self):
        """Drop every UDP datagram, old mapping or new, until unblock_udp."""
        self.udp_blocked = True

    def unblock_udp(self):
        self.udp_blocked = False

    def cut(self):
        """Reset every TCP connection the relay carries now.

        A cut lane fails at once, where a frozen one has to be suspected; it
        is the other way a client learns it needs a replacement.
        """
        with self.lock:
            live, self.live = self.live, []
        for pair in live:
            for conn in pair:
                try:
                    conn.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
                    conn.close()
                except OSError:
                    pass

    def _accept_tcp(self):
        while not self.stopping.is_set():
            try:
                down, _ = self.tcp.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            try:
                up = socket.create_connection(self.upstream, timeout=5)
            except OSError:
                down.close()
                continue
            up.settimeout(None)
            generation = self._current()
            with self.lock:
                self.live.append((down, up))
            for src, dst in ((down, up), (up, down)):
                threading.Thread(target=self._pipe, args=(src, dst, generation), daemon=True).start()

    def _pipe(self, src, dst, generation):
        try:
            while True:
                data = src.recv(1 << 16)
                if self._dead(generation):
                    # Stop reading: the sender's window fills and the lane
                    # goes silent, as it does on a black-holed path.
                    self.stopping.wait()
                    return
                if not data:
                    dst.shutdown(socket.SHUT_WR)
                    return
                dst.sendall(data)
        except OSError:
            return
        finally:
            if self.stopping.is_set():
                for item in (src, dst):
                    try:
                        item.close()
                    except OSError:
                        pass

    def _pump_udp(self):
        selector = selectors.DefaultSelector()
        selector.register(self.udp, selectors.EVENT_READ, None)
        mappings = {}
        while not self.stopping.is_set():
            for key, _ in selector.select(timeout=0.5):
                sock = key.fileobj
                while True:
                    try:
                        data, peer = sock.recvfrom(65536)
                    except (BlockingIOError, InterruptedError):
                        break
                    except OSError:
                        return
                    if key.data is None:
                        mapping = mappings.get(peer)
                        if mapping is None:
                            up = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                            up.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 8 << 20)
                            up.connect(self.upstream)
                            up.setblocking(False)
                            mapping = (up, self._current())
                            mappings[peer] = mapping
                            selector.register(up, selectors.EVENT_READ, (peer, mapping[1]))
                        if not self._dead(mapping[1]) and not self.udp_blocked:
                            try:
                                mapping[0].send(data)
                            except OSError:
                                pass
                    else:
                        client, generation = key.data
                        if not self._dead(generation) and not self.udp_blocked:
                            try:
                                self.udp.sendto(data, client)
                            except OSError:
                                pass
        for up, _ in mappings.values():
            up.close()

    def close(self):
        self.stopping.set()
        for item in (self.tcp, self.udp):
            try:
                item.close()
            except OSError:
                pass


# --- SOCKS5 -----------------------------------------------------------------


class IntegrityError(RuntimeError):
    """Bytes arrived, but not the bytes that were sent."""


def recv_exact(conn, length):
    data = bytearray()
    while len(data) < length:
        part = conn.recv(length - len(data))
        if not part:
            raise EOFError("connection closed")
        data.extend(part)
    return bytes(data)


def socks_connect(proxy_port, destination_port, timeout):
    conn = socket.create_connection(("127.0.0.1", proxy_port), timeout=timeout)
    conn.sendall(b"\x05\x01\x00")
    if recv_exact(conn, 2) != b"\x05\x00":
        raise RuntimeError("SOCKS method refused")
    conn.sendall(b"\x05\x01\x00\x01" + socket.inet_aton("127.0.0.1") + struct.pack("!H", destination_port))
    reply = recv_exact(conn, 4)
    if reply[1] != 0:
        raise RuntimeError(f"SOCKS connect refused with {reply[1]}")
    kind = reply[3]
    recv_exact(conn, {1: 4, 4: 16}.get(kind, 0) or recv_exact(conn, 1)[0])
    recv_exact(conn, 2)
    return conn


def download(proxy_port, destinations, size, timeout, on_progress=None):
    started = time.monotonic()
    conn = socks_connect(proxy_port, destinations.download.getsockname()[1], timeout)
    conn.settimeout(timeout)
    digest = hashlib.sha256()
    received = 0
    with conn:
        conn.sendall(f"{size}\n".encode())
        while True:
            part = conn.recv(1 << 16)
            if not part:
                break
            digest.update(part)
            received += len(part)
            if received > size:
                raise IntegrityError(f"download delivered more than the {size} bytes sent")
            if on_progress is not None:
                on_progress(received)
    elapsed = time.monotonic() - started
    if received != size or digest.hexdigest() != expected_digest(size):
        if received == size:
            raise IntegrityError(f"downloaded {size} bytes that do not match what was sent")
        raise RuntimeError(f"download ended at {received} of {size} bytes")
    return {"bytes": received, "seconds": round(elapsed, 3), "mbit_per_second": round(size * 8 / elapsed / 1e6, 1)}


def upload(proxy_port, destinations, size, timeout, on_progress=None):
    started = time.monotonic()
    conn = socks_connect(proxy_port, destinations.sink.getsockname()[1], timeout)
    conn.settimeout(timeout)
    sent = 0
    with conn:
        for piece in stream_chunks(size):
            conn.sendall(piece)
            sent += len(piece)
            if on_progress is not None:
                on_progress(sent)
        conn.shutdown(socket.SHUT_WR)
        answer = recv_exact(conn, 8 + 32)
    elapsed = time.monotonic() - started
    (count,) = struct.unpack("!Q", answer[:8])
    if count != size or answer[8:].hex() != expected_digest(size):
        raise IntegrityError(f"destination received {count} of {size} bytes, digest match {answer[8:].hex() == expected_digest(size)}")
    return {"bytes": count, "seconds": round(elapsed, 3), "mbit_per_second": round(size * 8 / elapsed / 1e6, 1)}


def dns_query(name):
    transaction = secrets.randbits(16)
    question = b"".join(bytes([len(label)]) + label.encode() for label in name.split(".")) + b"\x00"
    packet = struct.pack("!HHHHHH", transaction, 0x0100, 1, 0, 0, 0) + question + b"\x00\x01\x00\x01"
    return transaction, packet


def udp_dns(proxy_port, destinations, timeout, attempts=5):
    control = socket.create_connection(("127.0.0.1", proxy_port), timeout=timeout)
    with control:
        control.sendall(b"\x05\x01\x00")
        if recv_exact(control, 2) != b"\x05\x00":
            raise RuntimeError("SOCKS method refused")
        control.sendall(b"\x05\x03\x00\x01\x00\x00\x00\x00\x00\x00")
        reply = recv_exact(control, 4)
        if reply[1] != 0 or reply[3] != 1:
            raise RuntimeError(f"UDP associate refused: {reply!r}")
        relay_host = socket.inet_ntoa(recv_exact(control, 4))
        (relay_port,) = struct.unpack("!H", recv_exact(control, 2))
        if relay_host == "0.0.0.0":
            relay_host = "127.0.0.1"
        udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        udp.bind(("127.0.0.1", 0))
        udp.settimeout(2)
        dns_port = destinations.dns.getsockname()[1]
        header = b"\x00\x00\x00\x01" + socket.inet_aton("127.0.0.1") + struct.pack("!H", dns_port)
        last = None
        with udp:
            for attempt in range(1, attempts + 1):
                transaction, packet = dns_query(DNS_NAME)
                started = time.monotonic()
                udp.sendto(header + packet, (relay_host, relay_port))
                try:
                    data, _ = udp.recvfrom(4096)
                except socket.timeout as error:
                    last = error
                    continue
                answer = data[10:]
                if struct.unpack("!H", answer[:2])[0] != transaction or socket.inet_ntoa(answer[-4:]) != DNS_ANSWER:
                    raise RuntimeError("DNS answer does not match the query")
                return {"answer": DNS_ANSWER, "attempt": attempt, "milliseconds": round((time.monotonic() - started) * 1000, 1)}
        raise RuntimeError(f"no DNS answer after {attempts} attempts: {last}")


# --- binaries and processes -------------------------------------------------


def build_new(repo, output_dir):
    binaries = output_dir / "bin"
    binaries.mkdir(parents=True, exist_ok=True)
    new = binaries / "queqiaod-new"
    log("building the current tree")
    subprocess.run(["go", "build", "-o", str(new), "./cmd/queqiaod"], cwd=repo, check=True)
    return new


def build_old(repo, output_dir, old_ref):
    binaries = output_dir / "bin"
    binaries.mkdir(parents=True, exist_ok=True)
    old = binaries / "queqiaod-old"
    source = pathlib.Path(tempfile.mkdtemp(prefix="queqiao-old-")) / "src"
    log(f"building {old_ref} in a temporary worktree")
    subprocess.run(["git", "worktree", "add", "--detach", str(source), old_ref], cwd=repo, check=True, capture_output=True)
    try:
        subprocess.run(["go", "build", "-o", str(old), "./cmd/queqiaod"], cwd=source, check=True)
    finally:
        subprocess.run(["git", "worktree", "remove", "--force", str(source)], cwd=repo, check=False, capture_output=True)
        shutil.rmtree(source.parent, ignore_errors=True)
    return old


def version_of(binary):
    return subprocess.run([str(binary), "--version"], check=True, capture_output=True, text=True).stdout.strip()


class Process:
    def __init__(self, args, log_path):
        self.log_path = log_path
        self.stderr = open(log_path.with_suffix(".stderr"), "w")
        self.process = subprocess.Popen(args, stdout=self.stderr, stderr=subprocess.STDOUT)

    def stop(self):
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
        self.stderr.close()

    def records(self):
        result = []
        try:
            with open(self.log_path) as handle:
                for line in handle:
                    try:
                        result.append(json.loads(line))
                    except ValueError:
                        continue
        except OSError:
            pass
        return result


def runtime_flags(log_path, metrics_port):
    return [
        "--log-file", str(log_path), "--log-format", "json", "--log-level", "debug",
        "--log-stderr=false", "--metrics-listen", f"127.0.0.1:{metrics_port}",
    ]


def scrape(metrics_port):
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{metrics_port}/metrics", timeout=5) as response:
            text = response.read().decode()
    except OSError:
        return {}
    values = {}
    for line in text.splitlines():
        if line.startswith("#") or " " not in line:
            continue
        name, value = line.rsplit(" ", 1)
        try:
            values[name] = float(value)
        except ValueError:
            continue
    return values


class Gateway:
    """A provider, its gateway process, and a stall relay in front of it."""

    def __init__(self, binary, work, label):
        self.binary, self.work, self.label = binary, work, label
        self.port, self.front, self.metrics = free_port(), free_port(), free_port()
        state = work / "provider"
        run = lambda *args: subprocess.run([str(binary), "provider", *args], check=True, capture_output=True, text=True)
        run("init", "--state", str(state), "--name", f"interop {label}", "--endpoint", f"127.0.0.1:{self.front}")
        run("add-user", "--state", str(state), "--name", "interop")
        self.state = state
        self.relay = StallRelay(self.front, self.port)
        self.process = Process(
            [str(binary), "server", "--state", str(state), "--listen", f"127.0.0.1:{self.port}",
             "--transport", "auto", "--allow-private-destinations", *runtime_flags(work / "gateway.log", self.metrics)],
            work / "gateway.log",
        )
        wait_for_tcp(self.port, 30, self.process.process)

    def invite(self):
        out = subprocess.run(
            [str(self.binary), "provider", "invite", "--state", str(self.state), "--user", "interop"],
            check=True, capture_output=True, text=True,
        )
        match = re.search(r"queqiao://enroll/\S+", out.stdout + out.stderr)
        if match is None:
            raise RuntimeError("provider invite printed no invitation")
        return match.group(0)

    def stop(self):
        self.process.stop()
        self.relay.close()


class Client:
    def __init__(self, binary, gateway, work, transport):
        self.binary, self.work = binary, work
        work.mkdir(parents=True, exist_ok=True)
        self.profile = work / "profile.json"
        enrolled = subprocess.run(
            [str(binary), "enroll", "--invite", gateway.invite(), "--profile", str(self.profile),
             "--device-name", f"interop-{transport}", "--local-address", "127.0.0.1", "--timeout", "30s"],
            capture_output=True, text=True,
        )
        if enrolled.returncode != 0:
            # The command line carries a live invitation; report without it.
            detail = re.sub(r"queqiao://enroll/\S+", "queqiao://enroll/<redacted>", enrolled.stderr.strip())
            raise RuntimeError(f"enrollment exited with {enrolled.returncode}: {detail[-300:]}")
        self.socks, self.metrics = free_port(), free_port()
        # One TCP lane: a stalled striped bundle is a lane-failure case, and
        # the stall watchdog rescues a lone lane.
        lanes = ["--tcp-fallback-lanes", "1"]
        self.process = Process(
            [str(binary), "client", "--profile", str(self.profile), "--listen", f"127.0.0.1:{self.socks}",
             "--transport", transport, "--local-address", "127.0.0.1", *lanes,
             "--no-auto-renew", *runtime_flags(work / "client.log", self.metrics)],
            work / "client.log",
        )
        wait_for_tcp(self.socks, 30, self.process.process)

    def stop(self):
        self.process.stop()


def negotiated_wires(client, gateway):
    """Which wire versions this pairing's lanes used, from whichever end can say."""
    wires = set()
    for record in client.process.records():
        if record.get("msg") == "outer lane authenticated" and "wire" in record:
            wires.add(int(record["wire"]))
    metrics = scrape(gateway.metrics)
    for version in (1, 2):
        if metrics.get(f'queqiao_data_connections_total{{wire="{version}"}}', 0) > 0:
            wires.add(version)
    return sorted(wires)


def join_refusals(gateway):
    """JOINs the gateway answered with a RESET, as either generation logs them."""
    return sum(1 for record in gateway.process.records() if record.get("msg") == "lane join refused")


def failure_kind(error):
    """What a failed transfer looked like from the application's side."""
    if isinstance(error, socket.timeout):
        return "timeout"
    if isinstance(error, (ConnectionError, EOFError, BrokenPipeError)):
        return "closed"
    return "short"


def lane_joins(gateway, carrier=None):
    """Lane JOINs the gateway admitted, optionally only on one carrier."""
    return sum(
        1 for record in gateway.process.records()
        if record.get("msg") == "lane joined" and (carrier is None or record.get("transport") == carrier)
    )


# --- the matrix --------------------------------------------------------------


PAIRINGS = (
    # The baseline: what the released build does on its own. A check the old
    # client fails against its own gateway is a limit of that client, and the
    # new gateway is held to the baseline rather than to more.
    ("old-client/old-gateway", "old", "old", None),
    ("old-client/new-gateway", "old", "new", 1),
    ("new-client/old-gateway", "new", "old", 1),
    ("new-client/new-gateway", "new", "new", 2),
)


def disrupted_transfer(client, gateway, destinations, direction, carrier, disrupt, args):
    """A transfer disrupted partway through, which a lane JOIN must rescue.

    The JOIN must be admitted after the disruption and on carrier, the one the
    rescue is supposed to use. A failure is returned rather than raised, with
    the phase it happened in, so it can be compared with what the released
    build does against its own gateway: only a failure to be rescued is ever
    excused, never one before the disruption or a corrupted transfer.
    """
    applied = {}

    def at(progress):
        if not applied and progress >= args.stall_after_mib << 20:
            applied["joins"] = lane_joins(gateway, carrier)
            applied["refusals"] = join_refusals(gateway)
            disrupt()
            applied["at"] = time.monotonic()

    size = args.stall_mib << 20
    outcome = {"direction": direction, "carrier": carrier, "completed": False}
    try:
        if direction == "download":
            outcome.update(download(client.socks, destinations, size, args.timeout, at))
        else:
            outcome.update(upload(client.socks, destinations, size, args.timeout, at))
        outcome["completed"] = True
        if not applied:
            outcome.update(phase="setup", error="the transfer finished before the disruption was applied")
            return outcome
        outcome["lane_joins"] = lane_joins(gateway, carrier) - applied["joins"]
        outcome["join_refusals"] = join_refusals(gateway) - applied["refusals"]
        if outcome["lane_joins"] < 1:
            outcome.update(phase="rescue", kind="unrescued", error=f"the flow completed without a {carrier} lane joining it after the disruption")
    except IntegrityError as error:
        outcome.update(phase="integrity", error=str(error))
    except Exception as error:  # noqa: BLE001 - compared with the baseline
        outcome["phase"] = "rescue" if applied else "setup"
        outcome["kind"] = failure_kind(error)
        outcome["error"] = f"{type(error).__name__}: {error}"
        if applied:
            outcome["lane_joins"] = lane_joins(gateway, carrier) - applied["joins"]
            outcome["join_refusals"] = join_refusals(gateway) - applied["refusals"]
    return outcome


def run_case(name, client_bin, gateway, destinations, transport, args, work):
    result = {"pairing": name, "transport": transport, "checks": {}, "ok": False}
    client = None
    clients = []
    try:
        checks = result["checks"]
        timeout = args.timeout
        client = Client(client_bin, gateway, work, transport)
        clients.append(client)
        checks["download"] = download(client.socks, destinations, args.download_mib << 20, timeout)
        log(f"  {name} {transport}: download {checks['download']}")
        checks["udp_dns"] = udp_dns(client.socks, destinations, timeout)
        log(f"  {name} {transport}: UDP DNS {checks['udp_dns']}")
        # A stalled QUIC flow is a download; a TCP one an upload, because only
        # a client with bytes it cannot send rescues a lone TCP lane.
        direction = "download" if transport == "quic" else "upload"
        disruptions = [("stall", gateway.relay.freeze)]
        if transport == "tcp":
            disruptions.append(("lane_cut", gateway.relay.cut))
        for check, disrupt in disruptions:
            checks[check] = disrupted_transfer(client, gateway, destinations, direction, transport, disrupt, args)
            log(f"  {name} {transport}: {check} {checks[check]}")
        result["wires"] = negotiated_wires(client, gateway)
        if transport == "quic":
            # UDP withdrawn mid-download: an auto client hands the flow off to
            # TCP with a JOIN, the one old-client TCP JOIN loopback can force.
            auto = Client(client_bin, gateway, work / "auto", "auto")
            clients.append(auto)
            try:
                checks["tcp_handoff"] = disrupted_transfer(auto, gateway, destinations, "download", "tcp", gateway.relay.block_udp, args)
            finally:
                gateway.relay.unblock_udp()
            log(f"  {name} {transport}: tcp_handoff {checks['tcp_handoff']}")
        result["ok"] = True
    except Exception as error:  # noqa: BLE001 - every failure is a result
        result["error"] = f"{type(error).__name__}: {error}"
        log(f"  {name} {transport}: FAILED {result['error']}")
    finally:
        if client is not None and "wires" not in result:
            result["wires"] = negotiated_wires(client, gateway)
        for item in clients:
            item.stop()
    return result


DISRUPTIONS = ("stall", "lane_cut", "tcp_handoff")


def judge(results, old_ref):
    """Fail a disrupted transfer unless the released build fails it too.

    The new gateway is held to the old client's own baseline: a rescue the old
    client never attempts against its own gateway is that client's limit. A
    pairing with the new client has no such allowance.
    """
    baseline = {item["transport"]: item for item in results if item["pairing"] == "old-client/old-gateway"}
    for item in results:
        if not item["ok"]:
            continue
        for check in DISRUPTIONS:
            outcome = item["checks"].get(check)
            if outcome is None or "error" not in outcome:
                continue
            if item["pairing"] == "old-client/old-gateway" and outcome.get("phase") == "rescue":
                outcome["baseline"] = f"{old_ref} client limitation"
                continue
            reference = baseline.get(item["transport"], {}).get("checks", {}).get(check)
            # Only a rescue the old client never achieved against its own
            # gateway either is excused: same phase, the transfer ending the
            # same way, and neither more JOINs admitted nor more refused than
            # there. A JOIN the new gateway refuses is never excused. Setup and integrity failures are never excused.
            if (
                item["pairing"].startswith("old-client/")
                and outcome.get("phase") == "rescue"
                and reference is not None
                and reference.get("phase") == "rescue"
                and outcome.get("completed") == reference.get("completed")
                and outcome.get("kind") == reference.get("kind")
                and outcome.get("lane_joins", 0) <= reference.get("lane_joins", 0)
                and outcome.get("join_refusals", 0) <= reference.get("join_refusals", 0)
            ):
                outcome["baseline"] = f"fails identically against the {old_ref} gateway"
                continue
            item["ok"] = False
            item["error"] = f"{check}: {outcome['error']}"


def main(arguments=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--output-dir", required=True, type=pathlib.Path, help="new directory for binaries, logs, and results")
    parser.add_argument("--old-ref", default=DEFAULT_OLD_REF, help="git ref of the released build to test against")
    parser.add_argument("--old-binary", type=pathlib.Path, help="use this old build instead of building --old-ref")
    parser.add_argument("--new-binary", type=pathlib.Path, help="use this new build instead of building the tree")
    parser.add_argument("--download-mib", type=int, default=50)
    parser.add_argument("--stall-mib", type=int, default=24)
    parser.add_argument("--stall-after-mib", type=int, default=2)
    parser.add_argument("--timeout", type=float, default=120)
    parser.add_argument("--transports", default="tcp,quic")
    parser.add_argument("--pairings", default=",".join(p[0] for p in PAIRINGS), help="comma-separated subset of the pairings to run")
    args = parser.parse_args(arguments)

    repo = pathlib.Path(__file__).resolve().parent.parent
    output = args.output_dir.resolve()
    names = [name for name in args.pairings.split(",") if name]
    known = {pairing[0] for pairing in PAIRINGS}
    unknown = sorted(set(names) - known)
    if not names or unknown:
        parser.error(f"--pairings must name some of {sorted(known)}; unknown: {unknown}")
    transports = [item for item in args.transports.split(",") if item]
    if not transports or not set(transports) <= {"tcp", "quic"}:
        parser.error("--transports must be a non-empty subset of tcp,quic")
    if output.exists():
        parser.error(f"output directory already exists: {output}")
    output.mkdir(parents=True)
    new = args.new_binary.resolve() if args.new_binary else build_new(repo, output)
    old = args.old_binary.resolve() if args.old_binary else build_old(repo, output, args.old_ref)
    binaries = {"old": old, "new": new}
    versions = {kind: version_of(path) for kind, path in binaries.items()}
    for kind, text in versions.items():
        log(f"{kind}: {text}")
    if not re.search(r"(^|\s)wire=2(\s|$)", versions["new"]) or "wire_accept=1,2" not in versions["new"]:
        raise SystemExit("the new build does not report wire=2 wire_accept=1,2")

    destinations = Destinations(args.download_mib << 20)
    results = []
    try:
        selected = set(names)
        for name, client_kind, gateway_kind, want_wire in PAIRINGS:
            if name not in selected:
                continue
            log(f"{name}")
            work = output / name.replace("/", "--")
            work.mkdir()
            gateway = Gateway(binaries[gateway_kind], work, gateway_kind)
            try:
                for transport in transports:
                    result = run_case(name, binaries[client_kind], gateway, destinations, transport, args, work / transport)
                    result["expected_wire"] = want_wire
                    # Neither v0.12.1 end reports its wire; it has only one.
                    if result["ok"] and want_wire is not None and result["wires"] != [want_wire]:
                        result["ok"] = False
                        result["error"] = f"negotiated wire {result['wires']}, expected [{want_wire}]"
                    results.append(result)
            finally:
                gateway.stop()
    finally:
        destinations.close()

    judge(results, args.old_ref if not args.old_binary else "supplied old build's")
    summary = {
        "format": "queqiao-wire-interop-v1",
        "finished_utc": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "commit": subprocess.run(["git", "rev-parse", "HEAD"], cwd=repo, capture_output=True, text=True).stdout.strip(),
        "old_ref": args.old_ref,
        "versions": versions,
        "results": results,
    }
    (output / "results.json").write_text(json.dumps(summary, indent=2) + "\n")
    print()
    def disruption(outcome):
        if not outcome:
            return "-"
        if "error" in outcome:
            return "baseline-fail" if "baseline" in outcome else "FAIL"
        return f"{outcome['carrier']} {outcome['seconds']}s j={outcome['lane_joins']}"

    print(f"{'pairing':<24} {'carrier':<7} {'wire':<4} {'download Mb/s':<13} {'udp dns':<8} {'stall':<20} {'lane cut':<20} {'tcp handoff':<20} result")
    for item in results:
        checks = item["checks"]
        dl = checks.get("download", {})
        print(
            f"{item['pairing']:<24} {item['transport']:<7} {','.join(map(str, item.get('wires', []))) or '-':<4} "
            f"{str(dl.get('mbit_per_second', '-')) if dl else '-':<13} "
            f"{'ok' if 'udp_dns' in checks else '-':<8} "
            f"{disruption(checks.get('stall')):<20} {disruption(checks.get('lane_cut')):<20} "
            f"{disruption(checks.get('tcp_handoff')):<20} "
            f"{'PASS' if item['ok'] else 'FAIL: ' + item.get('error', '')}"
        )
    for item in results:
        for check in DISRUPTIONS:
            outcome = item["checks"].get(check, {})
            if "baseline" in outcome:
                print(f"note: {item['pairing']} {item['transport']} {check}: {outcome['baseline']} ({outcome['error']})")
    failed = [item for item in results if not item["ok"]]
    if not results:
        print("no case ran")
        return 1
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
