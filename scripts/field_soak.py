#!/usr/bin/env python3
"""Run a checksummed mixed TCP/UDP soak through one queqiao SOCKS endpoint."""

from __future__ import annotations

import argparse
import datetime
import hashlib
import http.client
import io
import json
import pathlib
import secrets
import socket
import ssl
import struct
import subprocess
import sys
import time
import urllib.request

try:
    from scripts import udp_association_check as udp_check
except ModuleNotFoundError:  # direct execution from the scripts directory
    import udp_association_check as udp_check


def utc_now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="milliseconds")


def read_socks_reply(connection: socket.socket) -> None:
    header = udp_check.receive_exact(connection, 4)
    if header[0] != 5 or header[1] != 0 or header[2] != 0:
        raise RuntimeError(f"SOCKS CONNECT failed with reply {header[1]}")
    if header[3] == 1:
        udp_check.receive_exact(connection, 6)
    elif header[3] == 4:
        udp_check.receive_exact(connection, 18)
    elif header[3] == 3:
        length = udp_check.receive_exact(connection, 1)[0]
        udp_check.receive_exact(connection, length + 2)
    else:
        raise RuntimeError(f"SOCKS server returned address type {header[3]}")


def socks_connect(endpoint: tuple[str, int], destination: tuple[str, int], timeout: float) -> socket.socket:
    connection = socket.create_connection(endpoint, timeout)
    connection.settimeout(timeout)
    try:
        connection.sendall(b"\x05\x01\x00")
        if udp_check.receive_exact(connection, 2) != b"\x05\x00":
            raise RuntimeError("SOCKS server refused no-authentication mode")
        connection.sendall(b"\x05\x01\x00" + udp_check.encode_address(*destination))
        read_socks_reply(connection)
        return connection
    except Exception:
        connection.close()
        raise


def parse_https_response(response: bytes) -> tuple[int, bytes]:
    if b"\r\n\r\n" not in response:
        raise RuntimeError("HTTPS response has no complete header")

    class ResponseSocket:
        def makefile(self, mode):
            return io.BytesIO(response)

    try:
        parsed = http.client.HTTPResponse(ResponseSocket())
        parsed.begin()
        if not 200 <= parsed.status < 300:
            raise RuntimeError(f"HTTPS status {parsed.status}")
        # Decode chunked transfer framing and reject truncated Content-Length
        # bodies before counting this request as successful.
        body = parsed.read()
        return parsed.status, body
    except (http.client.HTTPException, ValueError) as error:
        raise RuntimeError(f"invalid or incomplete HTTPS response: {error}") from error


def verify_body_hash(result: dict, expected: str | None) -> None:
    if expected is not None and result["body_sha256"] != expected:
        raise RuntimeError("HTTPS body SHA-256 does not match the expected fixture")


def https_probe(
    endpoint: tuple[str, int], host: str, port: int, path: str, timeout: float, max_bytes: int
) -> dict:
    started = time.monotonic()
    connection = socks_connect(endpoint, (host, port), timeout)
    try:
        context = ssl.create_default_context()
        tls = context.wrap_socket(connection, server_hostname=host)
        connection = None
        try:
            request = (
                f"GET {path} HTTP/1.1\r\nHost: {host}\r\n"
                "User-Agent: queqiao-field-soak/1\r\nAccept: */*\r\nConnection: close\r\n\r\n"
            ).encode("ascii")
            tls.sendall(request)
            response = bytearray()
            while True:
                block = tls.recv(min(64 * 1024, max_bytes + 1 - len(response)))
                if not block:
                    break
                response.extend(block)
                if len(response) > max_bytes:
                    raise RuntimeError(f"HTTPS response exceeds {max_bytes} byte limit")
            status, body = parse_https_response(bytes(response))
            return {
                "status": "ok",
                "http_status": status,
                "body_bytes": len(body),
                "body_sha256": hashlib.sha256(body).hexdigest(),
                "seconds": round(time.monotonic() - started, 6),
            }
        finally:
            tls.close()
    finally:
        if connection is not None:
            connection.close()


def dns_probe(
    udp: socket.socket, destination: tuple[str, int], name: str, timeout: float
) -> dict:
    started = time.monotonic()
    transaction = secrets.randbelow(65536)
    question = udp_check.encode_dns_name(name) + struct.pack("!HH", 1, 1)
    dns = struct.pack("!HHHHHH", transaction, 0x0100, 1, 0, 0, 0) + question
    packet = b"\x00\x00\x00" + udp_check.encode_address(*destination) + dns
    udp.send(packet)
    deadline = started + timeout
    while True:
        udp.settimeout(max(0.001, deadline - time.monotonic()))
        reply = udp.recv(65535)
        if len(reply) < 4 or reply[:3] != b"\x00\x00\x00":
            raise ValueError("invalid SOCKS UDP header")
        _, _, offset = udp_check.decode_address(reply, 3)
        dns_reply = reply[offset:]
        if len(dns_reply) < 2:
            raise ValueError("truncated DNS reply")
        if struct.unpack("!H", dns_reply[:2])[0] != transaction:
            if time.monotonic() >= deadline:
                raise TimeoutError("timed out after stale DNS replies")
            continue
        udp_check.validate_dns_response(dns_reply, transaction, name)
        return {
            "status": "ok",
            "bytes": len(dns_reply),
            "seconds": round(time.monotonic() - started, 6),
        }


def metrics_snapshot(url: str | None) -> dict[str, float]:
    if not url:
        return {}
    with urllib.request.urlopen(url, timeout=5) as response:
        text = response.read(1024 * 1024).decode("utf-8")
    result: dict[str, float] = {}
    for line in text.splitlines():
        if not line or line.startswith("#") or not line.startswith("queqiao_"):
            continue
        name, separator, value = line.partition(" ")
        if separator:
            result[name] = float(value)
    return result


def process_snapshot(pid: int | None) -> dict:
    if pid is None:
        return {}
    output = subprocess.run(
        ["ps", "-o", "rss=", "-p", str(pid)], check=True, text=True, capture_output=True
    ).stdout.strip()
    result = {"pid": pid, "rss_kib": int(output)}
    proc_fds = pathlib.Path("/proc", str(pid), "fd")
    if proc_fds.is_dir():
        result["file_descriptors"] = len(list(proc_fds.iterdir()))
    elif sys.platform == "darwin":
        lsof = subprocess.run(
            ["lsof", "-a", "-p", str(pid), "-Fn"], check=True, text=True, capture_output=True
        ).stdout.splitlines()
        result["file_descriptors"] = sum(line.startswith("f") for line in lsof)
    return result


def write_json(path: pathlib.Path, value) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_checksums(directory: pathlib.Path) -> None:
    lines = []
    for path in sorted(directory.iterdir()):
        if path.is_file() and path.name != "SHA256SUMS":
            lines.append(f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.name}\n")
    (directory / "SHA256SUMS").write_text("".join(lines), encoding="utf-8")


def resources_settled(start_metrics: dict, end_metrics: dict, start_process: dict, end_process: dict) -> bool:
    for name in ("queqiao_active_flows", "queqiao_replay_bytes_in_use"):
        if name in start_metrics and end_metrics.get(name, float("inf")) > start_metrics[name]:
            return False
    if "file_descriptors" in start_process:
        if end_process.get("file_descriptors", float("inf")) > start_process["file_descriptors"]:
            return False
    return True


def main(arguments=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--socks", type=udp_check.parse_endpoint, default=("127.0.0.1", 1080))
    parser.add_argument("--dns-destination", type=udp_check.parse_endpoint, default=("1.1.1.1", 53))
    parser.add_argument("--dns-name", default="cloudflare.com")
    parser.add_argument("--https-host", default="api.ipify.org")
    parser.add_argument("--https-port", type=int, default=443)
    parser.add_argument("--https-path", default="/")
    parser.add_argument("--duration", type=float, required=True, help="soak duration in seconds")
    parser.add_argument("--interval", type=float, default=5, help="seconds between UDP probes")
    parser.add_argument("--https-every", type=int, default=12, help="run HTTPS every N UDP probes")
    parser.add_argument("--snapshot-every", type=int, default=60, help="record resources every N UDP probes")
    parser.add_argument("--timeout", type=float, default=5)
    parser.add_argument("--max-https-bytes", type=int, default=1024 * 1024)
    parser.add_argument("--min-udp-success-rate", type=float, default=0.95)
    parser.add_argument("--min-https-success-rate", type=float, default=1.0)
    parser.add_argument("--require-final-udp-successes", type=int, default=5)
    parser.add_argument("--expected-https-sha256", help="expected decoded body SHA-256 for a fixed fixture")
    parser.add_argument("--metrics-url")
    parser.add_argument("--pid", type=int)
    parser.add_argument("--settle-timeout", type=float, default=10)
    parser.add_argument("--label", required=True, help="opaque path/cell identifier")
    parser.add_argument("--output-dir", type=pathlib.Path, required=True)
    options = parser.parse_args(arguments)
    if (
        options.duration <= 0
        or options.interval < 0
        or options.https_every <= 0
        or options.snapshot_every <= 0
        or options.timeout <= 0
        or options.settle_timeout < 0
    ):
        parser.error(
            "duration, https-every, snapshot-every, and timeout must be positive; "
            "interval and settle-timeout must be non-negative"
        )
    if not 0 <= options.min_udp_success_rate <= 1 or not 0 <= options.min_https_success_rate <= 1:
        parser.error("minimum success rates must be between zero and one")
    if options.require_final_udp_successes < 0 or options.max_https_bytes <= 0:
        parser.error("final successes must be non-negative and max HTTPS bytes must be positive")
    if not 1 <= options.https_port <= 65535 or not options.https_path.startswith("/"):
        parser.error("HTTPS port must be between 1 and 65535 and path must start with /")
    if options.expected_https_sha256 is not None:
        options.expected_https_sha256 = options.expected_https_sha256.lower()
        if len(options.expected_https_sha256) != 64 or any(
            c not in "0123456789abcdef" for c in options.expected_https_sha256
        ):
            parser.error("expected HTTPS SHA-256 must contain 64 hexadecimal characters")
    if options.output_dir.exists():
        parser.error(f"output directory already exists: {options.output_dir}")
    options.output_dir.mkdir(parents=True)

    manifest = {
        "format": "queqiao-field-soak-v1",
        "label": options.label,
        "started_utc": utc_now(),
        "socks": f"{options.socks[0]}:{options.socks[1]}",
        "dns_destination": f"{options.dns_destination[0]}:{options.dns_destination[1]}",
        "dns_name": options.dns_name,
        "https_origin": f"{options.https_host}:{options.https_port}",
        "https_path": options.https_path,
        "expected_https_sha256": options.expected_https_sha256,
        "duration_seconds": options.duration,
        "interval_seconds": options.interval,
        "https_every": options.https_every,
        "snapshot_every": options.snapshot_every,
        "timeout_seconds": options.timeout,
        "minimum_udp_success_rate": options.min_udp_success_rate,
        "minimum_https_success_rate": options.min_https_success_rate,
        "required_final_udp_successes": options.require_final_udp_successes,
        "platform": sys.platform,
    }
    write_json(options.output_dir / "manifest.json", manifest)
    events_path = options.output_dir / "events.jsonl"
    start_metrics = metrics_snapshot(options.metrics_url)
    start_process = process_snapshot(options.pid)
    write_json(options.output_dir / "start-metrics.json", start_metrics)
    write_json(options.output_dir / "start-process.json", start_process)

    udp_successes = https_successes = udp_attempts = https_attempts = 0
    udp_outcomes: list[bool] = []
    interrupted = False
    fatal_error = ""
    control = udp = None
    started = time.monotonic()
    try:
        control, udp, relay = udp_check.open_association(options.socks, options.timeout)
        manifest["udp_relay"] = f"{relay[0]}:{relay[1]}"
        write_json(options.output_dir / "manifest.json", manifest)
        with events_path.open("x", encoding="utf-8", buffering=1) as events:
            index = 0
            while index == 0 or time.monotonic() - started < options.duration:
                index += 1
                event = {"type": "udp", "index": index, "started_utc": utc_now()}
                try:
                    event.update(dns_probe(udp, options.dns_destination, options.dns_name, options.timeout))
                    udp_successes += 1
                    udp_outcomes.append(True)
                except Exception as error:  # field evidence records and continues
                    event.update({"status": "failed", "error": str(error)[:500]})
                    udp_outcomes.append(False)
                udp_attempts += 1
                events.write(json.dumps(event, sort_keys=True) + "\n")

                if index == 1 or index % options.https_every == 0:
                    event = {"type": "https", "index": index, "started_utc": utc_now()}
                    try:
                        event.update(
                            https_probe(
                                options.socks,
                                options.https_host,
                                options.https_port,
                                options.https_path,
                                options.timeout,
                                options.max_https_bytes,
                            )
                        )
                        verify_body_hash(event, options.expected_https_sha256)
                        https_successes += 1
                    except Exception as error:  # field evidence records and continues
                        event.update({"status": "failed", "error": str(error)[:500]})
                    https_attempts += 1
                    events.write(json.dumps(event, sort_keys=True) + "\n")
                if index % options.snapshot_every == 0:
                    event = {
                        "type": "resource",
                        "index": index,
                        "started_utc": utc_now(),
                        "metrics": metrics_snapshot(options.metrics_url),
                        "process": process_snapshot(options.pid),
                    }
                    events.write(json.dumps(event, sort_keys=True) + "\n")
                remaining = options.duration - (time.monotonic() - started)
                if remaining <= 0:
                    break
                time.sleep(min(options.interval, remaining))
    except KeyboardInterrupt:
        interrupted = True
    except Exception as error:
        fatal_error = str(error)[:500]
        if not events_path.exists():
            events_path.write_text(
                json.dumps(
                    {"type": "fatal", "started_utc": utc_now(), "error": fatal_error},
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )
    finally:
        if udp is not None:
            udp.close()
        if control is not None:
            control.close()

    settle_deadline = time.monotonic() + options.settle_timeout
    while True:
        end_metrics = metrics_snapshot(options.metrics_url)
        end_process = process_snapshot(options.pid)
        if resources_settled(start_metrics, end_metrics, start_process, end_process):
            break
        if time.monotonic() >= settle_deadline:
            break
        time.sleep(0.25)
    settled = resources_settled(start_metrics, end_metrics, start_process, end_process)
    write_json(options.output_dir / "end-metrics.json", end_metrics)
    write_json(options.output_dir / "end-process.json", end_process)
    final_count = options.require_final_udp_successes
    final_successes = sum(udp_outcomes[-final_count:]) if final_count else 0
    udp_rate = udp_successes / udp_attempts if udp_attempts else 0
    https_rate = https_successes / https_attempts if https_attempts else 0
    passed = (
        not interrupted
        and not fatal_error
        and settled
        and udp_rate >= options.min_udp_success_rate
        and https_rate >= options.min_https_success_rate
        and final_successes == final_count
    )
    resources_checked = "file_descriptors" in start_process or any(
        name in start_metrics for name in ("queqiao_active_flows", "queqiao_replay_bytes_in_use")
    )
    summary = {
        "finished_utc": utc_now(),
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "interrupted": interrupted,
        "fatal_error": fatal_error,
        "udp_attempts": udp_attempts,
        "udp_successes": udp_successes,
        "udp_success_rate": udp_rate,
        "https_attempts": https_attempts,
        "https_successes": https_successes,
        "https_success_rate": https_rate,
        "final_udp_successes": final_successes,
        "required_final_udp_successes": final_count,
        "resources_checked": resources_checked,
        "body_integrity_checked": options.expected_https_sha256 is not None and https_attempts > 0,
        "resources_settled": settled if resources_checked else None,
        "metrics_delta": {
            key: end_metrics.get(key, 0) - start_metrics.get(key, 0)
            for key in sorted(set(start_metrics) | set(end_metrics))
        },
        "passed": passed,
    }
    write_json(options.output_dir / "summary.json", summary)
    write_checksums(options.output_dir)
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
