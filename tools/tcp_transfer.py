"""TCP file-transfer baseline for the RTP project.

Timing is deliberately the SAME as the RTP sender (app/file_sender.py):
    t_start : just before connect()          (RTP: just before the SYN)
    t_end   : when the receiver's integrity verdict arrives at the sender
              (RTP: when the final status message arrives; teardown excluded)
    goodput : 8 * file_bytes / (t_end - t_start) / 1e6   [Mbit/s]
The source SHA-256 is computed BEFORE t_start and sent in the header; the
receiver hashes the received bytes and replies with a 1-byte verdict.

Usage (run inside Mininet hosts, from the project root):
    python -m tools.tcp_transfer recv --host 10.0.3.10 --port 9000 --output-dir DIR --run-dir DIR --timeout 600
    python -m tools.tcp_transfer send --file F --host 10.0.3.10 --port 9000 --run-dir DIR --timeout 600 --cc reno
"""

from __future__ import annotations

import argparse
import hashlib
import json
import socket
import struct
import sys
import time
from pathlib import Path

TCP_CONGESTION = getattr(socket, "TCP_CONGESTION", 13)
CHUNK = 64 * 1024
HEADER = struct.Struct("!Q")          # 8-byte file size, followed by 32-byte SHA-256


def recv_exact(sock: socket.socket, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        part = sock.recv(n - len(buf))
        if not part:
            raise ConnectionError("peer closed connection early")
        buf += part
    return bytes(buf)


def tcp_info(sock: socket.socket):
    """Kernel TCP statistics (Linux struct tcp_info). None if unavailable."""
    try:
        raw = sock.getsockopt(socket.IPPROTO_TCP, socket.TCP_INFO, 192)
        v = struct.unpack_from("=8B24I", raw, 0)
        u32 = v[8:]
        return {
            "snd_mss": u32[2],
            "srtt_ms": u32[15] / 1000.0,
            "snd_ssthresh": u32[17],
            "snd_cwnd_segments": u32[18],
            "total_retrans": u32[23],
        }
    except Exception:
        return None


def write_json(run_dir: str | None, sub: str, name: str, data: dict) -> None:
    if not run_dir:
        return
    d = Path(run_dir)
    d.mkdir(parents=True, exist_ok=True)
    (d / name).write_text(json.dumps(data, indent=2))


def cmd_send(a) -> int:
    src = Path(a.file)
    size = src.stat().st_size
    h = hashlib.sha256()
    with open(src, "rb") as f:
        for blk in iter(lambda: f.read(CHUNK), b""):
            h.update(blk)
    digest = h.digest()                              # computed before the timer starts

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.setsockopt(socket.IPPROTO_TCP, TCP_CONGESTION, a.cc.encode())
    except OSError as e:
        print(f"cannot set congestion control '{a.cc}': {e}")
        return 2
    s.settimeout(a.timeout)

    status, reason, info = "failed", None, None
    t_start = t_end = None
    try:
        t_start = time.monotonic()                   # just before connect()
        s.connect((a.host, a.port))
        s.sendall(HEADER.pack(size) + digest)
        with open(src, "rb") as f:
            for blk in iter(lambda: f.read(CHUNK), b""):
                s.sendall(blk)
        verdict = recv_exact(s, 1)
        t_end = time.monotonic()                     # verdict received
        info = tcp_info(s)
        status = "success" if verdict == b"\x01" else "failed"
        if status == "failed":
            reason = "receiver reported hash mismatch"
    except (OSError, ConnectionError) as e:
        reason = f"{type(e).__name__}: {e}"
    finally:
        s.close()

    duration = (t_end - t_start) if (t_end is not None and t_start is not None) else None
    summary = {
        "file_bytes": size,
        "t_start": t_start,
        "t_end": t_end,
        "duration_s": duration,
        "goodput_mbps": (8 * size / duration / 1e6) if duration else None,
        "status": status,
        "exit_reason": reason or status,
        "protocol": "tcp",
        "cc": a.cc,
        "retx_total": info["total_retrans"] if info else None,   # kernel-counted retransmitted segments
        "final_srtt": (info["srtt_ms"] / 1000.0) if info else None,
        "tcp_info": info,
    }
    write_json(a.run_dir, "tx", "sender_summary.json", summary)
    print(status if not reason else f"{status}: {reason}")
    return 0 if status == "success" else 1


def cmd_recv(a) -> int:
    out = Path(a.output_dir)
    out.mkdir(parents=True, exist_ok=True)
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((a.host, a.port))
    srv.listen(1)
    srv.settimeout(a.timeout)

    status, reason, got, hash_ok = "failed", None, 0, False
    try:
        conn, _ = srv.accept()
        conn.settimeout(a.timeout)
        hdr = recv_exact(conn, HEADER.size + 32)
        (size,) = HEADER.unpack(hdr[:HEADER.size])
        want = hdr[HEADER.size:]
        h = hashlib.sha256()
        with open(out / "received.bin", "wb") as f:
            while got < size:
                blk = conn.recv(min(CHUNK, size - got))
                if not blk:
                    raise ConnectionError("sender closed before the file was complete")
                f.write(blk)
                h.update(blk)
                got += len(blk)
        hash_ok = h.digest() == want
        conn.sendall(b"\x01" if hash_ok else b"\x00")
        status = "success" if hash_ok else "failed"
        try:
            conn.recv(1)                             # wait for the sender to close
        except OSError:
            pass
        conn.close()
    except (OSError, ConnectionError) as e:
        reason = f"{type(e).__name__}: {e}"
    finally:
        srv.close()

    write_json(a.run_dir, "rx", "receiver_summary.json", {
        "protocol": "tcp", "bytes_received": got, "hash_ok": hash_ok,
        "status": status, "exit_reason": reason or status})
    print(status if not reason else f"{status}: {reason}")
    return 0 if status == "success" else 1


def main() -> int:
    ap = argparse.ArgumentParser(description="TCP file-transfer baseline")
    sub = ap.add_subparsers(dest="cmd", required=True)

    r = sub.add_parser("recv")
    r.add_argument("--host", default="0.0.0.0")
    r.add_argument("--port", type=int, default=9000)
    r.add_argument("--output-dir", required=True)
    r.add_argument("--run-dir")
    r.add_argument("--timeout", type=float, default=600)

    s = sub.add_parser("send")
    s.add_argument("--file", required=True)
    s.add_argument("--host", default="127.0.0.1")
    s.add_argument("--port", type=int, default=9000)
    s.add_argument("--run-dir")
    s.add_argument("--timeout", type=float, default=600)
    s.add_argument("--cc", default="reno", help="TCP congestion control (reno or cubic)")

    a = ap.parse_args()
    return cmd_recv(a) if a.cmd == "recv" else cmd_send(a)


if __name__ == "__main__":
    sys.exit(main())
