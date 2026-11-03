"""Robustness tests (Phase 9).

Covers:
  R1  malformed / garbage datagrams must not break or crash a transfer
  R2  sender with no receiver listening fails cleanly (no hang, no success)
  R3  sender behind a path that drops everything fails cleanly
  R4  receiver with no sender times out cleanly
  R5  interrupted transfer is NOT reported as success, and a fresh transfer
      afterwards (same output dir, new ports) still succeeds

ASSUMPTIONS (inferred from tests/test_sr.py, adjust if your code differs):
  - FileReceiver(bind_addr, output_dir, timeout, mode).run_once()
        -> tuple whose first element is an outcome string ("success" on success)
  - FileSender(server_addr, timeout, rto, mode, window_size).send_file(path)
        -> (outcome, exit_code); ("success", 0) on success
  - ImpairmentProxy has start() / stop()
  - HEADER_SIZE is exported by rtp.packet
"""

from __future__ import annotations

import random
import socket
import threading
import time
from pathlib import Path

from app.file_receiver import FileReceiver
from app.file_sender import FileSender
from rtp.packet import HEADER_SIZE
from tools.impair_proxy import DirectionConfig, ImpairmentProxy


# ---------------------------------------------------------------- helpers

def _find_free_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _make_file(tmp_path: Path, name: str, size: int) -> tuple[Path, bytes]:
    content = bytes(i % 251 for i in range(size))
    p = tmp_path / name
    p.write_bytes(content)
    return p, content


def _start_receiver(port: int, recv_dir: Path, timeout: float):
    recv_dir.mkdir(parents=True, exist_ok=True)
    receiver = FileReceiver(
        bind_addr=("127.0.0.1", port),
        output_dir=recv_dir,
        timeout=timeout,
        mode="sr",
    )
    results: list = []

    def worker():
        results.append(receiver.run_once())

    th = threading.Thread(target=worker, daemon=True)
    th.start()
    time.sleep(0.05)
    return th, results


def _make_sender(port: int, timeout: float = 3.0, window: int = 8) -> FileSender:
    return FileSender(
        server_addr=("127.0.0.1", port),
        timeout=timeout,
        rto=0.1,
        mode="sr",
        window_size=window,
    )


def _files_equal_to(recv_dir: Path, content: bytes) -> bool:
    return any(f.is_file() and f.read_bytes() == content for f in recv_dir.iterdir())


# ------------------------------------------------------------------ R1

def test_malformed_datagrams_do_not_break_transfer(tmp_path: Path):
    """Garbage datagrams aimed at the receiver are ignored; transfer still succeeds."""
    src, content = _make_file(tmp_path, "src.bin", 32 * 1024)
    recv_dir = tmp_path / "received"
    server_port = _find_free_port()

    th, results = _start_receiver(server_port, recv_dir, timeout=10.0)

    stop_flag = threading.Event()

    def garbage_sender():
        rng = random.Random(7)
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            while not stop_flag.is_set():
                kind = rng.randrange(4)
                if kind == 0:
                    data = b""                                   # empty datagram
                elif kind == 1:
                    data = bytes(rng.randrange(256) for _ in range(HEADER_SIZE - 1))  # too short
                elif kind == 2:
                    data = bytes(rng.randrange(256) for _ in range(HEADER_SIZE))      # random header
                else:
                    n = HEADER_SIZE + rng.randrange(1, 200)
                    data = bytes(rng.randrange(256) for _ in range(n))                # random + payload
                try:
                    sock.sendto(data, ("127.0.0.1", server_port))
                except OSError:
                    pass
                time.sleep(0.002)
        finally:
            sock.close()

    gth = threading.Thread(target=garbage_sender, daemon=True)
    gth.start()

    outcome, code = _make_sender(server_port, timeout=10.0, window=16).send_file(src)

    stop_flag.set()
    gth.join(timeout=2.0)
    th.join(timeout=10.0)

    assert outcome == "success"
    assert code == 0
    assert _files_equal_to(recv_dir, content)


# ------------------------------------------------------------------ R2

def test_sender_fails_cleanly_when_no_receiver(tmp_path: Path):
    src, _ = _make_file(tmp_path, "src.bin", 4 * 1024)
    dead_port = _find_free_port()  # nothing listening

    t0 = time.monotonic()
    outcome, code = _make_sender(dead_port, timeout=2.0).send_file(src)
    elapsed = time.monotonic() - t0

    assert outcome != "success"
    assert code != 0
    assert elapsed < 30.0  # bounded, no hang


# ------------------------------------------------------------------ R3

def test_sender_fails_cleanly_when_path_drops_everything(tmp_path: Path):
    src, _ = _make_file(tmp_path, "src.bin", 4 * 1024)
    recv_dir = tmp_path / "received"
    server_port = _find_free_port()
    proxy_port = _find_free_port()

    proxy = ImpairmentProxy(
        listen_addr=("127.0.0.1", proxy_port),
        target_addr=("127.0.0.1", server_port),
        c2s_config=DirectionConfig(drop_rate=1.0),
        s2c_config=DirectionConfig(drop_rate=1.0),
        seed=1,
    )
    proxy.start()
    th, results = _start_receiver(server_port, recv_dir, timeout=3.0)

    t0 = time.monotonic()
    outcome, code = _make_sender(proxy_port, timeout=2.0).send_file(src)
    elapsed = time.monotonic() - t0

    th.join(timeout=10.0)
    proxy.stop()

    assert outcome != "success"
    assert code != 0
    assert elapsed < 30.0
    assert not _files_equal_to(recv_dir, src.read_bytes())


# ------------------------------------------------------------------ R4

def test_receiver_times_out_when_no_sender(tmp_path: Path):
    recv_dir = tmp_path / "received"
    server_port = _find_free_port()

    th, results = _start_receiver(server_port, recv_dir, timeout=1.0)
    th.join(timeout=10.0)

    assert not th.is_alive()          # run_once returned, no hang
    assert len(results) == 1
    assert results[0][0] != "success"


# ------------------------------------------------------------------ R5

def test_interrupted_transfer_is_not_success_and_fresh_transfer_works(tmp_path: Path):
    src, content = _make_file(tmp_path, "big.bin", 256 * 1024)
    recv_dir = tmp_path / "received"

    # --- first transfer: kill the path partway through ---
    server_port = _find_free_port()
    proxy_port = _find_free_port()
    proxy = ImpairmentProxy(
        listen_addr=("127.0.0.1", proxy_port),
        target_addr=("127.0.0.1", server_port),
        c2s_config=DirectionConfig(delay_ms=20.0),
        s2c_config=DirectionConfig(delay_ms=20.0),
        seed=3,
    )
    proxy.start()
    th, results = _start_receiver(server_port, recv_dir, timeout=3.0)

    killer = threading.Timer(0.3, proxy.stop)
    killer.start()

    outcome, code = _make_sender(proxy_port, timeout=2.0, window=8).send_file(src)

    killer.join()
    th.join(timeout=15.0)

    assert outcome != "success"
    assert code != 0
    assert len(results) == 1
    assert results[0][0] != "success"
    # an incomplete transfer must not leave a file that looks complete
    assert not _files_equal_to(recv_dir, content)

    # --- second transfer: new ports, same output dir, no proxy ---
    server_port2 = _find_free_port()
    th2, results2 = _start_receiver(server_port2, recv_dir, timeout=10.0)
    outcome2, code2 = _make_sender(server_port2, timeout=10.0, window=16).send_file(src)
    th2.join(timeout=10.0)

    assert outcome2 == "success"
    assert code2 == 0
    assert _files_equal_to(recv_dir, content)