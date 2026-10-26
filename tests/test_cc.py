"""Tests for Phase 7: Congestion Control (Slow Start and AIMD).

Unit tests for CongestionControl:
- slow start +1 per ACK
- transition at ssthresh
- congestion avoidance about +1 per cwnd ACKs
- timeout reduction
- cap at max_window

Integration tests (asserted from cwnd.csv and transfers):
- on a clean link cwnd grows to the cap
- one forced loss gives a reduction, then slow start to the new ssthresh, then linear growth
- a burst loss reduces only once
- with a small rwnd the effective window is rwnd-limited while cwnd is larger
- 2 MB transfers complete with matching SHA-256 at 1% and 5% loss
"""

from __future__ import annotations

import csv
import hashlib
import math
import socket
import threading
import time
from pathlib import Path

import pytest

from app.file_receiver import FileReceiver
from app.file_sender import FileSender
from rtp.config import DEFAULT_MAX_WINDOW, FLAG_DATA
from rtp.cwnd import CongestionControl
from tools.impair_proxy import DirectionConfig, ImpairmentProxy


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def _compute_sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _read_cwnd_csv(csv_path: Path) -> list[dict[str, str]]:
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return list(reader)


# ---------------------------------------------------------------------------
# Unit tests
# ---------------------------------------------------------------------------

def test_cc_slow_start_plus_one_per_ack():
    """Slow start increases cwnd by exactly 1.0 per newly acked packet."""
    cc = CongestionControl(initial_cwnd=1.0, initial_ssthresh=16, max_window=32)
    assert cc.cwnd == 1.0
    assert cc.usable_window == 1
    assert cc.ssthresh == 16

    for expected in range(2, 10):
        transitioned = cc.on_new_ack()
        assert cc.cwnd == float(expected)
        assert cc.usable_window == expected
        assert transitioned is False


def test_cc_transition_at_ssthresh():
    """Transition from slow start to congestion avoidance occurs when cwnd >= ssthresh."""
    cc = CongestionControl(initial_cwnd=1.0, initial_ssthresh=4, max_window=32)

    # ACK 1: cwnd 1 -> 2 (< 4, no transition)
    assert cc.on_new_ack() is False
    assert cc.cwnd == 2.0

    # ACK 2: cwnd 2 -> 3 (< 4, no transition)
    assert cc.on_new_ack() is False
    assert cc.cwnd == 3.0

    # ACK 3: cwnd 3 -> 4 (== ssthresh 4, transitions to CA)
    assert cc.on_new_ack() is True
    assert cc.cwnd == 4.0

    # ACK 4: already in CA, +1/4 = 4.25
    assert cc.on_new_ack() is False
    assert math.isclose(cc.cwnd, 4.25)


def test_cc_congestion_avoidance_linear_growth():
    """In congestion avoidance, cwnd grows by approximately +1.0 per cwnd ACKs."""
    initial_w = 10.0
    cc = CongestionControl(initial_cwnd=initial_w, initial_ssthresh=10, max_window=64)
    assert cc.cwnd == 10.0

    # Deliver 10 ACKs
    for _ in range(10):
        assert cc.on_new_ack() is False

    # Sum of 1/w for 10 steps from 10.0 is approx 0.956
    assert 10.9 < cc.cwnd < 11.05
    assert cc.usable_window == 10


def test_cc_timeout_reduction():
    """Timeout sets ssthresh = max(floor(cwnd / 2), 2) and cwnd = 1.0."""
    # Case 1: large cwnd
    cc = CongestionControl(initial_cwnd=15.7, initial_ssthresh=32, max_window=64)
    cc.on_timeout()
    assert cc.ssthresh == 7  # floor(15.7 / 2) = 7
    assert cc.cwnd == 1.0
    assert cc.usable_window == 1

    # Case 2: small cwnd clamp to 2
    cc.cwnd = 3.5
    cc.on_timeout()
    assert cc.ssthresh == 2  # max(floor(3.5 / 2), 2) = max(1, 2) = 2
    assert cc.cwnd == 1.0

    # Case 3: cwnd = 1.0
    cc.on_timeout()
    assert cc.ssthresh == 2  # max(0, 2) = 2
    assert cc.cwnd == 1.0


def test_cc_cap_at_max_window():
    """Cwnd is strictly capped at max_window in both slow start and CA."""
    max_w = 5
    cc = CongestionControl(initial_cwnd=1.0, initial_ssthresh=10, max_window=max_w)

    # Slow start up to cap
    for _ in range(10):
        cc.on_new_ack()

    assert cc.cwnd == float(max_w)
    assert cc.usable_window == max_w

    # Additional ACKs in CA do not exceed max_window
    for _ in range(20):
        cc.on_new_ack()

    assert cc.cwnd == float(max_w)
    assert cc.usable_window == max_w


# ---------------------------------------------------------------------------
# Integration tests
# ---------------------------------------------------------------------------

def test_cc_clean_link_growth_to_cap(tmp_path: Path):
    """On a clean link cwnd grows to the cap."""
    size = 35 * 1024
    content = bytes([(i * 7) % 256 for i in range(size)])
    expected_digest = _compute_sha256(content)

    src_file = tmp_path / "clean_src.bin"
    src_file.write_bytes(content)

    recv_dir = tmp_path / "clean_recv"
    recv_dir.mkdir(parents=True, exist_ok=True)
    run_dir = tmp_path / "sender_run"
    run_dir.mkdir(parents=True, exist_ok=True)

    server_port = _find_free_port()
    receiver = FileReceiver(
        bind_addr=("127.0.0.1", server_port),
        output_dir=recv_dir,
        timeout=15.0,
        mode="sr",
        capacity=64,
    )

    results: list[tuple[str, str | None, int]] = []

    def recv_worker():
        results.append(receiver.run_once())

    th = threading.Thread(target=recv_worker, daemon=True)
    th.start()
    time.sleep(0.05)

    max_w = 16
    sender = FileSender(
        server_addr=("127.0.0.1", server_port),
        timeout=15.0,
        rto=0.08,
        mode="sr",
        window_size=max_w,
        run_dir=run_dir,
    )

    outcome, code = sender.send_file(src_file)
    th.join(timeout=15.0)

    assert outcome == "success"
    assert code == 0

    cwnd_csv = run_dir / "cwnd.csv"
    assert cwnd_csv.exists()
    rows = _read_cwnd_csv(cwnd_csv)
    assert len(rows) > 0

    # Final row has cwnd at the cap (16.0)
    final_cwnd = float(rows[-1]["cwnd"])
    assert final_cwnd == float(max_w)
    cwnds = [float(r["cwnd"]) for r in rows]
    assert max(cwnds) == float(max_w)

    dest_file = recv_dir / "clean_src.bin"
    assert dest_file.exists()
    assert _compute_sha256(dest_file.read_bytes()) == expected_digest


def test_cc_forced_single_loss_reduction_and_recovery(tmp_path: Path):
    """One forced loss gives a reduction, then slow start to the new ssthresh, then linear growth."""
    size = 60 * 1024
    content = bytes([(i * 13) % 256 for i in range(size)])
    expected_digest = _compute_sha256(content)

    src_file = tmp_path / "forced_loss_src.bin"
    src_file.write_bytes(content)

    recv_dir = tmp_path / "forced_loss_recv"
    recv_dir.mkdir(parents=True, exist_ok=True)
    run_dir = tmp_path / "forced_loss_run"
    run_dir.mkdir(parents=True, exist_ok=True)

    server_port = _find_free_port()
    proxy_port = _find_free_port()

    proxy = ImpairmentProxy(
        listen_addr=("127.0.0.1", proxy_port),
        target_addr=("127.0.0.1", server_port),
        seed=42,
    )
    # Drop packet seq=8 when cwnd is in slow start
    proxy.drop_next(direction="c2s", count=1, flags=FLAG_DATA, seq=8)
    proxy.start()

    receiver = FileReceiver(
        bind_addr=("127.0.0.1", server_port),
        output_dir=recv_dir,
        timeout=20.0,
        mode="sr",
        capacity=64,
    )

    results: list[tuple[str, str | None, int]] = []

    def recv_worker():
        results.append(receiver.run_once())

    th = threading.Thread(target=recv_worker, daemon=True)
    th.start()
    time.sleep(0.05)

    sender = FileSender(
        server_addr=("127.0.0.1", proxy_port),
        timeout=20.0,
        rto=0.08,
        mode="sr",
        window_size=32,
        run_dir=run_dir,
    )

    outcome, code = sender.send_file(src_file)
    th.join(timeout=20.0)
    proxy.stop()

    assert outcome == "success"
    assert code == 0

    rows = _read_cwnd_csv(run_dir / "cwnd.csv")
    events = [r["event"] for r in rows]
    assert "timeout" in events, f"Expected timeout event in {events}"

    # Find the timeout row
    timeout_idx = events.index("timeout")
    timeout_row = rows[timeout_idx]
    assert float(timeout_row["cwnd"]) == 1.0
    new_ssthresh = int(timeout_row["ssthresh"])
    assert new_ssthresh >= 2

    # After timeout: verify slow start up to new_ssthresh, then ss_exit or linear CA growth
    post_timeout_rows = rows[timeout_idx + 1:]
    post_events = [r["event"] for r in post_timeout_rows]
    assert "ss_exit" in post_events or any(float(r["cwnd"]) >= new_ssthresh for r in post_timeout_rows)

    dest_file = recv_dir / "forced_loss_src.bin"
    assert dest_file.exists()
    assert _compute_sha256(dest_file.read_bytes()) == expected_digest


def test_cc_burst_loss_reduces_only_once(tmp_path: Path):
    """A burst loss reduces cwnd only once due to the loss-epoch gate."""
    size = 60 * 1024
    content = bytes([(i * 19) % 256 for i in range(size)])
    expected_digest = _compute_sha256(content)

    src_file = tmp_path / "burst_loss_src.bin"
    src_file.write_bytes(content)

    recv_dir = tmp_path / "burst_loss_recv"
    recv_dir.mkdir(parents=True, exist_ok=True)
    run_dir = tmp_path / "burst_loss_run"
    run_dir.mkdir(parents=True, exist_ok=True)

    server_port = _find_free_port()
    proxy_port = _find_free_port()

    proxy = ImpairmentProxy(
        listen_addr=("127.0.0.1", proxy_port),
        target_addr=("127.0.0.1", server_port),
        seed=123,
    )
    # Drop a burst of 3 consecutive packets: seq 10, 11, 12
    proxy.drop_next(direction="c2s", count=1, flags=FLAG_DATA, seq=10)
    proxy.drop_next(direction="c2s", count=1, flags=FLAG_DATA, seq=11)
    proxy.drop_next(direction="c2s", count=1, flags=FLAG_DATA, seq=12)
    proxy.start()

    receiver = FileReceiver(
        bind_addr=("127.0.0.1", server_port),
        output_dir=recv_dir,
        timeout=20.0,
        mode="sr",
        capacity=64,
    )

    results: list[tuple[str, str | None, int]] = []

    def recv_worker():
        results.append(receiver.run_once())

    th = threading.Thread(target=recv_worker, daemon=True)
    th.start()
    time.sleep(0.05)

    sender = FileSender(
        server_addr=("127.0.0.1", proxy_port),
        timeout=20.0,
        rto=0.08,
        mode="sr",
        window_size=32,
        run_dir=run_dir,
    )

    outcome, code = sender.send_file(src_file)
    th.join(timeout=20.0)
    proxy.stop()

    assert outcome == "success"
    assert code == 0

    rows = _read_cwnd_csv(run_dir / "cwnd.csv")
    timeout_rows = [r for r in rows if r["event"] == "timeout"]
    # All 3 dropped packets are part of the same loss epoch, so only 1 reduction occurs
    assert len(timeout_rows) == 1, f"Expected exactly 1 timeout reduction, got {len(timeout_rows)}"

    dest_file = recv_dir / "burst_loss_src.bin"
    assert dest_file.exists()
    assert _compute_sha256(dest_file.read_bytes()) == expected_digest


def test_cc_rwnd_limited_effective_window(tmp_path: Path):
    """With a small rwnd the effective window is rwnd-limited while cwnd is larger."""
    size = 40 * 1024
    content = bytes([(i * 23) % 256 for i in range(size)])
    expected_digest = _compute_sha256(content)

    src_file = tmp_path / "rwnd_lim_src.bin"
    src_file.write_bytes(content)

    recv_dir = tmp_path / "rwnd_lim_recv"
    recv_dir.mkdir(parents=True, exist_ok=True)
    run_dir = tmp_path / "rwnd_lim_run"
    run_dir.mkdir(parents=True, exist_ok=True)

    server_port = _find_free_port()
    # Receiver capacity = 4, so rwnd <= 4
    receiver = FileReceiver(
        bind_addr=("127.0.0.1", server_port),
        output_dir=recv_dir,
        timeout=20.0,
        mode="sr",
        capacity=4,
    )

    results: list[tuple[str, str | None, int]] = []

    def recv_worker():
        results.append(receiver.run_once())

    th = threading.Thread(target=recv_worker, daemon=True)
    th.start()
    time.sleep(0.05)

    sender = FileSender(
        server_addr=("127.0.0.1", server_port),
        timeout=20.0,
        rto=0.08,
        mode="sr",
        window_size=32,
        run_dir=run_dir,
    )

    outcome, code = sender.send_file(src_file)
    th.join(timeout=20.0)

    assert outcome == "success"
    assert code == 0

    sr_sender = sender._last_sender
    assert sr_sender is not None
    assert sr_sender.limited_by_rwnd > 0, f"Expected limited_by_rwnd > 0, got {sr_sender.limited_by_rwnd}"

    rows = _read_cwnd_csv(run_dir / "cwnd.csv")
    rwnd_limited_rows = [
        r for r in rows
        if float(r["cwnd"]) > float(r["rwnd"]) and int(r["effective_window"]) <= int(r["rwnd"])
    ]
    assert len(rwnd_limited_rows) > 0, "Expected rows where effective_window is limited by rwnd while cwnd is larger"

    dest_file = recv_dir / "rwnd_lim_src.bin"
    assert dest_file.exists()
    assert _compute_sha256(dest_file.read_bytes()) == expected_digest


def _run_2mb_transfer_with_loss(tmp_path: Path, drop_rate: float, seed: int):
    size = 2 * 1024 * 1024  # 2 MB
    content = bytes([(i * 5 + 3) % 256 for i in range(size)])
    expected_digest = _compute_sha256(content)

    name = f"2mb_{int(drop_rate * 100)}pct"
    src_file = tmp_path / f"{name}_src.bin"
    src_file.write_bytes(content)

    recv_dir = tmp_path / f"{name}_recv"
    recv_dir.mkdir(parents=True, exist_ok=True)

    server_port = _find_free_port()
    proxy_port = _find_free_port()

    c2s = DirectionConfig(drop_rate=drop_rate)
    s2c = DirectionConfig(drop_rate=drop_rate)
    proxy = ImpairmentProxy(
        listen_addr=("127.0.0.1", proxy_port),
        target_addr=("127.0.0.1", server_port),
        c2s_config=c2s,
        s2c_config=s2c,
        seed=seed,
    )
    proxy.start()

    receiver = FileReceiver(
        bind_addr=("127.0.0.1", server_port),
        output_dir=recv_dir,
        timeout=90.0,
        mode="sr",
        capacity=64,
    )

    results: list[tuple[str, str | None, int]] = []

    def recv_worker():
        results.append(receiver.run_once())

    th = threading.Thread(target=recv_worker, daemon=True)
    th.start()
    time.sleep(0.05)

    sender = FileSender(
        server_addr=("127.0.0.1", proxy_port),
        timeout=90.0,
        rto=0.05,
        mode="sr",
        window_size=64,
    )

    outcome, code = sender.send_file(src_file)
    th.join(timeout=90.0)
    proxy.stop()

    assert outcome == "success"
    assert code == 0

    dest_file = recv_dir / f"{name}_src.bin"
    assert dest_file.exists()
    assert _compute_sha256(dest_file.read_bytes()) == expected_digest


@pytest.mark.slow
def test_cc_2mb_transfer_1pct_loss(tmp_path: Path):
    """2 MB transfer completes with matching SHA-256 at 1% loss."""
    _run_2mb_transfer_with_loss(tmp_path, drop_rate=0.01, seed=101)


@pytest.mark.slow
def test_cc_2mb_transfer_5pct_loss(tmp_path: Path):
    """2 MB transfer completes with matching SHA-256 at 5% loss."""
    _run_2mb_transfer_with_loss(tmp_path, drop_rate=0.05, seed=202)
