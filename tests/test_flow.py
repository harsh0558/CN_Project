"""Tests for Phase 6: Receiver Flow Control, Window Updates, and Persist Timer.

Spec-compliant tests:
1. with R=4 and a slow app, no packet is ever outstanding at seq >= send_base + rwnd (instrument the sender);
2. occupancy never exceeds R;
3. zero window is reached and recovered, including when the window-update ACK is lost (the probe recovers it);
4. a 1 MB transfer with R=4, a slow app and 5% loss in both directions completes with a matching SHA-256 (mark slow if over 5 s);
5. probes consume no sequence number and do not change the counter invariant data_sent_total = unique_data_packets + retx_total.
"""

from __future__ import annotations

import hashlib
import socket
import threading
import time
from pathlib import Path

import pytest

from app.file_receiver import FileReceiver
from app.file_sender import FileSender
from rtp.config import FLAG_ACK, FLAG_DATA, PROBE_ECHO_SEQ
from rtp.connection import RtpClient
from rtp.packet import Packet
from tools.impair_proxy import DirectionConfig, ImpairmentProxy


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def _compute_sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


# ---------------------------------------------------------------------------
# Test 1: No packet is ever outstanding at seq >= send_base + rwnd
# ---------------------------------------------------------------------------

def test_no_packet_outstanding_beyond_window(tmp_path: Path):
    """With R=4 and a slow app, no packet is ever outstanding at seq >= send_base + rwnd."""
    size = 30 * 1024
    content = bytes([(i * 3) % 256 for i in range(size)])
    expected_digest = _compute_sha256(content)

    src_file = tmp_path / "flow_wnd_src.bin"
    src_file.write_bytes(content)

    recv_dir = tmp_path / "flow_wnd_recv"
    recv_dir.mkdir(parents=True, exist_ok=True)

    server_port = _find_free_port()
    receiver = FileReceiver(
        bind_addr=("127.0.0.1", server_port),
        output_dir=recv_dir,
        timeout=15.0,
        mode="sr",
        capacity=4,
        app_read_rate=40.0,  # 40 packets/s
    )

    results: list[tuple[str, str | None, int]] = []

    def recv_worker():
        results.append(receiver.run_once())

    th = threading.Thread(target=recv_worker, daemon=True)
    th.start()
    time.sleep(0.05)

    sender = FileSender(
        server_addr=("127.0.0.1", server_port),
        timeout=15.0,
        rto=0.2,
        mode="sr",
        window_size=16,  # sender window is 16, throttled by receiver R=4
    )

    outcome, code = sender.send_file(src_file)
    th.join(timeout=15.0)

    assert outcome == "success"
    assert code == 0

    sr_sender = sender._last_sender
    assert sr_sender is not None
    # Sender instrumented check: no packet was ever transmitted at seq >= send_base + rwnd
    assert len(sr_sender.window_violations) == 0, (
        f"Detected window violations: {sr_sender.window_violations}"
    )

    dest_file = recv_dir / "flow_wnd_src.bin"
    assert dest_file.exists()
    assert _compute_sha256(dest_file.read_bytes()) == expected_digest


# ---------------------------------------------------------------------------
# Test 2: Occupancy never exceeds R
# ---------------------------------------------------------------------------

def test_occupancy_never_exceeds_r(tmp_path: Path):
    """Buffer occupancy (out-of-order + app_backlog) never exceeds capacity R."""
    size = 40 * 1024
    content = bytes([(i * 5) % 256 for i in range(size)])
    expected_digest = _compute_sha256(content)

    src_file = tmp_path / "flow_occ_src.bin"
    src_file.write_bytes(content)

    recv_dir = tmp_path / "flow_occ_recv"
    recv_dir.mkdir(parents=True, exist_ok=True)

    server_port = _find_free_port()
    proxy_port = _find_free_port()

    # Reordering to exercise out-of-order buffer together with app backlog
    c2s = DirectionConfig(reorder_rate=0.15, reorder_delay_ms=15.0)
    s2c = DirectionConfig()
    proxy = ImpairmentProxy(
        listen_addr=("127.0.0.1", proxy_port),
        target_addr=("127.0.0.1", server_port),
        c2s_config=c2s,
        s2c_config=s2c,
        seed=123,
    )
    proxy.start()

    run_dir = tmp_path / "receiver_run"
    receiver = FileReceiver(
        bind_addr=("127.0.0.1", server_port),
        output_dir=recv_dir,
        timeout=20.0,
        mode="sr",
        capacity=4,
        app_read_rate=30.0,
        run_dir=run_dir,
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
        rto=0.2,
        mode="sr",
        window_size=16,
    )

    outcome, code = sender.send_file(src_file)
    th.join(timeout=20.0)
    proxy.stop()

    assert outcome == "success"
    assert code == 0

    sr_receiver = receiver._last_receiver
    assert sr_receiver is not None
    assert sr_receiver.max_occupancy <= 4, (
        f"Receiver max_occupancy {sr_receiver.max_occupancy} exceeded capacity 4"
    )

    # Verify rcvbuf.csv was written with correct headers and occupancy values <= 4
    rcvbuf_csv = run_dir / "rcvbuf.csv"
    assert rcvbuf_csv.exists()
    lines = rcvbuf_csv.read_text(encoding="utf-8").strip().splitlines()
    assert lines[0] == "t,ooo_buffered,app_backlog,occupancy,rwnd"
    for line in lines[1:]:
        parts = line.split(",")
        assert len(parts) == 5
        occ = int(parts[3])
        assert occ <= 4, f"Logged occupancy {occ} exceeded capacity 4"

    dest_file = recv_dir / "flow_occ_src.bin"
    assert dest_file.exists()
    assert _compute_sha256(dest_file.read_bytes()) == expected_digest


# ---------------------------------------------------------------------------
# Test 3: Zero window reached and recovered, including lost window-update
# ---------------------------------------------------------------------------

def test_zero_window_reached_and_recovered_including_lost_window_update(tmp_path: Path):
    """Zero window is reached; dropping the window-update ACK is recovered by persist probe."""
    size = 20 * 1024
    content = bytes([(i * 13) % 256 for i in range(size)])
    expected_digest = _compute_sha256(content)

    src_file = tmp_path / "flow_zero_src.bin"
    src_file.write_bytes(content)

    recv_dir = tmp_path / "flow_zero_recv"
    recv_dir.mkdir(parents=True, exist_ok=True)

    server_port = _find_free_port()
    receiver = FileReceiver(
        bind_addr=("127.0.0.1", server_port),
        output_dir=recv_dir,
        timeout=15.0,
        mode="sr",
        capacity=4,
        app_read_rate=15.0,  # 15 pkts/s to reliably trigger zero-window
    )

    results: list[tuple[str, str | None, int]] = []

    def recv_worker():
        results.append(receiver.run_once())

    th = threading.Thread(target=recv_worker, daemon=True)
    th.start()
    time.sleep(0.05)

    sender = FileSender(
        server_addr=("127.0.0.1", server_port),
        timeout=15.0,
        rto=0.15,
        mode="sr",
        window_size=16,
    )

    orig_connect = RtpClient.connect
    dropped_window_update = False
    saw_zero_rwnd = False

    def hook_connect(self_client, server_addr, timeout=30.0):
        orig_connect(self_client, server_addr, timeout=timeout)
        orig_recv = self_client.transport.receive

        def hooked_receive(timeout=0.05):
            nonlocal dropped_window_update, saw_zero_rwnd
            res = orig_recv(timeout=timeout)
            if res is not None:
                pkt, addr = res
                if pkt.flags == FLAG_ACK:
                    if pkt.rwnd == 0:
                        saw_zero_rwnd = True
                    elif saw_zero_rwnd and pkt.rwnd > 0 and not dropped_window_update:
                        # Drop this window update ACK once so sender must rely on persist probe!
                        dropped_window_update = True
                        return None
            return res

        self_client.transport.receive = hooked_receive

    RtpClient.connect = hook_connect
    try:
        outcome, code = sender.send_file(src_file)
    finally:
        RtpClient.connect = orig_connect

    th.join(timeout=15.0)

    assert outcome == "success"
    assert code == 0
    assert saw_zero_rwnd is True, "Sender never observed rwnd == 0"
    assert dropped_window_update is True, "Window update ACK was not intercepted/dropped"

    sr_sender = sender._last_sender
    assert sr_sender is not None
    # Persist probe was sent and recovered the connection after window update loss
    assert sr_sender.probes_sent >= 1, (
        f"Expected at least 1 probe sent, got {sr_sender.probes_sent}"
    )

    dest_file = recv_dir / "flow_zero_src.bin"
    assert dest_file.exists()
    assert _compute_sha256(dest_file.read_bytes()) == expected_digest


# ---------------------------------------------------------------------------
# Test 4: 1 MB transfer with R=4, slow app and 5% loss in both directions
# ---------------------------------------------------------------------------

@pytest.mark.slow
def test_1mb_transfer_slow_app_loss(tmp_path: Path):
    """1 MB transfer with R=4, slow app and 5% loss in both directions; verify SHA-256."""
    size = 1024 * 1024  # 1 MB
    content = bytes([(i + 42) % 256 for i in range(size)])
    expected_digest = _compute_sha256(content)

    src_file = tmp_path / "one_mb.bin"
    src_file.write_bytes(content)

    recv_dir = tmp_path / "one_mb_recv"
    recv_dir.mkdir(parents=True, exist_ok=True)

    server_port = _find_free_port()
    proxy_port = _find_free_port()

    # 5% bidirectional drop
    c2s = DirectionConfig(drop_rate=0.05)
    s2c = DirectionConfig(drop_rate=0.05)
    proxy = ImpairmentProxy(
        listen_addr=("127.0.0.1", proxy_port),
        target_addr=("127.0.0.1", server_port),
        c2s_config=c2s,
        s2c_config=s2c,
        seed=999,
    )
    proxy.start()

    receiver = FileReceiver(
        bind_addr=("127.0.0.1", server_port),
        output_dir=recv_dir,
        timeout=60.0,
        mode="sr",
        capacity=4,
        app_read_rate=250.0,  # 250 packets/s
    )

    results: list[tuple[str, str | None, int]] = []

    def recv_worker():
        results.append(receiver.run_once())

    th = threading.Thread(target=recv_worker, daemon=True)
    th.start()
    time.sleep(0.05)

    sender = FileSender(
        server_addr=("127.0.0.1", proxy_port),
        timeout=60.0,
        rto=0.2,
        mode="sr",
        window_size=16,
    )

    outcome, code = sender.send_file(src_file)
    th.join(timeout=60.0)
    proxy.stop()

    assert outcome == "success"
    assert code == 0

    dest_file = recv_dir / "one_mb.bin"
    assert dest_file.exists()
    assert dest_file.stat().st_size == size
    assert _compute_sha256(dest_file.read_bytes()) == expected_digest


# ---------------------------------------------------------------------------
# Test 5: Probes consume no seq num and preserve counter invariant
# ---------------------------------------------------------------------------

def test_probes_consume_no_seq_and_maintain_counter_invariant(tmp_path: Path):
    """Probes consume no sequence number and maintain data_sent_total = unique_data_packets + retx_total."""
    size = 8 * 1024
    content = bytes([(i * 17) % 256 for i in range(size)])
    expected_digest = _compute_sha256(content)

    src_file = tmp_path / "flow_probe_src.bin"
    src_file.write_bytes(content)

    recv_dir = tmp_path / "flow_probe_recv"
    recv_dir.mkdir(parents=True, exist_ok=True)

    server_port = _find_free_port()
    receiver = FileReceiver(
        bind_addr=("127.0.0.1", server_port),
        output_dir=recv_dir,
        timeout=15.0,
        mode="sr",
        capacity=4,
        app_read_rate=4.0,  # 0.25s interval > 0.08s RTO -> guarantees persist probes fire
    )

    results: list[tuple[str, str | None, int]] = []

    def recv_worker():
        results.append(receiver.run_once())

    th = threading.Thread(target=recv_worker, daemon=True)
    th.start()
    time.sleep(0.05)

    sender = FileSender(
        server_addr=("127.0.0.1", server_port),
        timeout=15.0,
        rto=0.08,
        mode="sr",
        window_size=16,
    )

    outcome, code = sender.send_file(src_file)
    th.join(timeout=15.0)

    assert outcome == "success"
    assert code == 0

    sr_sender = sender._last_sender
    assert sr_sender is not None

    # Probes were sent due to low capacity and slow app
    assert sr_sender.probes_sent > 0, f"Expected probes sent > 0, got {sr_sender.probes_sent}"

    # Invariant: data_sent_total == unique_data_packets + retx_total
    assert sr_sender.data_sent_total == sr_sender.unique_data_packets + sr_sender.retx_total

    # Probes consume no sequence number: next_seq_num equals unique_data_packets
    assert sr_sender.next_seq_num == sr_sender.unique_data_packets

    dest_file = recv_dir / "flow_probe_src.bin"
    assert dest_file.exists()
    assert _compute_sha256(dest_file.read_bytes()) == expected_digest


# ---------------------------------------------------------------------------
# Test 6: Probe-reply race: data with seq == probe.seq sent before delayed
# probe reply arrives is dropped; sender must retransmit it and complete.
# ---------------------------------------------------------------------------

def test_probe_reply_race_does_not_falsely_ack_dropped_data(tmp_path: Path):
    """Data with seq equal to the probe's seq goes out before proxy-delayed probe
    reply arrives and is dropped; sender must retransmit it and transfer must complete."""
    size = 6 * 1024
    content = bytes([(i * 31) % 256 for i in range(size)])
    expected_digest = _compute_sha256(content)

    src_file = tmp_path / "probe_race_src.bin"
    src_file.write_bytes(content)

    recv_dir = tmp_path / "probe_race_recv"
    recv_dir.mkdir(parents=True, exist_ok=True)

    server_port = _find_free_port()
    proxy_port = _find_free_port()

    class ProbeRaceProxy(ImpairmentProxy):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.probe_seq = None
            self.probe_seen = threading.Event()

        def _process_packet(self, direction: str, data: bytes, out_sock, dest_addr):
            pkt = self._parse_packet_safe(data)
            is_probe = direction == "c2s" and pkt and pkt.flags == FLAG_DATA and len(pkt.payload) == 0
            super()._process_packet(direction, data, out_sock, dest_addr)
            if is_probe:
                self.probe_seq = pkt.seq
                self.probe_seen.set()
                # Drop the upcoming real data packet with this exact sequence number
                self.drop_next(direction="c2s", count=1, flags=FLAG_DATA, seq=pkt.seq)

    proxy = ProbeRaceProxy(
        listen_addr=("127.0.0.1", proxy_port),
        target_addr=("127.0.0.1", server_port),
        seed=42,
    )
    # Delay probe reply ACKs by 300 ms on s2c
    proxy.reorder_next(direction="s2c", count=1, delay_ms=300.0, flags=FLAG_ACK, seq=PROBE_ECHO_SEQ)
    proxy.start()

    receiver = FileReceiver(
        bind_addr=("127.0.0.1", server_port),
        output_dir=recv_dir,
        timeout=15.0,
        mode="sr",
        capacity=2,
        app_read_rate=4.0,  # 0.25s interval allows probe to fire before window opens
    )

    results: list[tuple[str, str | None, int]] = []

    def recv_worker():
        results.append(receiver.run_once())

    th = threading.Thread(target=recv_worker, daemon=True)
    th.start()
    time.sleep(0.05)

    sender = FileSender(
        server_addr=("127.0.0.1", proxy_port),
        timeout=15.0,
        rto=0.08,
        mode="sr",
        window_size=16,
    )

    outcome, code = sender.send_file(src_file)
    th.join(timeout=15.0)
    proxy.stop()

    assert outcome == "success"
    assert code == 0
    assert proxy.probe_seen.is_set(), "Probe was not sent or not seen by proxy"

    sr_sender = sender._last_sender
    assert sr_sender is not None
    assert sr_sender.retx_total >= 1, "Expected dropped packet to be retransmitted"

    dest_file = recv_dir / "probe_race_src.bin"
    assert dest_file.exists()
    assert _compute_sha256(dest_file.read_bytes()) == expected_digest

