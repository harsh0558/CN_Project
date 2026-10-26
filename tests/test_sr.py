"""Tests for Phase 4: Selective Repeat ARQ.

Validates all Phase 4 requirements:
1. More than one packet is outstanding at once (max_in_flight > 1).
2. Drop exactly one middle packet: exactly one retransmission occurs.
3. Lost ACK is recovered.
4. Duplicate packets are delivered to the application only once.
5. Reordering up to 10 positions still produces correct in-order delivery.
6. The receiver rejects seq >= rcv_base + R (dropped, not buffered, counted).
7. 5 MB transfer at 10% loss in both directions: SHA-256 matches.
8. 20 MB transfer with no impairment: SHA-256 matches.
9. Counter invariant data_sent_total = unique_data_packets + retx_total holds in every test.
10. Probe (length-0 DATA) is not buffered or delivered and gets an immediate ACK.
11. ACK echoing a non-IN_FLIGHT seq does not mark anything ACKED.
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
from rtp.config import (
    DEFAULT_RECEIVER_CAPACITY,
    FLAG_ACK,
    FLAG_DATA,
    PROBE_ECHO_SEQ,
    STATUS_OK,
)
from rtp.connection import RtpClient, RtpConnection, RtpServer, RtpState
from rtp.packet import Packet
from rtp.receiver import SelectiveRepeatReceiver
from rtp.sender import InFlightPacket, PacketState, SelectiveRepeatSender
from tools.impair_proxy import DirectionConfig, ImpairmentProxy


def _find_free_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _compute_sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


# ---------------------------------------------------------------------------
# Test 1: More than one packet is outstanding at once
# ---------------------------------------------------------------------------

def test_more_than_one_packet_outstanding_at_once(tmp_path: Path):
    """Instrument the sender and assert maximum in-flight count > 1."""
    size = 16 * 1024  # 16 packets
    content = bytes([i % 256 for i in range(size)])
    src_file = tmp_path / "pipelined.bin"
    src_file.write_bytes(content)

    recv_dir = tmp_path / "received"
    recv_dir.mkdir(parents=True, exist_ok=True)

    server_port = _find_free_port()
    proxy_port = _find_free_port()

    # Add small delay to keep pipeline full
    c2s = DirectionConfig(delay_ms=5.0)
    s2c = DirectionConfig(delay_ms=5.0)
    proxy = ImpairmentProxy(
        listen_addr=("127.0.0.1", proxy_port),
        target_addr=("127.0.0.1", server_port),
        c2s_config=c2s,
        s2c_config=s2c,
        seed=42,
    )
    proxy.start()

    receiver = FileReceiver(
        bind_addr=("127.0.0.1", server_port),
        output_dir=recv_dir,
        timeout=10.0,
        mode="sr",
    )

    results: list[tuple[str, str | None, int]] = []

    def recv_worker():
        results.append(receiver.run_once())

    th = threading.Thread(target=recv_worker, daemon=True)
    th.start()
    time.sleep(0.05)

    sender = FileSender(
        server_addr=("127.0.0.1", proxy_port),
        timeout=10.0,
        rto=0.2,
        mode="sr",
        window_size=16,
    )
    outcome, code = sender.send_file(src_file)

    th.join(timeout=10.0)
    proxy.stop()

    assert outcome == "success"
    assert code == 0
    assert len(results) == 1
    assert results[0][0] == "success"

    dest_file = recv_dir / "pipelined.bin"
    assert dest_file.exists()
    assert dest_file.read_bytes() == content

    assert sender._last_sender is not None
    sr_sender = sender._last_sender
    # Assert more than one packet was outstanding simultaneously
    assert sr_sender.max_in_flight > 1, f"Expected max_in_flight > 1, got {sr_sender.max_in_flight}"

    # Verify counter invariant
    assert sr_sender.data_sent_total == sr_sender.unique_data_packets + sr_sender.retx_total


# ---------------------------------------------------------------------------
# Test 2: Drop exactly one middle packet: exactly one retransmission
# ---------------------------------------------------------------------------

def test_drop_middle_packet_selective_retransmit(tmp_path: Path):
    """Drop exactly one middle packet; assert exactly one retransmission occurs."""
    size = 6 * 1024  # 6 chunks of data + 1 header chunk
    content = bytes([(i * 7) % 256 for i in range(size)])
    src_file = tmp_path / "middle_drop.bin"
    src_file.write_bytes(content)

    recv_dir = tmp_path / "received"
    recv_dir.mkdir(parents=True, exist_ok=True)

    server_port = _find_free_port()
    proxy_port = _find_free_port()

    proxy = ImpairmentProxy(
        listen_addr=("127.0.0.1", proxy_port),
        target_addr=("127.0.0.1", server_port),
        seed=123,
    )
    # Drop middle packet with sequence number 3 (c2s direction, DATA flag)
    proxy.drop_next(direction="c2s", count=1, flags=FLAG_DATA, seq=3)
    proxy.start()

    receiver = FileReceiver(
        bind_addr=("127.0.0.1", server_port),
        output_dir=recv_dir,
        timeout=10.0,
        mode="sr",
    )

    results: list[tuple[str, str | None, int]] = []

    def recv_worker():
        results.append(receiver.run_once())

    th = threading.Thread(target=recv_worker, daemon=True)
    th.start()
    time.sleep(0.05)

    sender = FileSender(
        server_addr=("127.0.0.1", proxy_port),
        timeout=10.0,
        rto=0.08,
        mode="sr",
        window_size=16,
    )
    outcome, code = sender.send_file(src_file)

    th.join(timeout=10.0)
    proxy.stop()

    assert outcome == "success"
    assert code == 0

    dest_file = recv_dir / "middle_drop.bin"
    assert dest_file.exists()
    assert dest_file.read_bytes() == content

    assert sender._last_sender is not None
    sr_sender = sender._last_sender

    # Exactly one retransmission occurred and no other packet was retransmitted
    assert sr_sender.retx_timeout == 1, f"Expected 1 retx_timeout, got {sr_sender.retx_timeout}"
    assert sr_sender.retx_total == 1
    assert sr_sender.data_sent_total == sr_sender.unique_data_packets + 1

    # Verify counter invariant
    assert sr_sender.data_sent_total == sr_sender.unique_data_packets + sr_sender.retx_total


# ---------------------------------------------------------------------------
# Test 3: Lost ACK is recovered
# ---------------------------------------------------------------------------

def test_lost_ack_recovery(tmp_path: Path):
    """A dropped ACK is recovered by timeout retransmission and duplicate ACK."""
    size = 4 * 1024
    content = bytes([(i + 3) % 256 for i in range(size)])
    src_file = tmp_path / "lost_ack.bin"
    src_file.write_bytes(content)

    recv_dir = tmp_path / "received"
    recv_dir.mkdir(parents=True, exist_ok=True)

    server_port = _find_free_port()
    proxy_port = _find_free_port()

    proxy = ImpairmentProxy(
        listen_addr=("127.0.0.1", proxy_port),
        target_addr=("127.0.0.1", server_port),
        seed=456,
    )
    # Drop one ACK returning from receiver to sender
    proxy.drop_next(direction="s2c", count=1, flags=FLAG_ACK)
    proxy.start()

    receiver = FileReceiver(
        bind_addr=("127.0.0.1", server_port),
        output_dir=recv_dir,
        timeout=10.0,
        mode="sr",
    )

    results: list[tuple[str, str | None, int]] = []

    def recv_worker():
        results.append(receiver.run_once())

    th = threading.Thread(target=recv_worker, daemon=True)
    th.start()
    time.sleep(0.05)

    sender = FileSender(
        server_addr=("127.0.0.1", proxy_port),
        timeout=10.0,
        rto=0.08,
        mode="sr",
        window_size=16,
    )
    outcome, code = sender.send_file(src_file)

    th.join(timeout=10.0)
    proxy.stop()

    assert outcome == "success"
    assert code == 0

    dest_file = recv_dir / "lost_ack.bin"
    assert dest_file.exists()
    assert dest_file.read_bytes() == content

    assert sender._last_sender is not None
    sr_sender = sender._last_sender
    assert sr_sender.data_sent_total == sr_sender.unique_data_packets + sr_sender.retx_total


# ---------------------------------------------------------------------------
# Test 4: Duplicate packets are delivered to the application only once
# ---------------------------------------------------------------------------

def test_duplicate_packets_delivered_once(tmp_path: Path):
    """Injected duplicate DATA packets are absorbed without duplicating delivery."""
    size = 8 * 1024
    content = bytes([(i * 13) % 256 for i in range(size)])
    src_file = tmp_path / "dup_test.bin"
    src_file.write_bytes(content)

    recv_dir = tmp_path / "received"
    recv_dir.mkdir(parents=True, exist_ok=True)

    server_port = _find_free_port()
    proxy_port = _find_free_port()

    # Deterministic duplication of 3 packets on c2s direction
    proxy = ImpairmentProxy(
        listen_addr=("127.0.0.1", proxy_port),
        target_addr=("127.0.0.1", server_port),
        seed=789,
    )
    proxy.duplicate_next(direction="c2s", count=3, flags=FLAG_DATA)
    proxy.start()

    receiver = FileReceiver(
        bind_addr=("127.0.0.1", server_port),
        output_dir=recv_dir,
        timeout=10.0,
        mode="sr",
    )

    results: list[tuple[str, str | None, int]] = []

    def recv_worker():
        results.append(receiver.run_once())

    th = threading.Thread(target=recv_worker, daemon=True)
    th.start()
    time.sleep(0.05)

    sender = FileSender(
        server_addr=("127.0.0.1", proxy_port),
        timeout=10.0,
        rto=0.1,
        mode="sr",
        window_size=16,
    )
    outcome, code = sender.send_file(src_file)

    th.join(timeout=10.0)
    proxy.stop()

    assert outcome == "success"
    assert code == 0

    dest_file = recv_dir / "dup_test.bin"
    assert dest_file.exists()
    # Content must match exactly (no duplicated bytes)
    assert dest_file.stat().st_size == size
    assert dest_file.read_bytes() == content

    assert receiver._last_receiver is not None
    sr_receiver = receiver._last_receiver
    assert sr_receiver.dup_received > 0

    assert sender._last_sender is not None
    assert sender._last_sender.data_sent_total == sender._last_sender.unique_data_packets + sender._last_sender.retx_total


# ---------------------------------------------------------------------------
# Test 5: Reordering up to 10 positions still produces in-order delivery
# ---------------------------------------------------------------------------

def test_reordering_up_to_10_positions(tmp_path: Path):
    """Reordering packets produces correct contiguous in-order file delivery."""
    size = 20 * 1024  # 20 packets
    content = bytes([(i * 17) % 256 for i in range(size)])
    src_file = tmp_path / "reorder_test.bin"
    src_file.write_bytes(content)

    recv_dir = tmp_path / "received"
    recv_dir.mkdir(parents=True, exist_ok=True)

    server_port = _find_free_port()
    proxy_port = _find_free_port()

    # Reorder packets by delaying by 35 ms
    c2s = DirectionConfig(reorder_rate=0.4, reorder_delay_ms=35.0)
    proxy = ImpairmentProxy(
        listen_addr=("127.0.0.1", proxy_port),
        target_addr=("127.0.0.1", server_port),
        c2s_config=c2s,
        seed=999,
    )
    proxy.start()

    receiver = FileReceiver(
        bind_addr=("127.0.0.1", server_port),
        output_dir=recv_dir,
        timeout=15.0,
        mode="sr",
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
        rto=0.15,
        mode="sr",
        window_size=16,
    )
    outcome, code = sender.send_file(src_file)

    th.join(timeout=15.0)
    proxy.stop()

    assert outcome == "success"
    assert code == 0

    dest_file = recv_dir / "reorder_test.bin"
    assert dest_file.exists()
    assert dest_file.read_bytes() == content

    assert receiver._last_receiver is not None
    assert receiver._last_receiver.ooo_received > 0

    assert sender._last_sender is not None
    assert sender._last_sender.data_sent_total == sender._last_sender.unique_data_packets + sender._last_sender.retx_total


# ---------------------------------------------------------------------------
# Test 6: Receiver rejects seq >= rcv_base + R (dropped, not buffered, counted)
# ---------------------------------------------------------------------------

def test_receiver_rejects_seq_beyond_window():
    """Receiver drops packets with seq >= rcv_base + capacity, does not buffer, counts."""
    # Create mock connection object
    class DummyStats:
        data_received = 0
        duplicate_control_count = 0
        out_of_window_packets_count = 0
        ack_sent = 0

    class DummyTransport:
        def __init__(self):
            self.sent = []

        def send_packet(self, pkt, addr):
            self.sent.append(pkt)

    class DummyConn:
        conn_id = 0x12345678
        peer_addr = ("127.0.0.1", 12345)
        initial_data_packet = None
        rcvbuf_capacity = 10
        stats = DummyStats()
        transport = DummyTransport()

    dummy_conn = DummyConn()
    capacity = 10
    receiver = SelectiveRepeatReceiver(dummy_conn, capacity=capacity)
    assert receiver.rcv_base == 0

    # Packet with seq=15 is outside window [0, 10)
    beyond_pkt = Packet(
        conn_id=dummy_conn.conn_id,
        seq=15,
        ack=0,
        sack=0,
        rwnd=10,
        flags=FLAG_DATA,
        payload=b"beyond_window_data",
    )
    receiver._process_data(beyond_pkt)

    # Must be dropped, not buffered, and counted
    assert 15 not in receiver.buffer
    assert receiver.dropped_beyond_window == 1
    assert dummy_conn.stats.out_of_window_packets_count == 1
    assert receiver._delivered_queue == []
    # Receiver sends a window-refresh ACK (echoed_seq=PROBE_ECHO_SEQ, ack=rcv_base, rwnd=current)
    assert len(dummy_conn.transport.sent) == 1
    refresh_ack = dummy_conn.transport.sent[0]
    assert refresh_ack.seq == PROBE_ECHO_SEQ
    assert refresh_ack.ack == receiver.rcv_base
    assert refresh_ack.rwnd == receiver.rwnd

    # In-window out-of-order packet (seq=5) is accepted
    in_window_pkt = Packet(
        conn_id=dummy_conn.conn_id,
        seq=5,
        ack=0,
        sack=0,
        rwnd=10,
        flags=FLAG_DATA,
        payload=b"in_window_data",
    )
    receiver._process_data(in_window_pkt)
    assert 5 in receiver.buffer
    assert receiver.ooo_received == 1
    assert len(dummy_conn.transport.sent) == 2
    ack = dummy_conn.transport.sent[1]
    assert ack.flags == FLAG_ACK
    assert ack.ack == 0
    # Bit 4 in SACK bitmap corresponds to ack + 1 + 4 = 5
    assert (ack.sack & (1 << 4)) != 0


# ---------------------------------------------------------------------------
# Test 7: 5 MB transfer at 10% loss in both directions: SHA-256 matches
# ---------------------------------------------------------------------------

@pytest.mark.slow
def test_5mb_transfer_10_percent_loss(tmp_path: Path):
    """5 MB transfer through 10% bidirectional loss proxy; verify exact SHA-256."""
    size = 5 * 1024 * 1024  # 5 MB
    content = bytes([i % 256 for i in range(size)])
    expected_digest = _compute_sha256(content)

    src_file = tmp_path / "five_mb.bin"
    src_file.write_bytes(content)

    recv_dir = tmp_path / "received"
    recv_dir.mkdir(parents=True, exist_ok=True)

    server_port = _find_free_port()
    proxy_port = _find_free_port()

    # 10% drop in both directions with fixed seed
    c2s = DirectionConfig(drop_rate=0.10)
    s2c = DirectionConfig(drop_rate=0.10)
    proxy = ImpairmentProxy(
        listen_addr=("127.0.0.1", proxy_port),
        target_addr=("127.0.0.1", server_port),
        c2s_config=c2s,
        s2c_config=s2c,
        seed=42,
    )
    proxy.start()

    receiver = FileReceiver(
        bind_addr=("127.0.0.1", server_port),
        output_dir=recv_dir,
        timeout=30.0,
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
        timeout=30.0,
        rto=0.08,
        mode="sr",
        window_size=32,
    )
    t0 = time.monotonic()
    outcome, code = sender.send_file(src_file)
    elapsed = time.monotonic() - t0

    th.join(timeout=30.0)
    proxy.stop()

    assert outcome == "success"
    assert code == 0

    dest_file = recv_dir / "five_mb.bin"
    assert dest_file.exists()
    assert dest_file.stat().st_size == size

    with open(dest_file, "rb") as f:
        dest_digest = hashlib.sha256(f.read()).hexdigest()
    assert dest_digest == expected_digest

    assert sender._last_sender is not None
    sr_sender = sender._last_sender
    assert sr_sender.retx_timeout > 0  # 10% loss must produce retransmissions
    assert sr_sender.data_sent_total == sr_sender.unique_data_packets + sr_sender.retx_total


# ---------------------------------------------------------------------------
# Test 8: 20 MB transfer with no impairment: SHA-256 matches
# ---------------------------------------------------------------------------

@pytest.mark.slow
def test_20mb_transfer_no_impairment(tmp_path: Path):
    """20 MB clean transfer with pipelining; verify exact SHA-256."""
    size = 20 * 1024 * 1024  # 20 MB
    content = bytes([i % 256 for i in range(size)])
    expected_digest = _compute_sha256(content)

    src_file = tmp_path / "twenty_mb.bin"
    src_file.write_bytes(content)

    recv_dir = tmp_path / "received"
    recv_dir.mkdir(parents=True, exist_ok=True)

    server_port = _find_free_port()

    receiver = FileReceiver(
        bind_addr=("127.0.0.1", server_port),
        output_dir=recv_dir,
        timeout=30.0,
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
        server_addr=("127.0.0.1", server_port),
        timeout=30.0,
        rto=0.2,
        mode="sr",
        window_size=64,
    )
    outcome, code = sender.send_file(src_file)

    th.join(timeout=30.0)

    assert outcome == "success"
    assert code == 0

    dest_file = recv_dir / "twenty_mb.bin"
    assert dest_file.exists()
    assert dest_file.stat().st_size == size

    with open(dest_file, "rb") as f:
        dest_digest = hashlib.sha256(f.read()).hexdigest()
    assert dest_digest == expected_digest

    assert sender._last_sender is not None
    sr_sender = sender._last_sender
    assert sr_sender.retx_timeout == 0  # No impairment -> zero retx
    assert sr_sender.data_sent_total == sr_sender.unique_data_packets + sr_sender.retx_total


# ---------------------------------------------------------------------------
# Test 9: Counter invariant holds in every test
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("loss_rate,reorder_rate", [
    (0.0, 0.0),
    (0.05, 0.0),
    (0.0, 0.1),
    (0.08, 0.08),
])
def test_counter_invariant_in_every_test(tmp_path: Path, loss_rate: float, reorder_rate: float):
    """Assert data_sent_total == unique_data_packets + retx_total across conditions."""
    size = 12 * 1024
    content = bytes([(i + 5) % 256 for i in range(size)])
    src_file = tmp_path / f"invariant_{loss_rate}_{reorder_rate}.bin"
    src_file.write_bytes(content)

    recv_dir = tmp_path / f"received_{loss_rate}_{reorder_rate}"
    recv_dir.mkdir(parents=True, exist_ok=True)

    server_port = _find_free_port()
    proxy_port = _find_free_port()

    c2s = DirectionConfig(drop_rate=loss_rate, reorder_rate=reorder_rate, reorder_delay_ms=20.0)
    s2c = DirectionConfig(drop_rate=loss_rate)
    proxy = ImpairmentProxy(
        listen_addr=("127.0.0.1", proxy_port),
        target_addr=("127.0.0.1", server_port),
        c2s_config=c2s,
        s2c_config=s2c,
        seed=777,
    )
    proxy.start()

    receiver = FileReceiver(
        bind_addr=("127.0.0.1", server_port),
        output_dir=recv_dir,
        timeout=15.0,
        mode="sr",
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

    assert sender._last_sender is not None
    sr_sender = sender._last_sender
    # Core invariant
    assert sr_sender.data_sent_total == sr_sender.unique_data_packets + sr_sender.retx_total
    assert sr_sender.retx_total == sr_sender.retx_timeout
    assert sr_sender.data_sent_total >= sr_sender.unique_data_packets


# ---------------------------------------------------------------------------
# Extra Test 1: Probe (length-0 DATA) is not buffered/delivered and gets an ACK
# ---------------------------------------------------------------------------

def test_zero_length_data_probe_not_buffered_or_delivered_and_gets_ack():
    """DATA packet with length 0 is a probe: replies with ACK, not buffered/delivered."""
    class DummyStats:
        data_received = 0
        duplicate_control_count = 0
        out_of_window_packets_count = 0
        ack_sent = 0

    class DummyTransport:
        def __init__(self):
            self.sent = []

        def send_packet(self, pkt, addr):
            self.sent.append(pkt)

    class DummyConn:
        conn_id = 0x55AA55AA
        peer_addr = ("127.0.0.1", 20000)
        initial_data_packet = None
        rcvbuf_capacity = 64
        stats = DummyStats()
        transport = DummyTransport()

    dummy_conn = DummyConn()
    receiver = SelectiveRepeatReceiver(dummy_conn, capacity=64)
    assert receiver.rcv_base == 0

    # Probe packet: length == 0
    probe_pkt = Packet(
        conn_id=dummy_conn.conn_id,
        seq=5,
        ack=0,
        sack=0,
        rwnd=64,
        flags=FLAG_DATA,
        payload=b"",
    )
    receiver._process_data(probe_pkt)

    # Must NOT advance rcv_base, must NOT buffer, must NOT deliver, must NOT count as duplicate
    assert receiver.rcv_base == 0
    assert 5 not in receiver.buffer
    assert receiver._delivered_queue == []
    assert receiver.dup_received == 0
    assert receiver.ooo_received == 0

    # Must send immediate ACK echoing PROBE_ECHO_SEQ and advertising rcv_base (0)
    assert len(dummy_conn.transport.sent) == 1
    ack = dummy_conn.transport.sent[0]
    assert ack.flags == FLAG_ACK
    assert ack.seq == PROBE_ECHO_SEQ
    assert ack.ack == 0
    assert ack.rwnd == 64


# ---------------------------------------------------------------------------
# Extra Test 2: ACK echoing non-IN_FLIGHT seq does not mark anything ACKED
# ---------------------------------------------------------------------------

def test_ack_echoing_non_in_flight_seq_ignored():
    """ACK echoing a seq that is UNSENT or already ACKED does not mark anything ACKED."""
    class DummyConn:
        conn_id = 0x11223344
        peer_addr = ("127.0.0.1", 30000)
        rwnd = 64
        seq_next = 0

    dummy_conn = DummyConn()
    sender = SelectiveRepeatSender(dummy_conn, window_size=16)

    # Setup packet 0 as UNSENT
    p0 = InFlightPacket(seq=0, payload=b"chunk0", state=PacketState.UNSENT)
    # Setup packet 1 as ACKED
    p1 = InFlightPacket(seq=1, payload=b"chunk1", state=PacketState.ACKED)
    # Setup packet 2 as IN_FLIGHT
    p2 = InFlightPacket(seq=2, payload=b"chunk2", state=PacketState.IN_FLIGHT)

    sender.packets[0] = p0
    sender.packets[1] = p1
    sender.packets[2] = p2

    # Case A: ACK echoing UNSENT packet (seq 0)
    ack_unsent = Packet(
        conn_id=dummy_conn.conn_id,
        seq=0,
        ack=0,
        sack=0,
        rwnd=64,
        flags=FLAG_ACK,
        payload=b"",
    )
    sender._process_ack(ack_unsent)
    assert p0.state == PacketState.UNSENT, "UNSENT packet was erroneously marked ACKED"

    # Case B: ACK echoing already ACKED packet (seq 1)
    ack_already = Packet(
        conn_id=dummy_conn.conn_id,
        seq=1,
        ack=0,
        sack=0,
        rwnd=64,
        flags=FLAG_ACK,
        payload=b"",
    )
    sender._process_ack(ack_already)
    assert p1.state == PacketState.ACKED

    # Case C: ACK echoing non-existent seq (seq 99)
    ack_nonexistent = Packet(
        conn_id=dummy_conn.conn_id,
        seq=99,
        ack=0,
        sack=0,
        rwnd=64,
        flags=FLAG_ACK,
        payload=b"",
    )
    sender._process_ack(ack_nonexistent)
    assert 99 not in sender.packets

    # Case D: ACK echoing genuinely IN_FLIGHT packet (seq 2)
    ack_flight = Packet(
        conn_id=dummy_conn.conn_id,
        seq=2,
        ack=0,
        sack=0,
        rwnd=64,
        flags=FLAG_ACK,
        payload=b"",
    )
    sender._process_ack(ack_flight)
    assert p2.state == PacketState.ACKED, "IN_FLIGHT packet should be marked ACKED"
    assert p2.deadline == 0.0


# ---------------------------------------------------------------------------
# Fix A Test: ACK ack > next_seq_num is ignored mid-transfer
# ---------------------------------------------------------------------------

def test_ack_greater_than_next_seq_num_ignored_mid_transfer(tmp_path: Path):
    """An ACK with ack > next_seq_num is ignored, counted as unexpected, send_base <= next_seq_num, transfer succeeds."""
    size = 20 * 1024
    content = bytes([(i * 7) % 256 for i in range(size)])
    expected_digest = _compute_sha256(content)

    src_file = tmp_path / "fix_a_src.bin"
    src_file.write_bytes(content)

    recv_dir = tmp_path / "fix_a_recv"
    recv_dir.mkdir(parents=True, exist_ok=True)

    server_port = _find_free_port()
    receiver = FileReceiver(
        bind_addr=("127.0.0.1", server_port),
        output_dir=recv_dir,
        timeout=10.0,
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
        server_addr=("127.0.0.1", server_port),
        timeout=10.0,
        rto=0.2,
        mode="sr",
        window_size=16,
    )

    # Wrap client connect to intercept transport.receive on client
    orig_connect = RtpClient.connect
    injected = False

    def hook_connect(self_client, server_addr, timeout=30.0):
        orig_connect(self_client, server_addr, timeout=timeout)
        orig_recv = self_client.transport.receive

        def hooked_receive(timeout=0.05):
            nonlocal injected
            res = orig_recv(timeout=timeout)
            if res is not None and not injected:
                pkt, addr = res
                # Inject a bogus future ACK acknowledging data never sent
                if pkt.flags == FLAG_ACK and pkt.ack > 0:
                    injected = True
                    bogus_pkt = Packet(
                        conn_id=self_client.conn_id,
                        seq=0,
                        ack=99999,
                        sack=0,
                        rwnd=64,
                        flags=FLAG_ACK,
                        payload=b"",
                    )
                    # Return the bogus packet first, then original
                    return (bogus_pkt, addr)
            return res

        self_client.transport.receive = hooked_receive

    RtpClient.connect = hook_connect
    try:
        outcome, code = sender.send_file(src_file)
    finally:
        RtpClient.connect = orig_connect

    th.join(timeout=10.0)

    assert outcome == "success"
    assert code == 0
    assert injected is True

    sr_sender = sender._last_sender
    assert sr_sender is not None
    assert sr_sender.send_base <= sr_sender.next_seq_num
    assert sr_sender.conn.stats.unexpected_packets_count >= 1

    dest_file = recv_dir / "fix_a_src.bin"
    assert dest_file.exists()
    assert _compute_sha256(dest_file.read_bytes()) == expected_digest


# ---------------------------------------------------------------------------
# Fix B Test: FIN with wrong seq mid-transfer is ignored
# ---------------------------------------------------------------------------

def test_fin_with_wrong_seq_ignored_mid_transfer(tmp_path: Path):
    """A FIN with wrong seq mid-transfer is ignored, counted as unexpected, and transfer completes."""
    from rtp.config import FLAG_FIN
    from rtp.receiver import StopAndWaitReceiver

    # 1. Test in SelectiveRepeatReceiver
    size = 25 * 1024
    content = bytes([(i * 11) % 256 for i in range(size)])
    expected_digest = _compute_sha256(content)

    src_file = tmp_path / "fix_b_src.bin"
    src_file.write_bytes(content)

    recv_dir = tmp_path / "fix_b_recv"
    recv_dir.mkdir(parents=True, exist_ok=True)

    server_port = _find_free_port()
    receiver = FileReceiver(
        bind_addr=("127.0.0.1", server_port),
        output_dir=recv_dir,
        timeout=10.0,
        mode="sr",
        capacity=64,
    )

    results: list[tuple[str, str | None, int]] = []
    fin_injected = False

    def recv_worker():
        results.append(receiver.run_once())

    th = threading.Thread(target=recv_worker, daemon=True)
    th.start()
    time.sleep(0.05)

    sender = FileSender(
        server_addr=("127.0.0.1", server_port),
        timeout=10.0,
        rto=0.2,
        mode="sr",
        window_size=16,
    )

    orig_connect = RtpClient.connect

    def hook_connect(self_client, server_addr, timeout=30.0):
        orig_connect(self_client, server_addr, timeout=timeout)
        orig_send = self_client.transport.send_packet

        def hooked_send(pkt, addr):
            nonlocal fin_injected
            orig_send(pkt, addr)
            # Send an out-of-order/bogus FIN mid-transfer (wrong sequence number)
            if pkt.flags == FLAG_DATA and pkt.seq == 3 and not fin_injected:
                fin_injected = True
                bogus_fin = Packet(
                    conn_id=self_client.conn_id,
                    seq=9999,  # definitely != rcv_base
                    ack=0,
                    sack=0,
                    rwnd=64,
                    flags=FLAG_FIN,
                    payload=b"",
                )
                orig_send(bogus_fin, addr)

        self_client.transport.send_packet = hooked_send

    RtpClient.connect = hook_connect
    try:
        outcome, code = sender.send_file(src_file)
    finally:
        RtpClient.connect = orig_connect

    th.join(timeout=10.0)

    assert outcome == "success"
    assert code == 0
    assert fin_injected is True

    sr_receiver = receiver._last_receiver
    assert sr_receiver is not None
    assert sr_receiver.conn.stats.unexpected_packets_count >= 1

    dest_file = recv_dir / "fix_b_src.bin"
    assert dest_file.exists()
    assert _compute_sha256(dest_file.read_bytes()) == expected_digest

    # 2. Test in StopAndWaitReceiver directly
    class DummyStats:
        fin_received = 0
        unexpected_packets_count = 0
        duplicate_control_count = 0
        data_received = 0
        ack_sent = 0
        foreign_conn_id_count = 0

    class DummyTransport:
        def __init__(self, packets):
            self.queue = list(packets)
            self.sent = []

        def receive(self, timeout=0.1):
            if self.queue:
                return self.queue.pop(0), ("127.0.0.1", 10000)
            return None

        def send_packet(self, pkt, addr):
            self.sent.append(pkt)

    class DummyConn:
        conn_id = 0x12345678
        peer_addr = ("127.0.0.1", 10000)
        initial_data_packet = None
        rcvbuf_capacity = 1
        stats = DummyStats()
        def __init__(self, packets):
            self.transport = DummyTransport(packets)

    data_pkt = Packet(conn_id=0x12345678, seq=0, ack=0, sack=0, rwnd=1, flags=FLAG_DATA, payload=b"hello")
    bad_fin = Packet(conn_id=0x12345678, seq=999, ack=0, sack=0, rwnd=1, flags=FLAG_FIN, payload=b"")
    good_fin = Packet(conn_id=0x12345678, seq=1, ack=0, sack=0, rwnd=1, flags=FLAG_FIN, payload=b"")

    dummy_conn = DummyConn([bad_fin, data_pkt, good_fin])
    saw_receiver = StopAndWaitReceiver(dummy_conn)

    # First receive chunk should ignore bad_fin and return data_pkt payload
    chunk = saw_receiver.receive_chunk(timeout=1.0)
    assert chunk == b"hello"
    assert dummy_conn.stats.unexpected_packets_count == 1
    assert saw_receiver.eof_received is False

    # Second receive chunk should process good_fin and return b""
    eof = saw_receiver.receive_chunk(timeout=1.0)
    assert eof == b""
    assert saw_receiver.eof_received is True
