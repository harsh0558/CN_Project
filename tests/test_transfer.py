"""Integration tests for Stop-and-Wait file transfer and application-layer reliability.

Validates all Phase 3 requirements per docs/PROTOCOL.md:
- File sizes: 0B, 1B, 1KB, exact multiple of 1024, non-multiple of 1024, 1MB.
- 1MB transfer through impairment proxy with 5% loss in both directions.
- Resilience against lost ACKs, duplicated data, and delayed packets.
- Forced hash mismatch returns status 0x01 and creates no final file.
- Interrupted transfer leaves only .part file and reports incomplete.
- Receiver never reports success before the hash check.
"""

from __future__ import annotations

import hashlib
import os
import tempfile
import threading
import time
from pathlib import Path
import pytest

from app.file_receiver import FileReceiver
from app.file_sender import FileSender
from rtp.config import STATUS_MISMATCH, STATUS_OK
from tools.impair_proxy import DirectionConfig, ImpairmentProxy


def _compute_sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


@pytest.mark.parametrize(
    "size,name",
    [
        (0, "empty.bin"),
        (1, "single_byte.bin"),
        (1024, "one_kb.bin"),
        (2048, "exact_multiple.bin"),
        (1500, "non_multiple.bin"),
        (1024 * 1024, "one_mb.bin"),
    ],
)
def test_file_transfer_various_sizes(size: int, name: str, tmp_path: Path):
    """Test reliable transfer across all required file size boundaries."""
    src_file = tmp_path / name
    # Deterministic content
    content = bytes([i % 256 for i in range(size)])
    src_file.write_bytes(content)

    recv_dir = tmp_path / "received"
    recv_dir.mkdir(parents=True, exist_ok=True)

    receiver = FileReceiver(bind_addr=("127.0.0.1", 0), output_dir=recv_dir, timeout=10.0)
    # Get port
    server_port = receiver.bind_addr[1]
    # Receiver creates server on run_once, so let's pre-bind to get ephemeral port
    temp_sock = receiver.server = None

    results: list[tuple[str, str | None, int]] = []

    # Bind receiver to ephemeral port
    import socket

    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()

    receiver.bind_addr = ("127.0.0.1", port)

    def recv_worker():
        res = receiver.run_once()
        results.append(res)

    th = threading.Thread(target=recv_worker, daemon=True)
    th.start()
    time.sleep(0.02)

    # Use fast RTO for unit testing
    sender = FileSender(server_addr=("127.0.0.1", port), timeout=10.0, rto=0.08)
    outcome, code = sender.send_file(src_file)

    th.join(timeout=10.0)

    assert outcome == "success"
    assert code == 0
    assert len(results) == 1
    recv_outcome, final_path, teardown_status = results[0]
    assert recv_outcome == "success"
    assert teardown_status == STATUS_OK

    dest_file = recv_dir / name
    assert dest_file.exists()
    assert dest_file.stat().st_size == size
    assert dest_file.read_bytes() == content
    assert not (recv_dir / f"{name}.part").exists()


@pytest.mark.slow
def test_transfer_1mb_through_proxy_with_5_percent_loss(tmp_path: Path):
    """Test 1 MB file transfer through impairment proxy with 5% loss in both directions."""
    size = 1024 * 1024  # 1 MB
    content = bytes([(i * 7 + 13) % 256 for i in range(size)])
    src_file = tmp_path / "transfer_1mb.bin"
    src_file.write_bytes(content)

    recv_dir = tmp_path / "recv_proxy"
    recv_dir.mkdir(parents=True, exist_ok=True)

    import socket

    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind(("127.0.0.1", 0))
    server_port = s.getsockname()[1]
    s.close()

    receiver = FileReceiver(bind_addr=("127.0.0.1", server_port), output_dir=recv_dir, timeout=30.0)

    # Impairment proxy: 5% loss in both directions with fixed seed
    c2s_cfg = DirectionConfig(drop_rate=0.05)
    s2c_cfg = DirectionConfig(drop_rate=0.05)
    proxy = ImpairmentProxy(
        listen_addr=("127.0.0.1", 0),
        target_addr=("127.0.0.1", server_port),
        c2s_config=c2s_cfg,
        s2c_config=s2c_cfg,
        seed=42,
    )

    try:
        proxy.start()
        proxy_port = proxy.bound_listen_addr[1]

        results: list[tuple[str, str | None, int]] = []

        def recv_worker():
            results.append(receiver.run_once())

        th = threading.Thread(target=recv_worker, daemon=True)
        th.start()
        time.sleep(0.02)

        sender = FileSender(
            server_addr=("127.0.0.1", proxy_port),
            timeout=30.0,
            rto=0.04,  # Fast RTO ensures quick recovery under 5% loss
        )
        outcome, code = sender.send_file(src_file)
        th.join(timeout=30.0)

        assert outcome == "success"
        assert code == 0

        assert len(results) == 1
        recv_outcome, final_path, status = results[0]
        assert recv_outcome == "success"
        assert status == STATUS_OK

        dest_file = recv_dir / "transfer_1mb.bin"
        assert dest_file.exists()
        assert dest_file.stat().st_size == size
        assert _compute_sha256(dest_file.read_bytes()) == _compute_sha256(content)

        # Confirm proxy dropped packets and retransmission occurred
        assert proxy.c2s_stats.dropped > 0 or proxy.s2c_stats.dropped > 0
    finally:
        proxy.stop()


def test_transfer_resilience_lost_ack_duplicated_data_delayed_packets(tmp_path: Path):
    """Test transfer under combined impairments: lost ACKs, duplicated data, and delays."""
    size = 32 * 1024  # 32 KB
    content = bytes([(i * 3) % 256 for i in range(size)])
    src_file = tmp_path / "resilient.bin"
    src_file.write_bytes(content)

    recv_dir = tmp_path / "recv_resilient"
    recv_dir.mkdir(parents=True, exist_ok=True)

    import socket

    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind(("127.0.0.1", 0))
    server_port = s.getsockname()[1]
    s.close()

    receiver = FileReceiver(bind_addr=("127.0.0.1", server_port), output_dir=recv_dir, timeout=15.0)

    # 10% duplicate data, 10% delay on c2s, 10% drop on s2c (lost ACKs)
    c2s_cfg = DirectionConfig(duplicate_rate=0.10, delay_ms=10.0)
    s2c_cfg = DirectionConfig(drop_rate=0.10)
    proxy = ImpairmentProxy(
        listen_addr=("127.0.0.1", 0),
        target_addr=("127.0.0.1", server_port),
        c2s_config=c2s_cfg,
        s2c_config=s2c_cfg,
        seed=777,
    )

    try:
        proxy.start()
        proxy_port = proxy.bound_listen_addr[1]

        results = []

        def recv_worker():
            results.append(receiver.run_once())

        th = threading.Thread(target=recv_worker, daemon=True)
        th.start()
        time.sleep(0.02)

        sender = FileSender(server_addr=("127.0.0.1", proxy_port), timeout=15.0, rto=0.05)
        outcome, code = sender.send_file(src_file)
        th.join(timeout=15.0)

        assert outcome == "success"
        assert code == 0

        dest_file = recv_dir / "resilient.bin"
        assert dest_file.exists()
        assert dest_file.read_bytes() == content
    finally:
        proxy.stop()


def test_forced_hash_mismatch_returns_status_mismatch_and_no_final_file(tmp_path: Path):
    """Verify that a forced SHA-256 mismatch results in STATUS_MISMATCH and no final file."""
    content = b"Authentic payload that will have mismatched declared hash"
    src_file = tmp_path / "mismatch_test.bin"
    src_file.write_bytes(content)

    recv_dir = tmp_path / "recv_mismatch"
    recv_dir.mkdir(parents=True, exist_ok=True)

    import socket

    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind(("127.0.0.1", 0))
    server_port = s.getsockname()[1]
    s.close()

    receiver = FileReceiver(bind_addr=("127.0.0.1", server_port), output_dir=recv_dir, timeout=5.0)

    results = []

    def recv_worker():
        results.append(receiver.run_once())

    th = threading.Thread(target=recv_worker, daemon=True)
    th.start()
    time.sleep(0.02)

    # Send with corrupt_hash=True
    sender = FileSender(server_addr=("127.0.0.1", server_port), timeout=5.0, rto=0.08)
    outcome, code = sender.send_file(src_file, corrupt_hash=True)
    th.join(timeout=5.0)

    # Sender must report failed (exit code 1)
    assert outcome == "failed"
    assert code == 1

    # Receiver outcome must be failed and status STATUS_MISMATCH
    assert len(results) == 1
    recv_outcome, final_path, status = results[0]
    assert recv_outcome == "failed"
    assert status == STATUS_MISMATCH

    # Final file must NOT exist; .part file remains
    assert not (recv_dir / "mismatch_test.bin").exists()
    assert (recv_dir / "mismatch_test.bin.part").exists()


def test_interrupted_transfer_leaves_only_part_file_and_reports_incomplete(tmp_path: Path):
    """Verify interrupted transfer leaves only .part file, no final file, and reports incomplete."""
    from rtp.connection import RtpClient
    from rtp.sender import StopAndWaitSender
    from app.framing import encode_stream_header

    declared_size = 10000
    content = b"A" * 1000  # Only send 1000 bytes out of 10000 declared
    digest = hashlib.sha256(b"A" * declared_size).digest()

    recv_dir = tmp_path / "recv_interrupted"
    recv_dir.mkdir(parents=True, exist_ok=True)

    import socket

    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind(("127.0.0.1", 0))
    server_port = s.getsockname()[1]
    s.close()

    receiver = FileReceiver(bind_addr=("127.0.0.1", server_port), output_dir=recv_dir, timeout=0.8)

    results = []

    def recv_worker():
        results.append(receiver.run_once())

    th = threading.Thread(target=recv_worker, daemon=True)
    th.start()
    time.sleep(0.02)

    client = RtpClient(initial_rto=0.08)
    client.connect(("127.0.0.1", server_port), timeout=2.0)
    sender = StopAndWaitSender(client, timeout=0.08)

    # Send header claiming 10,000 bytes
    hdr = encode_stream_header("interrupted.bin", declared_size, digest)
    sender.send_stream(hdr)

    # Send only 1000 bytes
    sender.send_packet_stop_and_wait(content)

    # Abruptly abort client without sending remaining data or FIN
    client.transport.close()

    th.join(timeout=3.0)

    assert len(results) == 1
    recv_outcome, final_path, status = results[0]
    assert recv_outcome == "incomplete"

    # Only .part file exists, final file does NOT exist
    assert not (recv_dir / "interrupted.bin").exists()
    assert (recv_dir / "interrupted.bin.part").exists()
    assert (recv_dir / "interrupted.bin.part").stat().st_size == 1000


@pytest.mark.parametrize(
    "bad_name,expected_err",
    [
        ("", "empty"),
        ("   ", "empty"),
        ("../../etc/passwd", "path separators"),
        (r"..\..\windows\system32\cmd.exe", "path separators"),
        ("subdir/test.bin", "path separators"),
        ("bad..name", ".."),
        ("A" * 300, "length exceeds"),
    ],
)
def test_filename_sanitization_and_rejection(bad_name: str, expected_err: str, tmp_path: Path):
    """Verify that dangerous or invalid filenames are strictly rejected with status 0x01."""
    from rtp.connection import RtpClient
    from rtp.sender import StopAndWaitSender
    from app.framing import encode_stream_header

    recv_dir = tmp_path / "recv_sanitize"
    recv_dir.mkdir(parents=True, exist_ok=True)

    import socket

    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind(("127.0.0.1", 0))
    server_port = s.getsockname()[1]
    s.close()

    receiver = FileReceiver(
        bind_addr=("127.0.0.1", server_port),
        output_dir=recv_dir,
        timeout=2.0,
        max_filename_length=255,
    )

    results = []

    def recv_worker():
        results.append(receiver.run_once())

    th = threading.Thread(target=recv_worker, daemon=True)
    th.start()
    time.sleep(0.02)

    client = RtpClient(initial_rto=0.08)
    client.connect(("127.0.0.1", server_port), timeout=2.0)
    sender = StopAndWaitSender(client, timeout=0.08)

    # Construct header with illegal name
    hdr = encode_stream_header(bad_name, 100, hashlib.sha256(b"X" * 100).digest())
    sender.send_stream(hdr)

    # Perform teardown
    status = client.close(timeout=2.0)
    th.join(timeout=3.0)

    # Must fail with status 0x01
    assert status == STATUS_MISMATCH
    assert len(results) == 1
    assert results[0][0] == "failed"

    # Crucial: NO files written anywhere in or outside recv_dir
    assert list(recv_dir.iterdir()) == []
    parent_files = list(tmp_path.glob("passwd")) + list(tmp_path.glob("cmd.exe"))
    assert parent_files == []


def test_file_size_exceeding_max_limit_rejected(tmp_path: Path):
    """Verify declared file_size exceeding configured max_file_size is rejected."""
    from rtp.connection import RtpClient
    from rtp.sender import StopAndWaitSender
    from app.framing import encode_stream_header

    recv_dir = tmp_path / "recv_size_limit"
    recv_dir.mkdir(parents=True, exist_ok=True)

    import socket

    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind(("127.0.0.1", 0))
    server_port = s.getsockname()[1]
    s.close()

    # Configure small max_file_size = 5000 bytes
    receiver = FileReceiver(
        bind_addr=("127.0.0.1", server_port),
        output_dir=recv_dir,
        timeout=2.0,
        max_file_size=5000,
    )

    results = []

    def recv_worker():
        results.append(receiver.run_once())

    th = threading.Thread(target=recv_worker, daemon=True)
    th.start()
    time.sleep(0.02)

    client = RtpClient(initial_rto=0.08)
    client.connect(("127.0.0.1", server_port), timeout=2.0)
    sender = StopAndWaitSender(client, timeout=0.08)

    # Claim size 10,000 > 5,000
    hdr = encode_stream_header("oversized.bin", 10000, hashlib.sha256(b"").digest())
    sender.send_stream(hdr)
    status = client.close(timeout=2.0)
    th.join(timeout=3.0)

    assert status == STATUS_MISMATCH
    assert results[0][0] == "failed"
    assert list(recv_dir.iterdir()) == []


def test_transfer_fewer_bytes_than_declared_size(tmp_path: Path):
    """Verify FIN arriving with fewer bytes than declared size results in status 0x01 and .part only."""
    from rtp.connection import RtpClient
    from rtp.sender import StopAndWaitSender
    from app.framing import encode_stream_header

    recv_dir = tmp_path / "recv_fewer"
    recv_dir.mkdir(parents=True, exist_ok=True)

    import socket

    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind(("127.0.0.1", 0))
    server_port = s.getsockname()[1]
    s.close()

    receiver = FileReceiver(bind_addr=("127.0.0.1", server_port), output_dir=recv_dir, timeout=2.0)

    results = []

    def recv_worker():
        results.append(receiver.run_once())

    th = threading.Thread(target=recv_worker, daemon=True)
    th.start()
    time.sleep(0.02)

    client = RtpClient(initial_rto=0.08)
    client.connect(("127.0.0.1", server_port), timeout=2.0)
    sender = StopAndWaitSender(client, timeout=0.08)

    # Claim 2000 bytes
    payload = b"B" * 500  # Send only 500 bytes
    hdr = encode_stream_header("short.bin", 2000, hashlib.sha256(b"B" * 2000).digest())
    sender.send_stream(hdr)
    sender.send_packet_stop_and_wait(payload)

    # Send FIN (close)
    status = client.close(timeout=2.0)
    th.join(timeout=3.0)

    assert status == STATUS_MISMATCH
    assert results[0][0] == "failed"
    # Final file not created, .part file remains
    assert not (recv_dir / "short.bin").exists()
    assert (recv_dir / "short.bin.part").exists()
    assert (recv_dir / "short.bin.part").stat().st_size == 500


def test_transfer_more_bytes_than_declared_size(tmp_path: Path):
    """Verify sending more bytes than declared results in status 0x01 and no final file."""
    from rtp.connection import RtpClient
    from rtp.sender import StopAndWaitSender
    from app.framing import encode_stream_header

    recv_dir = tmp_path / "recv_more"
    recv_dir.mkdir(parents=True, exist_ok=True)

    import socket

    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind(("127.0.0.1", 0))
    server_port = s.getsockname()[1]
    s.close()

    receiver = FileReceiver(bind_addr=("127.0.0.1", server_port), output_dir=recv_dir, timeout=2.0)

    results = []

    def recv_worker():
        results.append(receiver.run_once())

    th = threading.Thread(target=recv_worker, daemon=True)
    th.start()
    time.sleep(0.02)

    client = RtpClient(initial_rto=0.08)
    client.connect(("127.0.0.1", server_port), timeout=2.0)
    sender = StopAndWaitSender(client, timeout=0.08)

    # Claim 200 bytes, but send 500 bytes
    hdr = encode_stream_header("long.bin", 200, hashlib.sha256(b"C" * 200).digest())
    sender.send_stream(hdr)
    sender.send_packet_stop_and_wait(b"C" * 500)

    status = client.close(timeout=2.0)
    th.join(timeout=3.0)

    assert status == STATUS_MISMATCH
    assert results[0][0] == "failed"
    assert not (recv_dir / "long.bin").exists()


def test_stop_and_wait_stale_duplicate_ack_ignored():
    """Verify stale duplicate ACK arriving while a new packet is in flight is ignored."""
    from rtp.connection import RtpServer, RtpClient
    from rtp.sender import StopAndWaitSender
    from rtp.receiver import StopAndWaitReceiver
    from rtp.packet import Packet
    from rtp.config import FLAG_ACK
    from rtp.transport import UDPTransport

    server = RtpServer(bind_addr=("127.0.0.1", 0), initial_rto=0.1)
    server_addr = server.transport.local_addr

    accepted_conn = []

    def server_worker():
        conn = server.accept(timeout=5.0)
        accepted_conn.append(conn)
        rec = StopAndWaitReceiver(conn)
        # Read two chunks
        p1 = rec.receive_chunk(timeout=2.0)
        p2 = rec.receive_chunk(timeout=2.0)
        conn.close(status=STATUS_OK, timeout=2.0)

    th = threading.Thread(target=server_worker, daemon=True)
    th.start()
    time.sleep(0.02)

    client = RtpClient(initial_rto=0.1)
    client.connect(server_addr, timeout=2.0)
    sender = StopAndWaitSender(client, timeout=0.15)

    with UDPTransport(bind_addr=("127.0.0.1", 0)) as injector:
        # Send packet 0: successfully acknowledged (ack=1)
        sender.send_packet_stop_and_wait(b"Packet Zero")
        assert sender.seq_next == 1

        # While packet 1 is being sent, inject a stale ACK with ack=1 (acknowledging packet 0)
        def inject_stale():
            time.sleep(0.01)
            stale_ack = Packet(
                conn_id=client.conn_id,
                seq=0,
                ack=1,  # Stale! seq_next is 1, so this echoes old ack
                sack=0,
                rwnd=64,
                flags=FLAG_ACK,
                payload=b"",
            )
            injector.send_packet(stale_ack, client.transport.local_addr)

        inj_th = threading.Thread(target=inject_stale, daemon=True)
        inj_th.start()

        # Send packet 1: the stale ACK must be ignored, and only true ACK(ack=2) advances sender
        sender.send_packet_stop_and_wait(b"Packet One")
        assert sender.seq_next == 2

        inj_th.join(timeout=1.0)

    status = client.close(timeout=2.0)
    th.join(timeout=2.0)
    server.close()
    assert status == STATUS_OK

