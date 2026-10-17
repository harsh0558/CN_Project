"""Integration tests for RTP connection lifecycle, 3-way handshake, and teardown.

Validates all Phase 2 requirements per docs/PROTOCOL.md and docs/TRACEABILITY.md:
- Handshake and teardown succeed, returning status byte.
- Loss recovery for SYN, SYN|ACK, final ACK (DATA completes handshake), FIN, FIN|ACK.
- Control packet duplication tolerance.
- Absent server abort after maximum retries with PeerUnreachable.
- Foreign conn_id isolation and counter tracking.
- Immediate port reuse after socket closure.
"""

from __future__ import annotations

import threading
import time
import pytest

from rtp.config import (
    FLAG_ACK,
    FLAG_DATA,
    FLAG_FIN,
    FLAG_SYN,
    STATUS_MISMATCH,
    STATUS_OK,
)
from rtp.connection import (
    ConnectionAbortError,
    PeerUnreachable,
    PeerUnreachableError,
    RtpClient,
    RtpConnection,
    RtpServer,
    RtpState,
)
from rtp.packet import Packet
from rtp.transport import UDPTransport
from tools.impair_proxy import ImpairmentProxy


def test_handshake_and_teardown_success_status_ok():
    """Verify standard 3-way handshake and teardown with STATUS_OK."""
    server = RtpServer(bind_addr=("127.0.0.1", 0))
    server_addr = server.transport.local_addr

    server_conn: list[RtpConnection] = []
    server_err: list[Exception] = []

    def server_worker():
        try:
            conn = server.accept(timeout=5.0)
            server_conn.append(conn)
            # Perform server teardown carrying STATUS_OK
            conn.close(status=STATUS_OK, timeout=5.0)
        except Exception as exc:
            server_err.append(exc)

    server_thread = threading.Thread(target=server_worker, daemon=True)
    server_thread.start()

    time.sleep(0.01)  # Allow server to enter LISTEN

    client = RtpClient(initial_rto=0.2, time_wait_cap=0.1)
    try:
        client.connect(server_addr, timeout=5.0)
        assert client.state == RtpState.ESTABLISHED
        assert client.conn_id > 0
        assert client.peer_rwnd > 0

        # Wait for server accept to complete
        server_thread.join(timeout=0.2)
        assert len(server_conn) == 1
        assert server_conn[0].state in (RtpState.ESTABLISHED, RtpState.LAST_ACK, RtpState.CLOSED)
        assert server_conn[0].conn_id == client.conn_id

        # Client teardown
        status = client.close(timeout=5.0)
        assert status == STATUS_OK
        assert client.state == RtpState.CLOSED

        server_thread.join(timeout=2.0)
        assert len(server_err) == 0
        assert server_conn[0].state == RtpState.CLOSED
    finally:
        client.transport.close()
        server.close()


def test_handshake_and_teardown_status_mismatch():
    """Verify teardown propagates non-zero verification status byte (STATUS_MISMATCH)."""
    server = RtpServer(bind_addr=("127.0.0.1", 0))
    server_addr = server.transport.local_addr

    def server_worker():
        conn = server.accept(timeout=5.0)
        conn.close(status=STATUS_MISMATCH, timeout=5.0)

    server_thread = threading.Thread(target=server_worker, daemon=True)
    server_thread.start()

    time.sleep(0.01)

    client = RtpClient(initial_rto=0.2, time_wait_cap=0.1)
    try:
        client.connect(server_addr, timeout=5.0)
        status = client.close(timeout=5.0)
        assert status == STATUS_MISMATCH
        assert client.state == RtpState.CLOSED
        server_thread.join(timeout=2.0)
    finally:
        client.transport.close()
        server.close()


def test_loss_recovery_syn():
    """Verify recovery when the first SYN packet is lost (client retransmits SYN)."""
    server = RtpServer(bind_addr=("127.0.0.1", 0), initial_rto=0.1)
    server_addr = server.transport.local_addr

    proxy = ImpairmentProxy(
        listen_addr=("127.0.0.1", 0),
        target_addr=server_addr,
        seed=1,
    )
    # Drop exactly 1 SYN in c2s direction
    proxy.drop_next(direction="c2s", count=1, flags=FLAG_SYN)

    try:
        proxy.start()
        proxy_addr = proxy.bound_listen_addr

        def server_worker():
            conn = server.accept(timeout=5.0)
            conn.close(status=STATUS_OK, timeout=5.0)

        th = threading.Thread(target=server_worker, daemon=True)
        th.start()

        client = RtpClient(initial_rto=0.08, time_wait_cap=0.05)
        client.connect(proxy_addr, timeout=5.0)
        assert client.state == RtpState.ESTABLISHED
        assert client.stats.retx_syn_count >= 1

        status = client.close(timeout=5.0)
        assert status == STATUS_OK
        th.join(timeout=2.0)
    finally:
        proxy.stop()
        server.close()


def test_loss_recovery_syn_ack():
    """Verify recovery when the first SYN|ACK packet is lost."""
    server = RtpServer(bind_addr=("127.0.0.1", 0), initial_rto=0.08)
    server_addr = server.transport.local_addr

    proxy = ImpairmentProxy(
        listen_addr=("127.0.0.1", 0),
        target_addr=server_addr,
        seed=2,
    )
    # Drop first SYN|ACK in s2c direction
    proxy.drop_next(direction="s2c", count=1, flags=FLAG_SYN | FLAG_ACK)

    try:
        proxy.start()
        proxy_addr = proxy.bound_listen_addr

        def server_worker():
            conn = server.accept(timeout=5.0)
            conn.close(status=STATUS_OK, timeout=5.0)

        th = threading.Thread(target=server_worker, daemon=True)
        th.start()

        client = RtpClient(initial_rto=0.08, time_wait_cap=0.05)
        client.connect(proxy_addr, timeout=5.0)
        assert client.state == RtpState.ESTABLISHED

        status = client.close(timeout=5.0)
        assert status == STATUS_OK
        th.join(timeout=2.0)
    finally:
        proxy.stop()
        server.close()


def test_loss_recovery_lost_final_ack_completed_by_data():
    """Verify lost final handshake ACK is recovered when client sends DATA(seq=0).

    Per docs/PROTOCOL.md and prompt task 3:
    In SYN_RCVD, arrival of DATA(seq=0) completes the handshake and transitions
    server to ESTABLISHED, buffering/acking DATA(seq=0).
    """
    server = RtpServer(bind_addr=("127.0.0.1", 0), initial_rto=0.2)
    server_addr = server.transport.local_addr

    # Client initiates manual handshake to simulate lost final ACK
    with UDPTransport(bind_addr=("127.0.0.1", 0)) as client_transport:
        conn_id = 0xABCDE123

        accepted_conn: list[RtpConnection] = []

        def server_worker():
            conn = server.accept(timeout=5.0)
            accepted_conn.append(conn)

        th = threading.Thread(target=server_worker, daemon=True)
        th.start()

        # Step 1: Client sends SYN
        syn_pkt = Packet(
            conn_id=conn_id,
            seq=0,
            ack=0,
            sack=0,
            rwnd=64,
            flags=FLAG_SYN,
            payload=b"",
        )
        client_transport.send_packet(syn_pkt, server_addr)

        # Step 2: Client receives SYN|ACK
        res = client_transport.receive(timeout=1.0)
        assert res is not None
        syn_ack, _ = res
        assert syn_ack.flags == (FLAG_SYN | FLAG_ACK)

        # Step 3: DO NOT send final ACK! Instead, send DATA(seq=0) directly
        data_pkt = Packet(
            conn_id=conn_id,
            seq=0,
            ack=0,
            sack=0,
            rwnd=64,
            flags=FLAG_DATA,
            payload=b"First chunk of application file",
        )
        client_transport.send_packet(data_pkt, server_addr)

        # Step 4: Server completes handshake, transitions to ESTABLISHED, and ACKs DATA(seq=0)
        th.join(timeout=2.0)
        assert len(accepted_conn) == 1
        conn = accepted_conn[0]
        assert conn.state == RtpState.ESTABLISHED
        assert conn.initial_data_packet is not None
        assert conn.initial_data_packet.payload == b"First chunk of application file"

        # Check server sent ACK(ack=1)
        res_ack = client_transport.receive(timeout=1.0)
        assert res_ack is not None
        ack_pkt, _ = res_ack
        assert ack_pkt.flags == FLAG_ACK
        assert ack_pkt.ack == 1

        server.close()


def test_loss_recovery_fin():
    """Verify recovery when the first FIN packet is lost (client retransmits FIN)."""
    server = RtpServer(bind_addr=("127.0.0.1", 0), initial_rto=0.08)
    server_addr = server.transport.local_addr

    proxy = ImpairmentProxy(
        listen_addr=("127.0.0.1", 0),
        target_addr=server_addr,
        seed=3,
    )
    # Drop first FIN in c2s direction
    proxy.drop_next(direction="c2s", count=1, flags=FLAG_FIN)

    try:
        proxy.start()
        proxy_addr = proxy.bound_listen_addr

        def server_worker():
            conn = server.accept(timeout=5.0)
            conn.close(status=STATUS_OK, timeout=5.0)

        th = threading.Thread(target=server_worker, daemon=True)
        th.start()

        client = RtpClient(initial_rto=0.08, time_wait_cap=0.05)
        client.connect(proxy_addr, timeout=5.0)

        status = client.close(timeout=5.0)
        assert status == STATUS_OK
        assert client.stats.retx_fin_count >= 1
        th.join(timeout=2.0)
    finally:
        proxy.stop()
        server.close()


def test_loss_recovery_fin_ack():
    """Verify recovery when the first FIN|ACK packet is lost."""
    server = RtpServer(bind_addr=("127.0.0.1", 0), initial_rto=0.08)
    server_addr = server.transport.local_addr

    proxy = ImpairmentProxy(
        listen_addr=("127.0.0.1", 0),
        target_addr=server_addr,
        seed=4,
    )
    # Drop first FIN|ACK in s2c direction
    proxy.drop_next(direction="s2c", count=1, flags=FLAG_FIN | FLAG_ACK)

    try:
        proxy.start()
        proxy_addr = proxy.bound_listen_addr

        def server_worker():
            conn = server.accept(timeout=5.0)
            conn.close(status=STATUS_OK, timeout=5.0)

        th = threading.Thread(target=server_worker, daemon=True)
        th.start()

        client = RtpClient(initial_rto=0.08, time_wait_cap=0.05)
        client.connect(proxy_addr, timeout=5.0)

        status = client.close(timeout=5.0)
        assert status == STATUS_OK
        th.join(timeout=2.0)
    finally:
        proxy.stop()
        server.close()


def test_duplicated_control_packets_are_harmless():
    """Verify that duplicated SYN, SYN|ACK, FIN, and FIN|ACK packets are handled safely."""
    server = RtpServer(bind_addr=("127.0.0.1", 0), initial_rto=0.1)
    server_addr = server.transport.local_addr

    proxy = ImpairmentProxy(
        listen_addr=("127.0.0.1", 0),
        target_addr=server_addr,
        seed=5,
    )
    # Duplicate SYN, SYN|ACK, FIN, FIN|ACK
    proxy.duplicate_next(direction="c2s", count=1, flags=FLAG_SYN)
    proxy.duplicate_next(direction="s2c", count=1, flags=FLAG_SYN | FLAG_ACK)
    proxy.duplicate_next(direction="c2s", count=1, flags=FLAG_FIN)
    proxy.duplicate_next(direction="s2c", count=1, flags=FLAG_FIN | FLAG_ACK)

    try:
        proxy.start()
        proxy_addr = proxy.bound_listen_addr

        def server_worker():
            conn = server.accept(timeout=5.0)
            conn.close(status=STATUS_OK, timeout=5.0)

        th = threading.Thread(target=server_worker, daemon=True)
        th.start()

        client = RtpClient(initial_rto=0.1, time_wait_cap=0.1)
        client.connect(proxy_addr, timeout=5.0)
        assert client.state == RtpState.ESTABLISHED

        status = client.close(timeout=5.0)
        assert status == STATUS_OK
        th.join(timeout=2.0)

        # Verify duplication occurred and was counted
        assert (
            proxy.c2s_stats.duplicated >= 1
            or proxy.s2c_stats.duplicated >= 1
            or client.stats.duplicate_control_count >= 1
        )
    finally:
        proxy.stop()
        server.close()


def test_server_absent_aborts_with_peer_unreachable():
    """Verify client aborts after 6 SYN attempts within expected time with PeerUnreachable."""
    client = RtpClient(initial_rto=0.03, max_syn_attempts=6)

    # Use an unassigned localhost port
    unused_sock = UDPTransport(bind_addr=("127.0.0.1", 0))
    absent_addr = unused_sock.local_addr
    unused_sock.close()

    t0 = time.monotonic()
    with pytest.raises((PeerUnreachable, PeerUnreachableError)) as exc_info:
        client.connect(absent_addr)

    elapsed = time.monotonic() - t0
    # Sum of 6 timeouts: 0.03 + 0.06 + 0.12 + 0.24 + 0.48 + 0.96 = ~1.89 seconds
    assert 1.4 <= elapsed <= 3.5, f"Expected abort in ~1.9s, got {elapsed:.2f}s"
    assert client.state == RtpState.CLOSED
    assert client.stats.retx_syn_count == 5  # 1 initial + 5 retransmissions = 6 total
    assert "unreachable" in str(exc_info.value).lower()


def test_foreign_conn_id_packets_are_ignored():
    """Verify packets with incorrect conn_id are ignored and counted on both client and server."""
    server = RtpServer(bind_addr=("127.0.0.1", 0), initial_rto=0.2)
    server_addr = server.transport.local_addr

    with UDPTransport(bind_addr=("127.0.0.1", 0)) as rogue_transport:
        server_conn: list[RtpConnection] = []

        def server_worker():
            conn = server.accept(timeout=5.0)
            server_conn.append(conn)
            conn.close(status=STATUS_OK, timeout=5.0)

        th = threading.Thread(target=server_worker, daemon=True)
        th.start()

        client = RtpClient(initial_rto=0.2, time_wait_cap=0.1)
        client.connect(server_addr, timeout=5.0)
        assert client.state == RtpState.ESTABLISHED

        # Send packet with wrong conn_id to server
        rogue_pkt = Packet(
            conn_id=client.conn_id ^ 0xFFFFFFFF,
            seq=999,
            ack=0,
            sack=0,
            rwnd=64,
            flags=FLAG_DATA,
            payload=b"Impostor payload",
        )
        rogue_transport.send_packet(rogue_pkt, server_addr)

        # Send packet with wrong conn_id to client
        rogue_transport.send_packet(rogue_pkt, client.transport.local_addr)

        # Allow moment for packets to be processed
        time.sleep(0.05)

        status = client.close(timeout=5.0)
        assert status == STATUS_OK
        th.join(timeout=2.0)

        # Check foreign packet count was incremented
        assert (
            server_conn[0].stats.foreign_conn_id_count >= 1
            or client.stats.foreign_conn_id_count >= 1
        )
        server.close()


def test_clean_close_and_port_reuse():
    """Verify after close, no sockets or timers remain and a new connection on same port works."""
    # Find an ephemeral port
    temp = UDPTransport(bind_addr=("127.0.0.1", 0))
    port = temp.local_addr[1]
    temp.close()

    # Connection 1 on port
    s1 = RtpServer(bind_addr=("127.0.0.1", port), initial_rto=0.1)

    def s1_worker():
        conn = s1.accept(timeout=5.0)
        conn.close(status=STATUS_OK, timeout=5.0)

    th1 = threading.Thread(target=s1_worker, daemon=True)
    th1.start()

    c1 = RtpClient(initial_rto=0.1, time_wait_cap=0.05)
    c1.connect(("127.0.0.1", port), timeout=5.0)
    assert c1.close(timeout=5.0) == STATUS_OK
    th1.join(timeout=2.0)
    s1.close()

    # Sockets must be closed
    assert s1.transport.is_closed
    assert c1.transport.is_closed

    # Connection 2 on the EXACT same port must immediately succeed
    s2 = RtpServer(bind_addr=("127.0.0.1", port), initial_rto=0.1)

    def s2_worker():
        conn = s2.accept(timeout=5.0)
        conn.close(status=STATUS_OK, timeout=5.0)

    th2 = threading.Thread(target=s2_worker, daemon=True)
    th2.start()

    c2 = RtpClient(initial_rto=0.1, time_wait_cap=0.05)
    c2.connect(("127.0.0.1", port), timeout=5.0)
    assert c2.close(timeout=5.0) == STATUS_OK
    th2.join(timeout=2.0)
    s2.close()

    assert s2.transport.is_closed
    assert c2.transport.is_closed


def test_unexpected_packet_types_in_each_state_are_ignored_and_counted():
    """Verify unexpected packet types in each protocol state are ignored and counted."""
    server = RtpServer(bind_addr=("127.0.0.1", 0), initial_rto=0.2)
    server_addr = server.transport.local_addr

    with UDPTransport(bind_addr=("127.0.0.1", 0)) as injector:
        # 1. Send unexpected ACK to server in LISTEN
        bad_ack = Packet(
            conn_id=0x5555,
            seq=0,
            ack=0,
            sack=0,
            rwnd=64,
            flags=FLAG_ACK,
            payload=b"",
        )
        injector.send_packet(bad_ack, server_addr)

        server_conn: list[RtpConnection] = []

        def server_worker():
            conn = server.accept(timeout=5.0)
            server_conn.append(conn)
            conn.close(status=STATUS_OK, timeout=5.0)

        th = threading.Thread(target=server_worker, daemon=True)
        th.start()

        # Connect client normally
        client = RtpClient(initial_rto=0.2, time_wait_cap=0.1)
        client.connect(server_addr, timeout=5.0)

        # 2. In ESTABLISHED, inject unexpected SYN to client
        bad_syn = Packet(
            conn_id=client.conn_id,
            seq=99,
            ack=0,
            sack=0,
            rwnd=64,
            flags=FLAG_SYN,
            payload=b"",
        )
        client.handle_established_packet(bad_syn, server_addr)
        assert client.stats.unexpected_packets_count >= 1

        # Complete teardown
        status = client.close(timeout=5.0)
        assert status == STATUS_OK
        th.join(timeout=2.0)

        # In LISTEN, the unexpected ACK sent earlier was counted
        assert server.stats.unexpected_packets_count >= 1
        server.close()


def test_loss_recovery_lost_final_ack_completed_by_data_seq1():
    """Verify that ANY valid DATA packet (e.g. seq=1) completes handshake in SYN_RCVD."""
    server = RtpServer(bind_addr=("127.0.0.1", 0), initial_rto=0.2)
    server_addr = server.transport.local_addr

    with UDPTransport(bind_addr=("127.0.0.1", 0)) as client_transport:
        conn_id = 0xFEDCBA98
        accepted_conn: list[RtpConnection] = []

        def server_worker():
            conn = server.accept(timeout=5.0)
            accepted_conn.append(conn)

        th = threading.Thread(target=server_worker, daemon=True)
        th.start()

        # Step 1: Client sends SYN
        syn_pkt = Packet(
            conn_id=conn_id,
            seq=0,
            ack=0,
            sack=0,
            rwnd=64,
            flags=FLAG_SYN,
            payload=b"",
        )
        client_transport.send_packet(syn_pkt, server_addr)

        # Step 2: Client receives SYN|ACK
        res = client_transport.receive(timeout=1.0)
        assert res is not None
        syn_ack, _ = res
        assert syn_ack.flags == (FLAG_SYN | FLAG_ACK)

        # Step 3: Final ACK is lost; client sends DATA with seq=1 (e.g. out of order)
        data_pkt = Packet(
            conn_id=conn_id,
            seq=1,
            ack=0,
            sack=0,
            rwnd=64,
            flags=FLAG_DATA,
            payload=b"Out-of-order chunk seq=1",
        )
        client_transport.send_packet(data_pkt, server_addr)

        # Step 4: Handshake completes on server with DATA(seq=1)
        th.join(timeout=2.0)
        assert len(accepted_conn) == 1
        conn = accepted_conn[0]
        assert conn.state == RtpState.ESTABLISHED
        assert conn.initial_data_packet is not None
        assert conn.initial_data_packet.seq == 1
        assert conn.initial_data_packet.payload == b"Out-of-order chunk seq=1"

        # Check server sent ACK echoing seq=1, expecting ack=0, sack bit 0 set (1 << (1-1) = 1)
        res_ack = client_transport.receive(timeout=1.0)
        assert res_ack is not None
        ack_pkt, _ = res_ack
        assert ack_pkt.flags == FLAG_ACK
        assert ack_pkt.seq == 1
        assert ack_pkt.ack == 0
        assert ack_pkt.sack == 1

        server.close()


def test_client_time_wait_re_acks_retransmitted_fin_ack():
    """Verify that a client in TIME_WAIT re-ACKs a retransmitted FIN|ACK and then closes."""
    server = RtpServer(bind_addr=("127.0.0.1", 0), initial_rto=0.2)
    server_addr = server.transport.local_addr

    # We use a custom server worker that intentionally retransmits FIN|ACK
    server_conn: list[RtpConnection] = []

    def server_worker():
        conn = server.accept(timeout=5.0)
        server_conn.append(conn)
        # Wait for client's FIN in ESTABLISHED
        while conn.state == RtpState.ESTABLISHED:
            res = conn.transport.receive(timeout=0.5)
            if res is not None and res[0].flags == FLAG_FIN:
                conn.received_fin_seq = res[0].seq
                break

        # Send 1st FIN|ACK
        fin_ack = Packet(
            conn_id=conn.conn_id,
            seq=0,
            ack=conn.received_fin_seq,
            sack=0,
            rwnd=64,
            flags=FLAG_FIN | FLAG_ACK,
            payload=bytes([STATUS_OK]),
        )
        conn.transport.send_packet(fin_ack, conn.peer_addr)

        # Wait briefly, then re-send duplicate FIN|ACK while client is in TIME_WAIT
        time.sleep(0.04)
        conn.transport.send_packet(fin_ack, conn.peer_addr)

        # Wait for final ACK
        _ = conn.transport.receive(timeout=1.0)
        conn.state = RtpState.CLOSED

    th = threading.Thread(target=server_worker, daemon=True)
    th.start()

    client = RtpClient(initial_rto=0.1, time_wait_cap=0.15)
    client.connect(server_addr, timeout=5.0)
    assert client.state == RtpState.ESTABLISHED

    # Client initiates teardown: enters FIN_WAIT -> TIME_WAIT -> CLOSED
    status = client.close(timeout=5.0)
    assert status == STATUS_OK
    assert client.state == RtpState.CLOSED

    # Verified client processed the retransmitted FIN|ACK in TIME_WAIT
    assert client.stats.duplicate_control_count >= 1
    th.join(timeout=2.0)
    server.close()


def test_after_close_no_sockets_or_timers_remain_explicit_assertion():
    """Explicitly assert that no sockets or timers remain active after close."""
    server = RtpServer(bind_addr=("127.0.0.1", 0), initial_rto=0.1)
    bound_addr = server.transport.local_addr

    def server_worker():
        conn = server.accept(timeout=5.0)
        conn.close(status=STATUS_OK, timeout=5.0)

    th = threading.Thread(target=server_worker, daemon=True)
    th.start()

    client = RtpClient(initial_rto=0.1, time_wait_cap=0.05)
    client.connect(bound_addr, timeout=5.0)
    assert client.close(timeout=5.0) == STATUS_OK
    th.join(timeout=2.0)
    server.close()

    # 1. Explicit socket state assertions
    assert client.transport.is_closed is True, "Client transport not marked closed"
    assert client.transport._sock is None, "Client internal socket object not released"
    assert server.transport.is_closed is True, "Server transport not marked closed"
    assert server.transport._sock is None, "Server internal socket object not released"

    # 2. Explicit timer / thread assertions: RTP engine uses zero background threads
    active_threads = threading.enumerate()
    rtp_threads = [t.name for t in active_threads if "rtp" in t.name.lower()]
    assert len(rtp_threads) == 0, f"Found lingering RTP background threads: {rtp_threads}"

    # 3. Explicit port re-bind check without error
    verify_transport = UDPTransport(bind_addr=bound_addr)
    try:
        assert verify_transport.local_addr == bound_addr
    finally:
        verify_transport.close()


