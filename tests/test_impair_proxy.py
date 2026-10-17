"""Unit tests for ImpairmentProxy (tools/impair_proxy.py)."""

import time
import pytest

from rtp.config import FLAG_ACK, FLAG_DATA, FLAG_FIN, FLAG_SYN
from rtp.packet import Packet
from rtp.transport import UDPTransport
from tools.impair_proxy import DirectionConfig, ImpairmentProxy


def test_proxy_targeted_drop_flag():
    server_transport = UDPTransport(bind_addr=("127.0.0.1", 0))
    server_addr = server_transport.local_addr

    proxy = ImpairmentProxy(
        listen_addr=("127.0.0.1", 0),
        target_addr=server_addr,
        seed=123,
    )
    # Drop exactly 1 SYN in c2s direction
    proxy.drop_next(direction="c2s", count=1, flags=FLAG_SYN)

    try:
        proxy.start()
        proxy_addr = proxy.bound_listen_addr

        with UDPTransport(bind_addr=("127.0.0.1", 0)) as client_transport:
            syn_pkt = Packet(
                conn_id=0x1111,
                seq=0,
                ack=0,
                sack=0,
                rwnd=64,
                flags=FLAG_SYN,
                payload=b"",
            )
            # 1st SYN: dropped by proxy
            client_transport.send_packet(syn_pkt, proxy_addr)
            res = server_transport.receive(timeout=0.05)
            assert res is None
            assert proxy.c2s_stats.dropped == 1

            # 2nd SYN: forwarded by proxy
            client_transport.send_packet(syn_pkt, proxy_addr)
            res = server_transport.receive(timeout=0.5)
            assert res is not None
            pkt, _ = res
            assert pkt.conn_id == 0x1111
            assert pkt.flags == FLAG_SYN
            assert proxy.c2s_stats.forwarded == 1
    finally:
        proxy.stop()
        server_transport.close()


def test_proxy_bidirectional_traffic_and_seeded_delay():
    server_transport = UDPTransport(bind_addr=("127.0.0.1", 0))
    server_addr = server_transport.local_addr

    # Add 40ms delay from client to server
    c2s_cfg = DirectionConfig(delay_ms=40.0)
    proxy = ImpairmentProxy(
        listen_addr=("127.0.0.1", 0),
        target_addr=server_addr,
        c2s_config=c2s_cfg,
        seed=42,
    )

    try:
        proxy.start()
        proxy_addr = proxy.bound_listen_addr

        with UDPTransport(bind_addr=("127.0.0.1", 0)) as client_transport:
            pkt = Packet(
                conn_id=0x2222,
                seq=1,
                ack=0,
                sack=0,
                rwnd=64,
                flags=FLAG_DATA,
                payload=b"Delayed Packet",
            )
            t0 = time.monotonic()
            client_transport.send_packet(pkt, proxy_addr)

            # Receive on server
            res = server_transport.receive(timeout=0.5)
            elapsed = time.monotonic() - t0
            assert res is not None
            # Verified delay occurred
            assert elapsed >= 0.035
            assert proxy.c2s_stats.delayed == 1
            assert proxy.c2s_stats.forwarded == 1

            # Server replies to proxy
            reply_pkt = Packet(
                conn_id=0x2222,
                seq=1,
                ack=2,
                sack=0,
                rwnd=64,
                flags=FLAG_ACK,
                payload=b"",
            )
            # Reply from server arrives on proxy's server_sock
            server_transport.send_packet(reply_pkt, proxy.bound_server_addr)
            res_client = client_transport.receive(timeout=0.5)
            assert res_client is not None
            rec_reply, _ = res_client
            assert rec_reply.conn_id == 0x2222
            assert rec_reply.ack == 2
            assert proxy.s2c_stats.forwarded == 1
    finally:
        proxy.stop()
        server_transport.close()


def test_proxy_targeted_duplication():
    server_transport = UDPTransport(bind_addr=("127.0.0.1", 0))
    server_addr = server_transport.local_addr

    proxy = ImpairmentProxy(
        listen_addr=("127.0.0.1", 0),
        target_addr=server_addr,
        seed=99,
    )
    proxy.duplicate_next(direction="c2s", count=1, flags=FLAG_FIN)

    try:
        proxy.start()
        proxy_addr = proxy.bound_listen_addr

        with UDPTransport(bind_addr=("127.0.0.1", 0)) as client_transport:
            fin_pkt = Packet(
                conn_id=0x3333,
                seq=10,
                ack=0,
                sack=0,
                rwnd=64,
                flags=FLAG_FIN,
                payload=b"",
            )
            client_transport.send_packet(fin_pkt, proxy_addr)

            # Server should receive 2 copies
            p1 = server_transport.receive(timeout=0.5)
            assert p1 is not None
            p2 = server_transport.receive(timeout=0.5)
            assert p2 is not None
            assert p1[0].conn_id == 0x3333
            assert p2[0].conn_id == 0x3333
            assert proxy.c2s_stats.duplicated == 1
            assert proxy.c2s_stats.forwarded == 2
    finally:
        proxy.stop()
        server_transport.close()


def test_proxy_reordering_delivers_packets_out_of_order():
    """Verify that proxy reordering actually delivers packets out of order."""
    server_transport = UDPTransport(bind_addr=("127.0.0.1", 0))
    server_addr = server_transport.local_addr

    proxy = ImpairmentProxy(
        listen_addr=("127.0.0.1", 0),
        target_addr=server_addr,
        seed=101,
    )
    # Reorder seq=1 by delaying it 60ms
    proxy.reorder_next(direction="c2s", count=1, delay_ms=60.0, seq=1)

    try:
        proxy.start()
        proxy_addr = proxy.bound_listen_addr

        with UDPTransport(bind_addr=("127.0.0.1", 0)) as client_transport:
            pkt1 = Packet(
                conn_id=0x4444,
                seq=1,
                ack=0,
                sack=0,
                rwnd=64,
                flags=FLAG_DATA,
                payload=b"Packet One",
            )
            pkt2 = Packet(
                conn_id=0x4444,
                seq=2,
                ack=0,
                sack=0,
                rwnd=64,
                flags=FLAG_DATA,
                payload=b"Packet Two",
            )

            # Send packet 1, then packet 2 immediately
            client_transport.send_packet(pkt1, proxy_addr)
            time.sleep(0.005)
            client_transport.send_packet(pkt2, proxy_addr)

            # Receive on server: packet 2 must arrive BEFORE packet 1
            rec1 = server_transport.receive(timeout=0.5)
            assert rec1 is not None, "First packet did not arrive"
            rec2 = server_transport.receive(timeout=0.5)
            assert rec2 is not None, "Second packet did not arrive"

            # Verify inverted arrival order
            assert rec1[0].seq == 2, f"Expected seq=2 first, got seq={rec1[0].seq}"
            assert rec2[0].seq == 1, f"Expected seq=1 second, got seq={rec2[0].seq}"
            assert proxy.c2s_stats.reordered == 1
    finally:
        proxy.stop()
        server_transport.close()


def test_proxy_same_seed_gives_identical_pattern_on_two_runs():
    """Verify that identical PRNG seeds produce the exact same impairment outcomes."""
    seed = 123456

    def simulate_run(rng_seed: int) -> list[str]:
        events: list[str] = []
        cfg = DirectionConfig(drop_rate=0.3, duplicate_rate=0.2, corrupt_rate=0.1)
        p = ImpairmentProxy(
            listen_addr=("127.0.0.1", 0),
            target_addr=("127.0.0.1", 9999),
            c2s_config=cfg,
            seed=rng_seed,
        )
        for i in range(20):
            pkt = Packet(
                conn_id=0x5555,
                seq=i,
                ack=0,
                sack=0,
                rwnd=64,
                flags=FLAG_DATA,
                payload=f"Payload {i}".encode(),
            )
            raw = pkt.encode()
            # Evaluate outcomes
            drop = p.rng.random() < cfg.drop_rate
            dup = p.rng.random() < cfg.duplicate_rate
            corrupt = p.rng.random() < cfg.corrupt_rate
            events.append(f"{i}:drop={drop},dup={dup},corrupt={corrupt}")
        p.stop()
        return events

    run1 = simulate_run(seed)
    run2 = simulate_run(seed)

    assert run1 == run2, "Runs with identical seed produced divergent outcomes"
    assert any("drop=True" in e for e in run1), "Expected at least one drop event"
    assert any("dup=True" in e for e in run1), "Expected at least one duplicate event"

