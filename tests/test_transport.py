"""Unit and integration tests for UDPTransport (rtp/transport.py)."""

import socket
import pytest

from rtp.config import FLAG_ACK, FLAG_DATA, FLAG_SYN
from rtp.packet import Packet
from rtp.transport import (
    DEFAULT_SOCKET_BUFFER_SIZE,
    TransportClosedError,
    UDPTransport,
)


def test_transport_bind_and_local_addr():
    transport = UDPTransport(bind_addr=("127.0.0.1", 0))
    try:
        host, port = transport.local_addr
        assert host == "127.0.0.1"
        assert port > 0
        assert not transport.is_closed
    finally:
        transport.close()
    assert transport.is_closed


def test_transport_send_and_receive_packet():
    with UDPTransport(bind_addr=("127.0.0.1", 0)) as t1, UDPTransport(
        bind_addr=("127.0.0.1", 0)
    ) as t2:
        pkt = Packet(
            conn_id=0x12345678,
            seq=42,
            ack=0,
            sack=0,
            rwnd=64,
            flags=FLAG_DATA,
            payload=b"Hello Transport!",
        )
        sent = t1.send_packet(pkt, t2.local_addr)
        assert sent == 26 + len(b"Hello Transport!")

        res = t2.receive(timeout=1.0)
        assert res is not None
        received_pkt, from_addr = res
        assert received_pkt.conn_id == 0x12345678
        assert received_pkt.seq == 42
        assert received_pkt.payload == b"Hello Transport!"
        assert from_addr == t1.local_addr

        assert t1.packets_sent == 1
        assert t2.packets_received == 1
        assert t2.decode_error_count == 0


def test_transport_drops_and_counts_corrupted_datagrams():
    with UDPTransport(bind_addr=("127.0.0.1", 0)) as sender, UDPTransport(
        bind_addr=("127.0.0.1", 0)
    ) as receiver:
        # Send raw corrupted bytes (e.g. invalid header length, corrupt CRC, or garbage)
        sender.send_raw(b"Short", receiver.local_addr)
        sender.send_raw(b"X" * 30, receiver.local_addr)  # invalid header/CRC

        # Then send a valid packet
        valid_pkt = Packet(
            conn_id=0x9999,
            seq=1,
            ack=0,
            sack=0,
            rwnd=32,
            flags=FLAG_SYN,
            payload=b"",
        )
        sender.send_packet(valid_pkt, receiver.local_addr)

        # Receive with timeout: invalid datagrams must be skipped and counted
        res = receiver.receive(timeout=1.0)
        assert res is not None
        rec_pkt, _ = res
        assert rec_pkt.conn_id == 0x9999
        assert receiver.decode_error_count == 2
        assert receiver.packets_received == 1


def test_transport_timeout_returns_none():
    with UDPTransport(bind_addr=("127.0.0.1", 0)) as transport:
        res = transport.receive(timeout=0.05)
        assert res is None


def test_transport_closed_operations_raise_error():
    transport = UDPTransport(bind_addr=("127.0.0.1", 0))
    addr = transport.local_addr
    transport.close()

    with pytest.raises(TransportClosedError):
        _ = transport.local_addr

    with pytest.raises(TransportClosedError):
        _ = transport.sock

    with pytest.raises(TransportClosedError):
        transport.send_raw(b"hello", addr)

    with pytest.raises(TransportClosedError):
        transport.receive(timeout=0.1)
