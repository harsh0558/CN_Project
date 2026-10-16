"""Unit tests for the RTP packet codec, header formats, boundaries, corruption, and fuzzing."""

from __future__ import annotations

import os
import random
import struct
import pytest

from rtp.config import (
    FLAG_ACK,
    FLAG_DATA,
    FLAG_FIN,
    FLAG_RST,
    FLAG_SYN,
    HEADER_FORMAT,
    HEADER_SIZE,
    MAX_PAYLOAD_SIZE,
    VALID_FLAG_COMBINATIONS,
)
from rtp.packet import (
    Packet,
    PacketCRCError,
    PacketDecodingError,
    PacketEncodingError,
    PacketError,
    PacketInvalidFlagsError,
    PacketOversizedError,
    PacketPayloadTooLargeError,
    PacketReservedNonZeroError,
    PacketTruncatedError,
)


def test_struct_size_is_exactly_26_bytes() -> None:
    """Requirement: The packet header struct format must be exactly 26 bytes."""
    calc_size = struct.calcsize(HEADER_FORMAT)
    assert calc_size == 26
    assert HEADER_SIZE == 26


def test_round_trip_all_protocol_flag_combinations() -> None:
    """Requirement: Round-trip encoding and decoding must succeed for every valid flag combination."""
    test_cases = [
        (FLAG_SYN, b""),
        (FLAG_SYN | FLAG_ACK, b""),
        (FLAG_ACK, b""),
        (FLAG_DATA, b"test-payload-12345"),
        (FLAG_DATA, b""),  # Zero-window probe is a zero-length DATA packet
        (FLAG_FIN, b""),
        (FLAG_FIN | FLAG_ACK, b"\x00"),  # SHA-256 verification status byte
    ]

    for flags, payload in test_cases:
        pkt = Packet(
            conn_id=0x12345678,
            seq=100,
            ack=50,
            sack=0b101,
            rwnd=32,
            flags=flags,
            payload=payload,
        )
        encoded = pkt.encode()
        assert len(encoded) == HEADER_SIZE + len(payload)

        decoded = Packet.decode(encoded)
        assert decoded.conn_id == pkt.conn_id
        assert decoded.seq == pkt.seq
        assert decoded.ack == pkt.ack
        assert decoded.sack == pkt.sack
        assert decoded.rwnd == pkt.rwnd
        assert decoded.flags == pkt.flags
        assert decoded.payload == pkt.payload


def test_boundary_values() -> None:
    """Requirement: Boundaries (seq 0 and 2^32-1, rwnd 0 and 65535, length 0 and 1024)."""
    boundary_cases = [
        # Minimum boundaries
        {"conn_id": 0, "seq": 0, "ack": 0, "sack": 0, "rwnd": 0, "payload": b""},
        # Maximum boundaries
        {
            "conn_id": 0xFFFFFFFF,
            "seq": 0xFFFFFFFF,
            "ack": 0xFFFFFFFF,
            "sack": 0xFFFFFFFF,
            "rwnd": 65535,
            "payload": b"X" * 1024,
        },
        # Intermediate boundary: max payload with seq=0
        {"conn_id": 1, "seq": 0, "ack": 0, "sack": 0, "rwnd": 64, "payload": b"A" * 1024},
    ]

    for case in boundary_cases:
        pkt = Packet(
            conn_id=case["conn_id"],
            seq=case["seq"],
            ack=case["ack"],
            sack=case["sack"],
            rwnd=case["rwnd"],
            flags=FLAG_DATA,
            payload=case["payload"],
        )
        encoded = pkt.encode()
        decoded = Packet.decode(encoded)
        assert decoded == pkt


def test_encode_out_of_bounds_raises_encoding_error() -> None:
    """Encoding values outside type boundaries must raise PacketEncodingError."""
    # Negative seq
    with pytest.raises(PacketEncodingError):
        Packet(1, -1, 0, 0, 64, FLAG_DATA, b"").encode()

    # seq > 2^32 - 1
    with pytest.raises(PacketEncodingError):
        Packet(1, 0x1_0000_0000, 0, 0, 64, FLAG_DATA, b"").encode()

    # rwnd > 65535
    with pytest.raises(PacketEncodingError):
        Packet(1, 0, 0, 0, 65536, FLAG_DATA, b"").encode()

    # payload > 1024
    with pytest.raises(PacketEncodingError):
        Packet(1, 0, 0, 0, 64, FLAG_DATA, b"Z" * 1025).encode()

    # Invalid flags
    with pytest.raises(PacketEncodingError):
        Packet(1, 0, 0, 0, 64, 0x00, b"").encode()


def test_single_bit_flip_detection() -> None:
    """Requirement: Flipping any single bit in any header byte or payload byte must be detected."""
    original_pkt = Packet(
        conn_id=0xCAFEBABE,
        seq=42,
        ack=24,
        sack=0x0000000F,
        rwnd=16,
        flags=FLAG_DATA,
        payload=b"BitFlipDetectionPayloadExample12345678",
    )
    raw_bytes = bytearray(original_pkt.encode())
    total_bits = len(raw_bytes) * 8

    # Ensure clean baseline decodes
    assert Packet.decode(bytes(raw_bytes)) == original_pkt

    for bit_index in range(total_bits):
        byte_index = bit_index // 8
        bit_offset = bit_index % 8

        # Flip single bit
        raw_bytes[byte_index] ^= (1 << bit_offset)

        # Decoding MUST fail with PacketError
        with pytest.raises(PacketError):
            Packet.decode(bytes(raw_bytes))

        # Revert bit for next test
        raw_bytes[byte_index] ^= (1 << bit_offset)


def test_truncated_datagrams_rejected() -> None:
    """Requirement: Datagrams shorter than minimum header (26 bytes) are rejected."""
    for length in range(26):
        data = b"\x00" * length
        with pytest.raises(PacketTruncatedError):
            Packet.decode(data)


def test_payload_length_mismatch_rejected() -> None:
    """Requirement: Header length field mismatch with actual datagram is rejected."""
    valid_pkt = Packet(
        conn_id=1, seq=0, ack=0, sack=0, rwnd=64, flags=FLAG_DATA, payload=b"1234567890"
    )
    raw = valid_pkt.encode()

    # Truncate payload (claims 10 bytes, given 5 bytes)
    truncated = raw[:-5]
    with pytest.raises(PacketTruncatedError):
        Packet.decode(truncated)

    # Oversized payload (claims 10 bytes, given 15 bytes)
    oversized = raw + b"extra"
    with pytest.raises(PacketOversizedError):
        Packet.decode(oversized)


def test_payload_exceeding_max_payload_size_rejected() -> None:
    """Header claiming length > 1024 is rejected with PacketPayloadTooLargeError."""
    # Construct a raw header with length = 1025
    header = struct.pack(HEADER_FORMAT, 1, 0, 0, 0, 64, FLAG_DATA, 0, 1025, 0)
    data = header + (b"X" * 1025)
    with pytest.raises(PacketPayloadTooLargeError):
        Packet.decode(data)


def test_reserved_byte_nonzero_rejected() -> None:
    """Reserved byte != 0 must be rejected."""
    # reserved byte set to 1
    header = struct.pack(HEADER_FORMAT, 1, 0, 0, 0, 64, FLAG_DATA, 1, 0, 0)
    with pytest.raises(PacketReservedNonZeroError):
        Packet.decode(header)


def test_invalid_flags_rejected() -> None:
    """Invalid flag combination must be rejected."""
    # flags = 0x00
    header = struct.pack(HEADER_FORMAT, 1, 0, 0, 0, 64, 0x00, 0, 0, 0)
    with pytest.raises(PacketInvalidFlagsError):
        Packet.decode(header)

    # flags = SYN | FIN (invalid combination)
    header2 = struct.pack(HEADER_FORMAT, 1, 0, 0, 0, 64, FLAG_SYN | FLAG_FIN, 0, 0, 0)
    with pytest.raises(PacketInvalidFlagsError):
        Packet.decode(header2)


@pytest.mark.slow
def test_fuzz_10000_random_byte_strings() -> None:
    """Requirement: 10,000 random byte strings produce only PacketError or a valid packet."""
    rng = random.Random(1337)

    for _ in range(10_000):
        # Generate random length between 0 and 1200 bytes
        length = rng.randint(0, 1200)
        random_bytes = rng.randbytes(length)

        try:
            pkt = Packet.decode(random_bytes)
            # If it succeeded, it must be an instance of Packet
            assert isinstance(pkt, Packet)
        except PacketError:
            # Expected outcome for malformed / corrupted bytes
            pass
