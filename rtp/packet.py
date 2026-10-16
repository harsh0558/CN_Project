"""Packet codec and data structures for RTP over UDP.

Header Layout (26 bytes, big-endian '!IIIIHBBHI'):
  0                   1                   2                   3
  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 |                        conn_id (32)                           |
 +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 |                          seq (32)                             |
 +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 |                          ack (32)                             |
 +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 |                         sack (32)                             |
 +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 |           rwnd (16)           |   flags (8)   |  reserved (8) |
 +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 |          length (16)          |            crc32 (32)         |
 +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 |                       crc32 (cont.)                           |
 +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 |                     payload (0 - 1024 bytes)                  |
 +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
"""

from __future__ import annotations

import struct
import zlib
from dataclasses import dataclass
from typing import Final

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


class PacketError(Exception):
    """Base exception for all packet errors."""
    pass


class PacketEncodingError(PacketError):
    """Raised when encoding fails due to invalid fields."""
    pass


class PacketDecodingError(PacketError):
    """Raised when decoding fails on arbitrary malformed bytes."""
    pass


class PacketLengthMismatchError(PacketDecodingError):
    """Base error for length discrepancies."""
    pass


class PacketTruncatedError(PacketLengthMismatchError):
    """Packet is shorter than the minimum header or shorter than declared payload."""
    pass


class PacketOversizedError(PacketLengthMismatchError):
    """Packet contains extra bytes beyond the declared payload length."""
    pass


class PacketPayloadTooLargeError(PacketDecodingError):
    """Declared payload length exceeds MAX_PAYLOAD_SIZE (1024 bytes)."""
    pass


class PacketReservedNonZeroError(PacketDecodingError):
    """Reserved byte is non-zero."""
    pass


class PacketInvalidFlagsError(PacketDecodingError):
    """Flag combination is invalid."""
    pass


class PacketCRCError(PacketDecodingError):
    """CRC-32 checksum failed."""
    pass


@dataclass(slots=True)
class Packet:
    """Represents a validated RTP packet."""

    conn_id: int
    seq: int
    ack: int
    sack: int
    rwnd: int
    flags: int
    payload: bytes = b""

    @property
    def is_syn(self) -> bool:
        return bool(self.flags & FLAG_SYN)

    @property
    def is_ack(self) -> bool:
        return bool(self.flags & FLAG_ACK)

    @property
    def is_fin(self) -> bool:
        return bool(self.flags & FLAG_FIN)

    @property
    def is_data(self) -> bool:
        return bool(self.flags & FLAG_DATA)

    @property
    def is_rst(self) -> bool:
        return bool(self.flags & FLAG_RST)

    def encode(self) -> bytes:
        """Encode packet to wire format with calculated CRC-32."""
        payload_len = len(self.payload)
        if payload_len > MAX_PAYLOAD_SIZE:
            raise PacketEncodingError(
                f"Payload size {payload_len} exceeds maximum allowed {MAX_PAYLOAD_SIZE}"
            )
        if not (0 <= self.conn_id <= 0xFFFFFFFF):
            raise PacketEncodingError(f"Invalid conn_id: {self.conn_id}")
        if not (0 <= self.seq <= 0xFFFFFFFF):
            raise PacketEncodingError(f"Invalid seq: {self.seq}")
        if not (0 <= self.ack <= 0xFFFFFFFF):
            raise PacketEncodingError(f"Invalid ack: {self.ack}")
        if not (0 <= self.sack <= 0xFFFFFFFF):
            raise PacketEncodingError(f"Invalid sack: {self.sack}")
        if not (0 <= self.rwnd <= 0xFFFF):
            raise PacketEncodingError(f"Invalid rwnd: {self.rwnd}")
        if self.flags not in VALID_FLAG_COMBINATIONS:
            raise PacketEncodingError(f"Invalid flags combination: 0x{self.flags:02x}")

        # Pack header with crc32 set to 0 for checksum calculation
        header_zero_crc = struct.pack(
            HEADER_FORMAT,
            self.conn_id,
            self.seq,
            self.ack,
            self.sack,
            self.rwnd,
            self.flags,
            0,  # reserved byte always 0
            payload_len,
            0,  # crc field zeroed
        )

        crc = zlib.crc32(header_zero_crc + self.payload) & 0xFFFFFFFF

        # Pack final header with calculated CRC
        header = struct.pack(
            HEADER_FORMAT,
            self.conn_id,
            self.seq,
            self.ack,
            self.sack,
            self.rwnd,
            self.flags,
            0,
            payload_len,
            crc,
        )
        return header + self.payload

    @classmethod
    def decode(cls, data: bytes) -> Packet:
        """Decode and validate a raw datagram into a Packet instance.

        Raises only PacketError subclasses on any invalid input.
        """
        try:
            if not isinstance(data, (bytes, bytearray)):
                raise PacketTruncatedError("Raw packet data must be bytes or bytearray")

            total_len = len(data)
            if total_len < HEADER_SIZE:
                raise PacketTruncatedError(
                    f"Packet too short: received {total_len} bytes, minimum is {HEADER_SIZE}"
                )

            (
                conn_id,
                seq,
                ack,
                sack,
                rwnd,
                flags,
                reserved,
                length,
                crc,
            ) = struct.unpack(HEADER_FORMAT, data[:HEADER_SIZE])

            if reserved != 0:
                raise PacketReservedNonZeroError(f"Reserved byte must be 0, got {reserved}")

            if length > MAX_PAYLOAD_SIZE:
                raise PacketPayloadTooLargeError(
                    f"Payload length {length} exceeds maximum allowed {MAX_PAYLOAD_SIZE}"
                )

            actual_payload_len = total_len - HEADER_SIZE
            if actual_payload_len < length:
                raise PacketTruncatedError(
                    f"Truncated payload: header claims {length} bytes, received {actual_payload_len}"
                )
            if actual_payload_len > length:
                raise PacketOversizedError(
                    f"Oversized packet: header claims {length} bytes, received {actual_payload_len}"
                )

            if flags not in VALID_FLAG_COMBINATIONS:
                raise PacketInvalidFlagsError(f"Invalid flags combination: 0x{flags:02x}")

            payload = bytes(data[HEADER_SIZE:])

            # Verify CRC-32
            header_zero_crc = struct.pack(
                HEADER_FORMAT,
                conn_id,
                seq,
                ack,
                sack,
                rwnd,
                flags,
                0,
                length,
                0,
            )
            computed_crc = zlib.crc32(header_zero_crc + payload) & 0xFFFFFFFF
            if crc != computed_crc:
                raise PacketCRCError(
                    f"CRC mismatch: packet has 0x{crc:08x}, computed 0x{computed_crc:08x}"
                )

            return cls(
                conn_id=conn_id,
                seq=seq,
                ack=ack,
                sack=sack,
                rwnd=rwnd,
                flags=flags,
                payload=payload,
            )
        except PacketError:
            raise
        except Exception as exc:
            raise PacketDecodingError(f"Failed to decode packet: {exc}") from exc
