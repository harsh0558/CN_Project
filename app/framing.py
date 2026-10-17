"""Application-layer stream framing for RTP file transfer.

Header layout per R-APP-01:
  [u16 filename_len][filename (UTF-8)][u64 file_size][32-byte sha256][data...]
"""

from __future__ import annotations

import struct
from dataclasses import dataclass


class FramingError(Exception):
    """Raised when application stream header format is invalid."""
    pass


@dataclass(slots=True)
class StreamHeader:
    filename: str
    file_size: int
    sha256_digest: bytes


def encode_stream_header(filename: str, file_size: int, sha256_digest: bytes) -> bytes:
    """Encode application stream header into wire format bytes."""
    name_bytes = filename.encode("utf-8")
    if len(name_bytes) > 0xFFFF:
        raise FramingError(f"Filename length {len(name_bytes)} exceeds uint16 max")
    if len(sha256_digest) != 32:
        raise FramingError(f"SHA-256 digest must be exactly 32 bytes, got {len(sha256_digest)}")
    if file_size < 0 or file_size > 0xFFFFFFFFFFFFFFFF:
        raise FramingError(f"Invalid file size: {file_size}")

    prefix = struct.pack("!H", len(name_bytes)) + name_bytes
    suffix = struct.pack("!Q32s", file_size, sha256_digest)
    return prefix + suffix


class StreamHeaderParser:
    """Incremental parser for application stream headers from incoming byte chunks."""

    def __init__(self) -> None:
        self.buffer = bytearray()
        self.header: StreamHeader | None = None
        self._parsed = False

    @property
    def is_parsed(self) -> bool:
        return self._parsed

    def feed(self, chunk: bytes) -> bytes:
        """Feed bytes into parser.

        If header is not yet parsed, buffers until complete, parses header,
        and returns any remaining bytes that belong to the file payload.
        If header is already parsed, returns the chunk directly.
        """
        if self._parsed:
            return chunk

        self.buffer.extend(chunk)

        # Step 1: Need at least 2 bytes for filename length
        if len(self.buffer) < 2:
            return b""

        name_len = struct.unpack("!H", self.buffer[:2])[0]
        # Total header length = 2 (name_len) + name_len + 8 (file_size) + 32 (sha256)
        total_header_len = 2 + name_len + 8 + 32

        if len(self.buffer) < total_header_len:
            return b""

        # Extract filename
        name_bytes = self.buffer[2 : 2 + name_len]
        try:
            filename = name_bytes.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise FramingError(f"Filename is not valid UTF-8: {exc}") from exc

        # Extract file size and sha256
        file_size, sha256_digest = struct.unpack(
            "!Q32s", self.buffer[2 + name_len : total_header_len]
        )

        self.header = StreamHeader(
            filename=filename,
            file_size=file_size,
            sha256_digest=sha256_digest,
        )
        self._parsed = True

        remaining_payload = bytes(self.buffer[total_header_len:])
        self.buffer.clear()
        return remaining_payload
