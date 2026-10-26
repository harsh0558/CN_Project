"""UDP transport wrapper for Reliable Transport Protocol (RTP).

Provides a thin, non-blocking UDP socket abstraction with enlarged SO_RCVBUF
and SO_SNDBUF, select-based receive timeouts, and automatic dropping and
counting of corrupted / undecodable datagrams.
"""

from __future__ import annotations

import logging
import select
import socket
import time
from typing import Final

from rtp.packet import Packet, PacketCRCError, PacketError

logger = logging.getLogger("rtp.transport")

DEFAULT_SOCKET_BUFFER_SIZE: Final[int] = 1024 * 1024  # 1 MB


class TransportError(Exception):
    """Base exception for transport-layer failures."""
    pass


class TransportClosedError(TransportError):
    """Raised when performing operations on a closed transport."""
    pass


class UDPTransport:
    """Non-blocking UDP transport layer for RTP endpoints."""

    def __init__(
        self,
        bind_addr: tuple[str, int] | None = None,
        rcvbuf: int = DEFAULT_SOCKET_BUFFER_SIZE,
        sndbuf: int = DEFAULT_SOCKET_BUFFER_SIZE,
    ) -> None:
        self.rcvbuf = rcvbuf
        self.sndbuf = sndbuf
        self._sock: socket.socket | None = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.setblocking(False)

        # Allow immediate reuse of the address/port
        try:
            self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        except OSError:
            pass

        if hasattr(socket, "SO_REUSEPORT"):
            try:
                self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            except OSError:
                pass

        # Configure enlarged socket send and receive buffers
        self._apply_buffer_sizes()

        # Telemetry counters
        self.packets_sent: int = 0
        self.packets_received: int = 0
        self.bytes_sent: int = 0
        self.bytes_received: int = 0
        self.decode_error_count: int = 0
        self.crc_errors: int = 0
        self.invalid_packets: int = 0

        if bind_addr is not None:
            self.bind(bind_addr)
        else:
            self.bind(("127.0.0.1", 0))

    def _apply_buffer_sizes(self) -> None:
        if self._sock is None:
            return
        try:
            self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, self.rcvbuf)
        except OSError as exc:
            logger.warning("Could not set SO_RCVBUF to %d: %s", self.rcvbuf, exc)

        try:
            self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, self.sndbuf)
        except OSError as exc:
            logger.warning("Could not set SO_SNDBUF to %d: %s", self.sndbuf, exc)

    @property
    def is_closed(self) -> bool:
        return self._sock is None

    @property
    def local_addr(self) -> tuple[str, int]:
        if self._sock is None:
            raise TransportClosedError("Cannot get address of closed transport")
        host, port = self._sock.getsockname()
        if host == "0.0.0.0":
            host = "127.0.0.1"
        return (host, port)

    @property
    def sock(self) -> socket.socket:
        if self._sock is None:
            raise TransportClosedError("Transport socket is closed")
        return self._sock

    def bind(self, addr: tuple[str, int]) -> tuple[str, int]:
        """Bind socket to the specified (host, port) tuple."""
        if self._sock is None:
            raise TransportClosedError("Cannot bind a closed transport")
        self._sock.bind(addr)
        bound = self._sock.getsockname()
        logger.debug("Bound UDPTransport to %s", bound)
        return bound

    def send_packet(self, packet: Packet, addr: tuple[str, int]) -> int:
        """Encode and transmit an RTP packet to destination address."""
        if self._sock is None:
            raise TransportClosedError("Cannot send on a closed transport")
        raw_data = packet.encode()
        return self.send_raw(raw_data, addr)

    def send_raw(self, data: bytes, addr: tuple[str, int]) -> int:
        """Send raw bytes over the UDP socket."""
        if self._sock is None:
            raise TransportClosedError("Cannot send on a closed transport")
        sent = self._sock.sendto(data, addr)
        self.packets_sent += 1
        self.bytes_sent += sent
        return sent

    def receive(self, timeout: float | None = 0.0) -> tuple[Packet, tuple[str, int]] | None:
        """Wait up to `timeout` seconds for a valid RTP packet.

        Datagrams failing decoding (e.g. CRC mismatch, truncation, invalid flags)
        are dropped, logged, counted in `decode_error_count`, and skipped.

        Returns (Packet, (addr, port)) on success, or None on timeout.
        """
        if self._sock is None:
            raise TransportClosedError("Cannot receive on a closed transport")

        start_time = time.monotonic()
        while True:
            if self._sock is None:
                return None

            remaining: float | None = None
            if timeout is not None:
                elapsed = time.monotonic() - start_time
                remaining = max(0.0, timeout - elapsed)
                if remaining <= 0.0 and timeout > 0.0:
                    return None

            try:
                r_ready, _, _ = select.select([self._sock], [], [], remaining)
            except (ValueError, OSError):
                # Socket closed or invalid during select
                return None

            if not r_ready:
                return None

            try:
                data, addr = self._sock.recvfrom(65535)
            except BlockingIOError:
                return None
            except OSError as exc:
                if self._sock is None:
                    return None
                logger.debug("recvfrom error: %s", exc)
                return None

            self.bytes_received += len(data)

            try:
                packet = Packet.decode(data)
                self.packets_received += 1
                return packet, addr
            except PacketCRCError as err:
                self.decode_error_count += 1
                self.crc_errors += 1
                logger.warning(
                    "Dropped CRC-corrupted datagram (%d bytes) from %s: %s",
                    len(data),
                    addr,
                    err,
                )
                if timeout == 0.0:
                    return None
                if timeout is not None and (time.monotonic() - start_time) >= timeout:
                    return None
            except PacketError as err:
                self.decode_error_count += 1
                self.invalid_packets += 1
                logger.warning(
                    "Dropped invalid datagram (%d bytes) from %s: %s",
                    len(data),
                    addr,
                    err,
                )
                if timeout == 0.0:
                    return None
                if timeout is not None and (time.monotonic() - start_time) >= timeout:
                    return None

    def close(self) -> None:
        """Close the underlying UDP socket cleanly."""
        if self._sock is not None:
            try:
                self._sock.close()
            except OSError:
                pass
            self._sock = None
            logger.debug("UDPTransport closed")

    def __enter__(self) -> UDPTransport:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
