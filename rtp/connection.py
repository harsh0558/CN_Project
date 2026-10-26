"""Connection management and lifecycle state machines for RTP.

Implements client and server state machines exactly per docs/PROTOCOL.md:
- Handshake (SYN -> SYN|ACK -> ACK / DATA) with exponential backoff retries.
- Teardown (FIN -> FIN|ACK with status byte -> ACK) with client TIME_WAIT.
- Duplicate and unexpected packet tolerance with counters.
- Foreign conn_id isolation.
- Abort handling with PeerUnreachable and ConnectionAbortError.
"""

from __future__ import annotations

import enum
import logging
import random
import time
from dataclasses import dataclass, field
from typing import Final

from rtp.config import (
    DEFAULT_MAX_WINDOW,
    DEFAULT_RECEIVER_CAPACITY,
    FLAG_ACK,
    FLAG_DATA,
    FLAG_FIN,
    FLAG_SYN,
    INITIAL_RTO,
    MAX_FIN_ATTEMPTS,
    MAX_RTO,
    MAX_SYN_ATTEMPTS,
    MIN_RTO,
    STATUS_MISMATCH,
    STATUS_OK,
    TIME_WAIT_CAP,
)
from rtp.packet import Packet
from rtp.transport import UDPTransport

logger = logging.getLogger("rtp.connection")


class RtpState(enum.Enum):
    """Protocol states per docs/PROTOCOL.md."""
    CLOSED = "CLOSED"
    LISTEN = "LISTEN"
    SYN_SENT = "SYN_SENT"
    SYN_RCVD = "SYN_RCVD"
    ESTABLISHED = "ESTABLISHED"
    FIN_WAIT = "FIN_WAIT"
    LAST_ACK = "LAST_ACK"
    TIME_WAIT = "TIME_WAIT"


class RtpError(Exception):
    """Base exception for all RTP connection and protocol errors."""
    pass


class PeerUnreachable(RtpError):
    """Raised when the peer cannot be reached after maximum SYN attempts."""
    pass


PeerUnreachableError = PeerUnreachable


class ConnectionAbortError(RtpError):
    """Raised when a connection aborts due to retry exhaustion or idle inactivity."""
    pass


ConnectionAbort = ConnectionAbortError


class ConnectionTimeoutError(RtpError):
    """Raised when an operation times out."""
    pass


class InvalidStateError(RtpError):
    """Raised when an action is performed that is invalid in the current state."""
    pass


class ConnectionClosedError(RtpError):
    """Raised when an operation is attempted on a closed connection."""
    pass


@dataclass
class ConnectionStats:
    """Detailed telemetry counters for protocol packets and events."""
    syn_sent: int = 0
    syn_ack_sent: int = 0
    fin_sent: int = 0
    fin_ack_sent: int = 0
    ack_sent: int = 0
    data_sent: int = 0

    syn_received: int = 0
    syn_ack_received: int = 0
    fin_received: int = 0
    fin_ack_received: int = 0
    ack_received: int = 0
    data_received: int = 0

    retx_syn_count: int = 0
    retx_fin_count: int = 0
    retx_fin_ack_count: int = 0

    duplicate_control_count: int = 0
    foreign_conn_id_count: int = 0
    unexpected_packets_count: int = 0
    out_of_window_packets_count: int = 0


class RtpConnection:
    """Represents an established RTP connection on either endpoint."""

    def __init__(
        self,
        transport: UDPTransport,
        conn_id: int,
        peer_addr: tuple[str, int],
        state: RtpState = RtpState.ESTABLISHED,
        rcvbuf_capacity: int = DEFAULT_RECEIVER_CAPACITY,
        initial_rto: float = INITIAL_RTO,
        max_fin_attempts: int = MAX_FIN_ATTEMPTS,
        initial_data_packet: Packet | None = None,
        role: str = "ServerConn",
    ) -> None:
        self.transport = transport
        self.conn_id = conn_id
        self.peer_addr = peer_addr
        self.state = state
        self.rcvbuf_capacity = rcvbuf_capacity
        self.rto = initial_rto
        self.max_fin_attempts = max_fin_attempts
        self.initial_data_packet = initial_data_packet
        self.role = role
        self.stats = ConnectionStats()
        self.peer_rwnd: int = rcvbuf_capacity
        self.received_fin_seq: int | None = None
        self.status: int | None = None

    def _transition_to(self, new_state: RtpState) -> None:
        old_state = self.state
        self.state = new_state
        logger.info(
            "[%s] State transition: %s -> %s (conn_id=0x%08x)",
            self.role,
            old_state.name,
            new_state.name,
            self.conn_id,
        )

    def close(self, status: int = STATUS_OK, timeout: float | None = 30.0) -> None:
        """Perform server-side connection teardown per docs/PROTOCOL.md.

        Waits for client FIN (if not already received), transmits FIN|ACK
        carrying `status`, and transitions to LAST_ACK awaiting the final ACK.
        """
        if self.state == RtpState.CLOSED:
            return

        start_time = time.monotonic()

        # Step 1: Wait for client FIN in ESTABLISHED (if not already received)
        if self.state == RtpState.ESTABLISHED and self.received_fin_seq is None:
            while self.state == RtpState.ESTABLISHED:
                if timeout is not None and (time.monotonic() - start_time) >= timeout:
                    self._transition_to(RtpState.CLOSED)
                    raise ConnectionTimeoutError("Timed out waiting for client FIN in ESTABLISHED")

                remaining = (
                    max(0.01, timeout - (time.monotonic() - start_time))
                    if timeout is not None
                    else 1.0
                )
                res = self.transport.receive(timeout=min(remaining, 0.5))
                if res is None:
                    continue
                pkt, addr = res
                if pkt.conn_id != self.conn_id:
                    self.stats.foreign_conn_id_count += 1
                    logger.warning(
                        "[%s] Ignored foreign conn_id 0x%08x (expected 0x%08x)",
                        self.role,
                        pkt.conn_id,
                        self.conn_id,
                    )
                    continue

                if pkt.flags == FLAG_FIN:
                    self.stats.fin_received += 1
                    self.received_fin_seq = pkt.seq
                    break
                else:
                    self.stats.unexpected_packets_count += 1
                    logger.debug(
                        "[%s] Ignored unexpected packet flags 0x%02x in %s",
                        self.role,
                        pkt.flags,
                        self.state.name,
                    )

        fin_seq = self.received_fin_seq if self.received_fin_seq is not None else 0

        # Step 2: Send FIN|ACK and enter LAST_ACK
        status_byte = status & 0xFF
        self.status = status_byte
        fin_ack_pkt = Packet(
            conn_id=self.conn_id,
            seq=0,
            ack=fin_seq,
            sack=0,
            rwnd=self.rcvbuf_capacity,
            flags=FLAG_FIN | FLAG_ACK,
            payload=bytes([status_byte]),
        )
        self.transport.send_packet(fin_ack_pkt, self.peer_addr)
        self.stats.fin_ack_sent += 1
        self._transition_to(RtpState.LAST_ACK)

        # Step 3: Wait for client final ACK in LAST_ACK with retries
        current_timeout = self.rto
        attempts = 1
        timer_deadline = time.monotonic() + current_timeout

        while self.state == RtpState.LAST_ACK:
            now = time.monotonic()
            if timeout is not None and (now - start_time) >= timeout:
                self._transition_to(RtpState.CLOSED)
                break

            if now >= timer_deadline:
                if attempts >= self.max_fin_attempts:
                    logger.info(
                        "[%s] FIN|ACK retry limit reached (%d attempts); closing connection",
                        self.role,
                        self.max_fin_attempts,
                    )
                    self._transition_to(RtpState.CLOSED)
                    break
                self.transport.send_packet(fin_ack_pkt, self.peer_addr)
                self.stats.retx_fin_ack_count += 1
                self.stats.fin_ack_sent += 1
                attempts += 1
                current_timeout = min(current_timeout * 2.0, MAX_RTO)
                timer_deadline = time.monotonic() + current_timeout
                continue

            select_timeout = max(0.005, min(0.2, timer_deadline - now))
            res = self.transport.receive(timeout=select_timeout)
            if res is None:
                continue
            pkt, addr = res
            if pkt.conn_id != self.conn_id:
                self.stats.foreign_conn_id_count += 1
                continue

            if pkt.flags == FLAG_FIN:
                # Duplicate FIN received: re-send FIN|ACK per PROTOCOL.md
                self.stats.duplicate_control_count += 1
                self.stats.fin_received += 1
                self.transport.send_packet(fin_ack_pkt, self.peer_addr)
                self.stats.fin_ack_sent += 1
                continue

            if pkt.flags == FLAG_ACK and pkt.ack == fin_seq:
                self.stats.ack_received += 1
                self._transition_to(RtpState.CLOSED)
                break
            else:
                self.stats.unexpected_packets_count += 1
                continue


class RtpClient:
    """Client connection endpoint managing connection handshake and teardown."""

    def __init__(
        self,
        transport: UDPTransport | None = None,
        initial_rto: float = INITIAL_RTO,
        max_syn_attempts: int = MAX_SYN_ATTEMPTS,
        max_fin_attempts: int = MAX_FIN_ATTEMPTS,
        time_wait_cap: float = TIME_WAIT_CAP,
        rwnd: int = DEFAULT_RECEIVER_CAPACITY,
    ) -> None:
        self._owns_transport = transport is None
        self.transport = transport if transport is not None else UDPTransport()
        self.initial_rto = initial_rto
        self.rto = initial_rto
        self.max_syn_attempts = max_syn_attempts
        self.max_fin_attempts = max_fin_attempts
        self.time_wait_cap = time_wait_cap
        self.rwnd = rwnd
        self.peer_rwnd: int = rwnd
        self.conn_id: int = 0
        self.peer_addr: tuple[str, int] | None = None
        self.state: RtpState = RtpState.CLOSED
        self.stats = ConnectionStats()
        self.seq_next: int = 0
        self.teardown_status: int | None = None
        self.role = "Client"

    def _transition_to(self, new_state: RtpState) -> None:
        old_state = self.state
        self.state = new_state
        logger.info(
            "[%s] State transition: %s -> %s (conn_id=0x%08x)",
            self.role,
            old_state.name,
            new_state.name,
            self.conn_id,
        )

    def connect(
        self,
        server_addr: tuple[str, int],
        timeout: float | None = None,
    ) -> None:
        """Establish connection via 3-way handshake to server_addr.

        Retransmits SYN with exponential backoff.
        Raises PeerUnreachable if server does not respond within max attempts.
        """
        if self.state != RtpState.CLOSED:
            raise InvalidStateError(f"Cannot connect in state {self.state.name}")

        self.peer_addr = server_addr
        # Choose non-zero 32-bit integer per docs/PROTOCOL.md
        self.conn_id = random.randint(1, 0xFFFFFFFF)
        self.seq_next = 0
        self.rto = self.initial_rto

        syn_pkt = Packet(
            conn_id=self.conn_id,
            seq=0,
            ack=0,
            sack=0,
            rwnd=self.rwnd,
            flags=FLAG_SYN,
            payload=b"",
        )

        self.transport.send_packet(syn_pkt, self.peer_addr)
        self.stats.syn_sent += 1
        self._transition_to(RtpState.SYN_SENT)

        start_time = time.monotonic()
        current_timeout = self.initial_rto
        attempts = 1
        timer_deadline = start_time + current_timeout

        while self.state == RtpState.SYN_SENT:
            now = time.monotonic()
            if timeout is not None and (now - start_time) >= timeout:
                self._transition_to(RtpState.CLOSED)
                if self._owns_transport:
                    self.transport.close()
                raise ConnectionTimeoutError("connect() timed out")

            if now >= timer_deadline:
                if attempts >= self.max_syn_attempts:
                    self._transition_to(RtpState.CLOSED)
                    if self._owns_transport:
                        self.transport.close()
                    raise PeerUnreachable(
                        f"Server at {server_addr} unreachable after {self.max_syn_attempts} SYN attempts"
                    )
                # Retransmit SYN with exponential backoff
                self.transport.send_packet(syn_pkt, self.peer_addr)
                self.stats.syn_sent += 1
                self.stats.retx_syn_count += 1
                attempts += 1
                current_timeout = min(current_timeout * 2.0, MAX_RTO)
                timer_deadline = time.monotonic() + current_timeout
                continue

            select_timeout = max(0.005, timer_deadline - now)
            if timeout is not None:
                select_timeout = min(select_timeout, max(0.005, (start_time + timeout) - now))

            res = self.transport.receive(timeout=select_timeout)
            if res is None:
                continue

            pkt, addr = res
            if pkt.conn_id != self.conn_id:
                self.stats.foreign_conn_id_count += 1
                logger.warning(
                    "[%s] Ignored foreign conn_id 0x%08x in %s (expected 0x%08x)",
                    self.role,
                    pkt.conn_id,
                    self.state.name,
                    self.conn_id,
                )
                continue

            if pkt.flags == (FLAG_SYN | FLAG_ACK) and pkt.ack == 0:
                self.stats.syn_ack_received += 1
                self.peer_rwnd = pkt.rwnd
                # Send final handshake ACK
                ack_pkt = Packet(
                    conn_id=self.conn_id,
                    seq=0,
                    ack=0,
                    sack=0,
                    rwnd=self.rwnd,
                    flags=FLAG_ACK,
                    payload=b"",
                )
                self.transport.send_packet(ack_pkt, self.peer_addr)
                self.stats.ack_sent += 1
                self._transition_to(RtpState.ESTABLISHED)
                return
            else:
                self.stats.unexpected_packets_count += 1
                logger.warning(
                    "[%s] Ignored unexpected packet flags 0x%02x in %s",
                    self.role,
                    pkt.flags,
                    self.state.name,
                )

    def close(self, timeout: float | None = None) -> int:
        """Initiate client teardown via 3-way handshake and TIME_WAIT.

        Returns:
            int: The 1-byte verification status received in the FIN|ACK payload.
        """
        if self.state == RtpState.CLOSED:
            return self.teardown_status if self.teardown_status is not None else STATUS_OK

        if self.state != RtpState.ESTABLISHED:
            raise InvalidStateError(f"Cannot close in state {self.state.name}")

        assert self.peer_addr is not None

        fin_pkt = Packet(
            conn_id=self.conn_id,
            seq=self.seq_next,
            ack=0,
            sack=0,
            rwnd=self.rwnd,
            flags=FLAG_FIN,
            payload=b"",
        )
        self.transport.send_packet(fin_pkt, self.peer_addr)
        self.stats.fin_sent += 1
        self._transition_to(RtpState.FIN_WAIT)

        start_time = time.monotonic()
        current_timeout = self.rto
        attempts = 1
        timer_deadline = start_time + current_timeout

        # Step 1: Wait for FIN|ACK in FIN_WAIT
        while self.state == RtpState.FIN_WAIT:
            now = time.monotonic()
            if timeout is not None and (now - start_time) >= timeout:
                self._transition_to(RtpState.CLOSED)
                if self._owns_transport:
                    self.transport.close()
                raise ConnectionTimeoutError("close() timed out in FIN_WAIT")

            if now >= timer_deadline:
                if attempts >= self.max_fin_attempts:
                    self._transition_to(RtpState.CLOSED)
                    if self._owns_transport:
                        self.transport.close()
                    raise ConnectionAbortError(
                        f"Teardown aborted: no response to FIN after {self.max_fin_attempts} attempts"
                    )
                self.transport.send_packet(fin_pkt, self.peer_addr)
                self.stats.fin_sent += 1
                self.stats.retx_fin_count += 1
                attempts += 1
                current_timeout = min(current_timeout * 2.0, MAX_RTO)
                timer_deadline = time.monotonic() + current_timeout
                continue

            select_timeout = max(0.005, timer_deadline - now)
            res = self.transport.receive(timeout=select_timeout)
            if res is None:
                continue

            pkt, addr = res
            if pkt.conn_id != self.conn_id:
                self.stats.foreign_conn_id_count += 1
                continue

            if pkt.flags == (FLAG_FIN | FLAG_ACK) and pkt.ack == self.seq_next:
                self.stats.fin_ack_received += 1
                # Extract status byte
                self.teardown_status = pkt.payload[0] if len(pkt.payload) == 1 else STATUS_OK
                # Send final teardown ACK
                ack_pkt = Packet(
                    conn_id=self.conn_id,
                    seq=0,
                    ack=pkt.ack,
                    sack=0,
                    rwnd=self.rwnd,
                    flags=FLAG_ACK,
                    payload=b"",
                )
                self.transport.send_packet(ack_pkt, self.peer_addr)
                self.stats.ack_sent += 1
                self.status_received_at = time.monotonic()  # status received, before TIME_WAIT
                self._transition_to(RtpState.TIME_WAIT)
                break
            else:
                self.stats.unexpected_packets_count += 1

        # Step 2: TIME_WAIT state
        time_wait_duration = min(2.0 * self.rto, self.time_wait_cap)
        time_wait_deadline = time.monotonic() + time_wait_duration

        while self.state == RtpState.TIME_WAIT:
            remaining = time_wait_deadline - time.monotonic()
            if remaining <= 0:
                self._transition_to(RtpState.CLOSED)
                break

            res = self.transport.receive(timeout=max(0.005, min(remaining, 0.2)))
            if res is None:
                continue

            pkt, addr = res
            if pkt.conn_id != self.conn_id:
                self.stats.foreign_conn_id_count += 1
                continue

            if pkt.flags == (FLAG_FIN | FLAG_ACK):
                # Retransmitted FIN|ACK received; re-send ACK and restart TIME_WAIT timer
                self.stats.duplicate_control_count += 1
                self.stats.fin_ack_received += 1
                ack_pkt = Packet(
                    conn_id=self.conn_id,
                    seq=0,
                    ack=pkt.ack,
                    sack=0,
                    rwnd=self.rwnd,
                    flags=FLAG_ACK,
                    payload=b"",
                )
                self.transport.send_packet(ack_pkt, self.peer_addr)
                self.stats.ack_sent += 1
                time_wait_deadline = time.monotonic() + time_wait_duration
            else:
                self.stats.unexpected_packets_count += 1

        # Cleanup underlying resources
        if self._owns_transport:
            self.transport.close()

        return self.teardown_status if self.teardown_status is not None else STATUS_OK

    def handle_established_packet(self, pkt: Packet, addr: tuple[str, int]) -> None:
        """Handle control packet arrivals while in ESTABLISHED.

        Per PROTOCOL.md:
        - Duplicate SYN|ACK: re-send ACK(seq=0, ack=0).
        - Foreign conn_id or unexpected flags: ignore and count.
        """
        if pkt.conn_id != self.conn_id:
            self.stats.foreign_conn_id_count += 1
            return

        if pkt.flags == (FLAG_SYN | FLAG_ACK) and pkt.ack == 0:
            self.stats.duplicate_control_count += 1
            self.stats.syn_ack_received += 1
            ack_pkt = Packet(
                conn_id=self.conn_id,
                seq=0,
                ack=0,
                sack=0,
                rwnd=self.rwnd,
                flags=FLAG_ACK,
                payload=b"",
            )
            self.transport.send_packet(ack_pkt, addr)
            self.stats.ack_sent += 1
        else:
            self.stats.unexpected_packets_count += 1

    def __enter__(self) -> RtpClient:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        try:
            if self.state == RtpState.ESTABLISHED:
                self.close()
            elif self.state != RtpState.CLOSED:
                self.state = RtpState.CLOSED
                if self._owns_transport:
                    self.transport.close()
        except Exception:
            if self._owns_transport:
                self.transport.close()


class RtpServer:
    """Server listener endpoint accepting RTP connections."""

    def __init__(
        self,
        bind_addr: tuple[str, int] = ("127.0.0.1", 9000),
        transport: UDPTransport | None = None,
        initial_rto: float = INITIAL_RTO,
        max_syn_attempts: int = MAX_SYN_ATTEMPTS,
        max_fin_attempts: int = MAX_FIN_ATTEMPTS,
        rcvbuf_capacity: int = DEFAULT_RECEIVER_CAPACITY,
    ) -> None:
        self._owns_transport = transport is None
        self.transport = (
            transport if transport is not None else UDPTransport(bind_addr=bind_addr)
        )
        self.initial_rto = initial_rto
        self.max_syn_attempts = max_syn_attempts
        self.max_fin_attempts = max_fin_attempts
        self.rcvbuf_capacity = rcvbuf_capacity
        self.state: RtpState = RtpState.LISTEN
        self.stats = ConnectionStats()
        self.role = "Server"

    def _transition_to(self, new_state: RtpState, conn_id: int = 0) -> None:
        old_state = self.state
        self.state = new_state
        logger.info(
            "[%s] State transition: %s -> %s (conn_id=0x%08x)",
            self.role,
            old_state.name,
            new_state.name,
            conn_id,
        )

    def accept(self, timeout: float | None = None) -> RtpConnection:
        """Listen for client SYN, complete 3-way handshake, and return connection.

        Handles duplicate SYN retransmission, lost final ACK (completed by DATA(seq=0)),
        and foreign conn_id packets.
        """
        if self.state not in (RtpState.LISTEN, RtpState.CLOSED):
            raise InvalidStateError(f"Cannot accept in state {self.state.name}")

        self.state = RtpState.LISTEN
        start_time = time.monotonic()

        # Step 1: Wait for SYN in LISTEN
        client_addr: tuple[str, int] | None = None
        conn_id: int = 0

        while self.state == RtpState.LISTEN:
            if timeout is not None and (time.monotonic() - start_time) >= timeout:
                raise ConnectionTimeoutError("accept() timed out waiting for SYN")

            remaining = (
                max(0.01, (start_time + timeout) - time.monotonic())
                if timeout is not None
                else 0.5
            )
            res = self.transport.receive(timeout=min(remaining, 0.5))
            if res is None:
                continue

            pkt, addr = res
            if pkt.flags == FLAG_SYN and pkt.seq == 0:
                self.stats.syn_received += 1
                conn_id = pkt.conn_id
                client_addr = addr
                self._transition_to(RtpState.SYN_RCVD, conn_id=conn_id)
                break
            else:
                self.stats.unexpected_packets_count += 1
                logger.debug(
                    "[%s] In LISTEN, ignored non-SYN packet flags=0x%02x from %s",
                    self.role,
                    pkt.flags,
                    addr,
                )

        assert client_addr is not None

        # Step 2: Send SYN|ACK and enter SYN_RCVD with retries
        syn_ack_pkt = Packet(
            conn_id=conn_id,
            seq=0,
            ack=0,
            sack=0,
            rwnd=self.rcvbuf_capacity,
            flags=FLAG_SYN | FLAG_ACK,
            payload=b"",
        )
        self.transport.send_packet(syn_ack_pkt, client_addr)
        self.stats.syn_ack_sent += 1

        current_timeout = self.initial_rto
        attempts = 1
        timer_deadline = time.monotonic() + current_timeout
        initial_data_packet: Packet | None = None

        while self.state == RtpState.SYN_RCVD:
            now = time.monotonic()
            if timeout is not None and (now - start_time) >= timeout:
                self._transition_to(RtpState.LISTEN, conn_id=conn_id)
                raise ConnectionTimeoutError("accept() timed out in SYN_RCVD")

            if now >= timer_deadline:
                if attempts >= self.max_syn_attempts:
                    self._transition_to(RtpState.LISTEN, conn_id=conn_id)
                    raise ConnectionAbortError(
                        f"Handshake failed: no response to SYN|ACK after {self.max_syn_attempts} attempts"
                    )
                self.transport.send_packet(syn_ack_pkt, client_addr)
                self.stats.syn_ack_sent += 1
                self.stats.retx_syn_count += 1
                attempts += 1
                current_timeout = min(current_timeout * 2.0, MAX_RTO)
                timer_deadline = time.monotonic() + current_timeout
                continue

            select_timeout = max(0.005, timer_deadline - now)
            res = self.transport.receive(timeout=select_timeout)
            if res is None:
                continue

            pkt, addr = res
            if pkt.conn_id != conn_id:
                self.stats.foreign_conn_id_count += 1
                logger.warning(
                    "[%s] In SYN_RCVD, ignored foreign conn_id 0x%08x from %s (expected 0x%08x)",
                    self.role,
                    pkt.conn_id,
                    addr,
                    conn_id,
                )
                continue

            # Duplicate SYN received: re-send SYN|ACK
            if pkt.flags == FLAG_SYN and pkt.seq == 0:
                self.stats.duplicate_control_count += 1
                self.stats.syn_received += 1
                self.transport.send_packet(syn_ack_pkt, client_addr)
                self.stats.syn_ack_sent += 1
                continue

            # Final handshake ACK received: handshake complete
            if pkt.flags == FLAG_ACK and pkt.seq == 0 and pkt.ack == 0:
                self.stats.ack_received += 1
                self._transition_to(RtpState.ESTABLISHED, conn_id=conn_id)
                break

            # Lost final ACK: arrival of ANY valid DATA packet completes handshake per docs/PROTOCOL.md
            if pkt.flags == FLAG_DATA:
                self.stats.data_received += 1
                self._transition_to(RtpState.ESTABLISHED, conn_id=conn_id)
                initial_data_packet = pkt
                ack_val = 1 if pkt.seq == 0 else 0
                sack_val = 0
                if pkt.seq > 0 and 1 <= pkt.seq <= 32:
                    sack_val = 1 << (pkt.seq - 1)
                data_ack_pkt = Packet(
                    conn_id=conn_id,
                    seq=pkt.seq,
                    ack=ack_val,
                    sack=sack_val,
                    rwnd=self.rcvbuf_capacity,
                    flags=FLAG_ACK,
                    payload=b"",
                )
                self.transport.send_packet(data_ack_pkt, client_addr)
                self.stats.ack_sent += 1
                break

            self.stats.unexpected_packets_count += 1

        # Handshake complete: build RtpConnection instance
        conn = RtpConnection(
            transport=self.transport,
            conn_id=conn_id,
            peer_addr=client_addr,
            state=RtpState.ESTABLISHED,
            rcvbuf_capacity=self.rcvbuf_capacity,
            initial_rto=self.initial_rto,
            max_fin_attempts=self.max_fin_attempts,
            initial_data_packet=initial_data_packet,
            role="ServerConn",
        )
        return conn

    def close(self) -> None:
        """Close listening socket and release resources."""
        self.state = RtpState.CLOSED
        if self._owns_transport:
            self.transport.close()
        logger.debug("[%s] Server closed", self.role)

    def __enter__(self) -> RtpServer:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
