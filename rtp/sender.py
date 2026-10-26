"""Transmission engines for Reliable Transport Protocol (RTP).

Provides:
- StopAndWaitSender: Single-packet window, blocks per packet until ACKed.
- SelectiveRepeatSender: Pipelined sliding window with per-packet state
  (UNSENT/IN_FLIGHT/ACKED), per-packet monotonic deadlines, cumulative ACK +
  SACK bitmap processing, and selective retransmission of timed-out packets only.

Counter invariant (maintained by both senders):
    data_sent_total == unique_data_packets + retx_total
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Final, Iterable

from rtp.config import (
    DEFAULT_RECEIVER_CAPACITY,
    FLAG_ACK,
    FLAG_DATA,
    IDLE_TIMEOUT,
    MAX_CONSECUTIVE_TIMEOUTS,
    MAX_PAYLOAD_SIZE,
    PERSIST_TIMER_CAP,
    PROBE_ECHO_SEQ,
)
from rtp.connection import (
    ConnectionAbortError,
    RtpClient,
    RtpConnection,
    RtpState,
)
from rtp.cwnd import CongestionControl
from rtp.packet import Packet
from rtp.rto import RtoEstimator

logger = logging.getLogger("rtp.sender")

DEFAULT_STOP_AND_WAIT_RTO: Final[float] = 0.5   # 500 ms
DEFAULT_SR_WINDOW: Final[int] = 16               # packets
DEFAULT_SR_RTO: Final[float] = 0.5              # 500 ms fixed


# ---------------------------------------------------------------------------
# Shared per-packet bookkeeping
# ---------------------------------------------------------------------------

class PacketState(Enum):
    """Lifecycle state of an individual packet slot in the sender window."""
    UNSENT = 0
    IN_FLIGHT = 1
    ACKED = 2


@dataclass
class InFlightPacket:
    """Tracks per-packet metadata inside the SR sliding window."""
    seq: int
    payload: bytes
    state: PacketState = PacketState.UNSENT
    deadline: float = 0.0   # absolute monotonic() deadline for timeout check
    retry_count: int = 0
    send_time: float = 0.0  # monotonic() timestamp of transmission
    first_send_time: float = 0.0



# ---------------------------------------------------------------------------
# Stop-and-Wait sender
# ---------------------------------------------------------------------------

class StopAndWaitSender:
    """Sender implementing Stop-and-Wait ARQ over an RTP connection.

    Transmits exactly one DATA packet at a time, waits for a cumulative ACK,
    and retransmits on timeout up to max_retries times.

    Counter invariant: data_sent_total == unique_data_packets + retx_total
    """

    def __init__(
        self,
        connection: RtpClient | RtpConnection,
        timeout: float = DEFAULT_STOP_AND_WAIT_RTO,
        max_retries: int = MAX_CONSECUTIVE_TIMEOUTS,
    ) -> None:
        self.conn = connection
        self.timeout = timeout
        self.max_retries = max_retries
        self.seq_next: int = 0

        # Telemetry counters (invariant: data_sent_total == unique_data_packets + retx_total)
        self.data_sent_total: int = 0
        self.unique_data_packets: int = 0
        self.retx_timeout: int = 0
        self.bytes_sent_total: int = 0

    @property
    def retx_total(self) -> int:
        """Alias for retx_timeout per Phase 4 counter specification."""
        return self.retx_timeout

    def get_counters(self) -> dict[str, int]:
        """Return sender telemetry counters as a dictionary."""
        return {
            "data_sent_total": self.data_sent_total,
            "unique_data_packets": self.unique_data_packets,
            "retx_timeout": self.retx_timeout,
            "retx_total": self.retx_total,
            "bytes_sent_total": self.bytes_sent_total,
        }

    @property
    def conn_id(self) -> int:
        return self.conn.conn_id

    @property
    def peer_addr(self) -> tuple[str, int]:
        assert self.conn.peer_addr is not None
        return self.conn.peer_addr

    def send_packet_stop_and_wait(self, payload: bytes) -> None:
        """Send a single DATA packet and block until cumulatively ACKed.

        Raises:
            ConnectionAbortError: If max_retries is reached without an ACK.
        """
        if self.conn.state != RtpState.ESTABLISHED:
            raise ConnectionAbortError(
                f"Cannot send data in state {self.conn.state.name}"
            )

        seq = self.seq_next
        pkt = Packet(
            conn_id=self.conn_id,
            seq=seq,
            ack=0,
            sack=0,
            rwnd=self.conn.rwnd if hasattr(self.conn, "rwnd") else 64,
            flags=FLAG_DATA,
            payload=payload,
        )

        # First transmission
        self.conn.transport.send_packet(pkt, self.peer_addr)
        self.data_sent_total += 1
        self.unique_data_packets += 1
        self.conn.stats.data_sent += 1
        timer_deadline = time.monotonic() + self.timeout
        attempts = 0

        while True:
            now = time.monotonic()
            if now >= timer_deadline:
                attempts += 1
                if attempts >= self.max_retries:
                    raise ConnectionAbortError(
                        f"Transfer aborted: packet seq={seq} exceeded max retries"
                        f" ({self.max_retries})"
                    )
                self.conn.transport.send_packet(pkt, self.peer_addr)
                self.data_sent_total += 1
                self.retx_timeout += 1
                self.conn.stats.data_sent += 1
                timer_deadline = time.monotonic() + self.timeout
                continue

            select_timeout = max(0.005, timer_deadline - now)
            res = self.conn.transport.receive(timeout=select_timeout)
            if res is None:
                continue

            recv_pkt, _addr = res
            if recv_pkt.conn_id != self.conn_id:
                self.conn.stats.foreign_conn_id_count += 1
                continue

            if recv_pkt.flags == FLAG_ACK:
                self.conn.stats.ack_received += 1
                # Cumulative ACK must advance past the current seq
                if recv_pkt.ack > seq:
                    self.seq_next = recv_pkt.ack
                    if hasattr(self.conn, "seq_next"):
                        self.conn.seq_next = self.seq_next
                    self.bytes_sent_total += len(payload)
                    return
                # Stale / duplicate ACK — keep waiting
            else:
                self.conn.stats.unexpected_packets_count += 1

    def send_stream(self, stream_data: bytes | Iterable[bytes]) -> int:
        """Send a byte stream split into MAX_PAYLOAD_SIZE chunks via SAW."""
        if isinstance(stream_data, (bytes, bytearray, memoryview)):
            data = bytes(stream_data)
            total = len(data)
            offset = 0
            while offset < total:
                chunk = data[offset : offset + MAX_PAYLOAD_SIZE]
                self.send_packet_stop_and_wait(chunk)
                offset += len(chunk)
            if hasattr(self.conn, "seq_next"):
                self.conn.seq_next = self.seq_next
            return total

        total_bytes = 0
        for part in stream_data:
            part = bytes(part)
            offset = 0
            while offset < len(part):
                chunk = part[offset : offset + MAX_PAYLOAD_SIZE]
                self.send_packet_stop_and_wait(chunk)
                offset += len(chunk)
                total_bytes += len(chunk)
        if hasattr(self.conn, "seq_next"):
            self.conn.seq_next = self.seq_next
        return total_bytes


# ---------------------------------------------------------------------------
# Selective Repeat sender
# ---------------------------------------------------------------------------

class SelectiveRepeatSender:
    """Pipelined Selective Repeat ARQ sender.

    Spec-compliant behaviour (docs/PROTOCOL.md / Phase 4):
    - Window span: seq may be sent only if seq < send_base + W (fixed W).
    - Per-packet monotonic deadlines (fixed RTO, no exponential backoff here).
    - Cumulative ACK (ack field) advances send_base.
    - Echoed-seq marking only for packets currently IN_FLIGHT.
    - SACK bitmap: bit i (0 = LSB) acknowledges cum_ack + 1 + i.
    - Stale / duplicate ACKs are silently ignored.
    - Only the individually timed-out packet is retransmitted (not the window).
    - Aborts after MAX_CONSECUTIVE_TIMEOUTS (10) consecutive timeouts of same packet.
    - Aborts after IDLE_TIMEOUT (30s) without receiving any valid packet.

    Counter invariant: data_sent_total == unique_data_packets + retx_total
    """

    def __init__(
        self,
        connection: RtpClient | RtpConnection,
        window_size: int = DEFAULT_SR_WINDOW,
        rto: float | None = None,
        max_retries: int = MAX_CONSECUTIVE_TIMEOUTS,
        idle_timeout: float = IDLE_TIMEOUT,
        run_dir: str | Path | None = None,
    ) -> None:
        self.conn = connection
        self.window_size = window_size
        self.fixed_rto: float | None = float(rto) if rto is not None else None
        self.rto_estimator: RtoEstimator = RtoEstimator()
        self.max_retries = max_retries
        self.idle_timeout = idle_timeout
        self.run_dir: Path | None = Path(run_dir) if run_dir is not None else None

        self.send_base: int = 0       # lowest un-ACKed sequence number
        self.next_seq_num: int = 0    # next sequence number to allocate / send
        self.packets: dict[int, InFlightPacket] = {}

        # Loss epoch gate: at most one backoff per epoch
        self.loss_epoch_end: int | None = None
        self.loss_epoch_retry: int = 0

        # Trace logging
        self._start_time: float = 0.0
        self._rtt_csv_file = None

        # Flow control: initialize right_edge from peer (e.g. from SYN|ACK rwnd, ack=0)
        peer_rwnd = getattr(self.conn, "peer_rwnd", None)
        initial_rwnd = peer_rwnd if peer_rwnd is not None else DEFAULT_RECEIVER_CAPACITY
        self.right_edge: int = initial_rwnd
        self.persist_deadline: float | None = (
            (time.monotonic() + self.current_rto) if self.right_edge <= self.send_base else None
        )
        self.persist_interval: float | None = (
            self.current_rto if self.right_edge <= self.send_base else None
        )
        self.probes_sent: int = 0
        self.window_violations: list[tuple[int, int, int]] = []

        # Congestion control (units: packets, cwnd is a float)
        self.cc: CongestionControl = CongestionControl(max_window=self.window_size)
        self.limited_by_rwnd: int = 0
        self.limited_by_cwnd: int = 0
        self.limited_by_max: int = 0
        self._cwnd_csv_file = None

        # Telemetry counters
        # Invariant: data_sent_total == unique_data_packets + retx_total
        self.data_sent_total: int = 0
        self.unique_data_packets: int = 0
        self.retx_timeout: int = 0
        self.bytes_sent_total: int = 0
        self.max_in_flight: int = 0

    @property
    def rwnd(self) -> int:
        """Receive window available at peer: max(0, right_edge - send_base)."""
        return max(0, self.right_edge - self.send_base)

    @property
    def effective_window(self) -> int:
        """Effective transmission window: min(right_edge - send_base, floor(cwnd), max_window)."""
        rwnd_limit = max(0, self.right_edge - self.send_base)
        cwnd_limit = self.cc.usable_window
        return min(rwnd_limit, cwnd_limit, self.window_size)

    @property
    def current_rto(self) -> float:
        """Effective RTO: fixed if explicitly provided, else adaptive from estimator."""
        if self.fixed_rto is not None:
            return self.fixed_rto
        return self.rto_estimator.current_rto

    @property
    def rto(self) -> float:
        """Alias for current_rto."""
        return self.current_rto

    @rto.setter
    def rto(self, value: float | None) -> None:
        self.fixed_rto = float(value) if value is not None else None

    @property
    def retx_total(self) -> int:
        """Alias for retx_timeout per Phase 4 counter specification."""
        return self.retx_timeout

    def get_counters(self) -> dict[str, int]:
        """Return sender telemetry counters as a dictionary."""
        return {
            "data_sent_total": self.data_sent_total,
            "unique_data_packets": self.unique_data_packets,
            "retx_timeout": self.retx_timeout,
            "retx_total": self.retx_total,
            "bytes_sent_total": self.bytes_sent_total,
            "max_in_flight": self.max_in_flight,
        }

    @property
    def conn_id(self) -> int:
        return self.conn.conn_id

    @property
    def peer_addr(self) -> tuple[str, int]:
        assert self.conn.peer_addr is not None
        return self.conn.peer_addr

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _check_window_invariants(self) -> None:
        """Record any in-flight packet whose sequence number violates right_edge."""
        for p in self.packets.values():
            if p.state == PacketState.IN_FLIGHT:
                if p.seq >= self.right_edge:
                    self.window_violations.append((p.seq, self.send_base, self.right_edge))

    def _log_cwnd(self, event: str) -> None:
        """Log cwnd transition to cwnd.csv if run_dir was specified."""
        if self._cwnd_csv_file is not None:
            now = time.monotonic()
            t = now - self._start_time
            in_flight = sum(
                1 for p in self.packets.values() if p.state == PacketState.IN_FLIGHT
            )
            self._cwnd_csv_file.write(
                f"{t:.6f},{self.cc.cwnd:.4f},{self.cc.ssthresh},{self.rwnd},"
                f"{self.effective_window},{in_flight},{self.send_base},{event}\n"
            )
            self._cwnd_csv_file.flush()

    def _send_probe(self) -> None:
        """Transmit a zero-length DATA probe under zero-window condition.

        Probes consume no sequence number, are never counted as retransmissions,
        and never produce RTT samples.
        """
        now = time.monotonic()
        probe_pkt = Packet(
            conn_id=self.conn_id,
            seq=self.next_seq_num,  # next never-sent sequence number
            ack=0,
            sack=0,
            rwnd=self.conn.rwnd if hasattr(self.conn, "rwnd") else 64,
            flags=FLAG_DATA,
            payload=b"",
        )
        self.conn.transport.send_packet(probe_pkt, self.peer_addr)
        self.probes_sent += 1
        logger.debug(
            "[SR] Sent zero-window probe seq=%d (interval=%.3fs)",
            self.next_seq_num,
            self.persist_interval if self.persist_interval is not None else 0.0,
        )
        if self.persist_interval is None:
            self.persist_interval = self.current_rto
        self.persist_interval = min(self.persist_interval * 2.0, PERSIST_TIMER_CAP)
        self.persist_deadline = now + self.persist_interval

    def _send_packet(self, packet_info: InFlightPacket, *, initial: bool) -> None:
        """Encode and transmit *packet_info* on the wire; update its deadline.

        Args:
            packet_info: The slot to transmit.
            initial:     True for the first transmission; False for a retx.
        """
        now = time.monotonic()
        pkt = Packet(
            conn_id=self.conn_id,
            seq=packet_info.seq,
            ack=0,
            sack=0,
            rwnd=self.conn.rwnd if hasattr(self.conn, "rwnd") else 64,
            flags=FLAG_DATA,
            payload=packet_info.payload,
        )
        self.conn.transport.send_packet(pkt, self.peer_addr)
        if initial:
            packet_info.first_send_time = now
        packet_info.send_time = now
        packet_info.deadline = now + self.current_rto
        packet_info.state = PacketState.IN_FLIGHT
        self._check_window_invariants()

        # Maintain counter invariant
        self.data_sent_total += 1
        self.conn.stats.data_sent += 1
        if initial:
            self.unique_data_packets += 1
            self.bytes_sent_total += len(packet_info.payload)
        else:
            self.retx_timeout += 1

        # Track peak simultaneous in-flight count
        in_flight = sum(
            1 for p in self.packets.values() if p.state == PacketState.IN_FLIGHT
        )
        if in_flight > self.max_in_flight:
            self.max_in_flight = in_flight

    def _process_ack(self, ack_pkt: Packet) -> None:
        """Process a received ACK packet (cumulative + echoed-seq + SACK bitmap)."""
        # Step 2 Fix A: Ignore any ACK whose ack field is greater than self.next_seq_num
        if ack_pkt.ack > self.next_seq_num:
            self.conn.stats.unexpected_packets_count += 1
            return

        now = time.monotonic()
        cum_ack = ack_pkt.ack
        echoed_seq = ack_pkt.seq
        sack_bitmap = ack_pkt.sack

        # Update right edge of advertised receive window from peer
        self.right_edge = max(self.right_edge, ack_pkt.ack + ack_pkt.rwnd)
        if self.right_edge > self.send_base:
            self.persist_deadline = None
            self.persist_interval = None
        else:
            if self.persist_deadline is None:
                self.persist_interval = self.current_rto
                self.persist_deadline = now + self.persist_interval

        # Probe replies and window refreshes echo PROBE_ECHO_SEQ:
        # they refresh window information but must never mark any packet ACKED.
        if ack_pkt.seq == PROBE_ECHO_SEQ:
            return

        newly_acked = 0
        saw_ss_exit = False

        # 1. Echoed-seq: Karn's rule & state update
        # Sample RTT only from the ACK of a packet that was never retransmitted (Karn)
        if (
            echoed_seq in self.packets
            and self.packets[echoed_seq].state == PacketState.IN_FLIGHT
        ):
            pkt_slot = self.packets[echoed_seq]
            if pkt_slot.retry_count == 0:
                sample_rtt = now - pkt_slot.send_time
                self.rto_estimator.on_sample(sample_rtt)
                if (
                    self._rtt_csv_file is not None
                    and self.rto_estimator.srtt is not None
                    and self.rto_estimator.rttvar is not None
                ):
                    t = now - self._start_time
                    self._rtt_csv_file.write(
                        f"{t:.6f},{sample_rtt:.6f},{self.rto_estimator.srtt:.6f},"
                        f"{self.rto_estimator.rttvar:.6f},{self.rto_estimator.current_rto:.6f},"
                        f"{self.rto_estimator.backoff}\n"
                    )
                    self._rtt_csv_file.flush()

            pkt_slot.state = PacketState.ACKED
            pkt_slot.deadline = 0.0
            if self.cc.on_new_ack():
                saw_ss_exit = True
            newly_acked += 1

        # 2. SACK bitmap: bit i (0 = LSB) ⇒ cum_ack + 1 + i is selectively ACKed
        # Process SACK only if cum_ack >= send_base (not from a stale ACK)
        if sack_bitmap > 0 and cum_ack >= self.send_base:
            for i in range(32):
                if sack_bitmap & (1 << i):
                    sack_seq = cum_ack + 1 + i
                    if (
                        sack_seq in self.packets
                        and self.packets[sack_seq].state == PacketState.IN_FLIGHT
                    ):
                        self.packets[sack_seq].state = PacketState.ACKED
                        self.packets[sack_seq].deadline = 0.0
                        if self.cc.on_new_ack():
                            saw_ss_exit = True
                        newly_acked += 1

        # 3. Cumulative ACK: advance send_base; mark all packets below as ACKED
        if cum_ack > self.send_base:
            for s in range(self.send_base, min(cum_ack, self.next_seq_num)):
                if s in self.packets and self.packets[s].state == PacketState.IN_FLIGHT:
                    self.packets[s].state = PacketState.ACKED
                    self.packets[s].deadline = 0.0
                    if self.cc.on_new_ack():
                        saw_ss_exit = True
                    newly_acked += 1
            self.send_base = cum_ack

        # 4. Advance send_base past any contiguous ACKED packets
        while (
            self.send_base in self.packets
            and self.packets[self.send_base].state == PacketState.ACKED
        ):
            self.send_base += 1

        # Check loss epoch gate: an epoch ends when send_base passes highest seq sent at timeout
        if self.loss_epoch_end is not None and self.send_base > self.loss_epoch_end:
            self.loss_epoch_end = None
            self.loss_epoch_retry = 0

        # Maintain persist timer state after send_base advance
        if self.right_edge <= self.send_base:
            if self.persist_deadline is None:
                self.persist_interval = self.current_rto
                self.persist_deadline = now + self.persist_interval
        else:
            self.persist_deadline = None
            self.persist_interval = None

        # 5. Garbage-collect fully acknowledged entries strictly below send_base
        for s in list(self.packets.keys()):
            if s < self.send_base:
                del self.packets[s]

        self._check_window_invariants()

        # Log cwnd event if any packet was newly ACKed (at most one row per ACK)
        if newly_acked > 0:
            event = "ss_exit" if saw_ss_exit else "ack"
            self._log_cwnd(event=event)

    def _check_timeouts(self) -> None:
        """Retransmit every IN_FLIGHT packet whose per-packet deadline has expired."""
        now = time.monotonic()
        for seq in range(self.send_base, self.next_seq_num):
            p = self.packets.get(seq)
            if p is not None and p.state == PacketState.IN_FLIGHT and now >= p.deadline:
                p.retry_count += 1
                if p.retry_count >= self.max_retries:
                    raise ConnectionAbortError(
                        f"SR transfer aborted: packet seq={seq} exceeded max retries"
                        f" ({self.max_retries})"
                    )

                # Loss-epoch gate: at most one backoff and one cwnd reduction per epoch.
                # An epoch ends when send_base passes highest seq sent at timeout.
                if self.loss_epoch_end is None or self.send_base > self.loss_epoch_end:
                    self.loss_epoch_end = self.next_seq_num - 1
                    self.loss_epoch_retry = p.retry_count
                    self.rto_estimator.on_timeout()
                    self.cc.on_timeout()
                    self._log_cwnd(event="timeout")
                elif p.retry_count > self.loss_epoch_retry:
                    self.loss_epoch_end = self.next_seq_num - 1
                    self.loss_epoch_retry = p.retry_count
                    self.rto_estimator.on_timeout()
                    self.cc.on_timeout()
                    self._log_cwnd(event="timeout")

                logger.debug(
                    "[SR] Retransmitting seq=%d (attempt %d/%d)",
                    seq,
                    p.retry_count,
                    self.max_retries,
                )
                self._send_packet(p, initial=False)  # retx only this packet

    def _get_nearest_deadline(self) -> float:
        """Return the select() timeout until the earliest in-flight packet or persist deadline."""
        now = time.monotonic()
        earliest: float | None = None
        for p in self.packets.values():
            if p.state == PacketState.IN_FLIGHT:
                if earliest is None or p.deadline < earliest:
                    earliest = p.deadline
        if self.rwnd == 0 and self.persist_deadline is not None:
            if earliest is None or self.persist_deadline < earliest:
                earliest = self.persist_deadline
        if earliest is None:
            return min(0.05, self.idle_timeout)  # no in-flight packets; poll quickly
        return max(0.001, min(0.05, earliest - now))

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def send_stream(self, stream_data: bytes | Iterable[bytes]) -> int:
        """Stream *stream_data* pipelined through the SR sliding window.

        Collects all payload chunks, assigns sequence numbers, and drives the
        SR event loop until every chunk has been cumulatively acknowledged.
        The caller's connection ``seq_next`` attribute is updated on return.

        Returns:
            Total payload bytes accepted (excluding RTP headers).
        """
        if self.conn.state != RtpState.ESTABLISHED:
            raise ConnectionAbortError(
                f"Cannot send data in state {self.conn.state.name}"
            )

        # -- Collect all chunks ------------------------------------------
        chunks: list[bytes] = []
        if isinstance(stream_data, (bytes, bytearray, memoryview)):
            data = bytes(stream_data)
            offset = 0
            while offset < len(data):
                chunks.append(data[offset : offset + MAX_PAYLOAD_SIZE])
                offset += MAX_PAYLOAD_SIZE
        else:
            for part in stream_data:
                part = bytes(part)
                offset = 0
                while offset < len(part):
                    chunks.append(part[offset : offset + MAX_PAYLOAD_SIZE])
                    offset += MAX_PAYLOAD_SIZE

        total_chunks = len(chunks)
        chunk_idx = 0  # points at the next chunk to be allocated/sent
        last_valid_packet_time = time.monotonic()

        self._start_time = time.monotonic()
        if self.run_dir is not None:
            self.run_dir.mkdir(parents=True, exist_ok=True)
            self._rtt_csv_file = open(self.run_dir / "rtt.csv", "w", encoding="utf-8")
            self._rtt_csv_file.write("t,sample_rtt,srtt,rttvar,rto,backoff\n")
            self._rtt_csv_file.flush()

            self._cwnd_csv_file = open(self.run_dir / "cwnd.csv", "w", encoding="utf-8")
            self._cwnd_csv_file.write(
                "t,cwnd,ssthresh,rwnd,effective_window,in_flight,send_base,event\n"
            )
            self._cwnd_csv_file.flush()

        try:
            # -- SR event loop: run until send_base catches up to total_chunks -
            while self.send_base < total_chunks:

                # Check idle inactivity timeout
                now = time.monotonic()
                if (now - last_valid_packet_time) >= self.idle_timeout:
                    raise ConnectionAbortError(
                        f"SR transfer aborted: idle timeout ({self.idle_timeout}s) exceeded with no valid packet"
                    )

                # Check persist timer under zero-window
                if self.rwnd == 0 and chunk_idx < total_chunks:
                    if self.persist_deadline is None:
                        self.persist_interval = self.current_rto
                        self.persist_deadline = now + self.persist_interval
                    elif now >= self.persist_deadline:
                        self._send_probe()

                # Phase 1 — Fill the window: seq < send_base + W (W = min(rwnd, floor(cwnd), max_window))
                while chunk_idx < total_chunks:
                    rwnd_limit = max(0, self.right_edge - self.send_base)
                    cwnd_limit = self.cc.usable_window
                    max_limit = self.window_size
                    w = min(rwnd_limit, cwnd_limit, max_limit)

                    if self.next_seq_num < self.send_base + w:
                        if rwnd_limit < cwnd_limit and rwnd_limit < max_limit:
                            self.limited_by_rwnd += 1
                        elif cwnd_limit < max_limit and cwnd_limit <= rwnd_limit:
                            self.limited_by_cwnd += 1
                        else:
                            self.limited_by_max += 1

                        payload = chunks[chunk_idx]
                        p = InFlightPacket(seq=self.next_seq_num, payload=payload)
                        self.packets[self.next_seq_num] = p
                        self._send_packet(p, initial=True)
                        self.next_seq_num += 1
                        chunk_idx += 1
                    else:
                        break

                # Phase 2 — Check per-packet RTO deadlines
                self._check_timeouts()

                # Phase 3 — Block until an ACK arrives (or next deadline)
                timeout = self._get_nearest_deadline()
                res = self.conn.transport.receive(timeout=timeout)
                if res is not None:
                    recv_pkt, _addr = res
                    if recv_pkt.conn_id == self.conn_id:
                        last_valid_packet_time = time.monotonic()
                        if recv_pkt.flags == FLAG_ACK:
                            self.conn.stats.ack_received += 1
                            self._process_ack(recv_pkt)
                        else:
                            self.conn.stats.unexpected_packets_count += 1
                    else:
                        self.conn.stats.foreign_conn_id_count += 1

                # Phase 4 — Drain any burst of additional ACKs without blocking
                while True:
                    burst = self.conn.transport.receive(timeout=0.0)
                    if burst is None:
                        break
                    recv_pkt, _addr = burst
                    if recv_pkt.conn_id == self.conn_id:
                        last_valid_packet_time = time.monotonic()
                        if recv_pkt.flags == FLAG_ACK:
                            self.conn.stats.ack_received += 1
                            self._process_ack(recv_pkt)
                        else:
                            self.conn.stats.unexpected_packets_count += 1
                    else:
                        self.conn.stats.foreign_conn_id_count += 1

            # Synchronise conn.seq_next so teardown FIN has the right sequence
            if hasattr(self.conn, "seq_next"):
                self.conn.seq_next = self.next_seq_num

            return sum(len(c) for c in chunks)
        finally:
            if self._rtt_csv_file is not None:
                try:
                    self._rtt_csv_file.flush()
                    self._rtt_csv_file.close()
                except Exception:
                    pass
                self._rtt_csv_file = None

            if self._cwnd_csv_file is not None:
                try:
                    self._cwnd_csv_file.flush()
                    self._cwnd_csv_file.close()
                except Exception:
                    pass
                self._cwnd_csv_file = None

