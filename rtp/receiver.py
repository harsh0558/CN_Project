"""Receiving engines for Reliable Transport Protocol (RTP).

Provides:
- StopAndWaitReceiver: Strict in-order single-packet reception.
- SelectiveRepeatReceiver: Out-of-order buffering with capacity R, contiguous-
  prefix delivery, SACK bitmap generation, immediate per-packet ACKs, and
  zero-window probe passthrough (DATA with length 0 is ACKed but not buffered).
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Generator

from rtp.config import DEFAULT_RECEIVER_CAPACITY, FLAG_ACK, FLAG_DATA, FLAG_FIN
from rtp.connection import (
    ConnectionTimeoutError,
    RtpConnection,
    RtpState,
)
from rtp.packet import Packet

logger = logging.getLogger("rtp.receiver")


def compute_sack_bitmap(rcv_base: int, buffer: dict[int, bytes]) -> int:
    """Compute 32-bit SACK bitmap for sequence numbers [rcv_base+1, rcv_base+32].

    Bit i (0 ≤ i ≤ 31, LSB = bit 0) is set when packet (rcv_base + 1 + i)
    has been received and is sitting in the out-of-order buffer.
    """
    bitmap = 0
    for i in range(32):
        if (rcv_base + 1 + i) in buffer:
            bitmap |= 1 << i
    return bitmap


# ---------------------------------------------------------------------------
# Stop-and-Wait receiver
# ---------------------------------------------------------------------------

class StopAndWaitReceiver:
    """Receiver engine ensuring strict in-order delivery without duplicate consumption."""

    def __init__(self, connection: RtpConnection) -> None:
        self.conn = connection
        self.rcv_base: int = 0
        self.eof_received: bool = False
        self.fin_seq: int | None = None
        self._pending_chunks: list[bytes] = []

        # Consume any DATA packet that arrived during the handshake (lost-ACK path)
        if self.conn.initial_data_packet is not None:
            init_pkt = self.conn.initial_data_packet
            if init_pkt.seq == 0:
                self._pending_chunks.append(init_pkt.payload)
                self.rcv_base = 1
                # Send ACK for the handshake-completing DATA packet
                self._send_ack(echoed_seq=0)

    @property
    def conn_id(self) -> int:
        return self.conn.conn_id

    @property
    def peer_addr(self) -> tuple[str, int]:
        return self.conn.peer_addr

    def _send_ack(self, echoed_seq: int) -> None:
        ack_pkt = Packet(
            conn_id=self.conn_id,
            seq=echoed_seq,
            ack=self.rcv_base,
            sack=0,
            rwnd=self.conn.rcvbuf_capacity,
            flags=FLAG_ACK,
            payload=b"",
        )
        self.conn.transport.send_packet(ack_pkt, self.peer_addr)
        self.conn.stats.ack_sent += 1

    def receive_chunk(self, timeout: float = 30.0) -> bytes:
        """Return the next in-order payload chunk, blocking up to *timeout* seconds."""
        if self._pending_chunks:
            return self._pending_chunks.pop(0)
        if self.eof_received:
            return b""

        start_time = time.monotonic()

        while not self.eof_received:
            now = time.monotonic()
            if (now - start_time) >= timeout:
                raise ConnectionTimeoutError(
                    "Timed out waiting for data packet in SAW receive_chunk"
                )
            remaining = max(0.01, (start_time + timeout) - now)
            res = self.conn.transport.receive(timeout=min(remaining, 0.5))
            if res is None:
                continue

            pkt, _addr = res
            if pkt.conn_id != self.conn_id:
                self.conn.stats.foreign_conn_id_count += 1
                continue

            if pkt.flags == FLAG_DATA:
                self.conn.stats.data_received += 1
                if pkt.seq == self.rcv_base:
                    self.rcv_base += 1
                    self._send_ack(echoed_seq=pkt.seq)
                    return pkt.payload
                elif pkt.seq < self.rcv_base:
                    # Old duplicate — re-ACK and discard
                    self.conn.stats.duplicate_control_count += 1
                    self._send_ack(echoed_seq=pkt.seq)
                else:
                    # Future packet in SAW context — re-ACK current base
                    self._send_ack(echoed_seq=pkt.seq)

            elif pkt.flags == FLAG_FIN:
                if pkt.seq != self.rcv_base:
                    self.conn.stats.unexpected_packets_count += 1
                    continue
                self.conn.stats.fin_received += 1
                self.eof_received = True
                self.fin_seq = pkt.seq
                self.conn.received_fin_seq = pkt.seq
                return b""

            else:
                self.conn.stats.unexpected_packets_count += 1

        return b""

    def stream(self, timeout: float = 30.0) -> Generator[bytes, None, None]:
        """Yield all in-order payload chunks until EOF."""
        while not self.eof_received:
            chunk = self.receive_chunk(timeout=timeout)
            if chunk == b"":
                break
            yield chunk


# ---------------------------------------------------------------------------
# Selective Repeat receiver
# ---------------------------------------------------------------------------

class SelectiveRepeatReceiver:
    """Out-of-order buffering receiver with capacity R and contiguous delivery.

    Spec-compliant behaviour (docs/PROTOCOL.md / Phase 4):
    - Accepts packets with seq in [rcv_base, rcv_base + R); drops beyond window.
    - Re-ACKs packets already below rcv_base (old / duplicate).
    - Buffers in-window out-of-order packets; delivers contiguous prefix only.
    - ACKs every arriving DATA packet immediately with (cum_ack, SACK bitmap, rwnd).
    - DATA with length 0 is a zero-window probe: ACKed immediately, never buffered.
    - Counters: dup_received, ooo_received, dropped_beyond_window.
    """

    def __init__(
        self,
        connection: RtpConnection,
        capacity: int = DEFAULT_RECEIVER_CAPACITY,
        app_read_rate: float = 0.0,
        run_dir: str | Path | None = None,
    ) -> None:
        self.conn = connection
        self.capacity = capacity
        self.app_read_rate = float(app_read_rate)
        self.run_dir: Path | None = Path(run_dir) if run_dir is not None else None

        self.rcv_base: int = 0
        self.eof_received: bool = False
        self.fin_seq: int | None = None

        self.buffer: dict[int, bytes] = {}        # out-of-order holding area
        self._delivered_queue: list[bytes] = []   # contiguous in-order chunks ready for app

        # Telemetry counters
        self.dup_received: int = 0
        self.ooo_received: int = 0
        self.dropped_beyond_window: int = 0
        self.max_occupancy: int = 0

        # Rate-limiting app reads (timed drain)
        self._last_consume_time: float = 0.0

        # Trace logging
        self._start_time: float = time.monotonic()
        self._rcvbuf_file = None
        if self.run_dir is not None:
            self.run_dir.mkdir(parents=True, exist_ok=True)
            self._rcvbuf_file = open(self.run_dir / "rcvbuf.csv", "w", encoding="utf-8")
            self._rcvbuf_file.write("t,ooo_buffered,app_backlog,occupancy,rwnd\n")
            self._rcvbuf_file.flush()

        # Consume any DATA that arrived during the handshake
        if self.conn.initial_data_packet is not None:
            self._process_data(self.conn.initial_data_packet)

    @property
    def app_backlog(self) -> int:
        """In-order packets not yet consumed by the app."""
        return len(self._delivered_queue)

    @property
    def ooo_buffered(self) -> int:
        """Out-of-order buffered packets."""
        return len(self.buffer)

    @property
    def occupancy(self) -> int:
        """Total buffer occupancy (out-of-order + app_backlog), never above capacity R."""
        return len(self.buffer) + len(self._delivered_queue)

    @property
    def rwnd(self) -> int:
        """Advertised receive window in packets: max(0, capacity - app_backlog)."""
        return max(0, self.capacity - self.app_backlog)

    def _log_rcvbuf(self) -> None:
        occ = self.occupancy
        if occ > self.max_occupancy:
            self.max_occupancy = occ
        if self._rcvbuf_file is not None:
            t = time.monotonic() - self._start_time
            self._rcvbuf_file.write(
                f"{t:.6f},{self.ooo_buffered},{self.app_backlog},{occ},{self.rwnd}\n"
            )
            self._rcvbuf_file.flush()

    def get_counters(self) -> dict[str, int]:
        """Return receiver telemetry counters as a dictionary."""
        return {
            "dup_received": self.dup_received,
            "ooo_received": self.ooo_received,
            "dropped_beyond_window": self.dropped_beyond_window,
            "max_occupancy": self.max_occupancy,
        }

    @property
    def conn_id(self) -> int:
        return self.conn.conn_id

    @property
    def peer_addr(self) -> tuple[str, int]:
        return self.conn.peer_addr

    def _send_ack(self, echoed_seq: int) -> None:
        """Send immediate ACK with current cum_ack, SACK bitmap, and rwnd."""
        sack = compute_sack_bitmap(self.rcv_base, self.buffer)
        ack_pkt = Packet(
            conn_id=self.conn_id,
            seq=echoed_seq,
            ack=self.rcv_base,
            sack=sack,
            rwnd=self.rwnd,
            flags=FLAG_ACK,
            payload=b"",
        )
        self.conn.transport.send_packet(ack_pkt, self.peer_addr)
        self.conn.stats.ack_sent += 1

    def _send_window_update_ack(self) -> None:
        """Send unsolicited window-update ACK when rwnd goes from 0 to above 0."""
        self._send_ack(echoed_seq=max(0, self.rcv_base - 1))

    def _process_data(self, pkt: Packet) -> None:
        """Process an incoming DATA packet according to SR rules."""
        self.conn.stats.data_received += 1
        seq = pkt.seq

        # --- Zero-window probe: DATA with length 0 ---
        if len(pkt.payload) == 0:
            # Must not buffer it, deliver it, advance rcv_base or count it as duplicate
            self._send_ack(echoed_seq=seq)
            return

        # --- Old duplicate (below rcv_base) ---
        if seq < self.rcv_base:
            self.dup_received += 1
            self._send_ack(echoed_seq=seq)
            return

        # --- Beyond receive window: [rcv_base, rcv_base + rwnd) ---
        if seq >= self.rcv_base + self.rwnd:
            logger.debug(
                "[SR recv] Dropped seq=%d: beyond window [%d, %d)",
                seq, self.rcv_base, self.rcv_base + self.rwnd,
            )
            self.dropped_beyond_window += 1
            if hasattr(self.conn.stats, "out_of_window_packets_count"):
                self.conn.stats.out_of_window_packets_count += 1
            return

        # --- In-window duplicate (already buffered) ---
        if seq in self.buffer:
            self.dup_received += 1
            self._send_ack(echoed_seq=seq)
            return

        # --- In-order: seq == rcv_base ---
        if seq == self.rcv_base:
            self._delivered_queue.append(pkt.payload)
            self.rcv_base += 1
            # Drain any following contiguous buffered packets
            while self.rcv_base in self.buffer:
                self._delivered_queue.append(self.buffer.pop(self.rcv_base))
                self.rcv_base += 1
            assert self.occupancy <= self.capacity, (
                f"Occupancy {self.occupancy} exceeded capacity {self.capacity}"
            )
            self._log_rcvbuf()
            self._send_ack(echoed_seq=seq)
            return

        # --- Out-of-order: buffer and ACK ---
        self.ooo_received += 1
        self.buffer[seq] = pkt.payload
        assert self.occupancy <= self.capacity, (
            f"Occupancy {self.occupancy} exceeded capacity {self.capacity}"
        )
        self._log_rcvbuf()
        self._send_ack(echoed_seq=seq)

    def _pop_delivered_chunk(self) -> bytes:
        """Pop in-order chunk, log buffer state, and send window update if rwnd reopened."""
        prev_rwnd = self.rwnd
        chunk = self._delivered_queue.pop(0)
        self._log_rcvbuf()
        if prev_rwnd == 0 and self.rwnd > 0:
            self._send_window_update_ack()
        return chunk

    def receive_chunk(self, timeout: float = 30.0) -> bytes:
        """Return the next contiguous delivered payload chunk.

        Implements timed drain per app_read_rate while continuously servicing
        the socket so that incoming DATA, ACKs, and zero-window probes are
        never blocked or left unanswered.
        """
        start_time = time.monotonic()
        target_interval = (1.0 / self.app_read_rate) if self.app_read_rate > 0 else 0.0

        while True:
            now = time.monotonic()

            # 1. Check if application can consume an already delivered chunk
            if self._delivered_queue:
                time_since_last = now - self._last_consume_time
                if self.app_read_rate <= 0 or self._last_consume_time == 0.0 or time_since_last >= target_interval:
                    self._last_consume_time = now
                    return self._pop_delivered_chunk()

            # 2. Check EOF condition: FIN received and all delivered/buffered data drained
            if self.eof_received and not self.buffer and not self._delivered_queue:
                self.close()
                return b""

            # 3. Check overall timeout
            if (now - start_time) >= timeout:
                self.close()
                raise ConnectionTimeoutError("Timed out waiting for data in SR receiver")

            # 4. Service socket without blocking app consumption past target_interval
            remaining_total = max(0.001, (start_time + timeout) - now)
            if self._delivered_queue and self.app_read_rate > 0 and self._last_consume_time > 0.0:
                time_until_consume = max(0.001, target_interval - (now - self._last_consume_time))
                select_timeout = min(remaining_total, time_until_consume, 0.05)
            else:
                select_timeout = min(remaining_total, 0.05)

            res = self.conn.transport.receive(timeout=select_timeout)
            if res is None:
                continue

            pkt, _addr = res
            if pkt.conn_id != self.conn_id:
                self.conn.stats.foreign_conn_id_count += 1
                continue

            if pkt.flags == FLAG_DATA:
                self._process_data(pkt)

            elif pkt.flags == FLAG_FIN:
                if pkt.seq != self.rcv_base:
                    self.conn.stats.unexpected_packets_count += 1
                    continue
                self.conn.stats.fin_received += 1
                self.eof_received = True
                self.fin_seq = pkt.seq
                self.conn.received_fin_seq = pkt.seq

            else:
                self.conn.stats.unexpected_packets_count += 1

    def stream(self, timeout: float = 30.0) -> Generator[bytes, None, None]:
        """Yield all contiguous payload chunks until EOF (FIN received and buffer drained)."""
        while True:
            chunk = self.receive_chunk(timeout=timeout)
            if chunk == b"":
                break
            yield chunk

    def close(self) -> None:
        """Close log file and finalize receiver resources."""
        if self._rcvbuf_file is not None:
            try:
                self._rcvbuf_file.flush()
                self._rcvbuf_file.close()
            except Exception:
                pass
            self._rcvbuf_file = None
