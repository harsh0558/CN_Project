"""Deterministic UDP Impairment Proxy for RTP Testing.

Acts as a transparent bidirectional proxy between RTP endpoints with:
- Per-direction impairment controls (drop, delay, jitter, duplicate, reorder, corrupt).
- Seeded pseudo-random number generator for exact reproducibility.
- Targeted programmatic impairment triggers (e.g., drop specific flag combinations).
- Threaded background daemon mode and synchronous step mode.
- Command-line interface for standalone test runs.
"""

from __future__ import annotations

import argparse
import heapq
import logging
import random
import select
import socket
import threading
import time
from dataclasses import dataclass, field
from typing import Callable, Final

from rtp.packet import HEADER_SIZE, Packet, PacketError

logger = logging.getLogger("tools.impair_proxy")


@dataclass
class DirectionConfig:
    """Impairment parameters for one traffic direction."""
    drop_rate: float = 0.0
    delay_ms: float = 0.0
    jitter_ms: float = 0.0
    duplicate_rate: float = 0.0
    reorder_rate: float = 0.0
    reorder_delay_ms: float = 20.0
    corrupt_rate: float = 0.0


@dataclass(order=True)
class ScheduledDatagram:
    """Scheduled datagram queued for delayed transmission."""
    send_time: float
    sock: socket.socket = field(compare=False)
    data: bytes = field(compare=False)
    dest_addr: tuple[str, int] = field(compare=False)
    direction: str = field(compare=False)


@dataclass
class ImpairmentStats:
    """Telemetry counters for proxy forwarding and impairments."""
    received: int = 0
    forwarded: int = 0
    dropped: int = 0
    delayed: int = 0
    duplicated: int = 0
    reordered: int = 0
    corrupted: int = 0


@dataclass
class TargetedDropRule:
    direction: str
    count: int
    flags: int | None = None
    seq: int | None = None


@dataclass
class TargetedReorderRule:
    direction: str
    count: int
    delay_ms: float = 40.0
    flags: int | None = None
    seq: int | None = None


class ImpairmentProxy:
    """Bidirectional UDP proxy with deterministic impairment injection."""

    def __init__(
        self,
        listen_addr: tuple[str, int] = ("127.0.0.1", 9500),
        target_addr: tuple[str, int] = ("127.0.0.1", 9000),
        c2s_config: DirectionConfig | None = None,
        s2c_config: DirectionConfig | None = None,
        seed: int | None = 42,
    ) -> None:
        self.listen_addr = listen_addr
        self.target_addr = target_addr
        self.c2s_config = c2s_config if c2s_config is not None else DirectionConfig()
        self.s2c_config = s2c_config if s2c_config is not None else DirectionConfig()
        self.rng = random.Random(seed)

        self.c2s_stats = ImpairmentStats()
        self.s2c_stats = ImpairmentStats()

        self._targeted_drops: list[TargetedDropRule] = []
        self._targeted_dups: list[TargetedDropRule] = []
        self._targeted_reorders: list[TargetedReorderRule] = []

        # Socket setup
        self._client_sock: socket.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._client_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._client_sock.setblocking(False)
        self._client_sock.bind(self.listen_addr)
        self.bound_listen_addr = self._client_sock.getsockname()

        self._server_sock: socket.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_sock.setblocking(False)
        self._server_sock.bind((self.listen_addr[0], 0))
        self.bound_server_addr = self._server_sock.getsockname()

        self._client_addr: tuple[str, int] | None = None
        self._delayed_queue: list[ScheduledDatagram] = []
        self._running = False
        self._thread: threading.Thread | None = None

    def drop_next(
        self,
        direction: str = "c2s",
        count: int = 1,
        flags: int | None = None,
        seq: int | None = None,
    ) -> None:
        """Register a deterministic drop rule for the next matching packets."""
        self._targeted_drops.append(TargetedDropRule(direction, count, flags, seq))

    def duplicate_next(
        self,
        direction: str = "c2s",
        count: int = 1,
        flags: int | None = None,
        seq: int | None = None,
    ) -> None:
        """Register a deterministic duplication rule for the next matching packets."""
        self._targeted_dups.append(TargetedDropRule(direction, count, flags, seq))

    def reorder_next(
        self,
        direction: str = "c2s",
        count: int = 1,
        delay_ms: float = 40.0,
        flags: int | None = None,
        seq: int | None = None,
    ) -> None:
        """Register a deterministic reorder rule by delaying the next matching packet."""
        self._targeted_reorders.append(
            TargetedReorderRule(direction, count, delay_ms, flags, seq)
        )

    def _should_targeted_reorder(self, direction: str, data: bytes) -> float | None:
        pkt = self._parse_packet_safe(data)
        for rule in self._targeted_reorders:
            if rule.direction != direction or rule.count <= 0:
                continue
            if rule.flags is not None:
                if pkt is None or pkt.flags != rule.flags:
                    continue
            if rule.seq is not None:
                if pkt is None or pkt.seq != rule.seq:
                    continue
            rule.count -= 1
            return rule.delay_ms
        return None

    def _should_targeted_drop(self, direction: str, data: bytes) -> bool:
        pkt = self._parse_packet_safe(data)
        for rule in self._targeted_drops:
            if rule.direction != direction or rule.count <= 0:
                continue
            if rule.flags is not None:
                if pkt is None or pkt.flags != rule.flags:
                    continue
            if rule.seq is not None:
                if pkt is None or pkt.seq != rule.seq:
                    continue
            rule.count -= 1
            return True
        return False

    def _should_targeted_dup(self, direction: str, data: bytes) -> bool:
        pkt = self._parse_packet_safe(data)
        for rule in self._targeted_dups:
            if rule.direction != direction or rule.count <= 0:
                continue
            if rule.flags is not None:
                if pkt is None or pkt.flags != rule.flags:
                    continue
            if rule.seq is not None:
                if pkt is None or pkt.seq != rule.seq:
                    continue
            rule.count -= 1
            return True
        return False

    def _parse_packet_safe(self, data: bytes) -> Packet | None:
        if len(data) < HEADER_SIZE:
            return None
        try:
            return Packet.decode(data)
        except Exception:
            return None

    def _corrupt_data(self, data: bytes) -> bytes:
        if not data:
            return data
        ba = bytearray(data)
        # Flip a bit in the byte array (excluding conn_id if possible, or anywhere)
        byte_idx = self.rng.randint(0, len(ba) - 1)
        bit_idx = self.rng.randint(0, 7)
        ba[byte_idx] ^= 1 << bit_idx
        return bytes(ba)

    def _process_packet(
        self,
        direction: str,
        data: bytes,
        out_sock: socket.socket,
        dest_addr: tuple[str, int],
    ) -> None:
        stats = self.c2s_stats if direction == "c2s" else self.s2c_stats
        config = self.c2s_config if direction == "c2s" else self.s2c_config
        stats.received += 1

        # Check targeted drop rule
        if self._should_targeted_drop(direction, data):
            stats.dropped += 1
            logger.info("[%s] Targeted drop applied to packet (%d bytes)", direction, len(data))
            return

        # Check probabilistic drop
        if config.drop_rate > 0.0 and self.rng.random() < config.drop_rate:
            stats.dropped += 1
            logger.debug("[%s] Probabilistic drop (%d bytes)", direction, len(data))
            return

        # Check corruption
        if config.corrupt_rate > 0.0 and self.rng.random() < config.corrupt_rate:
            data = self._corrupt_data(data)
            stats.corrupted += 1

        # Check targeted or probabilistic duplication
        should_dup = self._should_targeted_dup(direction, data) or (
            config.duplicate_rate > 0.0 and self.rng.random() < config.duplicate_rate
        )

        now = time.monotonic()
        delay_sec = 0.0
        if config.delay_ms > 0.0:
            jitter = (
                self.rng.uniform(-config.jitter_ms, config.jitter_ms)
                if config.jitter_ms > 0.0
                else 0.0
            )
            delay_sec = max(0.0, (config.delay_ms + jitter) / 1000.0)

        # Check reordering (delay this packet longer than usual)
        targeted_reorder_ms = self._should_targeted_reorder(direction, data)
        if targeted_reorder_ms is not None:
            delay_sec += targeted_reorder_ms / 1000.0
            stats.reordered += 1
        elif config.reorder_rate > 0.0 and self.rng.random() < config.reorder_rate:
            delay_sec += config.reorder_delay_ms / 1000.0
            stats.reordered += 1

        if delay_sec > 0.001:
            stats.delayed += 1
            send_time = now + delay_sec
            heapq.heappush(
                self._delayed_queue,
                ScheduledDatagram(send_time, out_sock, data, dest_addr, direction),
            )
            if should_dup:
                stats.duplicated += 1
                heapq.heappush(
                    self._delayed_queue,
                    ScheduledDatagram(send_time + 0.002, out_sock, data, dest_addr, direction),
                )
        else:
            stats.forwarded += 1
            try:
                out_sock.sendto(data, dest_addr)
            except OSError as exc:
                stats.forwarded -= 1
                logger.debug("[%s] Error sending immediately: %s", direction, exc)

            if should_dup:
                stats.duplicated += 1
                stats.forwarded += 1
                try:
                    out_sock.sendto(data, dest_addr)
                except OSError:
                    stats.forwarded -= 1

    def _flush_delayed_queue(self) -> None:
        now = time.monotonic()
        while self._delayed_queue and self._delayed_queue[0].send_time <= now:
            item = heapq.heappop(self._delayed_queue)
            stats = self.c2s_stats if item.direction == "c2s" else self.s2c_stats
            stats.forwarded += 1
            try:
                item.sock.sendto(item.data, item.dest_addr)
            except OSError as exc:
                stats.forwarded -= 1
                logger.debug("[%s] Error sending delayed packet: %s", item.direction, exc)

    def step(self, timeout: float = 0.005) -> None:
        """Run a single iteration of packet forwarding and delayed flushing."""
        self._flush_delayed_queue()

        now = time.monotonic()
        select_timeout = timeout
        if self._delayed_queue:
            next_time = self._delayed_queue[0].send_time
            time_until_send = max(0.0, next_time - now)
            select_timeout = min(timeout, time_until_send)

        try:
            r_ready, _, _ = select.select(
                [self._client_sock, self._server_sock],
                [],
                [],
                select_timeout,
            )
        except (ValueError, OSError):
            return

        for sock in r_ready:
            try:
                data, addr = sock.recvfrom(65535)
            except (BlockingIOError, OSError):
                continue

            if sock is self._client_sock:
                # Client -> Server
                self._client_addr = addr
                self._process_packet("c2s", data, self._server_sock, self.target_addr)
            elif sock is self._server_sock:
                # Server -> Client
                if self._client_addr is not None:
                    self._process_packet("s2c", data, self._client_sock, self._client_addr)

        self._flush_delayed_queue()

    def _run_loop(self) -> None:
        while self._running:
            self.step(timeout=0.01)

    def start(self) -> ImpairmentProxy:
        """Start the proxy event loop in a background daemon thread."""
        if self._running:
            return self
        self._running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True, name="ImpairmentProxy")
        self._thread.start()
        logger.info(
            "ImpairmentProxy running: listen=%s -> target=%s",
            self.bound_listen_addr,
            self.target_addr,
        )
        return self

    def stop(self) -> None:
        """Stop the background thread and close sockets."""
        self._running = False
        if self._thread is not None and self._thread.is_alive():
            self._thread.join(timeout=1.0)
            self._thread = None

        try:
            self._client_sock.close()
        except OSError:
            pass
        try:
            self._server_sock.close()
        except OSError:
            pass
        logger.info("ImpairmentProxy stopped")

    def __enter__(self) -> ImpairmentProxy:
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="RTP Impairment UDP Proxy")
    parser.add_argument("--listen-host", default="127.0.0.1", help="Listening IP host")
    parser.add_argument("--listen-port", type=int, default=9500, help="Listening UDP port")
    parser.add_argument("--server-host", default="127.0.0.1", help="Target server IP host")
    parser.add_argument("--server-port", type=int, default=9000, help="Target server UDP port")
    parser.add_argument("--seed", type=int, default=42, help="PRNG seed for deterministic runs")

    # Impairments
    parser.add_argument("--drop-c2s", type=float, default=0.0, help="Client-to-Server drop rate")
    parser.add_argument("--drop-s2c", type=float, default=0.0, help="Server-to-Client drop rate")
    parser.add_argument("--delay-c2s", type=float, default=0.0, help="C2S base delay in ms")
    parser.add_argument("--delay-s2c", type=float, default=0.0, help="S2C base delay in ms")
    parser.add_argument("--jitter-c2s", type=float, default=0.0, help="C2S jitter in ms")
    parser.add_argument("--jitter-s2c", type=float, default=0.0, help="S2C jitter in ms")
    parser.add_argument("--dup-c2s", type=float, default=0.0, help="C2S duplicate rate")
    parser.add_argument("--dup-s2c", type=float, default=0.0, help="S2C duplicate rate")
    parser.add_argument("--reorder-c2s", type=float, default=0.0, help="C2S reorder rate")
    parser.add_argument("--reorder-s2c", type=float, default=0.0, help="S2C reorder rate")

    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    c2s_cfg = DirectionConfig(
        drop_rate=args.drop_c2s,
        delay_ms=args.delay_c2s,
        jitter_ms=args.jitter_c2s,
        duplicate_rate=args.dup_c2s,
        reorder_rate=args.reorder_c2s,
    )
    s2c_cfg = DirectionConfig(
        drop_rate=args.drop_s2c,
        delay_ms=args.delay_s2c,
        jitter_ms=args.jitter_s2c,
        duplicate_rate=args.dup_s2c,
        reorder_rate=args.reorder_s2c,
    )

    proxy = ImpairmentProxy(
        listen_addr=(args.listen_host, args.listen_port),
        target_addr=(args.server_host, args.server_port),
        c2s_config=c2s_cfg,
        s2c_config=s2c_cfg,
        seed=args.seed,
    )

    try:
        proxy.start()
        print(f"Proxy active: {args.listen_host}:{args.listen_port} -> {args.server_host}:{args.server_port}")
        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:
        print("\nShutting down proxy...")
    finally:
        proxy.stop()


if __name__ == "__main__":
    main()
