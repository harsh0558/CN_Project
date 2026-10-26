"""RTP File Sender application.

Frames a local file with [u16][name][u64][32-byte sha256][data], establishes
an RTP connection, transmits the stream via StopAndWaitSender or
SelectiveRepeatSender (--mode saw|sr, default sr), performs graceful teardown,
and reports outcomes (success, failed, timeout, incomplete).
"""

from __future__ import annotations

import argparse
import hashlib
import logging
import os
import random
import sys
import time
from pathlib import Path

from app.framing import encode_stream_header
from rtp.config import STATUS_MISMATCH, STATUS_OK
from rtp.connection import (
    ConnectionAbortError,
    ConnectionTimeoutError,
    PeerUnreachable,
    RtpClient,
)
from rtp.metrics import SenderSummary, compute_goodput_mbps, write_summary_json
from rtp.sender import SelectiveRepeatSender, StopAndWaitSender

logger = logging.getLogger("app.file_sender")


class FileSender:
    """Manages the client side of an RTP file transfer."""

    def __init__(
        self,
        server_addr: tuple[str, int] = ("127.0.0.1", 9000),
        timeout: float = 30.0,
        rto: float | None = None,
        max_retries: int = 10,
        mode: str = "sr",
        window_size: int = 64,
        run_dir: str | Path | None = None,
        seed: int | None = None,
    ) -> None:
        self.seed = seed
        if seed is not None:
            random.seed(seed)
        self._t_start: float | None = None
        self._t_end: float | None = None
        self._file_bytes: int = 0
        self._exit_reason: str = ""
        self.server_addr = server_addr
        self.timeout = timeout
        self.rto = rto
        self.max_retries = max_retries
        self.mode = mode.lower()
        self.window_size = window_size
        self.run_dir = run_dir
        self._last_sender: StopAndWaitSender | SelectiveRepeatSender | None = None

    def send_file(
        self,
        filepath: str | Path,
        override_filename: str | None = None,
        corrupt_hash: bool = False,
    ) -> tuple[str, int]:
        """Transmit a file; writes sender_summary.json when run_dir is set."""
        self._t_start = None
        self._t_end = None
        self._file_bytes = 0
        self._exit_reason = ""
        outcome, code = self._send_file_impl(filepath, override_filename, corrupt_hash)
        if self._t_end is None:
            self._t_end = time.monotonic()
        if self.run_dir is not None:
            self._write_summary(outcome)
        return outcome, code

    def _write_summary(self, outcome: str) -> None:
        s = self._last_sender
        c = s.get_counters() if s is not None else {}
        t_start = self._t_start if self._t_start is not None else self._t_end
        duration = max(0.0, self._t_end - t_start)
        goodput = compute_goodput_mbps(self._file_bytes, duration) if outcome == "success" else 0.0
        est = getattr(s, "rto_estimator", None)
        cc = getattr(s, "cc", None)
        d = SenderSummary(
            file_bytes=self._file_bytes,
            t_start=t_start,
            t_end=self._t_end,
            duration_s=duration,
            goodput_mbps=goodput,
            data_sent_total=c.get("data_sent_total", 0),
            unique_data_packets=c.get("unique_data_packets", 0),
            retx_total=c.get("retx_total", 0),
            retx_timeout=c.get("retx_timeout", 0),
            retx_other=c.get("retx_other", 0),
            acks_received=c.get("acks_received", 0),
            dup_acks=c.get("dup_acks", 0),
            final_srtt=getattr(est, "srtt", None),
            final_rto=getattr(s, "current_rto", None),
            status=outcome,
            exit_reason=self._exit_reason or outcome,
        ).to_dict()
        d.update(
            mode=self.mode,
            window=self.window_size,
            seed=self.seed,
            max_in_flight=c.get("max_in_flight", 0),
            probes_sent=getattr(s, "probes_sent", 0),
            limited_by_rwnd=getattr(s, "limited_by_rwnd", 0),
            limited_by_cwnd=getattr(s, "limited_by_cwnd", 0),
            limited_by_max=getattr(s, "limited_by_max", 0),
            final_cwnd=getattr(cc, "cwnd", None),
            final_ssthresh=getattr(cc, "ssthresh", None),
        )
        write_summary_json(Path(self.run_dir) / "sender_summary.json", d)

    def _send_file_impl(
        self,
        filepath: str | Path,
        override_filename: str | None = None,
        corrupt_hash: bool = False,
    ) -> tuple[str, int]:
        """Transmit a local file to the remote RTP receiver.

        Returns:
            tuple[str, int]: (outcome: "success" | "failed" | "timeout" | "incomplete",
                              exit_code)
        """
        path = Path(filepath)
        if not path.is_file():
            logger.error("File not found: %s", filepath)
            print("incomplete")
            return "incomplete", 3

        filename = override_filename if override_filename is not None else path.name
        file_size = path.stat().st_size
        self._file_bytes = file_size

        # Compute SHA-256 digest
        hasher = hashlib.sha256()
        with open(path, "rb") as f:
            while chunk := f.read(65536):
                hasher.update(chunk)
        digest = hasher.digest()

        if corrupt_hash:
            ba = bytearray(digest)
            ba[0] ^= 0xFF
            digest = bytes(ba)

        header_bytes = encode_stream_header(filename, file_size, digest)

        client = RtpClient(initial_rto=self.rto if self.rto is not None else 1.0)
        self._t_start = time.monotonic()  # just before connect/SYN
        try:
            client.connect(self.server_addr, timeout=self.timeout)
        except (PeerUnreachable, ConnectionTimeoutError):
            self._exit_reason = "connect timed out / peer unreachable"
            print("timeout")
            return "timeout", 2
        except Exception as exc:
            self._exit_reason = f"connect failed: {exc}"
            logger.error("Connect failed: %s", exc)
            print("incomplete")
            return "incomplete", 3

        try:
            if self.mode == "sr":
                # Selective Repeat: pipeline header + file data as stream
                def data_generator():
                    yield header_bytes
                    with open(path, "rb") as f:
                        while chunk := f.read(65536):
                            yield chunk

                sr_sender = SelectiveRepeatSender(
                    client,
                    window_size=self.window_size,
                    rto=self.rto,
                    max_retries=self.max_retries,
                    run_dir=self.run_dir,
                )
                self._last_sender = sr_sender
                sr_sender.send_stream(data_generator())
            else:
                # Stop-and-Wait: send header then file chunk-by-chunk
                saw_sender = StopAndWaitSender(
                    client,
                    timeout=self.rto if self.rto is not None else 0.5,
                    max_retries=self.max_retries,
                )
                self._last_sender = saw_sender
                saw_sender.send_stream(header_bytes)
                with open(path, "rb") as f:
                    while chunk := f.read(1024):
                        saw_sender.send_packet_stop_and_wait(chunk)

            # Graceful teardown: receive status byte from receiver
            status = client.close(timeout=self.timeout)
            self._t_end = getattr(client, "status_received_at", None) or time.monotonic()  # excludes TIME_WAIT

            if status == STATUS_OK:
                print("success")
                return "success", 0
            else:
                self._exit_reason = "receiver reported hash/size mismatch"
                print("failed")
                return "failed", 1

        except (PeerUnreachable, ConnectionTimeoutError):
            self._exit_reason = "peer unreachable / timed out"
            print("timeout")
            try:
                client.transport.close()
            except Exception:
                pass
            return "timeout", 2

        except ConnectionAbortError as exc:
            self._exit_reason = str(exc)
            logger.warning("Transfer aborted on timeout: %s", exc)
            print("timeout")
            try:
                client.transport.close()
            except Exception:
                pass
            return "timeout", 2

        except Exception as exc:
            self._exit_reason = f"transfer error: {exc}"
            logger.error("Transfer error: %s", exc)
            print("incomplete")
            try:
                client.transport.close()
            except Exception:
                pass
            return "incomplete", 3


def main() -> None:
    parser = argparse.ArgumentParser(description="RTP File Sender CLI")
    parser.add_argument("--file", required=True, help="Path to file to transmit")
    parser.add_argument("--host", default="127.0.0.1", help="Target server host")
    parser.add_argument("--port", type=int, default=9000, help="Target server UDP port")
    parser.add_argument("--timeout", type=float, default=30.0, help="Timeout in seconds")
    parser.add_argument(
        "--rto",
        type=float,
        default=None,
        help="Fixed RTO in seconds (default: adaptive RFC 6298 estimator)",
    )
    parser.add_argument(
        "--run-dir",
        type=str,
        default=None,
        help="Directory to save metric trace CSVs (e.g. rtt.csv)",
    )
    parser.add_argument(
        "--mode",
        choices=["saw", "sr"],
        default="sr",
        help="Transmission mode: saw = Stop-and-Wait, sr = Selective Repeat (default: sr)",
    )
    parser.add_argument(
        "--window",
        type=int,
        default=64,
        help="Selective Repeat window size in packets (default: 64)",
    )
    parser.add_argument("--seed", type=int, default=None, help="Seed for Python random")
    parser.add_argument(
        "--corrupt-hash",
        action="store_true",
        help="Force SHA-256 mismatch (for testing)",
    )

    args = parser.parse_args()
    sender = FileSender(
        server_addr=(args.host, args.port),
        timeout=args.timeout,
        rto=args.rto,
        mode=args.mode,
        window_size=args.window,
        run_dir=args.run_dir,
        seed=args.seed,
    )
    outcome, code = sender.send_file(args.file, corrupt_hash=args.corrupt_hash)
    sys.exit(code)



if __name__ == "__main__":
    main()
