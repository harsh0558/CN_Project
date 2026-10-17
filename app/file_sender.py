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
import sys
from pathlib import Path

from app.framing import encode_stream_header
from rtp.config import STATUS_MISMATCH, STATUS_OK
from rtp.connection import (
    ConnectionAbortError,
    ConnectionTimeoutError,
    PeerUnreachable,
    RtpClient,
)
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
        window_size: int = 16,
        run_dir: str | Path | None = None,
    ) -> None:
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
        try:
            client.connect(self.server_addr, timeout=self.timeout)
        except (PeerUnreachable, ConnectionTimeoutError):
            print("timeout")
            return "timeout", 2
        except Exception as exc:
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

            if status == STATUS_OK:
                print("success")
                return "success", 0
            else:
                print("failed")
                return "failed", 1

        except (PeerUnreachable, ConnectionTimeoutError):
            print("timeout")
            try:
                client.transport.close()
            except Exception:
                pass
            return "timeout", 2

        except ConnectionAbortError as exc:
            logger.warning("Transfer aborted on timeout: %s", exc)
            print("timeout")
            try:
                client.transport.close()
            except Exception:
                pass
            return "timeout", 2

        except Exception as exc:
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
        default=16,
        help="Selective Repeat window size in packets (default: 16)",
    )
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
    )
    outcome, code = sender.send_file(args.file, corrupt_hash=args.corrupt_hash)
    sys.exit(code)



if __name__ == "__main__":
    main()
