"""RTP File Receiver application.

Receives framed files over RTP, writes to <name>.part, hashes incrementally
with SHA-256, and atomically renames to <name> only upon valid digest verification.
Replies with status 0x00 (OK) or 0x01 (Mismatch) in the FIN|ACK teardown.
"""

from __future__ import annotations

import argparse
import hashlib
import logging
import os
import sys
from pathlib import Path

from app.framing import StreamHeader, StreamHeaderParser
from rtp.config import DEFAULT_RECEIVER_CAPACITY, STATUS_MISMATCH, STATUS_OK
from rtp.connection import ConnectionAbortError, ConnectionTimeoutError, RtpServer
from rtp.receiver import SelectiveRepeatReceiver, StopAndWaitReceiver

logger = logging.getLogger("app.file_receiver")


MAX_FILENAME_LENGTH: int = 255
DEFAULT_MAX_FILE_SIZE: int = 4 * 1024 * 1024 * 1024  # 4 GiB


def sanitize_filename(filename: str, max_length: int = MAX_FILENAME_LENGTH) -> str:
    """Validate and sanitize filename from stream.

    Rejects:
    - Empty names or names containing only whitespace.
    - Path separators ('/' or '\\').
    - Directory traversal sequences ('..').
    - Names exceeding max_length bytes.
    Raises ValueError on invalid filename.
    """
    if not filename or not filename.strip():
        raise ValueError("Filename cannot be empty")
    if "/" in filename or "\\" in filename:
        raise ValueError("Filename cannot contain path separators")
    if ".." in filename:
        raise ValueError("Filename cannot contain '..'")
    base = Path(filename).name
    if not base or base in (".", ".."):
        raise ValueError("Invalid filename base")
    if len(base.encode("utf-8")) > max_length:
        raise ValueError(f"Filename length exceeds {max_length} bytes")
    return base


class FileReceiver:
    """Manages the server side of an RTP file transfer."""

    def __init__(
        self,
        bind_addr: tuple[str, int] = ("127.0.0.1", 9000),
        output_dir: str | Path = ".",
        timeout: float = 30.0,
        max_file_size: int = DEFAULT_MAX_FILE_SIZE,
        max_filename_length: int = MAX_FILENAME_LENGTH,
        mode: str = "sr",
        capacity: int = DEFAULT_RECEIVER_CAPACITY,
        app_read_rate: float = 0.0,
        run_dir: str | Path | None = None,
    ) -> None:
        self.bind_addr = bind_addr
        self.output_dir = Path(output_dir).resolve()
        self.timeout = timeout
        self.max_file_size = max_file_size
        self.max_filename_length = max_filename_length
        self.mode = mode.lower()
        self.capacity = capacity
        self.app_read_rate = float(app_read_rate)
        self.run_dir = Path(run_dir) if run_dir is not None else None
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.server: RtpServer | None = None
        self._last_receiver: StopAndWaitReceiver | SelectiveRepeatReceiver | None = None

    def run_once(self) -> tuple[str, str | None, int]:
        """Accept one file transfer and process it.

        Returns:
            tuple[str, str | None, int]:
                (outcome: "success" | "failed" | "incomplete" | "timeout",
                 final_filepath,
                 status_byte: 0x00 or 0x01)
        """
        self.server = RtpServer(
            bind_addr=self.bind_addr,
            rcvbuf_capacity=self.capacity,
        )
        try:
            conn = self.server.accept(timeout=self.timeout)
        except (ConnectionTimeoutError, TimeoutError):
            print("timeout")
            self.server.close()
            return "timeout", None, STATUS_MISMATCH
        except Exception as exc:
            logger.error("Error accepting connection: %s", exc)
            print("incomplete")
            self.server.close()
            return "incomplete", None, STATUS_MISMATCH

        if self.mode == "saw":
            receiver = StopAndWaitReceiver(conn)
        else:
            receiver = SelectiveRepeatReceiver(
                conn,
                capacity=self.capacity,
                app_read_rate=self.app_read_rate,
                run_dir=self.run_dir,
            )
        self._last_receiver = receiver

        parser = StreamHeaderParser()
        hasher = hashlib.sha256()

        part_file_path: Path | None = None
        final_file_path: Path | None = None
        part_fp = None
        bytes_written = 0
        outcome = "incomplete"
        teardown_status = STATUS_MISMATCH
        header_valid = False

        try:
            for chunk in receiver.stream(timeout=self.timeout):
                payload_bytes = parser.feed(chunk)
                if parser.is_parsed and not header_valid:
                    assert parser.header is not None
                    # Validate filename and size limits
                    try:
                        safe_filename = sanitize_filename(
                            parser.header.filename, max_length=self.max_filename_length
                        )
                        if parser.header.file_size > self.max_file_size:
                            raise ValueError(
                                f"Declared size {parser.header.file_size} exceeds max {self.max_file_size}"
                            )
                        header_valid = True
                    except ValueError as err:
                        logger.warning("Header validation rejected: %s", err)
                        outcome = "failed"
                        teardown_status = STATUS_MISMATCH
                        # Drain remaining stream without writing anything
                        break

                    final_file_path = self.output_dir / safe_filename
                    # Ensure final path is strictly inside output_dir
                    if not str(final_file_path.resolve()).startswith(str(self.output_dir)):
                        outcome = "failed"
                        teardown_status = STATUS_MISMATCH
                        break

                    part_file_path = self.output_dir / f"{safe_filename}.part"
                    part_fp = open(part_file_path, "wb")

                if part_fp is not None and payload_bytes:
                    part_fp.write(payload_bytes)
                    hasher.update(payload_bytes)
                    bytes_written += len(payload_bytes)

            if part_fp is not None:
                part_fp.flush()
                part_fp.close()
                part_fp = None

            # Verify integrity and size constraints
            if header_valid and parser.is_parsed and parser.header is not None:
                if bytes_written == parser.header.file_size:
                    computed_digest = hasher.digest()
                    if computed_digest == parser.header.sha256_digest:
                        # Valid hash match! Atomically rename
                        assert part_file_path is not None and final_file_path is not None
                        os.replace(part_file_path, final_file_path)
                        outcome = "success"
                        teardown_status = STATUS_OK
                        print("success")
                    else:
                        # Hash mismatch: keep .part, do NOT create final file
                        outcome = "failed"
                        teardown_status = STATUS_MISMATCH
                        print("failed")
                else:
                    # Size discrepancy (fewer or more bytes than declared)
                    outcome = "failed"
                    teardown_status = STATUS_MISMATCH
                    print("failed")
            else:
                outcome = "failed" if not header_valid and parser.is_parsed else "incomplete"
                teardown_status = STATUS_MISMATCH
                print(outcome)

        except Exception as exc:
            logger.warning("Transfer aborted: %s", exc)
            if part_fp is not None:
                part_fp.flush()
                part_fp.close()
            outcome = "incomplete"
            teardown_status = STATUS_MISMATCH
            print("incomplete")

        finally:
            try:
                conn.close(status=teardown_status, timeout=self.timeout)
            except Exception as exc:
                logger.debug("Error closing connection: %s", exc)
            self.server.close()

        resolved_path = str(final_file_path) if outcome == "success" else None
        return outcome, resolved_path, teardown_status


def main() -> None:
    parser = argparse.ArgumentParser(description="RTP File Receiver CLI")
    parser.add_argument("--host", default="127.0.0.1", help="Listening IP host")
    parser.add_argument("--port", type=int, default=9000, help="Listening UDP port")
    parser.add_argument("--output-dir", default=".", help="Directory to store received files")
    parser.add_argument("--timeout", type=float, default=30.0, help="Operation timeout in seconds")
    parser.add_argument(
        "--mode",
        choices=["saw", "sr"],
        default="sr",
        help="Receiving mode: saw = Stop-and-Wait, sr = Selective Repeat (default: sr)",
    )
    parser.add_argument(
        "--window",
        type=int,
        default=64,
        help="Receiver window capacity in packets (default: 64)",
    )
    parser.add_argument(
        "--rto",
        type=float,
        default=0.5,
        help="Initial RTO in seconds (default: 0.5)",
    )
    parser.add_argument(
        "--app-read-rate",
        type=float,
        default=0.0,
        help="Application read rate in packets/s (0 = unlimited)",
    )
    parser.add_argument(
        "--run-dir",
        default=None,
        help="Directory to save run traces including rcvbuf.csv",
    )

    args = parser.parse_args()
    receiver = FileReceiver(
        bind_addr=(args.host, args.port),
        output_dir=args.output_dir,
        timeout=args.timeout,
        mode=args.mode,
        capacity=args.window,
        app_read_rate=args.app_read_rate,
        run_dir=args.run_dir,
    )
    outcome, _, _ = receiver.run_once()

    if outcome == "success":
        sys.exit(0)
    elif outcome == "failed":
        sys.exit(1)
    elif outcome == "timeout":
        sys.exit(2)
    else:
        sys.exit(3)


if __name__ == "__main__":
    main()
