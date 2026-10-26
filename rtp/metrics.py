"""Central metrics, counter definitions, and trace writers for RTP.

Trace CSV formats per docs/AGENT_CONTEXT.md:
- cwnd.csv: (t,cwnd,ssthresh,rwnd,effective_window,in_flight,send_base,event)
- rtt.csv: (t,sample_rtt,srtt,rttvar,rto,backoff)
- rcvbuf.csv: (t,ooo_buffered,app_backlog,occupancy,rwnd)

Summary JSON files:
- Sender fields: file_bytes, t_start, t_end, duration_s, goodput_mbps,
  data_sent_total, unique_data_packets, retx_total, retx_timeout,
  retx_other, acks_received, dup_acks, final_srtt, final_rto, status, exit_reason.
- Receiver fields: dup_received, ooo_received, invalid_packets, crc_errors,
  max_occupancy, sha256_ok, status.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, TextIO


def compute_goodput_mbps(file_bytes: int, duration_s: float) -> float:
    """Compute goodput in Mbit/s per AGENT_CONTEXT.md: 8 * file_bytes / (t_end - t_start).

    Retransmitted payload is never counted.
    """
    if duration_s <= 0.0 or file_bytes <= 0:
        return 0.0
    return (8.0 * file_bytes) / (duration_s * 1_000_000.0)


@dataclass
class SenderSummary:
    file_bytes: int
    t_start: float
    t_end: float
    duration_s: float
    goodput_mbps: float
    data_sent_total: int
    unique_data_packets: int
    retx_total: int
    retx_timeout: int
    retx_other: int
    acks_received: int
    dup_acks: int
    final_srtt: float | None
    final_rto: float | None
    status: str
    exit_reason: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ReceiverSummary:
    dup_received: int
    ooo_received: int
    invalid_packets: int
    crc_errors: int
    max_occupancy: int
    sha256_ok: bool
    status: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def write_summary_json(path: str | Path, summary_data: dict[str, Any]) -> None:
    """Write summary dictionary to a JSON file cleanly."""
    target_path = Path(path)
    target_path.parent.mkdir(parents=True, exist_ok=True)
    with open(target_path, "w", encoding="utf-8") as f:
        json.dump(summary_data, f, indent=2)


class CwndTraceWriter:
    """Writes cwnd event transitions to cwnd.csv.

    Columns: t,cwnd,ssthresh,rwnd,effective_window,in_flight,send_base,event
    """

    def __init__(self, filepath_or_dir: str | Path, start_time: float = 0.0) -> None:
        path = Path(filepath_or_dir)
        if path.is_dir() or not path.suffix:
            path = path / "cwnd.csv"
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self.start_time = start_time
        self.file: TextIO | None = open(path, "w", encoding="utf-8")
        self.file.write("t,cwnd,ssthresh,rwnd,effective_window,in_flight,send_base,event\n")
        self.file.flush()

    def log(
        self,
        t: float,
        cwnd: float,
        ssthresh: float | int,
        rwnd: int,
        effective_window: int,
        in_flight: int,
        send_base: int,
        event: str,
    ) -> None:
        if self.file is not None:
            self.file.write(
                f"{t:.6f},{cwnd:.4f},{ssthresh},{rwnd},{effective_window},{in_flight},{send_base},{event}\n"
            )
            self.file.flush()

    def close(self) -> None:
        if self.file is not None:
            try:
                self.file.flush()
                self.file.close()
            except Exception:
                pass
            self.file = None


class RttTraceWriter:
    """Writes RTT estimator transitions to rtt.csv.

    Columns: t,sample_rtt,srtt,rttvar,rto,backoff
    """

    def __init__(self, filepath_or_dir: str | Path, start_time: float = 0.0) -> None:
        path = Path(filepath_or_dir)
        if path.is_dir() or not path.suffix:
            path = path / "rtt.csv"
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self.start_time = start_time
        self.file: TextIO | None = open(path, "w", encoding="utf-8")
        self.file.write("t,sample_rtt,srtt,rttvar,rto,backoff\n")
        self.file.flush()

    def log(
        self,
        t: float,
        sample_rtt: float,
        srtt: float,
        rttvar: float,
        rto: float,
        backoff: int,
    ) -> None:
        if self.file is not None:
            self.file.write(
                f"{t:.6f},{sample_rtt:.6f},{srtt:.6f},{rttvar:.6f},{rto:.6f},{backoff}\n"
            )
            self.file.flush()

    def close(self) -> None:
        if self.file is not None:
            try:
                self.file.flush()
                self.file.close()
            except Exception:
                pass
            self.file = None


class RcvbufTraceWriter:
    """Writes receiver buffer state transitions to rcvbuf.csv.

    Columns: t,ooo_buffered,app_backlog,occupancy,rwnd
    """

    def __init__(self, filepath_or_dir: str | Path, start_time: float = 0.0) -> None:
        path = Path(filepath_or_dir)
        if path.is_dir() or not path.suffix:
            path = path / "rcvbuf.csv"
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self.start_time = start_time
        self.file: TextIO | None = open(path, "w", encoding="utf-8")
        self.file.write("t,ooo_buffered,app_backlog,occupancy,rwnd\n")
        self.file.flush()

    def log(
        self,
        t: float,
        ooo_buffered: int,
        app_backlog: int,
        occupancy: int,
        rwnd: int,
    ) -> None:
        if self.file is not None:
            self.file.write(
                f"{t:.6f},{ooo_buffered},{app_backlog},{occupancy},{rwnd}\n"
            )
            self.file.flush()

    def close(self) -> None:
        if self.file is not None:
            try:
                self.file.flush()
                self.file.close()
            except Exception:
                pass
            self.file = None
