import json
import math
import random
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

from app.file_receiver import FileReceiver
from app.file_sender import FileSender
from app.framing import encode_stream_header
from rtp.metrics import compute_goodput_mbps

ROOT = Path(__file__).resolve().parents[1]
FILE_BYTES = 1_000_000


def _free_udp_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


@pytest.fixture(scope="module")
def run(tmp_path_factory):
    base = tmp_path_factory.mktemp("m")
    src = base / "in.bin"
    src.write_bytes(random.Random(1).randbytes(FILE_BYTES))
    run_dir = base / "run"
    port = _free_udp_port()
    rx = FileReceiver(("127.0.0.1", port), output_dir=base / "out", timeout=20, run_dir=run_dir)
    res = {}
    th = threading.Thread(target=lambda: res.update(rx=rx.run_once()))
    th.start()
    time.sleep(0.2)
    tx = FileSender(("127.0.0.1", port), timeout=20, run_dir=run_dir)
    res["tx"] = tx.send_file(src)
    th.join(30)
    return run_dir, res


def test_goodput_counts_file_bytes_only():
    # retransmissions never enter the formula: only file_bytes and duration
    assert compute_goodput_mbps(1_000_000, 2.0) == pytest.approx(4.0)
    assert compute_goodput_mbps(0, 1.0) == 0.0
    assert compute_goodput_mbps(1000, 0.0) == 0.0


def test_transfer_and_summaries(run):
    run_dir, res = run
    assert res["tx"][0] == "success" and res["rx"][0] == "success"
    s = json.loads((run_dir / "sender_summary.json").read_text())
    r = json.loads((run_dir / "receiver_summary.json").read_text())
    assert s["status"] == "success" and r["sha256_ok"] is True
    assert s["file_bytes"] == FILE_BYTES
    assert s["goodput_mbps"] == pytest.approx(8 * FILE_BYTES / s["duration_s"] / 1e6)
    for k in ("final_srtt", "final_rto", "retx_other", "dup_acks"):
        assert k in s
    for k in ("max_occupancy", "invalid_packets", "crc_errors"):
        assert k in r


def test_counter_invariants(run):
    run_dir, _ = run
    s = json.loads((run_dir / "sender_summary.json").read_text())
    assert s["data_sent_total"] == s["unique_data_packets"] + s["retx_total"]
    hdr = len(encode_stream_header("in.bin", FILE_BYTES, b"\0" * 32))
    assert s["unique_data_packets"] == math.ceil(hdr / 1024) + math.ceil(FILE_BYTES / 1024)


def test_traces_and_plots(run):
    run_dir, _ = run
    for name in ("cwnd.csv", "rtt.csv", "rcvbuf.csv"):
        lines = (run_dir / name).read_text().splitlines()
        assert len(lines) > 1, name
    pytest.importorskip("matplotlib")
    subprocess.run([sys.executable, str(ROOT / "tools" / "plot_run.py"), str(run_dir)],
                   check=True, cwd=ROOT)
    for png in ("cwnd.png", "rtt.png", "rcvbuf.png"):
        assert (run_dir / png).stat().st_size > 0
