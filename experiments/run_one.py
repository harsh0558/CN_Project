#!/usr/bin/env python3
"""Run ONE experiment condition (custom RTP or TCP baseline) inside Mininet.

Usage (system python, NOT the venv, because Mininet is installed system-wide):
    sudo python3 experiments/run_one.py --rtt 100 --loss 1 --run 1

What it does:
    1. builds the H1-R1-R2-H2 topology (topology/topo.py)
    2. applies netem on the R1-R2 link: RTT/2 delay each direction, loss and
       optional reordering on both directions
    3. pings H1->H2 to record the real RTT of the emulated path
    4. starts the receiver on H2, runs the sender on H1, times the run
    5. verifies the SHA-256 of the received file against the source
    6. writes results/<tag>/result.json (+ logs, summaries, traces)

Failed or timed-out runs are recorded with status "failed"; nothing is hidden.
"""

import argparse
import hashlib
import json
import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "topology"))

from mininet.log import setLogLevel  # noqa: E402
from topo import build  # noqa: E402

PY = ROOT / ".venv" / "bin" / "python"   # project venv python (runs the RTP code)


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def load_json(path: Path):
    try:
        return json.loads(path.read_text())
    except Exception:
        return None


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("--proto", choices=["rtp", "tcp"], default="rtp", help="protocol to test")
    ap.add_argument("--cc", default="reno", help="TCP congestion control (tcp only): reno or cubic")
    ap.add_argument("--rtt", type=float, required=True, help="target RTT in ms (split equally over both directions)")
    ap.add_argument("--loss", type=float, default=0.0, help="loss percent, applied on BOTH directions")
    ap.add_argument("--reorder", type=float, default=0.0, help="reorder percent (netem), both directions")
    ap.add_argument("--run", type=int, default=1, help="run number (1,2,3,...)")
    ap.add_argument("--file", default="test_20mb.bin", help="file to send (relative to project root)")
    ap.add_argument("--window", type=int, default=64, help="window size in packets (sender and receiver)")
    ap.add_argument("--timeout", type=int, default=1800, help="protocol timeout in seconds")
    ap.add_argument("--evidence", action="store_true", help="also save routing tables + traceroute")
    args = ap.parse_args()

    if os.geteuid() != 0:
        sys.exit("Run with sudo: sudo python3 experiments/run_one.py ...")

    os.chdir(ROOT)
    src = ROOT / args.file
    if not src.exists():
        sys.exit(f"Source file not found: {src}")

    tag = f"{args.proto}_rtt{args.rtt:g}_loss{args.loss:g}"
    if args.reorder > 0:
        tag += f"_reo{args.reorder:g}"
    tag += f"_run{args.run}"
    run_dir = ROOT / "results" / tag
    if run_dir.exists():
        sys.exit(f"{run_dir} already exists - refusing to overwrite. Delete it or change --run.")
    (run_dir / "received").mkdir(parents=True)

    one_way = args.rtt / 2.0
    netem = f"delay {one_way:g}ms"
    if args.loss > 0:
        netem += f" loss {args.loss:g}%"
    if args.reorder > 0:
        netem += f" reorder {args.reorder:g}%"

    setLogLevel("warning")
    net = build()
    result = {"tag": tag, "protocol": args.proto, "tcp_cc": args.cc if args.proto == "tcp" else None, "rtt_ms_target": args.rtt, "loss_pct": args.loss,
              "reorder_pct": args.reorder, "run": args.run, "file": args.file,
              "window": args.window, "netem": netem}
    rx = tx = None
    try:
        h1, h2, r1, r2 = net.get("h1", "h2", "r1", "r2")

        # --- impairments on the R1-R2 link, both directions ---
        r1.cmd(f"tc qdisc add dev r1-eth1 root netem {netem}")
        r2.cmd(f"tc qdisc add dev r2-eth0 root netem {netem}")
        (run_dir / "netem.txt").write_text(
            f"netem options: {netem}\n\n" + r1.cmd("tc qdisc show dev r1-eth1") + r2.cmd("tc qdisc show dev r2-eth0"))

        # --- evidence: real RTT of the emulated path (ping output saved verbatim) ---
        (run_dir / "ping.txt").write_text(h1.cmd("ping -c 6 -i 0.3 -W 2 10.0.3.10"))
        if args.evidence:
            (run_dir / "topology_evidence.txt").write_text(
                "== r1 ip route ==\n" + r1.cmd("ip route") + "\n== r2 ip route ==\n" + r2.cmd("ip route") +
                "\n== h1 traceroute ==\n" + h1.cmd("traceroute -n 10.0.3.10"))

        # --- start receiver on h2 ---
        rx_log = open(run_dir / "rx.log", "w")
        if args.proto == "rtp":
            rx_cmd = [str(PY), "-m", "app.file_receiver", "--host", "10.0.3.10", "--port", "9000",
                      "--output-dir", str(run_dir / "received"), "--timeout", str(args.timeout),
                      "--window", str(args.window), "--run-dir", str(run_dir / "rx")]
        else:
            rx_cmd = [str(PY), "-m", "tools.tcp_transfer", "recv", "--host", "10.0.3.10", "--port", "9000",
                      "--output-dir", str(run_dir / "received"), "--timeout", str(args.timeout),
                      "--run-dir", str(run_dir / "rx")]
        rx = h2.popen(rx_cmd, stdout=rx_log, stderr=subprocess.STDOUT)
        time.sleep(1.0)

        # --- run sender on h1 (timed) ---
        tx_log = open(run_dir / "tx.log", "w")
        t0 = time.monotonic()
        if args.proto == "rtp":
            tx_cmd = [str(PY), "-m", "app.file_sender", "--file", str(src), "--host", "10.0.3.10",
                      "--port", "9000", "--timeout", str(args.timeout), "--window", str(args.window),
                      "--run-dir", str(run_dir / "tx")]
        else:
            tx_cmd = [str(PY), "-m", "tools.tcp_transfer", "send", "--file", str(src), "--host", "10.0.3.10",
                      "--port", "9000", "--timeout", str(args.timeout), "--cc", args.cc,
                      "--run-dir", str(run_dir / "tx")]
        tx = h1.popen(tx_cmd, stdout=tx_log, stderr=subprocess.STDOUT)
        killed = False
        try:
            tx_rc = tx.wait(timeout=args.timeout + 60)
        except subprocess.TimeoutExpired:
            tx.kill()
            tx_rc = None
            killed = True
        wall = time.monotonic() - t0

        try:
            rx.wait(timeout=30)
        except subprocess.TimeoutExpired:
            rx.kill()
        tx_log.close()
        rx_log.close()

        # --- integrity check ---
        dest = run_dir / "received" / (src.name if args.proto == "rtp" else "received.bin")
        hash_ok = dest.exists() and sha256(dest) == sha256(src)
        if dest.exists():
            dest.unlink()   # keep disk usage small; hash result is recorded

        tx_sum = load_json(run_dir / "tx" / "sender_summary.json")
        rx_sum = load_json(run_dir / "rx" / "receiver_summary.json")
        ok = (tx_rc == 0) and hash_ok

        result.update({
            "status": "success" if ok else "failed",
            "sender_return_code": tx_rc,
            "killed_on_timeout": killed,
            "hash_ok": hash_ok,
            "wall_time_s": round(wall, 3),
            "sender_summary": tx_sum,
            "receiver_summary": rx_sum,
        })
    finally:
        for p in (tx, rx):
            if p is not None and p.poll() is None:
                p.kill()
        net.stop()

    (run_dir / "result.json").write_text(json.dumps(result, indent=2))

    s = result.get("sender_summary") or {}
    print(f"[{tag}] status={result.get('status')} hash_ok={result.get('hash_ok')} "
          f"wall={result.get('wall_time_s')}s goodput={s.get('goodput_mbps')} Mbit/s "
          f"retx={s.get('retx_total')} srtt={s.get('final_srtt')}")
    return 0 if result.get("status") == "success" else 1


if __name__ == "__main__":
    sys.exit(main())
