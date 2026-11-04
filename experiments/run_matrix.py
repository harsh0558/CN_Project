#!/usr/bin/env python3
"""Run the whole experiment matrix (RTP and TCP) one run at a time, unattended.

    sudo python3 experiments/run_matrix.py --dry-run          # list what would run
    sudo python3 experiments/run_matrix.py                    # run everything
    nohup sudo python3 experiments/run_matrix.py > results/matrix.log 2>&1 &   # overnight

Design:
  * Order: pass 1 = run 1 of every condition, then pass 2, then pass 3. If you
    run out of time you still have complete coverage with fewer repeats.
  * Resumable: a run whose results/<tag>/result.json exists is skipped.
    A half-finished folder (no result.json) is deleted and redone.
  * Failed / timed-out runs are NOT retried or hidden: run_one.py records them
    as status "failed" in result.json and the matrix moves on.
  * Each run is a separate call to experiments/run_one.py, so any single run
    can be inspected or repeated by hand.
"""

import argparse
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RESULTS = ROOT / "results"

LOSSES = [0, 1, 5, 10]          # percent
RTTS = [20, 100, 200]           # ms
PROTOS = ["rtp", "tcp"]
REORDER_PCT = 1                 # extra scenario: 1% reordering at 0% loss


def tag_for(proto, rtt, loss, reorder, run):
    t = f"{proto}_rtt{rtt:g}_loss{loss:g}"
    if reorder > 0:
        t += f"_reo{reorder:g}"
    return f"{t}_run{run}"


def build_plan(args):
    conditions = [(rtt, loss, 0) for rtt in args.rtts for loss in args.losses]
    if not args.no_reorder:
        conditions += [(rtt, 0, REORDER_PCT) for rtt in args.rtts]
    plan = []
    for run in range(1, args.runs + 1):
        for rtt, loss, reo in conditions:
            for proto in args.protos:
                plan.append((proto, rtt, loss, reo, run))
    return plan


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("--file", default="test_20mb.bin")
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument("--rtts", type=float, nargs="+", default=RTTS)
    ap.add_argument("--losses", type=float, nargs="+", default=LOSSES)
    ap.add_argument("--protos", nargs="+", default=PROTOS, choices=PROTOS)
    ap.add_argument("--no-reorder", action="store_true", help="skip the 1%% reordering scenario")
    ap.add_argument("--cc", default="reno", help="TCP congestion control for the TCP runs")
    ap.add_argument("--timeout", type=int, default=1800, help="per-run protocol timeout (s)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    plan = build_plan(args)
    todo = [p for p in plan if not (RESULTS / tag_for(*p) / "result.json").exists()]
    print(f"planned runs: {len(plan)}   already done: {len(plan) - len(todo)}   to run: {len(todo)}", flush=True)

    if args.dry_run:
        for p in todo:
            print("  ", tag_for(*p))
        return 0
    if os.geteuid() != 0:
        sys.exit("Run with sudo.")

    os.chdir(ROOT)
    subprocess.run(["mn", "-c"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    t_all = time.monotonic()
    counts = {"success": 0, "failed": 0, "error": 0}

    for i, (proto, rtt, loss, reo, run) in enumerate(todo, 1):
        tag = tag_for(proto, rtt, loss, reo, run)
        partial = RESULTS / tag
        if partial.exists():
            shutil.rmtree(partial)          # interrupted earlier, redo cleanly

        cmd = [sys.executable, str(ROOT / "experiments" / "run_one.py"),
               "--proto", proto, "--rtt", f"{rtt:g}", "--loss", f"{loss:g}", "--run", str(run),
               "--file", args.file, "--timeout", str(args.timeout), "--cc", args.cc]
        if reo > 0:
            cmd += ["--reorder", f"{reo:g}"]
        if run == 1:
            cmd += ["--evidence"]           # routing tables + traceroute, once per condition

        stamp = time.strftime("%H:%M:%S")
        print(f"[{stamp}] ({i}/{len(todo)}) starting {tag}", flush=True)
        t0 = time.monotonic()
        try:
            r = subprocess.run(cmd, capture_output=True, text=True, timeout=args.timeout + 300)
            line = (r.stdout.strip().splitlines() or ["(no output)"])[-1]
            if r.returncode == 0:
                counts["success"] += 1
            elif r.returncode == 1:
                counts["failed"] += 1
            else:
                counts["error"] += 1
                line += f"  | rc={r.returncode} stderr: {r.stderr.strip()[-300:]}"
        except subprocess.TimeoutExpired:
            counts["error"] += 1
            line = "HARD TIMEOUT - run_one.py killed"
            subprocess.run(["mn", "-c"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        print(f"           {line}   [{time.monotonic() - t0:.0f}s]", flush=True)

    # results were written by root; give them back to the normal user
    user = os.environ.get("SUDO_USER")
    if user:
        subprocess.run(["chown", "-R", f"{user}:{user}", str(RESULTS)])

    mins = (time.monotonic() - t_all) / 60
    print(f"\nDONE in {mins:.1f} min: {counts}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
