"""Usage: python tools/plot_run.py <run_dir>"""
import csv
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


def _read(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _col(rows, k):
    return [float(r[k]) for r in rows]


def _plot(run, csv_name, png_name, series, title, ylabel, mark_timeouts=False):
    p = run / csv_name
    if not p.exists():
        print("skip (missing):", p)
        return None
    rows = _read(p)
    if not rows:
        print("skip (empty):", p)
        return None
    t = _col(rows, "t")
    fig, ax = plt.subplots(figsize=(8, 4))
    for key, style in series:
        ax.plot(t, _col(rows, key), style, label=key, linewidth=1)
    if mark_timeouts:
        tt = [float(r["t"]) for r in rows if r.get("event") == "timeout"]
        ax.plot(tt, [0] * len(tt), "rx", label="timeout")
    ax.set_xlabel("time (s)")
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.legend()
    ax.grid(alpha=0.3)
    out = run / png_name
    fig.tight_layout()
    fig.savefig(out, dpi=120)
    plt.close(fig)
    return out


def main(run_dir):
    run = Path(run_dir)
    outs = [
        _plot(run, "cwnd.csv", "cwnd.png", [("cwnd", "-"), ("ssthresh", "--"), ("effective_window", ":")],
              "Congestion window", "packets", mark_timeouts=True),
        _plot(run, "rtt.csv", "rtt.png", [("sample_rtt", "."), ("srtt", "-"), ("rto", "--")],
              "RTT estimation", "seconds"),
        _plot(run, "rcvbuf.csv", "rcvbuf.png", [("occupancy", "-"), ("rwnd", "--")],
              "Receiver buffer", "packets"),
    ]
    return [o for o in outs if o is not None]


if __name__ == "__main__":
    print([str(p) for p in main(sys.argv[1])])
