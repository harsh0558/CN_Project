import glob, json, os, re
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

OUT = "analysis_out"
os.makedirs(OUT, exist_ok=True)

def ping_avg(path):
    """Median of per-reply RTTs (robust to the first-packet ARP outlier); fallback: summary min."""
    try:
        txt = open(path).read()
    except OSError:
        return None
    times = sorted(float(x) for x in re.findall(r"time=([\d.]+) ms", txt))
    if times:
        n = len(times)
        return times[n // 2] if n % 2 else (times[n // 2 - 1] + times[n // 2]) / 2
    m = re.findall(r"= ([\d.]+)/", txt)
    return float(m[-1]) if m else None

rows = []
for f in sorted(glob.glob("results/*/result.json")):
    d = json.load(open(f)); dr = os.path.dirname(f)
    s = d["sender_summary"]
    proto = os.path.basename(dr).split("_")[0]
    row = dict(proto=proto, rtt=d["rtt_ms_target"], loss=d["loss_pct"],
               reo=d.get("reorder_pct") or 0, run=d["run"], dir=dr,
               duration=s["duration_s"], goodput=s["goodput_mbps"],
               retx=s["retx_total"], srtt=s.get("final_srtt"),
               ping=ping_avg(os.path.join(dr, "ping.txt")),
               ok=d["status"] == "success" and d["hash_ok"] is True)
    if proto == "rtp":
        r = d["receiver_summary"]
        if not r:
            try:
                r = json.load(open(os.path.join(dr, "rx", "receiver_summary.json")))
            except Exception:
                r = {}
                print("WARNING: no receiver summary for", dr)
        row.update(retx_timeout=s["retx_timeout"], retx_other=s["retx_other"],
                   max_in_flight=s["max_in_flight"], rcv_max_occ=r.get("max_occupancy"),
                   lim_cwnd=s["limited_by_cwnd"], lim_rwnd=s["limited_by_rwnd"])
        try:
            row["mean_srtt"] = pd.read_csv(os.path.join(dr, "tx/rtt.csv"))["srtt"].mean()
        except Exception:
            row["mean_srtt"] = None
    rows.append(row)

df = pd.DataFrame(rows)
df.to_csv(f"{OUT}/per_run.csv", index=False)
g = df.groupby(["proto", "rtt", "loss", "reo"])
summ = g.agg(n=("run", "count"),
             goodput_mean=("goodput", "mean"), goodput_std=("goodput", "std"),
             goodput_min=("goodput", "min"), goodput_max=("goodput", "max"),
             retx_mean=("retx", "mean"), retx_std=("retx", "std"),
             duration_mean=("duration", "mean"), srtt_mean=("srtt", "mean"),
             ping_median=("ping", "median")).reset_index()
summ.to_csv(f"{OUT}/summary.csv", index=False)
print(summ.round(3).to_string())
print("\nall ok:", df["ok"].all(), "| runs:", len(df))

main = summ[summ.reo == 0]
RTTS, LOSS = [20, 100, 200], [0, 1, 5, 10]
COL = {"rtp": "tab:blue", "tcp": "tab:red"}

def lines(metric, ylabel, fname, log=False):
    fig, ax = plt.subplots(1, 3, figsize=(14, 4), sharey=True)
    for a, rtt in zip(ax, RTTS):
        for p in ["rtp", "tcp"]:
            t = main[(main.proto == p) & (main.rtt == rtt)].sort_values("loss")
            a.errorbar(t.loss, t[metric + "_mean"], yerr=t[metric + "_std"],
                       marker="o", capsize=3, label=p.upper(), color=COL[p])
        a.set_title(f"RTT {rtt} ms"); a.set_xlabel("Loss (%)"); a.grid(alpha=.3)
        if log: a.set_yscale("log")
    ax[0].set_ylabel(ylabel); ax[0].legend()
    fig.tight_layout(); fig.savefig(f"{OUT}/{fname}", dpi=150); plt.close(fig)

lines("goodput", "Goodput (Mbit/s)", "goodput_vs_loss.png", log=True)
lines("retx", "Retransmitted packets", "retx_vs_loss.png", log=True)

fig, ax = plt.subplots(1, 2, figsize=(10, 4), sharey=True)
for a, p in zip(ax, ["rtp", "tcp"]):
    for l in LOSS:
        t = main[(main.proto == p) & (main.loss == l)].sort_values("rtt")
        a.errorbar(t.rtt, t.goodput_mean, yerr=t.goodput_std, marker="o", capsize=3, label=f"{l}% loss")
    a.set_title(p.upper()); a.set_xlabel("RTT (ms)"); a.set_yscale("log"); a.grid(alpha=.3)
ax[0].set_ylabel("Goodput (Mbit/s)"); ax[0].legend()
fig.tight_layout(); fig.savefig(f"{OUT}/goodput_vs_rtt.png", dpi=150); plt.close(fig)

r = df[(df.proto == "rtp") & (df.reo == 0)].groupby(["rtt", "loss"]).agg(
    srtt=("mean_srtt", "mean"), ping=("ping", "median")).reset_index()
r.to_csv(f"{OUT}/rtt_estimate_vs_ping.csv", index=False)
fig, ax = plt.subplots(figsize=(6, 5))
for l in LOSS:
    t = r[r.loss == l]; ax.scatter(t.ping, t.srtt * 1000, label=f"{l}% loss")
ax.plot([0, 220], [0, 220], "k--", lw=1, label="ideal")
ax.set_xlabel("Measured ping RTT (ms)"); ax.set_ylabel("RTP mean SRTT (ms)")
ax.legend(); ax.grid(alpha=.3); fig.tight_layout()
fig.savefig(f"{OUT}/rtt_estimate_vs_ping.png", dpi=150); plt.close(fig)

def grid(csv, xcol, ycols, fname, ylabel):
    fig, ax = plt.subplots(3, 4, figsize=(16, 9))
    for i, rtt in enumerate(RTTS):
        for j, l in enumerate(LOSS):
            a = ax[i][j]
            q = df[(df.proto == "rtp") & (df.rtt == rtt) & (df.loss == l) & (df.reo == 0) & (df.run == 1)]
            if len(q):
                c = pd.read_csv(os.path.join(q.iloc[0].dir, csv))
                for y in ycols: a.plot(c[xcol], c[y], lw=.7, label=y)
            a.set_title(f"RTT {rtt} ms, loss {l}%", fontsize=9); a.grid(alpha=.3)
            if i == 2: a.set_xlabel("time (s)")
            if j == 0: a.set_ylabel(ylabel)
    ax[0][0].legend(fontsize=7); fig.tight_layout()
    fig.savefig(f"{OUT}/{fname}", dpi=130); plt.close(fig)

grid("tx/cwnd.csv", "t", ["cwnd", "ssthresh"], "cwnd_traces.png", "packets")
grid("rx/rcvbuf.csv", "t", ["occupancy"], "rcvbuf_traces.png", "buffered packets")
print("plots written to", OUT)
