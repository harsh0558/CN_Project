import sys
from pathlib import Path
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

tag = sys.argv[1] if len(sys.argv) > 1 else "rtp_rtt20_loss1_run1"
t0 = float(sys.argv[2]) if len(sys.argv) > 2 else 0.0
t1 = float(sys.argv[3]) if len(sys.argv) > 3 else 10.0
d = Path(tag) if "/" in tag else Path("results") / tag
cw = pd.read_csv(d / "tx/cwnd.csv")
rt = pd.read_csv(d / "tx/rtt.csv")
rb = pd.read_csv(d / "rx/rcvbuf.csv")
sel = lambda df: df[(df.t >= t0) & (df.t <= t1)]
cw, rt, rb = sel(cw), sel(rt), sel(rb)

fig, ax = plt.subplots(3, 1, figsize=(10, 9), sharex=True)
ax[0].step(cw.t, cw.cwnd, where="post", label="cwnd")
ax[0].step(cw.t, cw.ssthresh, where="post", label="ssthresh", alpha=.7)
ax[0].step(cw.t, cw.rwnd, where="post", label="rwnd", alpha=.5)
ax[0].set_ylabel("packets"); ax[0].legend(); ax[0].grid(alpha=.3)
ax[1].step(rb.t, rb.occupancy, where="post", label="occupancy (ooo + backlog)")
ax[1].step(rb.t, rb.ooo_buffered, where="post", label="out-of-order buffered", alpha=.7)
ax[1].set_ylabel("buffered packets"); ax[1].legend(); ax[1].grid(alpha=.3)
ax[2].plot(rt.t, rt.srtt * 1000, label="SRTT")
ax[2].plot(rt.t, rt.rto * 1000, label="RTO")
ax[2].set_ylabel("ms"); ax[2].set_xlabel("time (s)"); ax[2].legend(); ax[2].grid(alpha=.3)
ax[0].set_title(f"{tag}: {t0}-{t1} s")
plt.tight_layout()
out = Path("analysis_out") / f"zoom_{Path(tag).name}_{int(t0)}-{int(t1)}s.png"
plt.savefig(out, dpi=150)
print("wrote", out)
