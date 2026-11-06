# Project 15: Reliable Transport over UDP (Selective Repeat, Flow and Congestion Control)

A custom reliable transport protocol (RTP) over UDP, written in Python (standard library only for the protocol), with a file-transfer application, evaluated against TCP on a routed Mininet topology.

## Features
- 3-way SYN handshake and FIN teardown, with retries
- Selective Repeat with per-packet timers, SACK bitmap, in-order delivery, duplicate suppression
- RFC 6298-style adaptive RTO (Karn's rule, backoff; deviations listed in `docs/DEVIATIONS.md`)
- Receiver-advertised flow control with zero-window probing
- Slow start and AIMD congestion control (cwnd in packets)
- 20 MB file transfer verified with SHA-256
- Evaluation against TCP Reno under loss, RTT, reordering and duplication (netem)

## Repository layout
- `rtp/` protocol engine; `app/` file sender/receiver (RTP); `tools/` TCP baseline (`tcp_transfer.py`), file generator, analysis and plot scripts, Wireshark dissector (`rtp.lua`), impairment proxy (used by unit tests)
- `topology/` Mininet topology; `experiments/` run_one and run_matrix
- `tests/` pytest suite; `docs/` protocol spec, architecture, deviations, traceability, testing, setup
- `results/` per-run output; `analysis_out/` summary CSV and plots; `capture/` pcapng captures; `evidence/` screenshots

## Network topology
```
H1 10.0.1.10/24 -- R1 (10.0.1.1 | 10.0.2.1) -- R2 (10.0.2.2 | 10.0.3.1) -- H2 10.0.3.10/24
```
Three subnets (10.0.1.0/24, 10.0.2.0/24, 10.0.3.0/24), two Linux routers with static routes. `netem` (delay, loss, reordering, duplication) is applied on both directions of the R1-R2 link. No `tbf` rate limit is used (see `docs/DEVIATIONS.md`, section 9). Ports: RTP UDP 9000; TCP baseline uses its own TCP port.

## Where to run what
- **Windows**: editing and unit tests only.
- **Ubuntu VM** (VMware, Mininet via apt): topology, experiments, captures. Setup in `docs/SETUP_UBUNTU.md`.

## Quick start (Ubuntu VM terminal)
```bash
git clone https://github.com/harsh0558/CN_Project.git
cd CN_Project
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python tools/make_file.py --size 20000000 --output test_20mb.bin --seed 42   # 20,000,000 bytes (decimal MB)
python -m pytest -m "not slow" -v
```

## Run one experiment (Ubuntu VM terminal)
Mininet is installed system-wide, so use the system Python with sudo:
```bash
sudo /usr/bin/python3 experiments/run_one.py --proto rtp --rtt 100 --loss 1 --run 1 --evidence
sudo /usr/bin/python3 experiments/run_one.py --proto tcp --rtt 100 --loss 1 --run 1
```
Options: `--proto {rtp,tcp} --rtt MS --loss PCT --reorder PCT --duplicate PCT --run N --window PKTS --app-read-rate PKTS_PER_S --capture --evidence`.
Output: `results/<tag>/result.json` (status, hash_ok, goodput), logs, `tx/` and `rx/` traces. `--evidence` saves routing tables and a traceroute. `--capture` writes `capture/<tag>/h1.pcapng` and `h2.pcapng`.
RTT is split equally across both directions (RTT/2 netem delay each way) and checked with ping before each run.

## Run the full matrix
```bash
sudo /usr/bin/python3 experiments/run_matrix.py --dry-run     # list runs
nohup sudo /usr/bin/python3 experiments/run_matrix.py > results/matrix.log 2>&1 &
```
90 runs: loss 0/1/5/10% x RTT 20/100/200 ms (12 conditions) plus a 1% reordering scenario at 0% loss for each RTT (3 conditions), 3 runs each, RTP and TCP. Our run took about 7.5 hours; do not suspend the VM. The runner is resumable.

## Analyze (venv)
```bash
sudo chown -R $USER:$USER results capture analysis_out
python tools/analyze.py                  # analysis_out/summary.csv, per_run.csv, plots
python tools/plot_zoom.py rtp_rtt20_loss1_run1 0 10
```
Goodput = 20,000,000 bytes / transfer time (first SYN to last data ACK, excluding teardown wait), in Mbit/s.

## Demonstrations
```bash
# flow control: slow receiving application (1000 packets/s)
sudo /usr/bin/python3 experiments/run_one.py --proto rtp --rtt 20 --loss 0 --run 21 --app-read-rate 1000 --capture
# duplicate suppression: 5% duplication
sudo /usr/bin/python3 experiments/run_one.py --proto rtp --rtt 20 --loss 0 --duplicate 5 --run 1 --capture
```

## Packet captures and dissector
Copy `tools/rtp.lua` to `~/.local/lib/wireshark/plugins/` (fields `rtpu.*`), then `wireshark -r capture/<tag>/h2.pcapng`.

## Known limitations
See `docs/DEVIATIONS.md`: timeout-only loss detection (no fast retransmit), 0.2 s RTO floor, fixed 64-packet window, no sequence wraparound, no `tbf`. Requirement status and evidence: `docs/TRACEABILITY.md`.

## Troubleshooting
- `tshark` "no permission to read" for files under `~/Desktop`: copy the capture and `rtp.lua` to `/tmp` first.
- Files in `results/` or `capture/` owned by root after a run: `sudo chown -R $USER:$USER results capture`.
- Mininet leftovers after an interrupted run: `sudo mn -c`.
