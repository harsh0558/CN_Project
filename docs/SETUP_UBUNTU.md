# Ubuntu & Mininet Environment Setup

Used environment: Ubuntu VM (VMware) on a Windows laptop, Mininet installed with apt. Code can be edited and unit-tested on Windows; topology and impairment runs need the Ubuntu VM.

## Prerequisites
Ubuntu with Python 3, Mininet, `iproute2` (`tc`, `netem`), `tshark`/Wireshark, `traceroute`.

## Setup (Ubuntu VM terminal)
```bash
sudo apt update
sudo apt install -y mininet iproute2 tshark traceroute python3-venv python3-pip
git clone https://github.com/harsh0558/CN_Project.git
cd CN_Project
python3 -m venv .venv
source .venv/bin/activate
pip install pytest pandas matplotlib
```

## Run a single experiment
Mininet is installed system-wide, so run experiment scripts with the system Python, not the venv:
```bash
sudo /usr/bin/python3 experiments/run_one.py --proto rtp --rtt 20 --loss 1 --run 1 --evidence
```
Results go to `results/<tag>/`. With `--capture`, files go to `capture/<tag>/` and include `h1.pcapng` and `h2.pcapng`.

## Analysis (venv)
```bash
source .venv/bin/activate
python tools/analyze.py          # writes analysis_out/
python tools/plot_zoom.py rtp_rtt20_loss1_run1 0 10
```

## Wireshark dissector
Copy `tools/rtp.lua` to `~/.local/lib/wireshark/plugins/`; the filter fields are `rtpu.*`. To open a capture, e.g. `wireshark -r capture/<tag>/h2.pcapng`.

## Known issue
On the test VM, `tshark` reported "no permission to read" for files under `~/Desktop`, although `cat` could read them. Copying the file and the Lua script to `/tmp` worked.
