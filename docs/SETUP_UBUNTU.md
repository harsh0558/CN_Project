# Ubuntu & Mininet Environment Setup

Instructions for deploying RTP on an Ubuntu VM with Mininet (Phase 10+).

## Prerequisites
- Ubuntu 22.04 LTS (or compatible)
- Python 3.10+
- Mininet
- `iproute2` (`tc` and `netem`)
- `iperf3`
- `wireshark` / `tshark`

## Setup Commands
```bash
sudo apt update
sudo apt install -y mininet iproute2 iperf3 tshark python3-pip
pip install pytest pandas matplotlib
```
