# Project 15: Reliable Transport over UDP with Selective Repeat

A custom reliable transport protocol (RTP) implemented over UDP featuring:
- SYN/FIN connection management
- Selective Repeat sliding window transmission
- RFC 6298-style adaptive RTO with Karn's rule and loss epoch gating
- Receiver-advertised flow control (rwnd) with zero-window persist probing
- Slow start and AIMD congestion control
- Stream application transferring a 20 MB binary file verified by SHA-256
- Performance evaluation against TCP on a routed Mininet topology under netem impairments

## Directory Layout
- `rtp/`: Protocol engine implementation (std-lib only, single-threaded `select()`).
- `app/`: File transfer client and server endpoints (RTP and TCP).
- `tools/`: Deterministic test file generator, impairment proxy, and plotting tools.
- `tests/`: Automated unit and integration test suite (`pytest`).
- `topology/`: Routed 2-router Mininet topology (`H1 - R1 - R2 - H2`) and `netem`/`tbf` scripts.
- `experiments/`: Experiment matrix runner and cross-protocol analyzer.
- `capture/`: PCAP packet captures.
- `docs/`: Protocol specification, traceability matrix, deviations, and architectural guides.
- `results/`: Experiment output logs, CSV traces, and run summaries.
- `evidence/`: Verification test logs and visual traces.

## Quick Start
Install dependencies:
```bash
pip install -r requirements.txt
```

Run test suite:
```bash
python -m pytest -v
```

Generate 20 MB test file:
```bash
python tools/make_file.py --size 20000000 --output test_20mb.bin --seed 42
```
