# Testing Guide

Correctness tests (pytest, no Mininet needed) are separate from performance experiments (Mininet, `experiments/`).

## Running the tests
From the project root, with the virtual environment active:
```bash
python -m pytest -m "not slow" -v     # fast suite
python -m pytest -v                    # includes slow tests
```

## Test files
- `tests/test_packet.py`: header encode/decode, boundaries, CRC bit-flip detection, malformed packets, fuzzing.
- `tests/test_transport.py`, `tests/test_handshake.py`: UDP transport, handshake and teardown, lost SYN/FIN, absent peer.
- `tests/test_impair_proxy.py`: the impairment proxy.
- `tests/test_transfer.py`: file transfer, hash mismatch, interruption, filename sanitization.
- `tests/test_sr.py`: Selective Repeat, loss, reordering, duplicates, counter invariant.
- `tests/test_rto.py`: RTO estimator, Karn's rule, clamping, backoff gate.
- `tests/test_flow.py`: flow control, window updates, zero-window probe.
- `tests/test_cc.py`: slow start, congestion avoidance, timeout response.
- `tests/test_metrics.py`, `tests/test_timing.py`: metrics, summaries, goodput timing.
- `tests/test_robust.py`: malformed datagrams, peer disappearing, fresh transfer after interruption.

## Experiments (Ubuntu VM, Mininet)
See `SETUP_UBUNTU.md` and `experiments/README.md`. Each run writes `results/<tag>/result.json` with `status` and `hash_ok`; the received file is hashed against the source and then deleted.

Demonstrations:
```bash
# flow control: slow receiving application
sudo /usr/bin/python3 experiments/run_one.py --proto rtp --rtt 20 --loss 0 --run 21 --app-read-rate 1000 --capture
# duplicate suppression: 5% netem duplication
sudo /usr/bin/python3 experiments/run_one.py --proto rtp --rtt 20 --loss 0 --duplicate 5 --run 1 --capture
```
