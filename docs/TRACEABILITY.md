# Requirements Traceability Matrix

This document maps all architectural, protocol, and testing requirements to their implementing components, tests, verification evidence, and delivery status.

Status Labels:
- **Pending**: Not yet implemented.
- **Implemented**: Code written but awaiting test coverage or formal validation.
- **Tested**: Automated tests pass.
- **Verified**: Confirmed with reproducible evidence (test output, traces, or captures).

---

| Requirement ID | Requirement Description | Component | Test | Evidence | Status |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **R-PKT-01** | Header layout is exactly 26 bytes big-endian (`!IIIIHBBHI`) | `rtp/packet.py` | `tests/test_packet.py::test_struct_size_is_exactly_26_bytes` | `pytest tests/test_packet.py` passed (struct size = 26) | **Verified** |
| **R-PKT-02** | Round-trip encode/decode for all valid flag combinations (SYN, SYN\|ACK, ACK, DATA, FIN, FIN\|ACK) | `rtp/packet.py` | `tests/test_packet.py::test_round_trip_all_protocol_flag_combinations` | `pytest tests/test_packet.py` passed | **Verified** |
| **R-PKT-03** | Boundary handling (seq 0 & $2^{32}-1$, rwnd 0 & 65535, length 0 & 1024) | `rtp/packet.py` | `tests/test_packet.py::test_boundary_values`, `test_encode_out_of_bounds_raises_encoding_error` | `pytest tests/test_packet.py` passed | **Verified** |
| **R-PKT-04** | Bit-flip corruption detection across all header and payload bits via CRC-32 | `rtp/packet.py` | `tests/test_packet.py::test_single_bit_flip_detection` | `pytest tests/test_packet.py` passed (464/464 bit-flips detected) | **Verified** |
| **R-PKT-05** | Datagram rejection for truncated, oversized, non-zero reserved, and invalid flags | `rtp/packet.py` | `tests/test_packet.py::test_truncated_datagrams_rejected`, `test_payload_length_mismatch_rejected`, `test_reserved_byte_nonzero_rejected`, `test_invalid_flags_rejected` | `pytest tests/test_packet.py` passed | **Verified** |
| **R-PKT-06** | Fuzz robustness: 10,000 random byte strings produce only PacketError or valid packet | `rtp/packet.py` | `tests/test_packet.py::test_fuzz_10000_random_byte_strings` | `pytest tests/test_packet.py` passed (10,000 strings fuzzed in 0.58s) | **Verified** |
| **R-CONN-01** | 3-way handshake with SYN exponential backoff (6 attempts, 1s initial) | `rtp/connection.py` | `tests/test_handshake.py` | Pending Phase 2 | Pending |
| **R-CONN-02** | 3-way teardown with FIN\|ACK status byte and client TIME_WAIT | `rtp/connection.py` | `tests/test_handshake.py` | Pending Phase 2 | Pending |
| **R-CONN-03** | Inactivity timeout (30s) and consecutive timeout abort (10 retries) | `rtp/connection.py` | `tests/test_handshake.py` | Pending Phase 2 | Pending |
| **R-IMP-01** | Impairment proxy for deterministic drop, delay, duplication, and reordering | `tools/impair_proxy.py` | Proxy unit & integration tests | Pending Phase 2 | Pending |
| **R-APP-01** | Application stream format `[u16][name][u64][32-byte sha256][data]` and atomic rename | `app/file_receiver.py`, `app/file_sender.py` | `tests/test_transfer.py` | Pending Phase 3 | Pending |
| **R-APP-FILE** | Deterministic 20MB seeded test file generator | `tools/make_file.py` | Verification test script | Generated 20,000,000 bytes with seed 42 | **Verified** |
| **R-SR-01** | Selective Repeat sliding sender window ($s < send\_base + W$) with per-packet timers | `rtp/sender.py` | `tests/test_sr.py` | Pending Phase 4 | Pending |
| **R-SR-02** | Selective Repeat receiver buffer of capacity $R=64$ packets and contiguous delivery | `rtp/receiver.py` | `tests/test_sr.py` | Pending Phase 4 | Pending |
| **R-RTO-01** | RFC 6298 adaptive RTO estimator with Karn's rule and $[200\text{ms}, 60\text{s}]$ clamping | `rtp/rto.py` | `tests/test_rto.py` | Pending Phase 5 | Pending |
| **R-RTO-02** | Loss epoch gating: at most one backoff and one cwnd reduction per epoch | `rtp/rto.py` | `tests/test_rto.py` | Pending Phase 5 | Pending |
| **R-FC-01** | Receiver advertised flow control: $rwnd = \max(0, R - app\_backlog)$ | `rtp/receiver.py`, `rtp/sender.py` | `tests/test_flow_control.py` | Pending Phase 6 | Pending |
| **R-FC-02** | Zero-window persist timer (capped at 2s) with 0-length DATA probe | `rtp/sender.py`, `rtp/receiver.py` | `tests/test_flow_control.py` | Pending Phase 6 | Pending |
| **R-CC-01** | Slow start (+$1$ cwnd per ACK) and Congestion Avoidance AIMD (+$1/cwnd$ per ACK) | `rtp/cwnd.py` | `tests/test_congestion.py` | Pending Phase 7 | Pending |
| **R-CC-02** | Timeout loss reaction: $ssthresh = \max(\lfloor cwnd/2 \rfloor, 2)$, $cwnd = 1$ | `rtp/cwnd.py` | `tests/test_congestion.py` | Pending Phase 7 | Pending |
| **R-MET-01** | Event trace logging (`cwnd.csv`, `rtt.csv`, `rcvbuf.csv`) and `summary.json` | `rtp/metrics.py` | `tests/test_metrics.py` | Pending Phase 8 | Pending |
| **R-MET-02** | Invariant check: $data\_sent\_total == unique\_data\_packets + retx\_total$ | `rtp/metrics.py` | Summary assertion | Pending Phase 8 | Pending |
| **R-TOPO-01** | Mininet 2-router routed topology (`H1 - R1 - R2 - H2`) with netem & tbf | `topology/topo.py` | Ubuntu VM smoke tests | Pending Phase 10 | Pending |
| **R-TCP-01** | Baseline TCP client/server with matched measurement window ($t_{start}, t_{end}$) | `app/tcp_sender.py`, `app/tcp_receiver.py` | Comparison tests | Pending Phase 11 | Pending |
| **R-EXP-01** | Experiment matrix automation across loss, delay, and reordering scenarios | `experiments/run_matrix.py` | Experiment runs | Pending Phase 11 | Pending |
| **R-DOC-01** | Authoritative protocol specification including code-generated hexdumps | `docs/PROTOCOL.md` | Verification against spec | Verified against Phase 1 criteria | **Verified** |
