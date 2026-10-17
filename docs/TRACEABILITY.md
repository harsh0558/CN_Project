# Requirements Traceability Matrix

This document maps all architectural, protocol, and testing requirements to their implementing components, tests, verification evidence, and delivery status.

### Status Definitions
- **Pending**: Not yet implemented.
- **Implemented**: Code written and documentation drafted, awaiting test coverage.
- **Tested**: Validated and passing automated unit or component test suites.
- **Verified**: Confirmed with reproducible end-to-end evidence on the real Mininet topology.

---

| Requirement | Component | Test | Evidence | Status |
| :--- | :--- | :--- | :--- | :--- |
| **R-PKT-01**: Header layout is exactly 26 bytes big-endian (`!IIIIHBBHI`) | `rtp/packet.py` | `tests/test_packet.py::test_struct_size_is_exactly_26_bytes` | `pytest tests/test_packet.py` passed (struct size = 26) | **Tested** |
| **R-PKT-02**: Round-trip encode/decode for all valid flag combinations (SYN, SYN\|ACK, ACK, DATA, FIN, FIN\|ACK) | `rtp/packet.py` | `tests/test_packet.py::test_round_trip_all_protocol_flag_combinations`, `test_decode_accepts_standard_packet_types` | `pytest tests/test_packet.py` passed | **Tested** |
| **R-PKT-03**: Boundary handling (seq 0 & $2^{32}-1$, rwnd 0 & 65535, length 0 & 1024) | `rtp/packet.py` | `tests/test_packet.py::test_boundary_values`, `test_encode_out_of_bounds_raises_encoding_error` | `pytest tests/test_packet.py` passed | **Tested** |
| **R-PKT-04**: Bit-flip corruption detection across all header and payload bits via CRC-32 | `rtp/packet.py` | `tests/test_packet.py::test_single_bit_flip_detection` | `pytest tests/test_packet.py` passed (464/464 bit-flips detected) | **Tested** |
| **R-PKT-05**: Datagram rejection for truncated, oversized, non-zero reserved, and invalid flags | `rtp/packet.py` | `tests/test_packet.py::test_truncated_datagrams_rejected`, `test_payload_length_mismatch_rejected`, `test_reserved_byte_nonzero_rejected`, `test_decode_rejects_disallowed_flag_combinations` | `pytest tests/test_packet.py` passed | **Tested** |
| **R-PKT-06**: Fuzz robustness: 10,000 random byte strings produce only PacketError or valid packet | `rtp/packet.py` | `tests/test_packet.py::test_fuzz_10000_random_byte_strings` | `pytest tests/test_packet.py` passed (10,000 strings fuzzed) | **Tested** |
| **R-PKT-07**: Acceptance of DATA length=0 (probe) and FIN\|ACK 1-byte payload | `rtp/packet.py` | `tests/test_packet.py::test_decode_accepts_data_with_length_zero`, `test_decode_accepts_fin_ack_with_one_byte_payload` | `pytest tests/test_packet.py` passed | **Tested** |
| **R-CONN-01**: 3-way handshake with SYN exponential backoff (6 attempts, 1s initial) | `rtp/connection.py` | `tests/test_handshake.py::test_handshake_and_teardown_success_status_ok`, `test_loss_recovery_syn`, `test_server_absent_aborts_with_peer_unreachable` | All tests passing (`pytest tests/test_handshake.py`) | **Tested** |
| **R-CONN-02**: 3-way teardown with FIN\|ACK status byte and client TIME_WAIT | `rtp/connection.py` | `tests/test_handshake.py::test_handshake_and_teardown_status_mismatch`, `test_loss_recovery_fin`, `test_loss_recovery_fin_ack` | All tests passing (`pytest tests/test_handshake.py`) | **Tested** |
| **R-CONN-03**: Inactivity timeout (30s) and consecutive timeout abort (10 retries) | `rtp/connection.py` | `tests/test_handshake.py::test_server_absent_aborts_with_peer_unreachable` | Only SYN-phase abort tested; 10 consecutive timeouts and 30s idle abort planned for active data transfer phases | **Partial** |
| **R-IMP-01**: Impairment proxy for deterministic drop, delay, duplication, and reordering | `tools/impair_proxy.py` | `tests/test_impair_proxy.py`, `tests/test_handshake.py` | Proxy unit and integration tests passing | **Tested** |
| **R-APP-01**: Application stream format `[u16][name][u64][32-byte sha256][data]` and atomic rename | `app/file_receiver.py`, `app/file_sender.py`, `app/framing.py` | `tests/test_transfer.py` | 77/77 tests pass: 0B, 1B, 1KB, multiples, 1MB; 5% loss proxy; hash mismatch; interrupted; filename sanitization (7 cases); size limit; fewer/more bytes than declared; stale ACK ignored | **Tested** |
| **R-APP-FILE**: Deterministic 20MB seeded test file generator | `tools/make_file.py` | Execution test script | Generated 20,000,000 bytes with seed 42 | **Tested** |
| **R-SR-01**: Selective Repeat sliding sender window ($s < send\_base + W$) with per-packet timers | `rtp/sender.py` | `tests/test_sr.py` | 14/14 tests passing (`pytest -v tests/test_sr.py`), pipelined window, selective retx, probe handling | **Tested** |
| **R-SR-02**: Selective Repeat receiver buffer of capacity $R=64$ packets and contiguous delivery | `rtp/receiver.py` | `tests/test_sr.py` | 14/14 tests passing (`pytest -v tests/test_sr.py`), out-of-order buffering, SACK bitmap, probe passthrough | **Tested** |
| **R-RTO-01**: RFC 6298 adaptive RTO estimator with Karn's rule and $[200\text{ms}, 60\text{s}]$ clamping | `rtp/rto.py`, `rtp/sender.py` | `tests/test_rto.py` | 6/6 tests passing in `tests/test_rto.py`: init, hand-computed update, clamping, Karn rule, proxy delay convergence | **Tested** |
| **R-RTO-02**: Loss epoch gating: at most one backoff and one cwnd reduction per epoch | `rtp/sender.py` | `tests/test_rto.py` | Tested backoff gate in `rtp/sender.py` and `tests/test_rto.py` | **Tested** |
| **R-FC-01**: Receiver advertised flow control: $rwnd = \max(0, R - app\_backlog)$ | `rtp/receiver.py`, `rtp/sender.py` | `tests/test_flow.py` | All 5 tests passing in `tests/test_flow.py` (no packets beyond window, occupancy <= R, rcvbuf.csv) | **Tested** |
| **R-FC-02**: Zero-window persist timer (capped at 2s) with 0-length DATA probe | `rtp/sender.py`, `rtp/receiver.py` | `tests/test_flow.py` | All 5 tests passing in `tests/test_flow.py` (lost window-update recovery, invariant maintained) | **Tested** |
| **R-CC-01**: Slow start (+$1$ cwnd per ACK) and Congestion Avoidance AIMD (+$1/cwnd$ per ACK) | `rtp/cwnd.py` | `tests/test_congestion.py` | Pending Phase 7 | Pending |
| **R-CC-02**: Timeout loss reaction: $ssthresh = \max(\lfloor cwnd/2 \rfloor, 2)$, $cwnd = 1$ | `rtp/cwnd.py` | `tests/test_congestion.py` | Pending Phase 7 | Pending |
| **R-MET-01**: Event trace logging (`cwnd.csv`, `rtt.csv`, `rcvbuf.csv`) and `summary.json` | `rtp/metrics.py` | `tests/test_metrics.py` | Pending Phase 8 | Pending |
| **R-MET-02**: Invariant check: $data\_sent\_total == unique\_data\_packets + retx\_total$ | `rtp/sender.py`, `rtp/receiver.py` | `tests/test_sr.py::test_counter_invariant_in_every_test` | Verified in all 14 tests in `tests/test_sr.py` across loss and reordering | **Tested** |
| **R-TOPO-01**: Mininet 2-router routed topology (`H1 - R1 - R2 - H2`) with netem & tbf | `topology/topo.py` | Ubuntu VM smoke tests | Pending Phase 10 | Pending |
| **R-TCP-01**: Baseline TCP client/server with matched measurement window ($t_{start}, t_{end}$) | `app/tcp_sender.py`, `app/tcp_receiver.py` | Comparison tests | Pending Phase 11 | Pending |
| **R-EXP-01**: Experiment matrix automation across loss, delay, and reordering scenarios | `experiments/run_matrix.py` | Experiment runs | Pending Phase 11 | Pending |
| **R-DOC-01**: Authoritative protocol specification including code-generated hexdumps | `docs/PROTOCOL.md` | Verification against spec | Conforms to Phase 1 spec | **Implemented** |
