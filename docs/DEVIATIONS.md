# Intentional Protocol Deviations and Design Choices

This document records deliberate design deviations from standard RFCs or TCP Reno baseline semantics as approved in the project specification.

## 1. RFC 6298 Adaptive RTO Differences
RTP adapts the Jacobson/Karels RTT estimation equations from RFC 6298, but intentionally deviates in several core aspects. **RTP does not claim full RFC 6298 compliance.** The specific differences are:

1. **200 ms Minimum RTO (RFC 6298 §2.4)**:
   - *RFC 6298*: Rule (2.4) states that whenever RTO is computed, if it is less than 1.0 second, it SHOULD be rounded up to 1.0 second.
   - *RTP Deviation*: RTO is clamped to the range `[200 ms, 60 s]` (`MIN_RTO = 0.2` seconds).
   - *Rationale*: A 1.0 s floor is too coarse for low-latency networks and Mininet topologies with 20–200 ms RTTs, stalling throughput needlessly.

2. **No Timestamps (RFC 6298 §3 / RFC 7323)**:
   - *RFC 6298*: Recommends using TCP timestamps to measure RTT without ambiguity.
   - *RTP Deviation*: RTP headers have no timestamp option or timestamp field.
   - *Rationale*: RTT is sampled exclusively on the sender side using `time.monotonic()` between initial send and ACK receipt, strictly filtered by Karn's algorithm (no samples from retransmitted packets).

3. **Per-Packet Timers instead of One Timer (RFC 6298 §5)**:
   - *RFC 6298*: Section 5 specifies a single connection-wide retransmission timer managing the oldest unacknowledged packet (`SND.UNA`).
   - *RTP Deviation*: Selective Repeat maintains independent per-packet monotonic deadlines (`deadline = last_send + current_rto`) for all in-flight packets, retransmitting only the specific packet that timed out.
   - *Rationale*: Pipelined Selective Repeat requires selective retransmission of individual lost segments without retransmitting unexpired in-flight segments.

4. **Loss-Epoch Gate on Exponential Backoff (RFC 6298 §5.5)**:
   - *RFC 6298*: Rule (5.5) unconditionally doubles RTO on every timer expiration.
   - *RTP Deviation*: RTP implements a loss-epoch gate: at most one backoff and one cwnd reduction per loss epoch. An epoch begins on the first timeout and ends only when `send_base` passes the highest sequence number sent at the initial timeout (`epoch_end_seq = next_seq_num - 1`).
   - *Rationale*: With per-packet timers and multiple packets in flight, multiple unacknowledged packets from the same loss event expire in rapid succession. Doubling RTO for each packet would cause exponential backoff explosions (e.g., $2^4 = 16\times$). The epoch gate restricts backoff to once per loss epoch across the flight.

5. **Clock Granularity (RFC 6298 §2.1)**:
   - *RFC 6298*: Uses system clock tick granularity $G$.
   - *RTP Deviation*: RTP defines $G = 1\text{ ms}$ (`0.001 s`) in variance calculation `max(G, 4 * RTTVAR)`.
   - *Rationale*: Matches Python's `time.monotonic()` floating-point time base, while noting actual OS scheduler timer resolution limits (e.g., ~15.6 ms on Windows).

## 2. Loss Signals (Timeout-Driven Only; No Fast Retransmit)
- **TCP Reno Baseline**: Detects packet loss through 3 duplicate ACKs and triggers Fast Retransmit and Fast Recovery without waiting for RTO.
- **RTP Deviation**: Per-packet retransmission deadlines (timeouts) are the sole mechanism for loss detection and retransmission in the core protocol.
- **Rationale**: Project scope specifically requires pure Selective Repeat with adaptive RTO before any optional SACK-driven fast retransmission mechanisms.

## 3. Packet-Based Sequence Numbering
- **TCP Standard**: Sequence numbers count individual payload bytes.
- **RTP Deviation**: Sequence numbers count packets ($0, 1, 2, \dots$). First data packet is `seq = 0`.
- **Rationale**: Significantly simplifies window management, SACK bitmapping, and receive buffer indexing in single-threaded event loops.

## 4. Omission of Sequence Number Wraparound
- **Standard Transport**: Protocols handle 32-bit integer wraparound via modular arithmetic (e.g., RFC 7323 PAWS).
- **RTP Deviation**: Sequence wraparound is intentionally unhandled.
- **Rationale**: The target transfer is a 20 MB file, generating ~19,532 packets. A 32-bit sequence space supports $4,294,967,296$ packets, which could transfer $>4$ Terabytes before wraparound. A maximum file size limit is enforced.

## 5. Congestion Control Differences (RFC 5681 and TCP Reno)
- **Timeouts Only (No Fast Retransmit / Fast Recovery)**:
  - *RFC 5681 / Reno*: 3 duplicate ACKs trigger Fast Retransmit and Fast Recovery without waiting for an RTO expiration.
  - *RTP Deviation*: Packet loss is detected exclusively via per-packet RTO expiration. Duplicate ACKs and SACK bitmap arrivals mark received packets but never trigger early retransmission or enter Fast Recovery.
  - *Rationale*: Aligns with the core specification requirement for pure Selective Repeat ARQ loss recovery before optional fast-recovery optimizations.
- **Per-ACK Floating-Point Window Increase Rules**:
  - *RFC 5681 / Reno*: CWND is byte-based and increased by SMSS during slow start or $\text{SMSS} \times \text{SMSS} / \text{CWND}$ during congestion avoidance per ACK.
  - *RTP Deviation*: CWND is tracked as a floating-point number of packets. In slow start, `cwnd += 1.0` per newly acknowledged packet while `cwnd < ssthresh`. In congestion avoidance, `cwnd += 1.0 / cwnd` per newly acknowledged packet (AIMD linear growth adding $\sim 1$ packet per window of ACKs).
  - *Rationale*: Eliminates byte-level window tracking complexity while providing smooth AIMD convergence on packet-switched topologies.
- **Loss-Epoch Gating on Congestion Window Reductions**:
  - *RFC 5681 / Reno*: Reduces CWND on every distinct timeout event.
  - *RTP Deviation*: Timeout reductions (`ssthresh = max(floor(cwnd / 2), 2)`, `cwnd = 1.0`) are gated to at most once per loss epoch (ending when `send_base` passes the highest sequence number sent at the time of the initial timeout).
  - *Rationale*: In Selective Repeat with per-packet timers, multiple packets in a dropped burst expire in quick succession. Halving `ssthresh` repeatedly across a single dropped flight would reduce `ssthresh` to 2 and collapse throughput unnecessarily.


## 6. Fixed Maximum Window (64 packets)
- *TCP*: The window is limited only by the receiver's advertised window and the congestion window; there is no small fixed cap.
- *RTP Deviation*: All experiments use a 64-packet sender and receiver window (`--window`, default 64), and the initial `ssthresh` is 64. The effective window is `min(rwnd, cwnd, 64)`.
- *Consequence (measured)*: At 0% loss, cwnd rises through slow start and then stays at 64 (see `analysis_out/cwnd_traces.png`). RTP is then limited by the window, not by congestion control: goodput is at most 64 x 1024 B per RTT, about 26, 5.2 and 2.6 Mbit/s at 20, 100 and 200 ms. The measured 0% loss values at 20 and 100 ms (23.8 and 4.9 Mbit/s) are consistent with this bound.

## 7. Observed Effect of the 200 ms RTO Floor at 200 ms RTT
- At RTT = 200 ms the RTO floor (200 ms) equals the path RTT. The 0% loss runs at 200 ms show a mean of 441 retransmissions (std 296) even though no loss was configured, a repeated sawtooth in cwnd and ssthresh, and lower goodput (1.06 Mbit/s, below the 2.6 Mbit/s window bound).
- This is consistent with spurious timeouts caused by the floor; the cause has not been isolated by a separate experiment (for example, a run with a lower floor).

## 8. Experiment-Setup Notes
- netem loss, reordering and duplication are applied on both directions of the R1-R2 link, so ACKs are also lost, reordered and duplicated. The configured RTT is split equally over both directions and checked with ping before each run.
- `--app-read-rate` (receiver application read rate in packets/s, default 0 = unlimited) is used only in the flow-control demonstration. All matrix runs use the unlimited default.

## 9. No `tbf` Rate Limit on the R1-R2 Link
- *Plan (proposal)*: netem (delay, loss, reorder, duplicate) plus `tc tbf` rate limiting on the R1-R2 link as the bottleneck.
- *Actual setup*: only `netem` is applied (see `results/*/netem.txt`, `netem` field in each `result.json`). No `tbf` or any other rate limit was configured, so the link rate is limited only by the VM's virtual links.
- *Consequence*: TCP reaches about 285 Mbit/s at 20 ms RTT and 0% loss, and RTP is limited by its 64-packet window (Section 6) and not by a bottleneck rate. Congestion-induced loss comes only from netem's default 1000-packet queue limit. TCP shows retransmissions even at 0% configured loss (1157 to 4549 on average); queue overflow at this limit is the likely cause, but this has not been verified (the qdisc drop counters were not saved).
- *Rationale*: The instructor lists `tc netem` and `tc tbf` "where appropriate"; the required loss, delay, reordering and duplication scenarios are all provided by netem. Re-running the full matrix with a bottleneck rate was not done for time reasons.
