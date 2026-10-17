# Intentional Protocol Deviations and Design Choices

This document records deliberate design deviations from standard RFCs or TCP Reno baseline semantics as approved in the project specification.

## 1. RFC 6298 Adaptive RTO Differences
RTP adapts the Jacobson/Karels RTT estimation equations from RFC 6298, but intentionally deviates in several core aspects. **RTP does not claim full RFC 6298 compliance.** The specific differences are:

1. **200 ms Minimum RTO (RFC 6298 §2.4)**:
   - *RFC 6298*: Rule (2.4) mandates that whenever RTO is computed, if it is less than 1.0 second, it MUST be rounded up to 1.0 second.
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
- **Standard Transport**: Protocols handle 32-bit integer wraparound via modular arithmetic (e.g., RFC 1323 PAWS).
- **RTP Deviation**: Sequence wraparound is intentionally unhandled.
- **Rationale**: The target transfer is a 20 MB file, generating ~19,532 packets. A 32-bit sequence space supports $4,294,967,296$ packets, which could transfer $>4$ Terabytes before wraparound. A maximum file size limit is enforced.
