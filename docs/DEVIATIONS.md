# Intentional Protocol Deviations and Design Choices

This document records deliberate design deviations from standard RFCs or TCP Reno baseline semantics as approved in the project specification.

## 1. Minimum RTO Clamping (200 ms vs RFC 6298 1.0 s)
- **RFC 6298 Rule**: Section 2.4 specifies that RTO should be at least 1.0 second.
- **RTP Deviation**: Minimum RTO is clamped to `0.2` seconds (200 ms).
- **Rationale**: A 1.0 s minimum timer is excessively coarse for low-latency LAN and Mininet environments with RTTs under 50 ms. Clamping at 200 ms permits rapid retransmission without false timeouts under moderate jitter.

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
