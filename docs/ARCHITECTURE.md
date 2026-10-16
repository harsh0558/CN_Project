# Architecture Overview

RTP uses a single-threaded, non-blocking `select()` event loop for all socket I/O and timing management (`time.monotonic()`), eliminating threads, mutexes, and race conditions.

## Subsystems
- **Packet Codec (`rtp/packet.py`)**: Strict binary packing and unpacking with CRC-32 integrity validation.
- **Connection Management (`rtp/connection.py`)**: State machines for handshake, active transfer, and teardown.
- **Transmission Engine (`rtp/sender.py`, `rtp/receiver.py`)**: Selective Repeat sliding windows with per-packet state tracking.
- **Congestion & Flow Control (`rtp/cwnd.py`, `rtp/rto.py`)**: Slow start, AIMD, and RFC 6298 adaptive timers.
