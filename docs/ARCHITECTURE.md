# Architecture Overview

RTP is a reliable transport protocol over UDP. It uses a single-threaded, non-blocking `select()` event loop for all socket I/O and timing (`time.monotonic()`), so there are no threads or locks.

## Subsystems
- **Packet codec (`rtp/packet.py`)**: 26-byte big-endian header (`!IIIIHBBHI`): connection ID, sequence, cumulative ACK, SACK bitmap, receiver window, flags, reserved, payload length, CRC-32. Payload is up to 1024 bytes. See `PROTOCOL.md`.
- **UDP transport (`rtp/transport.py`)**: socket wrapper used by sender and receiver.
- **Connection management (`rtp/connection.py`)**: 3-way SYN handshake and FIN teardown with retries.
- **Selective Repeat (`rtp/sender.py`, `rtp/receiver.py`)**: sender window with per-packet timers and selective retransmission; receiver buffer with in-order delivery to the application and duplicate suppression.
- **Adaptive timeout (`rtp/rto.py`)**: RFC 6298-style SRTT/RTTVAR estimator with Karn's rule and exponential backoff (differences in `DEVIATIONS.md`).
- **Congestion control (`rtp/cwnd.py`)**: slow start and AIMD, cwnd in packets.
- **Flow control (`rtp/receiver.py`, `rtp/sender.py`)**: receiver advertises `rwnd`; zero-window handled with a persist timer and 0-length probe.
- **Metrics (`rtp/metrics.py`)**: `cwnd.csv`, `rtt.csv`, `rcvbuf.csv` and sender/receiver summary JSON.
- **File application (`app/`)**: `file_sender.py`, `file_receiver.py`, `framing.py`. The receiver checks the SHA-256 carried in the stream header and reports success only after the hash matches.

## Window rule
The sender may have at most `min(rwnd, floor(cwnd), configured window = 64)` packets in flight. `rwnd` is the receiver's advertised capacity, `cwnd` is the congestion window (both in packets).

## Experiment tooling
- `topology/topo.py`: Mininet topology.
- `experiments/run_one.py`: one run (RTP or TCP) in the topology; applies netem, pings, runs sender/receiver, checks the hash, writes `result.json`. Options: `--rtt --loss --reorder --duplicate --window --app-read-rate --capture --evidence`.
- `experiments/run_matrix.py`: runs the full matrix.
- `tools/tcp_transfer.py`: TCP baseline (Reno), same file, TCP kernel retransmission counter via `TCP_INFO`.
- `tools/analyze.py`, `tools/plot_run.py`, `tools/plot_zoom.py`: analysis and plots.
- `tools/impair_proxy.py`: software impairment proxy used by unit tests (not by the Mininet experiments).
- `tools/rtp.lua`: Wireshark dissector (protocol `rtpu`, fields `rtpu.*`).

## Network (Mininet)
| Node | Interface | Address |
| :--- | :--- | :--- |
| H1 | h1-eth0 | 10.0.1.10/24 (gateway 10.0.1.1) |
| R1 | r1-eth0 / r1-eth1 | 10.0.1.1/24 / 10.0.2.1/24 |
| R2 | r2-eth0 / r2-eth1 | 10.0.2.2/24 / 10.0.3.1/24 |
| H2 | h2-eth0 | 10.0.3.10/24 (gateway 10.0.3.1) |

Static routes on R1 and R2. netem is applied on the R1-R2 link in both directions (no `tbf`; see `DEVIATIONS.md` section 9). RTP uses UDP port 9000. tcpdump runs on H1 and H2 when `--capture` is given.
