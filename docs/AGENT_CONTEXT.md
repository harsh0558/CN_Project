# Agent context (read this file first; read other docs only if a prompt names a section)

Project: CN Project 15, reliable transport over UDP. Python 3.10+, standard library only inside rtp/; pytest for tests. Selective Repeat + RFC 6298-style RTO + receiver flow control + slow start/AIMD. Transfers a 20 MB file, evaluated against TCP on a Mininet topology with netem/tbf.
Packet format, state machines and Selective Repeat details are in docs/PROTOCOL.md (read only the section you need).

STATUS (update this one line at the end of every phase): Phases 1-6 done (packet codec, UDP transport, handshake/teardown, impairment proxy, Stop-and-Wait, file app, Selective Repeat with fixed window and fixed RTO, adaptive RTO with RFC 6298 Jacobson/Karels estimator, Karn's rule, loss-epoch gate, receiver flow control with app_backlog, window updates, persist timer, and zero-window probes). Next: Phase 7.

WORKING RULES
1. Do only the phase in the prompt. Do not change the architecture, packet format, algorithms or acceptance criteria. Do not refactor working code. If the spec looks wrong, stop and say so in the report.
2. rtp/ is single-threaded: one select() loop, time.monotonic() for all timing, no threads or locks. Python 3.10-compatible syntax only.
3. Never edit default constants in rtp/config.py to make tests faster. Tests pass shorter values as arguments.
4. Never fabricate results. Do not call a phase done unless its required tests pass.
5. Update only the relevant section of docs/PROTOCOL.md and only the changed rows of docs/TRACEABILITY.md. Status 'Tested' for unit-test results; 'Verified' only for results on the real topology.

TOKEN RULES (strict)
- Read only the files you must change (targeted line ranges or grep). Do not re-read whole files or docs you already know.
- Pytest: always `python -m pytest <file> -q --tb=short`. Never use -v. Run the fast suite (`python -m pytest -q`) ONCE at the end of the phase. Run slow tests (`python -m pytest -m slow -q`) ONCE at the end, and only if the phase changed transfer logic. Do not rerun suites after small edits. After a failure rerun only that test (`-k name -x`).
- Any test taking more than 5 s must carry @pytest.mark.slow. pytest.ini excludes slow tests by default.
- Do not write plans, walkthroughs, summaries or docs other than those the prompt names. Do not print large files or long outputs; write them to files. Do not ask questions unless truly blocked: make the conservative choice and list it under Deviations.
- FINAL REPORT FORMAT (about 12 lines, no source code, no per-test pytest output):
  PHASE N: DONE | NOT DONE
  Fast suite: X passed, Y failed. Slow: X passed, Y failed (or not run).
  Required tests: name=PASS/FAIL, comma-separated
  Files changed: names only
  Deviations: none | short list
  Open issues: none | short list
  If anything fails, include only the failing assertion lines (max 30 lines).

APPROVED DECISIONS
- RTO: alpha 1/8, beta 1/4, K 4, G 1 ms. First sample R: SRTT=R, RTTVAR=R/2, RTO=SRTT+max(G,4*RTTVAR). Later: RTTVAR=(1-beta)*RTTVAR+beta*|SRTT-R|, then SRTT=(1-alpha)*SRTT+alpha*R. Initial RTO 1 s. Clamp [200 ms, 60 s]. Karn: samples only from packets never retransmitted. Backoff doubles per timeout event and resets on a valid sample. Per-packet deadline = last_send + current RTO. No timestamps. Loss-epoch gate: at most one backoff and one cwnd reduction per epoch; an epoch ends when send_base passes the highest seq sent at the timeout.
- Flow control: app_backlog = in-order packets not yet consumed by the app. rwnd = max(0, R - app_backlog) in every ACK. Receiver accepts seq in [rcv_base, rcv_base + rwnd). Occupancy = out-of-order buffered + app_backlog, never above R. --app-read-rate in packets/s (0 = unlimited). Zero window: persist timer (starts at current RTO, doubles, cap 2 s) sends a zero-length DATA probe (seq = next never-sent seq, consumes no sequence number). Receiver sends a window-update ACK when rwnd goes from 0 to above 0.
- Congestion control (units: packets, cwnd is a float): initial cwnd 1, ssthresh = max_window (64). Slow start: +1 per newly acked packet while cwnd < ssthresh. Congestion avoidance: +1/cwnd per newly acked packet. Timeout (once per epoch): ssthresh = max(floor(cwnd/2), 2), cwnd = 1. Cap at max_window. No fast retransmit. Effective window W = min(rwnd, floor(cwnd), max_window).
- Measurement: goodput (Mbit/s) = 8 * file_bytes / (t_end - t_start). t_start = just before connect/SYN (TCP: before connect()). t_end = when the client receives the final status (custom: status byte in FIN|ACK; TCP: 1-byte status). File = 20,000,000 bytes, seeded. Retransmitted payload is never counted. Counters: data_sent_total = unique_data_packets + retx_total; retx_timeout; retx_other (0 in core).
- Trace CSV columns: cwnd.csv (t,cwnd,ssthresh,rwnd,effective_window,in_flight,send_base,event); rtt.csv (t,sample_rtt,srtt,rttvar,rto,backoff); rcvbuf.csv (t,ooo_buffered,app_backlog,occupancy,rwnd). One summary.json per side.
- Topology (fixed): H1 10.0.1.10/24 gw 10.0.1.1 -- R1 (10.0.1.1, 10.0.2.1) -- R2 (10.0.2.2, 10.0.3.1) -- H2 10.0.3.10/24 gw 10.0.3.1. Static routes: R1 to 10.0.3.0/24 via 10.0.2.2; R2 to 10.0.1.0/24 via 10.0.2.1. netem + tbf on both directions of R1-R2. Ports: UDP 9000 custom, TCP 9001 baseline, TCP 5201 iperf3.
- Experiments: loss {0,1,5,10}% x RTT {20,100,200} ms x 3 runs x {custom, tcp} = 72 runs, plus a reordering scenario (1% reordering, 0% loss, each RTT, 3 runs, both protocols = 18 runs).
- Exclusions: fast retransmit, SACK-driven recovery, pacing, QUIC, Lua dissector, sequence wraparound, multiple connections.
