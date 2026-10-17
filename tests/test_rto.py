"""Unit and integration tests for adaptive RTO estimator (Phase 5).

Tests:
1. First sample initialises SRTT/RTTVAR/RTO.
2. Second update matches a hand-computed value.
3. Clamp to min (200 ms) and max (60 s).
4. Backoff doubles on consecutive timeouts and resets after a valid sample.
5. Karn's rule: an ACK for a retransmitted packet gives no sample (seeded proxy drops first copy).
6. With 100 ms added each way by proxy, SRTT converges to 200 ms within +-40%
   (commenting Windows timer granularity).
"""

from __future__ import annotations

import socket
import threading
import time
from pathlib import Path
import pytest

from app.file_receiver import FileReceiver
from app.file_sender import FileSender
from rtp.config import FLAG_DATA, MAX_RTO, MIN_RTO
from rtp.rto import RtoEstimator
from tools.impair_proxy import DirectionConfig, ImpairmentProxy


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


# ---------------------------------------------------------------------------
# Test 1: First sample initialises SRTT/RTTVAR/RTO
# ---------------------------------------------------------------------------

def test_first_sample_initializes_srtt_rttvar_rto() -> None:
    """First RTT sample R sets SRTT=R, RTTVAR=R/2, RTO=SRTT+max(G, 4*RTTVAR)."""
    estimator = RtoEstimator()
    assert estimator.srtt is None
    assert estimator.rttvar is None
    assert estimator.current_rto == 1.0  # Initial default RTO before any sample
    assert estimator.backoff == 1

    sample_r = 0.400  # 400 ms sample
    estimator.on_sample(sample_r)

    # SRTT = R
    assert estimator.srtt == pytest.approx(0.400)
    # RTTVAR = R / 2
    assert estimator.rttvar == pytest.approx(0.200)
    # RTO = SRTT + max(0.001, 4 * 0.200) = 0.400 + 0.800 = 1.200 s
    assert estimator.current_rto == pytest.approx(1.200)
    assert estimator.backoff == 1


# ---------------------------------------------------------------------------
# Test 2: Second update matches a hand-computed value
# ---------------------------------------------------------------------------

def test_second_update_matches_hand_computed_value() -> None:
    """Second sample updates RTTVAR first, then SRTT, matching exact manual calculation."""
    estimator = RtoEstimator()
    # Sample 1: R1 = 0.200 s (200 ms)
    # SRTT = 0.200, RTTVAR = 0.100, RTO = 0.200 + max(0.001, 4 * 0.100) = 0.600
    estimator.on_sample(0.200)

    # Sample 2: R2 = 0.300 s (300 ms)
    # RFC 6298 / AGENT_CONTEXT parameters:
    # alpha = 1/8 = 0.125, beta = 1/4 = 0.25, G = 0.001, K = 4
    # RTTVAR = (1 - beta) * RTTVAR_old + beta * |SRTT_old - R2|
    #        = (1 - 0.25) * 0.100 + 0.25 * |0.200 - 0.300|
    #        = 0.075 + 0.025 = 0.1000
    # SRTT   = (1 - alpha) * SRTT_old + alpha * R2
    #        = (1 - 0.125) * 0.200 + 0.125 * 0.300
    #        = 0.175 + 0.0375 = 0.2125
    # RTO    = SRTT + max(G, 4 * RTTVAR)
    #        = 0.2125 + max(0.001, 4 * 0.1000) = 0.2125 + 0.4000 = 0.6125
    estimator.on_sample(0.300)

    assert estimator.srtt == pytest.approx(0.2125)
    assert estimator.rttvar == pytest.approx(0.1000)
    assert estimator.current_rto == pytest.approx(0.6125)


# ---------------------------------------------------------------------------
# Test 3: Clamp to min and max
# ---------------------------------------------------------------------------

def test_clamp_to_min_and_max() -> None:
    """RTO must be clamped within [MIN_RTO (200 ms), MAX_RTO (60 s)]."""
    # Lower clamp test: very small RTT (e.g., 5 ms)
    est_low = RtoEstimator()
    est_low.on_sample(0.005)
    # Raw RTO = 0.005 + 4 * 0.0025 = 0.015 s (15 ms)
    # Clamped to MIN_RTO (200 ms = 0.2 s)
    assert est_low.current_rto == pytest.approx(MIN_RTO)
    assert est_low.current_rto == pytest.approx(0.2)

    # Upper clamp test: very large RTT sample
    est_high = RtoEstimator()
    est_high.on_sample(25.0)
    # Raw RTO = 25.0 + 4 * 12.5 = 75.0 s
    # Clamped to MAX_RTO (60 s)
    assert est_high.current_rto == pytest.approx(MAX_RTO)
    assert est_high.current_rto == pytest.approx(60.0)

    # Backoff clamping to MAX_RTO
    est_backoff = RtoEstimator()
    est_backoff.on_sample(10.0)  # RTO = 30.0 s
    est_backoff.on_timeout()     # 30.0 * 2 = 60.0 s
    assert est_backoff.current_rto == pytest.approx(60.0)
    est_backoff.on_timeout()     # 30.0 * 4 = 120.0 s -> clamped to 60.0 s
    assert est_backoff.current_rto == pytest.approx(MAX_RTO)


# ---------------------------------------------------------------------------
# Test 4: Backoff doubles on consecutive timeouts and resets after a valid sample
# ---------------------------------------------------------------------------

def test_backoff_doubles_on_consecutive_timeouts_and_resets() -> None:
    """Exponential backoff doubles on consecutive timeouts and resets to 1 upon sample."""
    estimator = RtoEstimator()
    estimator.on_sample(0.200)  # Base RTO = 0.600 s
    base_rto = estimator.current_rto
    assert base_rto == pytest.approx(0.600)
    assert estimator.backoff == 1

    estimator.on_timeout()
    assert estimator.backoff == 2
    assert estimator.current_rto == pytest.approx(base_rto * 2)

    estimator.on_timeout()
    assert estimator.backoff == 4
    assert estimator.current_rto == pytest.approx(base_rto * 4)

    estimator.on_timeout()
    assert estimator.backoff == 8
    assert estimator.current_rto == pytest.approx(base_rto * 8)

    # Valid sample resets backoff to 1 and recalculates RTO from new sample
    estimator.on_sample(0.200)
    assert estimator.backoff == 1
    assert estimator.current_rto < base_rto * 8


# ---------------------------------------------------------------------------
# Test 5: Karn (an ACK for a retransmitted packet gives no sample)
# ---------------------------------------------------------------------------

def test_karn_retransmitted_packet_gives_no_sample(tmp_path: Path) -> None:
    """An ACK for a retransmitted packet gives no RTT sample; drop first copy via proxy."""
    # Create single-chunk file (under 1024 bytes) so exactly one DATA packet (seq 0) is sent
    src_file = tmp_path / "karn_test.bin"
    src_file.write_bytes(b"A" * 100)

    recv_dir = tmp_path / "recv_karn"
    recv_dir.mkdir(parents=True, exist_ok=True)

    server_port = _find_free_port()
    proxy_port = _find_free_port()

    # Seeded proxy dropping the first copy of seq 0 and seq 1 in client-to-server direction
    proxy = ImpairmentProxy(
        listen_addr=("127.0.0.1", proxy_port),
        target_addr=("127.0.0.1", server_port),
        seed=42,
    )
    proxy.drop_next(direction="c2s", count=1, flags=FLAG_DATA, seq=0)
    proxy.drop_next(direction="c2s", count=1, flags=FLAG_DATA, seq=1)
    proxy.start()

    receiver = FileReceiver(
        bind_addr=("127.0.0.1", server_port),
        output_dir=recv_dir,
        timeout=10.0,
        mode="sr",
    )

    results: list[tuple[str, str | None, int]] = []

    def recv_worker():
        results.append(receiver.run_once())

    th = threading.Thread(target=recv_worker, daemon=True)
    th.start()
    time.sleep(0.05)

    # Sender uses adaptive RTO (rto=None)
    sender = FileSender(
        server_addr=("127.0.0.1", proxy_port),
        timeout=10.0,
        mode="sr",
        window_size=16,
    )
    outcome, code = sender.send_file(src_file)

    th.join(timeout=10.0)
    proxy.stop()

    assert outcome == "success"
    assert code == 0

    sr_sender = sender._last_sender
    assert sr_sender is not None

    # Retransmissions occurred for the dropped packets (seq 0 and seq 1)
    assert sr_sender.retx_timeout == 2

    # Karn's rule verification: because all packets were retransmitted, their ACKs produced
    # NO sample. Therefore, the estimator never sampled RTT and SRTT remains None!
    assert sr_sender.rto_estimator.srtt is None
    assert sr_sender.rto_estimator.rttvar is None


# ---------------------------------------------------------------------------
# Test 6: With 100 ms added each way by proxy, SRTT converges to 200 ms +-40%
# ---------------------------------------------------------------------------

def test_srtt_convergence_with_proxy_delay(tmp_path: Path) -> None:
    """With 100 ms delay each way (200 ms RTT), SRTT converges to 200 ms within +-40%."""
    # Note on Windows timer granularity:
    # Windows standard timer interrupt resolution is ~15.6 ms. ImpairmentProxy scheduling
    # and socket select() loops run with typical jitter of 10-25 ms on Windows.
    # The +-40% tolerance window ([120 ms, 280 ms]) accommodates this OS clock granularity
    # while strictly verifying RFC 6298 SRTT convergence to the expected 200 ms proxy delay.

    # 10 chunks of data (10 * 1024 bytes) so multiple RTT samples update SRTT
    size = 10 * 1024
    content = bytes([(i * 11) % 256 for i in range(size)])
    src_file = tmp_path / "delay_test.bin"
    src_file.write_bytes(content)

    recv_dir = tmp_path / "recv_delay"
    recv_dir.mkdir(parents=True, exist_ok=True)
    run_dir = tmp_path / "run_metrics"

    server_port = _find_free_port()
    proxy_port = _find_free_port()

    # 100 ms delay added in both directions (c2s and s2c) -> RTT ~ 200 ms
    c2s_cfg = DirectionConfig(delay_ms=100.0)
    s2c_cfg = DirectionConfig(delay_ms=100.0)
    proxy = ImpairmentProxy(
        listen_addr=("127.0.0.1", proxy_port),
        target_addr=("127.0.0.1", server_port),
        c2s_config=c2s_cfg,
        s2c_config=s2c_cfg,
        seed=123,
    )
    proxy.start()

    receiver = FileReceiver(
        bind_addr=("127.0.0.1", server_port),
        output_dir=recv_dir,
        timeout=15.0,
        mode="sr",
    )

    results: list[tuple[str, str | None, int]] = []

    def recv_worker():
        results.append(receiver.run_once())

    th = threading.Thread(target=recv_worker, daemon=True)
    th.start()
    time.sleep(0.05)

    sender = FileSender(
        server_addr=("127.0.0.1", proxy_port),
        timeout=15.0,
        mode="sr",
        window_size=16,
        run_dir=run_dir,
    )
    outcome, code = sender.send_file(src_file)

    th.join(timeout=15.0)
    proxy.stop()

    assert outcome == "success"
    assert code == 0

    sr_sender = sender._last_sender
    assert sr_sender is not None

    srtt = sr_sender.rto_estimator.srtt
    assert srtt is not None

    # Expected RTT is 0.200 s (200 ms).
    # Convergence requirement: within +-40% ([0.120 s, 0.280 s]).
    assert 0.200 * 0.60 <= srtt <= 0.200 * 1.40, (
        f"SRTT {srtt:.4f}s outside +-40% of 0.200s ([0.120, 0.280])"
    )

    # Verify rtt.csv was generated and contains valid headers and rows
    rtt_csv = run_dir / "rtt.csv"
    assert rtt_csv.exists()
    lines = rtt_csv.read_text(encoding="utf-8").strip().splitlines()
    assert lines[0] == "t,sample_rtt,srtt,rttvar,rto,backoff"
    assert len(lines) > 1  # At least one sample was logged
