"""Tests for Phase 7: Congestion Control (Slow Start and AIMD).

Unit tests for CongestionControl:
- slow start +1 per ACK
- transition at ssthresh
- congestion avoidance about +1 per cwnd ACKs
- timeout reduction
- cap at max_window
"""

from __future__ import annotations

import math
import pytest

from rtp.config import DEFAULT_MAX_WINDOW
from rtp.cwnd import CongestionControl


def test_cc_slow_start_plus_one_per_ack():
    """Slow start increases cwnd by exactly 1.0 per newly acked packet."""
    cc = CongestionControl(initial_cwnd=1.0, initial_ssthresh=16, max_window=32)
    assert cc.cwnd == 1.0
    assert cc.usable_window == 1
    assert cc.ssthresh == 16

    for expected in range(2, 10):
        transitioned = cc.on_new_ack()
        assert cc.cwnd == float(expected)
        assert cc.usable_window == expected
        assert transitioned is False


def test_cc_transition_at_ssthresh():
    """Transition from slow start to congestion avoidance occurs when cwnd >= ssthresh."""
    cc = CongestionControl(initial_cwnd=1.0, initial_ssthresh=4, max_window=32)

    # ACK 1: cwnd 1 -> 2 (< 4, no transition)
    assert cc.on_new_ack() is False
    assert cc.cwnd == 2.0

    # ACK 2: cwnd 2 -> 3 (< 4, no transition)
    assert cc.on_new_ack() is False
    assert cc.cwnd == 3.0

    # ACK 3: cwnd 3 -> 4 (== ssthresh 4, transitions to CA)
    assert cc.on_new_ack() is True
    assert cc.cwnd == 4.0

    # ACK 4: already in CA, +1/4 = 4.25
    assert cc.on_new_ack() is False
    assert math.isclose(cc.cwnd, 4.25)


def test_cc_congestion_avoidance_linear_growth():
    """In congestion avoidance, cwnd grows by approximately +1.0 per cwnd ACKs."""
    initial_w = 10.0
    cc = CongestionControl(initial_cwnd=initial_w, initial_ssthresh=10, max_window=64)
    assert cc.cwnd == 10.0

    # Deliver 10 ACKs
    for _ in range(10):
        assert cc.on_new_ack() is False

    # Sum of 1/w for 10 steps from 10.0 is approx 0.956
    assert 10.9 < cc.cwnd < 11.05
    assert cc.usable_window == 10


def test_cc_timeout_reduction():
    """Timeout sets ssthresh = max(floor(cwnd / 2), 2) and cwnd = 1.0."""
    # Case 1: large cwnd
    cc = CongestionControl(initial_cwnd=15.7, initial_ssthresh=32, max_window=64)
    cc.on_timeout()
    assert cc.ssthresh == 7  # floor(15.7 / 2) = 7
    assert cc.cwnd == 1.0
    assert cc.usable_window == 1

    # Case 2: small cwnd clamp to 2
    cc.cwnd = 3.5
    cc.on_timeout()
    assert cc.ssthresh == 2  # max(floor(3.5 / 2), 2) = max(1, 2) = 2
    assert cc.cwnd == 1.0

    # Case 3: cwnd = 1.0
    cc.on_timeout()
    assert cc.ssthresh == 2  # max(0, 2) = 2
    assert cc.cwnd == 1.0


def test_cc_cap_at_max_window():
    """Cwnd is strictly capped at max_window in both slow start and CA."""
    max_w = 5
    cc = CongestionControl(initial_cwnd=1.0, initial_ssthresh=10, max_window=max_w)

    # Slow start up to cap
    for _ in range(10):
        cc.on_new_ack()

    assert cc.cwnd == float(max_w)
    assert cc.usable_window == max_w

    # Additional ACKs in CA do not exceed max_window
    for _ in range(20):
        cc.on_new_ack()

    assert cc.cwnd == float(max_w)
    assert cc.usable_window == max_w
