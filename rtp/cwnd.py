"""Congestion control engine: slow start and AIMD for Selective Repeat.

Units: packets, cwnd is a float.
Initial state: cwnd = 1.0, ssthresh = max_window (64).
Slow start: +1 per newly acked packet while cwnd < ssthresh.
Congestion avoidance: +1/cwnd per newly acked packet while cwnd >= ssthresh.
Timeout (once per epoch): ssthresh = max(floor(cwnd / 2), 2), cwnd = 1.0.
Cap at max_window.
"""

from __future__ import annotations

import math

from rtp.config import DEFAULT_MAX_WINDOW, INITIAL_CWND, INITIAL_SSTHRESH


class CongestionControl:
    """Pure class implementing slow start and AIMD congestion control."""

    def __init__(
        self,
        initial_cwnd: float = INITIAL_CWND,
        initial_ssthresh: int = INITIAL_SSTHRESH,
        max_window: int = DEFAULT_MAX_WINDOW,
    ) -> None:
        self._cwnd: float = float(initial_cwnd)
        self._ssthresh: int = int(initial_ssthresh)
        self._max_window: int = int(max_window)

    @property
    def cwnd(self) -> float:
        """Congestion window in packets (float)."""
        return self._cwnd

    @cwnd.setter
    def cwnd(self, val: float) -> None:
        self._cwnd = float(val)

    @property
    def ssthresh(self) -> int:
        """Slow start threshold in packets (int)."""
        return self._ssthresh

    @ssthresh.setter
    def ssthresh(self, val: int) -> None:
        self._ssthresh = int(val)

    @property
    def max_window(self) -> int:
        """Upper cap on congestion window (int)."""
        return self._max_window

    @property
    def usable_window(self) -> int:
        """Usable window: floor(cwnd)."""
        return math.floor(self._cwnd)

    def on_new_ack(self) -> bool:
        """Called once per IN_FLIGHT-to-ACKED transition.

        Never called for duplicate or stale ACKs.

        Returns:
            True if this ACK caused a transition from slow start to congestion
            avoidance (ss_exit), False otherwise.
        """
        was_ss = self._cwnd < self._ssthresh
        if was_ss:
            self._cwnd += 1.0
        else:
            self._cwnd += 1.0 / self._cwnd

        if self._cwnd > self._max_window:
            self._cwnd = float(self._max_window)

        is_ca = self._cwnd >= self._ssthresh
        return was_ss and is_ca

    def on_timeout(self) -> None:
        """Called upon timeout at the loss-epoch gate (at most once per epoch)."""
        self._ssthresh = max(math.floor(self._cwnd / 2), 2)
        self._cwnd = 1.0
