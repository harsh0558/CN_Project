"""Adaptive Retransmission Timeout (RTO) estimator (RFC 6298 adapted).

Pure class with no socket or I/O dependencies.
Calculates smoothed round-trip time (SRTT), RTT variation (RTTVAR),
and retransmission timeout (RTO) with exponential backoff on timeouts
and reset on valid sample.
"""

from __future__ import annotations

from rtp.config import (
    INITIAL_RTO,
    MAX_RTO,
    MIN_RTO,
    RTO_ALPHA,
    RTO_BETA,
    RTO_G,
    RTO_K,
)


class RtoEstimator:
    """RFC 6298-adapted RTO estimator for RTP.

    Attributes / Parameters:
        alpha: Weight for SRTT update (default: 1/8 = 0.125).
        beta:  Weight for RTTVAR update (default: 1/4 = 0.25).
        k:     Multiplier for RTTVAR in RTO calculation (default: 4).
        g:     Clock granularity in seconds (default: 1 ms = 0.001 s).
        min_rto: Lower bound for RTO clamp in seconds (default: 200 ms = 0.2 s).
        max_rto: Upper bound for RTO clamp in seconds (default: 60.0 s).
        initial_rto: Initial RTO before any RTT samples (default: 1.0 s).
    """

    def __init__(
        self,
        initial_rto: float = INITIAL_RTO,
        min_rto: float = MIN_RTO,
        max_rto: float = MAX_RTO,
        alpha: float = RTO_ALPHA,
        beta: float = RTO_BETA,
        k: int = RTO_K,
        g: float = RTO_G,
    ) -> None:
        self.min_rto: float = min_rto
        self.max_rto: float = max_rto
        self.alpha: float = alpha
        self.beta: float = beta
        self.k: int = k
        self.g: float = g

        self._srtt: float | None = None
        self._rttvar: float | None = None
        self._rto: float = initial_rto
        self._backoff: int = 1

    @property
    def srtt(self) -> float | None:
        """Smoothed round-trip time (SRTT) in seconds, or None if no samples."""
        return self._srtt

    @property
    def rttvar(self) -> float | None:
        """Round-trip time variation (RTTVAR) in seconds, or None if no samples."""
        return self._rttvar

    @property
    def backoff(self) -> int:
        """Exponential backoff multiplier (1, 2, 4, 8, ...)."""
        return self._backoff

    @property
    def current_rto(self) -> float:
        """Effective RTO in seconds, clamped to [min_rto, max_rto]."""
        effective = self._rto * self._backoff
        return min(max(effective, self.min_rto), self.max_rto)

    def on_sample(self, rtt: float) -> None:
        """Process a new round-trip time measurement.

        Resets exponential backoff to 1.
        Updates SRTT and RTTVAR according to RFC 6298 rules:
        - First sample R:
            SRTT = R
            RTTVAR = R / 2
            RTO = SRTT + max(G, K * RTTVAR)
        - Subsequent samples R:
            RTTVAR = (1 - beta) * RTTVAR + beta * |SRTT - R|
            SRTT = (1 - alpha) * SRTT + alpha * R
            RTO = SRTT + max(G, K * RTTVAR)
        Clamps RTO to [min_rto, max_rto].
        """
        rtt = float(rtt)
        self._backoff = 1

        if self._srtt is None or self._rttvar is None:
            # First RTT sample
            self._srtt = rtt
            self._rttvar = rtt / 2.0
        else:
            # Subsequent RTT samples: RTTVAR updated first, then SRTT
            self._rttvar = (1.0 - self.beta) * self._rttvar + self.beta * abs(self._srtt - rtt)
            self._srtt = (1.0 - self.alpha) * self._srtt + self.alpha * rtt

        raw_rto = self._srtt + max(self.g, self.k * self._rttvar)
        self._rto = min(max(raw_rto, self.min_rto), self.max_rto)

    def on_timeout(self) -> None:
        """Handle a timeout event: double the exponential backoff multiplier."""
        self._backoff *= 2
