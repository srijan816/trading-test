from __future__ import annotations

import os
import time


def _max_calls() -> int:
    """Read at call time so load_local_env() env vars are used."""
    return int(os.getenv("NEXUS_RATE_LIMIT_CALLS", "20"))


def _window_seconds() -> int:
    return int(os.getenv("NEXUS_RATE_LIMIT_WINDOW_SECONDS", "1800"))


def _cooldown_minutes() -> int:
    return int(os.getenv("NEXUS_COOLDOWN_RESET_MINUTES", "5"))


class NexusRateLimiter:
    """
    Limits total Nexus research calls to avoid hitting OpenRouter rate limits.

    Configurable via environment variables:
      NEXUS_RATE_LIMIT_CALLS          — max calls per window (default 20)
      NEXUS_RATE_LIMIT_WINDOW_SECONDS  — window size in seconds (default 1800 = 30 min)
      NEXUS_COOLDOWN_RESET_MINUTES    — cooldown after error in minutes (default 5)

    Env vars are read at call time (not init time) so that load_local_env()
    can set them before the first research cycle.
    """

    def __init__(self) -> None:
        self.call_timestamps: list[float] = []
        self._cooldown_until: float | None = None  # Unix timestamp when cooldown expires

    def _prune(self) -> None:
        now = time.time()
        self.call_timestamps = [ts for ts in self.call_timestamps if now - ts < _window_seconds()]

    def can_call(self) -> bool:
        self._prune()
        self._check_cooldown()
        return len(self.call_timestamps) < _max_calls()

    def _check_cooldown(self) -> bool:
        """Return True if still in cooldown; reset if cooldown has expired."""
        if self._cooldown_until is None:
            return False
        if time.time() >= self._cooldown_until:
            self._cooldown_until = None
            return False
        return True

    def record_call(self) -> None:
        self._prune()
        self.call_timestamps.append(time.time())
        # A successful call clears any pending cooldown
        self._cooldown_until = None

    def set_cooldown(self) -> None:
        """Put the research assistant into cooldown — resets after _cooldown_minutes."""
        self._cooldown_until = time.time() + (_cooldown_minutes() * 60)
        self.call_timestamps.clear()  # clear rate window too to avoid double-blocking

    def is_in_cooldown(self) -> bool:
        """Return True if in cooldown period, False if available or cooldown has expired."""
        self._check_cooldown()
        return self._cooldown_until is not None

    def cooldown_expires_in(self) -> float | None:
        """Return seconds until cooldown expires, or None if not in cooldown."""
        if self._cooldown_until is None:
            return None
        remaining = self._cooldown_until - time.time()
        return max(0.0, remaining)

    def remaining(self) -> int:
        self._prune()
        return max(0, _max_calls() - len(self.call_timestamps))
