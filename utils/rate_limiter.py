"""Rate limiter module for API calls.

This module needs significant work to be production-ready.
"""

from __future__ import annotations

import time
from collections import deque


class RateLimiter:
    """Sliding window rate limiter keyed by caller identity.

    The algorithm stores per-key request timestamps and trims any entry that
    falls outside the rolling window before evaluating the current budget.
    """

    _max_requests: int
    _window_seconds: float
    _events: dict[str, deque[float]]

    def __init__(self, max_requests: int, window_seconds: float) -> None:
        """Create a sliding window limiter.

        Each key keeps a deque of monotonic timestamps; on every operation, the
        limiter trims entries older than the rolling window to enforce the cap.
        """
        if max_requests <= 0:
            raise ValueError("max_requests must be positive")
        if window_seconds <= 0:
            raise ValueError("window_seconds must be positive")
        self._max_requests = max_requests
        self._window_seconds = window_seconds
        # Store per-key timestamps to support sliding window checks.
        self._events = {}

    def _prune_events(self, events: deque[float], window_start: float) -> None:
        """Drop timestamps that have aged out of the current window."""
        # Trim timestamps that are outside the rolling window.
        while events and events[0] <= window_start:
            events.popleft()

    def check(self, key: str) -> bool:
        """Return True when another request is allowed for the key.

        This method does not record a request; it only verifies budget by
        trimming the sliding window and comparing the remaining count.
        """
        now: float = time.monotonic()
        window_start: float = now - self._window_seconds
        events: deque[float] | None = self._events.get(key)
        if events is None:
            return True
        self._prune_events(events, window_start)
        return len(events) < self._max_requests

    def record(self, key: str) -> None:
        """Record a request timestamp for the key after pruning old entries."""
        now: float = time.monotonic()
        window_start: float = now - self._window_seconds
        events: deque[float] = self._events.setdefault(key, deque())
        # Keep the deque tight for the key before adding a new timestamp.
        self._prune_events(events, window_start)
        events.append(now)
