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

    def check(self, key: str) -> bool:
        """Return True when another request is allowed for the key.

        This method does not record a request; it only verifies budget.
        """
        now = time.monotonic()
        window_start = now - self._window_seconds
        events = self._events.get(key)
        if events is None:
            return True
        # Trim timestamps that are outside the rolling window.
        while events and events[0] <= window_start:
            events.popleft()
        return len(events) < self._max_requests

    def record(self, key: str) -> None:
        """Record a request timestamp for the key."""
        now = time.monotonic()
        window_start = now - self._window_seconds
        events = self._events.setdefault(key, deque())
        # Keep the deque tight for the key before adding a new timestamp.
        while events and events[0] <= window_start:
            events.popleft()
        events.append(now)
