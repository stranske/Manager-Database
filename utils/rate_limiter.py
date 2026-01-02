"""Rate limiter module for API calls.

This module needs significant work to be production-ready.
"""

from __future__ import annotations

import time
from collections import deque


class RateLimiter:
    """Sliding window rate limiter keyed by caller identity."""

    def __init__(self, max_requests: int, window_seconds: float) -> None:
        """Create a sliding window limiter.

        The limiter stores request timestamps per key and trims entries that fall
        outside the rolling window on every check/record.
        """
        if max_requests <= 0:
            raise ValueError("max_requests must be positive")
        if window_seconds <= 0:
            raise ValueError("window_seconds must be positive")
        self._max_requests = max_requests
        self._window_seconds = window_seconds
        self._events: dict[str, deque[float]] = {}

    def check(self, key: str) -> bool:
        """Return True when another request is allowed for the key."""
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
