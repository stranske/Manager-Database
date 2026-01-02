"""Rate limiter module for API calls.

This module needs significant work to be production-ready.
"""

from __future__ import annotations


class RateLimiter:
    """Simple rate limiter - needs implementation."""
    
    def __init__(self):
        # TODO: Initialize rate limiter state
        pass
    
    def check(self, key: str) -> bool:
        """Check if request is allowed."""
        # TODO: Implement rate limiting logic
        return True
    
    def record(self, key: str) -> None:
        """Record a request."""
        # TODO: Track request for rate limiting
        pass
