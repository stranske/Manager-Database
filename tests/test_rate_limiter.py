from __future__ import annotations

from pathlib import Path
import sys

import pytest

# Ensure repository root is on sys.path for direct package imports.
sys.path.append(str(Path(__file__).resolve().parents[1]))

from utils.rate_limiter import RateLimiter
import utils.rate_limiter as rate_limiter


def test_allows_within_limit_and_blocks_after(monkeypatch: pytest.MonkeyPatch) -> None:
    current_time = 0.0

    def fake_monotonic() -> float:
        return current_time

    monkeypatch.setattr(rate_limiter.time, "monotonic", fake_monotonic)

    limiter = RateLimiter(max_requests=2, window_seconds=10)
    assert limiter.check("client-a") is True
    limiter.record("client-a")
    assert limiter.check("client-a") is True
    limiter.record("client-a")
    assert limiter.check("client-a") is False

    current_time = 11.0
    assert limiter.check("client-a") is True


def test_independent_keys_do_not_share_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    current_time = 5.0

    def fake_monotonic() -> float:
        return current_time

    monkeypatch.setattr(rate_limiter.time, "monotonic", fake_monotonic)

    limiter = RateLimiter(max_requests=1, window_seconds=60)
    limiter.record("client-a")
    assert limiter.check("client-a") is False
    assert limiter.check("client-b") is True


def test_check_does_not_record(monkeypatch: pytest.MonkeyPatch) -> None:
    current_time = 1.0

    def fake_monotonic() -> float:
        return current_time

    monkeypatch.setattr(rate_limiter.time, "monotonic", fake_monotonic)

    limiter = RateLimiter(max_requests=1, window_seconds=60)
    assert limiter.check("client-a") is True
    assert limiter.check("client-a") is True
    limiter.record("client-a")
    assert limiter.check("client-a") is False


@pytest.mark.parametrize(
    ("max_requests", "window_seconds"),
    [(0, 1), (-1, 1), (1, 0), (1, -5)],
)
def test_invalid_configuration_raises(max_requests: int, window_seconds: float) -> None:
    with pytest.raises(ValueError):
        RateLimiter(max_requests=max_requests, window_seconds=window_seconds)
