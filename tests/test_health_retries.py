import sys
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from api import chat


@pytest.mark.asyncio
async def test_health_retry_backoff_timing(monkeypatch):
    attempts = []
    sleep_calls = []
    clock = {"now": 0.0}

    def _flaky():
        attempts.append("call")
        if len(attempts) < 4:
            raise RuntimeError("flaky dependency")

    def _fake_perf_counter():
        return clock["now"]

    def _fake_sleep(duration):
        sleep_calls.append(duration)
        clock["now"] += duration

    monkeypatch.setattr(chat.time, "perf_counter", _fake_perf_counter)
    monkeypatch.setattr(chat.time, "sleep", _fake_sleep)
    await chat._run_health_check_with_retries(_flaky, 1.0)
    chat._HEALTH_EXECUTOR.shutdown(wait=False, cancel_futures=True)
    assert attempts == ["call", "call", "call", "call"]
    assert sleep_calls == [0.1, 0.2, 0.4]
