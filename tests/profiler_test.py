from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

import profiler  # noqa: E402

if TYPE_CHECKING:
    from typing import Any


class _LoopProfiler:
    def __init__(self) -> None:
        self.log_calls = 0
        self.snapshot_calls = 0

    def log_diff(self) -> None:
        self.log_calls += 1

    def capture_diff(self) -> list[profiler.MemoryDiff]:
        self.snapshot_calls += 1
        return []


@pytest.mark.asyncio
async def test_run_profiler_loop_handles_cancelled_log_diff(monkeypatch: Any) -> None:
    class _CancelledLogProfiler(_LoopProfiler):
        def log_diff(self) -> None:
            raise asyncio.CancelledError

    profiler_impl = _CancelledLogProfiler()
    sleep_calls: list[int] = []

    async def fake_sleep(_interval: float) -> None:
        sleep_calls.append(1)
        if len(sleep_calls) > 1:
            raise asyncio.CancelledError

    monkeypatch.setattr(profiler.asyncio, "sleep", fake_sleep)

    with pytest.raises(asyncio.CancelledError):
        await profiler._run_profiler_loop(
            profiler_impl,
            0.1,
            log_enabled=True,
            snapshot_enabled=True,
            log_every_n=1,
            snapshot_every_n=1,
        )

    assert sleep_calls == [1]
    assert profiler_impl.snapshot_calls == 0


@pytest.mark.asyncio
async def test_run_profiler_loop_handles_cancelled_capture(monkeypatch: Any) -> None:
    class _CancelledCaptureProfiler(_LoopProfiler):
        def capture_diff(self) -> list[profiler.MemoryDiff]:
            raise asyncio.CancelledError

    profiler_impl = _CancelledCaptureProfiler()
    sleep_calls: list[int] = []

    async def fake_sleep(_interval: float) -> None:
        sleep_calls.append(1)
        if len(sleep_calls) > 1:
            raise asyncio.CancelledError

    monkeypatch.setattr(profiler.asyncio, "sleep", fake_sleep)

    with pytest.raises(asyncio.CancelledError):
        await profiler._run_profiler_loop(
            profiler_impl,
            0.1,
            log_enabled=True,
            snapshot_enabled=True,
            log_every_n=1,
            snapshot_every_n=1,
        )

    assert sleep_calls == [1]
    assert profiler_impl.log_calls == 1


@pytest.mark.asyncio
async def test_run_profiler_loop_multiple_cancellations_preserve_cadence(
    monkeypatch: Any,
) -> None:
    async def run_loop(iterations: int, log_every_n: int, snapshot_every_n: int) -> _LoopProfiler:
        profiler_impl = _LoopProfiler()
        sleep_calls: list[int] = []

        async def fake_sleep(_interval: float) -> None:
            sleep_calls.append(1)
            if len(sleep_calls) > iterations:
                raise asyncio.CancelledError

        monkeypatch.setattr(profiler.asyncio, "sleep", fake_sleep)

        with pytest.raises(asyncio.CancelledError):
            await profiler._run_profiler_loop(
                profiler_impl,
                0.1,
                log_enabled=True,
                snapshot_enabled=True,
                log_every_n=log_every_n,
                snapshot_every_n=snapshot_every_n,
            )

        assert len(sleep_calls) == iterations + 1
        assert profiler_impl.log_calls == iterations // log_every_n
        assert profiler_impl.snapshot_calls == iterations // snapshot_every_n
        return profiler_impl

    await run_loop(iterations=15, log_every_n=3, snapshot_every_n=4)
    await run_loop(iterations=22, log_every_n=2, snapshot_every_n=5)


@pytest.mark.asyncio
async def test_run_profiler_loop_logs_without_snapshots(monkeypatch: Any) -> None:
    profiler_impl = _LoopProfiler()
    sleep_calls: list[int] = []

    async def fake_sleep(_interval: float) -> None:
        sleep_calls.append(1)
        if len(sleep_calls) > 6:
            raise asyncio.CancelledError

    monkeypatch.setattr(profiler.asyncio, "sleep", fake_sleep)

    with pytest.raises(asyncio.CancelledError):
        await profiler._run_profiler_loop(
            profiler_impl,
            0.1,
            log_enabled=True,
            snapshot_enabled=False,
            log_every_n=2,
            snapshot_every_n=1,
        )

    assert len(sleep_calls) == 7
    assert profiler_impl.log_calls == 3
    assert profiler_impl.snapshot_calls == 0
