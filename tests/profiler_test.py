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
async def test_run_profiler_loop_multiple_cancellations_over_prolonged_period(
    monkeypatch: Any,
    caplog: pytest.LogCaptureFixture,
) -> None:
    class _CancellationProfiler(_LoopProfiler):
        def __init__(
            self,
            *,
            cancel_on_log_call: int | None = None,
            cancel_on_snapshot_call: int | None = None,
        ) -> None:
            super().__init__()
            self._cancel_on_log_call = cancel_on_log_call
            self._cancel_on_snapshot_call = cancel_on_snapshot_call

        def log_diff(self) -> None:
            self.log_calls += 1
            if self._cancel_on_log_call is not None and self.log_calls >= self._cancel_on_log_call:
                raise asyncio.CancelledError

        def capture_diff(self) -> list[profiler.MemoryDiff]:
            self.snapshot_calls += 1
            if (
                self._cancel_on_snapshot_call is not None
                and self.snapshot_calls >= self._cancel_on_snapshot_call
            ):
                raise asyncio.CancelledError
            return []

    async def run_with_sleep_cancel(
        *, iterations: int, log_every_n: int, snapshot_every_n: int
    ) -> _LoopProfiler:
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

    async def run_with_log_cancel(
        *, cancel_on_log_call: int, log_every_n: int, snapshot_every_n: int
    ) -> _LoopProfiler:
        profiler_impl = _CancellationProfiler(cancel_on_log_call=cancel_on_log_call)
        sleep_calls: list[int] = []

        async def fake_sleep(_interval: float) -> None:
            sleep_calls.append(1)

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

        cancel_iteration = cancel_on_log_call * log_every_n
        assert profiler_impl.log_calls == cancel_on_log_call
        assert profiler_impl.snapshot_calls == cancel_iteration // snapshot_every_n
        assert len(sleep_calls) == cancel_iteration
        return profiler_impl

    async def run_with_snapshot_cancel(
        *, cancel_on_snapshot_call: int, log_every_n: int, snapshot_every_n: int
    ) -> _LoopProfiler:
        profiler_impl = _CancellationProfiler(cancel_on_snapshot_call=cancel_on_snapshot_call)
        sleep_calls: list[int] = []

        async def fake_sleep(_interval: float) -> None:
            sleep_calls.append(1)

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

        cancel_iteration = cancel_on_snapshot_call * snapshot_every_n
        assert profiler_impl.snapshot_calls == cancel_on_snapshot_call
        assert profiler_impl.log_calls == cancel_iteration // log_every_n
        assert len(sleep_calls) == cancel_iteration
        return profiler_impl

    caplog.set_level("INFO")
    await run_with_sleep_cancel(iterations=30, log_every_n=4, snapshot_every_n=6)
    await run_with_log_cancel(cancel_on_log_call=8, log_every_n=2, snapshot_every_n=3)
    await run_with_snapshot_cancel(cancel_on_snapshot_call=5, log_every_n=3, snapshot_every_n=4)

    cancel_messages = [
        record.message
        for record in caplog.records
        if "profiler: loop cancelled during" in record.message
    ]
    assert any("during sleep" in message for message in cancel_messages)
    assert any("during log" in message for message in cancel_messages)
    assert any("during snapshot" in message for message in cancel_messages)


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
