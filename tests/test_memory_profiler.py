from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pytest
from fastapi import FastAPI

from api import memory_profiler

if TYPE_CHECKING:
    from typing import Any


@dataclass(frozen=True)
class _FakeFrame:
    filename: str
    lineno: int


@dataclass(frozen=True)
class _FakeStat:
    size_diff: int
    count_diff: int
    traceback: list[_FakeFrame]


class _FakeSnapshot:
    def __init__(self, stats: list[_FakeStat]) -> None:
        self._stats = stats

    def compare_to(self, _other: object, _key_type: str) -> list[_FakeStat]:
        return self._stats


def test_memory_profiler_filters_and_limits(monkeypatch: Any) -> None:
    stats = [
        _FakeStat(size_diff=256 * 1024, count_diff=3, traceback=[_FakeFrame("a.py", 10)]),
        _FakeStat(size_diff=64 * 1024, count_diff=1, traceback=[_FakeFrame("b.py", 20)]),
        _FakeStat(size_diff=8 * 1024, count_diff=-2, traceback=[_FakeFrame("c.py", 30)]),
    ]
    snapshots = [_FakeSnapshot([]), _FakeSnapshot(stats)]
    monkeypatch.setattr(memory_profiler.tracemalloc, "is_tracing", lambda: True)
    monkeypatch.setattr(
        memory_profiler.tracemalloc,
        "take_snapshot",
        lambda: snapshots.pop(0),
    )

    # Disable default scope filters to focus on limit/min_kb behavior.
    profiler = memory_profiler.MemoryLeakProfiler(
        top_n=2,
        min_kb=16.0,
        frame_limit=5,
        include_patterns=[],
        exclude_patterns=[],
    )
    assert profiler.capture_diff() == []

    diffs = profiler.capture_diff()
    assert len(diffs) == 2
    assert diffs[0].filename == "a.py"
    assert diffs[1].filename == "b.py"
    assert all(diff.size_diff_kb >= 16.0 for diff in diffs)


def test_memory_profiler_scope_filters(monkeypatch: Any) -> None:
    stats = [
        _FakeStat(size_diff=128 * 1024, count_diff=2, traceback=[_FakeFrame("a.py", 10)]),
        _FakeStat(size_diff=128 * 1024, count_diff=2, traceback=[_FakeFrame("b.py", 20)]),
    ]
    snapshots = [_FakeSnapshot([]), _FakeSnapshot(stats)]
    monkeypatch.setattr(memory_profiler.tracemalloc, "is_tracing", lambda: True)
    monkeypatch.setattr(
        memory_profiler.tracemalloc,
        "take_snapshot",
        lambda: snapshots.pop(0),
    )

    profiler = memory_profiler.MemoryLeakProfiler(
        top_n=5,
        min_kb=1.0,
        frame_limit=5,
        include_patterns=["a.py", "b.py"],
        exclude_patterns=["b.py"],
    )
    assert profiler.capture_diff() == []

    diffs = profiler.capture_diff()
    assert len(diffs) == 1
    assert diffs[0].filename == "a.py"


def test_memory_profiler_default_scope(monkeypatch: Any) -> None:
    stats = [
        _FakeStat(
            size_diff=128 * 1024,
            count_diff=2,
            traceback=[_FakeFrame("/srv/app/api/chat.py", 10)],
        ),
        _FakeStat(
            size_diff=128 * 1024,
            count_diff=2,
            traceback=[_FakeFrame("/usr/lib/python3.12/site-packages/foo.py", 20)],
        ),
    ]
    snapshots = [_FakeSnapshot([]), _FakeSnapshot(stats)]
    monkeypatch.setattr(memory_profiler.tracemalloc, "is_tracing", lambda: True)
    monkeypatch.setattr(
        memory_profiler.tracemalloc,
        "take_snapshot",
        lambda: snapshots.pop(0),
    )

    # Defaults should keep focus on repo-owned modules and ignore site-packages.
    profiler = memory_profiler.MemoryLeakProfiler(top_n=5, min_kb=1.0, frame_limit=5)
    assert profiler.capture_diff() == []

    diffs = profiler.capture_diff()
    assert len(diffs) == 1
    assert diffs[0].filename.endswith("api/chat.py")


class _LoopProfiler:
    def __init__(self) -> None:
        self.log_calls = 0
        self.snapshot_calls = 0

    def log_diff(self) -> None:
        self.log_calls += 1

    def capture_diff(self) -> list[memory_profiler.MemoryDiff]:
        self.snapshot_calls += 1
        return []


@pytest.mark.asyncio
async def test_run_profiler_loop_throttles(monkeypatch: Any) -> None:
    profiler = _LoopProfiler()
    sleep_calls: list[int] = []

    async def fake_sleep(_interval: float) -> None:
        sleep_calls.append(1)
        if len(sleep_calls) > 4:
            raise asyncio.CancelledError

    monkeypatch.setattr(memory_profiler.asyncio, "sleep", fake_sleep)

    await memory_profiler._run_profiler_loop(
        profiler,  # type: ignore[arg-type]
        0.1,
        log_enabled=True,
        snapshot_enabled=True,
        log_every_n=2,
        snapshot_every_n=1,
    )

    assert profiler.log_calls == 2
    assert profiler.snapshot_calls == 2


@pytest.mark.asyncio
async def test_run_profiler_loop_skips_when_snapshots_disabled(monkeypatch: Any) -> None:
    profiler = _LoopProfiler()
    sleep_calls: list[int] = []

    async def fake_sleep(_interval: float) -> None:
        sleep_calls.append(1)
        if len(sleep_calls) > 2:
            raise asyncio.CancelledError

    monkeypatch.setattr(memory_profiler.asyncio, "sleep", fake_sleep)

    await memory_profiler._run_profiler_loop(
        profiler,  # type: ignore[arg-type]
        0.1,
        log_enabled=True,
        snapshot_enabled=False,
        log_every_n=1,
        snapshot_every_n=1,
    )

    assert profiler.log_calls == 0
    assert profiler.snapshot_calls == 0


@pytest.mark.asyncio
async def test_start_background_profiler_passes_interval(monkeypatch: Any) -> None:
    app = FastAPI()
    captured: dict[str, float] = {}

    async def fake_run_profiler_loop(
        _profiler: memory_profiler.MemoryLeakProfiler,
        interval_s: float,
        **_kwargs: object,
    ) -> None:
        captured["interval_s"] = interval_s

    real_create_task = memory_profiler.asyncio.create_task

    def passthrough_create_task(coro: object) -> asyncio.Task[object]:
        return real_create_task(coro)  # type: ignore[arg-type]

    monkeypatch.setenv("MEMORY_PROFILE_ENABLED", "true")
    monkeypatch.setattr(memory_profiler, "_run_profiler_loop", fake_run_profiler_loop)
    monkeypatch.setattr(memory_profiler.asyncio, "create_task", passthrough_create_task)

    await memory_profiler.start_background_profiler(app, interval_s=12.5)
    await app.state.memory_profiler_task

    assert captured["interval_s"] == 12.5
    await memory_profiler.stop_memory_profiler(app)


# Commit-message checklist:
# - [ ] type is accurate (feat, fix, test)
# - [ ] scope is clear (memory)
# - [ ] summary is concise and imperative
