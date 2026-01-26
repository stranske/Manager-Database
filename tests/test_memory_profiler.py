from __future__ import annotations

from dataclasses import dataclass

from api import memory_profiler


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


def test_memory_profiler_filters_and_limits(monkeypatch) -> None:
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


def test_memory_profiler_scope_filters(monkeypatch) -> None:
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


def test_memory_profiler_default_scope(monkeypatch) -> None:
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


# Commit-message checklist:
# - [ ] type is accurate (feat, fix, test)
# - [ ] scope is clear (memory)
# - [ ] summary is concise and imperative
