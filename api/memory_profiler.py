"""Optional tracemalloc-based profiler to surface memory leak sources."""

from __future__ import annotations

import asyncio
import logging
import os
import tracemalloc
from dataclasses import dataclass
from fnmatch import fnmatch
from typing import Any

from fastapi import FastAPI

logger = logging.getLogger(__name__)


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def _env_csv(name: str) -> list[str]:
    raw = os.getenv(name)
    if not raw:
        return []
    return [item.strip() for item in raw.split(",") if item.strip()]


def _normalize_match_value(value: str) -> str:
    return value.replace("\\", "/")


@dataclass(frozen=True)
class MemoryDiff:
    filename: str
    lineno: int
    size_diff_kb: float
    count_diff: int


class MemoryLeakProfiler:
    """Capture periodic tracemalloc diffs to spotlight allocation growth."""

    def __init__(
        self,
        *,
        top_n: int = 10,
        min_kb: float = 64.0,
        frame_limit: int = 25,
        include_patterns: list[str] | None = None,
        exclude_patterns: list[str] | None = None,
    ) -> None:
        self._top_n = max(1, top_n)
        self._min_kb = max(0.0, min_kb)
        self._frame_limit = max(1, frame_limit)
        self._include_patterns = [
            _normalize_match_value(pattern) for pattern in (include_patterns or []) if pattern
        ]
        self._exclude_patterns = [
            _normalize_match_value(pattern) for pattern in (exclude_patterns or []) if pattern
        ]
        self._previous_snapshot: tracemalloc.Snapshot | None = None

    def _matches_scope(self, filename: str) -> bool:
        normalized = _normalize_match_value(filename)
        if self._include_patterns and not any(
            fnmatch(normalized, pattern) for pattern in self._include_patterns
        ):
            return False
        if self._exclude_patterns and any(
            fnmatch(normalized, pattern) for pattern in self._exclude_patterns
        ):
            return False
        return True

    def capture_diff(self) -> list[MemoryDiff]:
        if not tracemalloc.is_tracing():
            tracemalloc.start(self._frame_limit)
        snapshot = tracemalloc.take_snapshot()
        if self._previous_snapshot is None:
            self._previous_snapshot = snapshot
            return []
        stats = snapshot.compare_to(self._previous_snapshot, "lineno")
        self._previous_snapshot = snapshot
        diffs: list[MemoryDiff] = []
        for stat in stats:
            size_kb = stat.size_diff / 1024.0
            if size_kb < self._min_kb:
                continue
            frame = stat.traceback[0] if stat.traceback else None
            filename = frame.filename if frame is not None else "unknown"
            lineno = frame.lineno if frame is not None else 0
            if not self._matches_scope(filename):
                continue
            diffs.append(
                MemoryDiff(
                    filename=filename,
                    lineno=lineno,
                    size_diff_kb=size_kb,
                    count_diff=stat.count_diff,
                )
            )
            if len(diffs) >= self._top_n:
                break
        return diffs

    def log_diff(self) -> None:
        diffs = self.capture_diff()
        if not diffs:
            logger.info("memory_profiler: no significant allocation deltas detected")
            return
        logger.info("memory_profiler: top allocation deltas")
        for diff in diffs:
            logger.info(
                "memory_profiler: %+0.1f KB (%+d) at %s:%d",
                diff.size_diff_kb,
                diff.count_diff,
                diff.filename,
                diff.lineno,
            )


async def _run_profiler_loop(profiler: MemoryLeakProfiler, interval_s: float) -> None:
    while True:
        await asyncio.sleep(interval_s)
        profiler.log_diff()


async def start_memory_profiler(app: FastAPI) -> None:
    if not _env_bool("MEMORY_PROFILE_ENABLED", False):
        return
    interval_s = max(_env_float("MEMORY_PROFILE_INTERVAL_S", 300.0), 10.0)
    top_n = _env_int("MEMORY_PROFILE_TOP_N", 10)
    min_kb = _env_float("MEMORY_PROFILE_MIN_KB", 64.0)
    frame_limit = _env_int("MEMORY_PROFILE_FRAMES", 25)
    include_patterns = _env_csv("MEMORY_PROFILE_INCLUDE")
    exclude_patterns = _env_csv("MEMORY_PROFILE_EXCLUDE")
    profiler = MemoryLeakProfiler(
        top_n=top_n,
        min_kb=min_kb,
        frame_limit=frame_limit,
        include_patterns=include_patterns,
        exclude_patterns=exclude_patterns,
    )
    task = asyncio.create_task(_run_profiler_loop(profiler, interval_s))
    app.state.memory_profiler = profiler
    app.state.memory_profiler_task = task
    logger.info(
        (
            "memory_profiler: enabled interval=%ss top_n=%d min_kb=%0.1f frames=%d"
            " include=%s exclude=%s"
        ),
        interval_s,
        top_n,
        min_kb,
        frame_limit,
        include_patterns or "-",
        exclude_patterns or "-",
    )


async def stop_memory_profiler(app: FastAPI) -> None:
    task: Any = getattr(app.state, "memory_profiler_task", None)
    if task is None:
        return
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    app.state.memory_profiler_task = None
