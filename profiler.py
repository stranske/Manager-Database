"""Profiler helpers, including a tracemalloc-based memory leak profiler."""

from __future__ import annotations

import asyncio
import logging
import os
import tracemalloc
from dataclasses import dataclass
from fnmatch import fnmatch
from typing import Any, Protocol

from fastapi import FastAPI

logger = logging.getLogger(__name__)
DEFAULT_SCOPE_INCLUDE = (
    "*/api/*.py",
    "*/etl/*.py",
    "*/adapters/*.py",
    "*/scripts/*.py",
)
DEFAULT_SCOPE_EXCLUDE = (
    "*/site-packages/*",
    "*/dist-packages/*",
    "*/python*/lib/*",
)


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


def _default_scope_patterns() -> tuple[list[str], list[str]]:
    """Return default include/exclude patterns for memory profiling."""
    return list(DEFAULT_SCOPE_INCLUDE), list(DEFAULT_SCOPE_EXCLUDE)


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
        default_includes, default_excludes = _default_scope_patterns()
        if include_patterns is None:
            # Default to repo-focused modules to keep diagnostics targeted.
            include_patterns = default_includes
        if exclude_patterns is None:
            exclude_patterns = default_excludes
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


class Profiler(Protocol):
    def log_diff(self) -> None: ...

    def capture_diff(self) -> list[MemoryDiff]: ...


async def _run_profiler_loop(
    profiler: Profiler,
    interval_s: float,
    *,
    log_enabled: bool = True,
    snapshot_enabled: bool = True,
    log_every_n: int = 1,
    snapshot_every_n: int = 1,
) -> None:
    if interval_s <= 0:
        logger.warning(
            "profiler: interval_s=%s is non-positive; clamping to 0.1s",
            interval_s,
        )
        interval_s = 0.1
    iteration = 0
    log_every_n = max(1, log_every_n)
    snapshot_every_n = max(1, snapshot_every_n)
    while True:
        try:
            await asyncio.sleep(interval_s)
        except asyncio.CancelledError:
            logger.info(
                "profiler: loop cancelled during sleep (iteration=%s, log_every_n=%s, snapshot_every_n=%s)",
                iteration,
                log_every_n,
                snapshot_every_n,
            )
            raise
        iteration += 1
        should_log = log_enabled and iteration % log_every_n == 0
        should_snapshot = snapshot_enabled and iteration % snapshot_every_n == 0
        if should_log:
            try:
                profiler.log_diff()
            except asyncio.CancelledError:
                logger.info(
                    "profiler: loop cancelled during log (iteration=%s, log_every_n=%s, snapshot_every_n=%s)",
                    iteration,
                    log_every_n,
                    snapshot_every_n,
                )
                raise
        if should_snapshot:
            try:
                profiler.capture_diff()
            except asyncio.CancelledError:
                logger.info(
                    "profiler: loop cancelled during snapshot (iteration=%s, log_every_n=%s, snapshot_every_n=%s)",
                    iteration,
                    log_every_n,
                    snapshot_every_n,
                )
                raise


async def start_background_profiler(app: FastAPI, *, interval_s: float | None = None) -> None:
    """Start the background tracemalloc profiler.

    Args:
        interval_s: Optional interval (seconds) between snapshots. If unset, uses
            MEMORY_PROFILE_INTERVAL_S (default 300.0s).
    """
    if not _env_bool("MEMORY_PROFILE_ENABLED", False):
        return
    if interval_s is not None and interval_s <= 0:
        raise ValueError("interval_s must be positive")
    env_interval_s = _env_float("MEMORY_PROFILE_INTERVAL_S", 300.0)
    interval_s = interval_s if interval_s is not None else env_interval_s
    if interval_s <= 0:
        raise ValueError("interval_s must be positive")
    top_n = _env_int("MEMORY_PROFILE_TOP_N", 10)
    min_kb = _env_float("MEMORY_PROFILE_MIN_KB", 64.0)
    frame_limit = _env_int("MEMORY_PROFILE_FRAMES", 25)
    log_enabled = _env_bool("MEMORY_PROFILE_LOG_ENABLED", True)
    snapshot_enabled = _env_bool("MEMORY_PROFILE_SNAPSHOT_ENABLED", True)
    log_every_n = _env_int("MEMORY_PROFILE_LOG_EVERY_N", 1)
    snapshot_every_n = _env_int("MEMORY_PROFILE_SNAPSHOT_EVERY_N", 1)
    include_patterns: list[str] | None = _env_csv("MEMORY_PROFILE_INCLUDE")
    exclude_patterns: list[str] | None = _env_csv("MEMORY_PROFILE_EXCLUDE")
    if not include_patterns:
        # Keep defaults if the env var is unset or empty.
        include_patterns = None
    if not exclude_patterns:
        exclude_patterns = None
    if not snapshot_enabled:
        log_enabled = False
    if not snapshot_enabled and not log_enabled:
        logger.info("memory_profiler: enabled but snapshots/logging disabled")
        return
    profiler = MemoryLeakProfiler(
        top_n=top_n,
        min_kb=min_kb,
        frame_limit=frame_limit,
        include_patterns=include_patterns,
        exclude_patterns=exclude_patterns,
    )
    # Record the effective interval for diagnostics and tests.
    app.state.memory_profiler_interval_s = interval_s
    task = asyncio.create_task(
        _run_profiler_loop(
            profiler,
            interval_s,
            log_enabled=log_enabled,
            snapshot_enabled=snapshot_enabled,
            log_every_n=log_every_n,
            snapshot_every_n=snapshot_every_n,
        )
    )
    app.state.memory_profiler = profiler
    app.state.memory_profiler_task = task
    logger.info(
        (
            "memory_profiler: enabled interval=%ss top_n=%d min_kb=%0.1f frames=%d"
            " include=%s exclude=%s log=%s snapshots=%s log_every_n=%d snapshot_every_n=%d"
        ),
        interval_s,
        top_n,
        min_kb,
        frame_limit,
        include_patterns or "-",
        exclude_patterns or "-",
        "on" if log_enabled else "off",
        "on" if snapshot_enabled else "off",
        max(1, log_every_n),
        max(1, snapshot_every_n),
    )


async def start_memory_profiler(app: FastAPI, *, interval_s: float | None = None) -> None:
    await start_background_profiler(app, interval_s=interval_s)


async def stop_memory_profiler(app: FastAPI) -> None:
    task: Any = getattr(app.state, "memory_profiler_task", None)
    if task is None:
        return
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    app.state.memory_profiler_task = None


# Commit-message checklist:
# - [ ] type is accurate (feat, fix, test)
# - [ ] scope is clear (memory)
# - [ ] summary is concise and imperative
