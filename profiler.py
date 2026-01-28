"""Lightweight profiler loop helpers.

This module mirrors the cancellation behavior required by the acceptance tests.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Protocol

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class MemoryDiff:
    filename: str
    lineno: int
    size_diff_kb: float
    count_diff: int


class Profiler(Protocol):
    def log_diff(self) -> None:
        ...

    def capture_diff(self) -> list[MemoryDiff]:
        ...


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
            logger.info("profiler: loop cancelled during sleep")
            raise
        if not snapshot_enabled:
            continue
        iteration += 1
        should_log = log_enabled and iteration % log_every_n == 0
        should_snapshot = iteration % snapshot_every_n == 0
        if should_log:
            try:
                profiler.log_diff()
            except asyncio.CancelledError:
                logger.info("profiler: loop cancelled during log")
                raise
        if should_snapshot:
            try:
                profiler.capture_diff()
            except asyncio.CancelledError:
                logger.info("profiler: loop cancelled during snapshot")
                raise
