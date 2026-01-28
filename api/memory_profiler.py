"""Compatibility wrapper for profiler-based memory diagnostics."""

from __future__ import annotations

# Re-export profiler helpers to preserve existing import paths.
from profiler import (  # noqa: F401
    DEFAULT_SCOPE_EXCLUDE,
    DEFAULT_SCOPE_INCLUDE,
    MemoryDiff,
    MemoryLeakProfiler,
    _run_profiler_loop,
    start_background_profiler,
    start_memory_profiler,
    stop_memory_profiler,
)


# Commit-message checklist:
# - [ ] type is accurate (feat, fix, test)
# - [ ] scope is clear (memory)
# - [ ] summary is concise and imperative
