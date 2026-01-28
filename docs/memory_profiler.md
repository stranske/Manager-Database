# Memory profiler

The memory profiler uses `tracemalloc` to log periodic allocation deltas in
repo-owned modules.

## Configuration

- `MEMORY_PROFILE_ENABLED`: enable/disable the profiler.
- `MEMORY_PROFILE_INTERVAL_S`: default interval in seconds between snapshots
  (default: `300.0`). Must be a positive value.
- `MEMORY_PROFILE_TOP_N`: number of entries to log.
- `MEMORY_PROFILE_MIN_KB`: minimum delta size to log.
- `MEMORY_PROFILE_FRAMES`: frame limit passed to `tracemalloc`.
- `MEMORY_PROFILE_LOG_ENABLED`: toggle logging.
- `MEMORY_PROFILE_SNAPSHOT_ENABLED`: toggle snapshot capture.
- `MEMORY_PROFILE_LOG_EVERY_N`: log cadence (in loop iterations).
- `MEMORY_PROFILE_SNAPSHOT_EVERY_N`: snapshot cadence (in loop iterations).
- `MEMORY_PROFILE_INCLUDE`: CSV of include patterns.
- `MEMORY_PROFILE_EXCLUDE`: CSV of exclude patterns.

## Interval handling

`start_background_profiler()` accepts an optional `interval_s` argument. When
provided, it must be a positive number or a `ValueError` is raised. When
omitted, the profiler uses `MEMORY_PROFILE_INTERVAL_S` (default `300.0`).

The effective interval is stored on the application state as
`app.state.memory_profiler_interval_s` for diagnostics and tests.
