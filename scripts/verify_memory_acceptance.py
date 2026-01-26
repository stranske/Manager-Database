"""Verify memory stability and OOM-free criteria from monitoring data."""

from __future__ import annotations

import argparse
import importlib.util
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

_ANALYZE_MEMORY_PATH = Path(__file__).resolve().parent / "analyze_memory.py"
_PREPARE_REVIEW_PATH = Path(__file__).resolve().parent / "prepare_memory_review.py"
_DEFAULT_INPUT_PATH = Path("monitoring/memory_usage.csv")
_DEFAULT_OOM_LOG_PATHS = (
    Path("monitoring/oom_scan.log"),
    Path("monitoring/oom.log"),
)


def _load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Failed to load module {name} from {path}")
    module = importlib.util.module_from_spec(spec)
    import sys

    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


analyze_memory = _load_module("analyze_memory", _ANALYZE_MEMORY_PATH)
prepare_memory_review = _load_module("prepare_memory_review", _PREPARE_REVIEW_PATH)


@dataclass(frozen=True)
class AcceptanceStatus:
    window_hours: float
    observed_hours: float
    coverage_ratio: float
    coverage_min_ratio: float
    coverage_ready: bool
    stable_ready: bool
    stable_after_warmup: bool
    stable_remaining_hours: float
    oom_ready: bool | None
    oom_events_total: int
    oom_check_passed: bool | None
    oom_remaining_hours: float | None
    acceptance_met: bool


def evaluate_acceptance(
    samples: list[analyze_memory.MemorySample],
    *,
    min_hours: float,
    warmup_hours: float,
    max_slope_kb_per_hour: float,
    min_coverage_ratio: float,
    oom_log_paths: Sequence[Path],
    oom_min_hours: float,
) -> AcceptanceStatus:
    if not samples:
        raise ValueError("No memory samples available for acceptance verification")

    summary = analyze_memory.summarize_samples(samples)
    window_hours = summary.duration_s / 3600
    observed_hours = summary.observed_duration_s / 3600
    coverage_ratio = (
        summary.observed_duration_s / summary.duration_s if summary.duration_s > 0 else 0.0
    )
    coverage_ready = coverage_ratio >= min_coverage_ratio
    stable, _, _ = analyze_memory.evaluate_stability(
        samples,
        warmup_hours=warmup_hours,
        max_slope_kb_per_hour=max_slope_kb_per_hour,
    )
    stable_ready = observed_hours >= min_hours and coverage_ready
    stable_remaining_hours = max(0.0, min_hours - observed_hours)
    stable_after_warmup = stable_ready and stable

    if oom_log_paths:
        counts = prepare_memory_review.scan_oom_logs(oom_log_paths)
        oom_events_total = sum(counts.values())
        oom_ready = observed_hours >= oom_min_hours and coverage_ready
        oom_check_passed = oom_ready and oom_events_total == 0
        oom_remaining_hours = max(0.0, oom_min_hours - observed_hours)
    else:
        oom_events_total = 0
        oom_ready = None
        oom_check_passed = None
        oom_remaining_hours = None

    acceptance_met = stable_after_warmup and oom_check_passed is True

    return AcceptanceStatus(
        window_hours=window_hours,
        observed_hours=observed_hours,
        coverage_ratio=coverage_ratio,
        coverage_min_ratio=min_coverage_ratio,
        coverage_ready=coverage_ready,
        stable_ready=stable_ready,
        stable_after_warmup=stable_after_warmup,
        stable_remaining_hours=stable_remaining_hours,
        oom_ready=oom_ready,
        oom_events_total=oom_events_total,
        oom_check_passed=oom_check_passed,
        oom_remaining_hours=oom_remaining_hours,
        acceptance_met=acceptance_met,
    )


def render_report(status: AcceptanceStatus) -> str:
    def _format_optional(value: bool | None) -> str:
        if value is None:
            return "skipped"
        return str(value).lower()

    def _format_optional_hours(value: float | None) -> str:
        if value is None:
            return "skipped"
        return f"{value:.2f}"

    return "\n".join(
        [
            "# Memory Acceptance Check",
            f"window_hours: {status.window_hours:.2f}",
            f"observed_hours: {status.observed_hours:.2f}",
            f"coverage_ratio: {status.coverage_ratio:.2f}",
            f"coverage_min_ratio: {status.coverage_min_ratio:.2f}",
            f"coverage_ready: {str(status.coverage_ready).lower()}",
            f"stable_ready_24h: {str(status.stable_ready).lower()}",
            f"stable_after_warmup: {str(status.stable_after_warmup).lower()}",
            f"stable_remaining_hours: {status.stable_remaining_hours:.2f}",
            f"oom_ready_48h: {_format_optional(status.oom_ready)}",
            f"oom_events_total: {status.oom_events_total}",
            f"oom_check_passed: {_format_optional(status.oom_check_passed)}",
            f"oom_remaining_hours: {_format_optional_hours(status.oom_remaining_hours)}",
            f"acceptance_criteria_met: {str(status.acceptance_met).lower()}",
        ]
    )


def resolve_oom_log_paths(
    log_paths: Sequence[str],
    log_dirs: Sequence[str],
    pattern: str,
) -> list[Path]:
    resolved: list[Path] = []
    seen: set[Path] = set()

    def _add(path: Path) -> None:
        key = path.resolve() if path.exists() else path
        if key in seen:
            return
        seen.add(key)
        resolved.append(path)

    for raw_path in log_paths:
        _add(Path(raw_path))

    if not log_paths and not log_dirs:
        for default_path in _DEFAULT_OOM_LOG_PATHS:
            if default_path.exists() and default_path.is_file():
                _add(default_path)
        return resolved

    if log_dirs:
        if not pattern:
            raise ValueError("OOM log pattern cannot be empty.")
        for raw_dir in log_dirs:
            directory = Path(raw_dir).expanduser()
            if not directory.exists():
                raise ValueError(f"OOM log directory does not exist: {directory}")
            if not directory.is_dir():
                raise ValueError(f"OOM log directory is not a directory: {directory}")
            for entry in sorted(directory.glob(pattern)):
                if entry.is_file():
                    _add(entry)

    return resolved


def resolve_input_paths(raw_inputs: Sequence[str]) -> list[Path]:
    if not raw_inputs:
        return [_DEFAULT_INPUT_PATH]
    return [Path(path) for path in raw_inputs]


def load_samples_from_inputs(raw_inputs: Sequence[str]) -> list[analyze_memory.MemorySample]:
    samples: list[analyze_memory.MemorySample] = []
    for path in resolve_input_paths(raw_inputs):
        samples.extend(analyze_memory.load_samples(str(path)))
    return samples


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Verify memory stability and OOM-free acceptance criteria."
    )
    parser.add_argument(
        "--input",
        action="append",
        default=[],
        help=("CSV path to analyze (repeatable, default: monitoring/memory_usage.csv)."),
    )
    parser.add_argument(
        "--pid",
        type=int,
        default=None,
        help="Optional PID to filter on (default: analyze all).",
    )
    parser.add_argument(
        "--min-hours",
        type=float,
        default=24.0,
        help="Minimum hours required for stability check (default: 24).",
    )
    parser.add_argument(
        "--warmup-hours",
        type=float,
        default=1.0,
        help="Warmup hours to exclude before stability checks (default: 1).",
    )
    parser.add_argument(
        "--max-slope-kb-per-hour",
        type=float,
        default=5.0,
        help="Maximum post-warmup RSS slope to treat as stable (default: 5).",
    )
    parser.add_argument(
        "--min-coverage-ratio",
        type=float,
        default=0.9,
        help="Minimum coverage ratio required (default: 0.9).",
    )
    parser.add_argument(
        "--oom-log",
        action="append",
        default=[],
        help=(
            "Log file path to scan for OOM markers (repeatable, default: "
            "monitoring/oom_scan.log or monitoring/oom.log if present)."
        ),
    )
    parser.add_argument(
        "--oom-log-dir",
        action="append",
        default=[],
        help="Directory to scan for OOM logs (repeatable).",
    )
    parser.add_argument(
        "--oom-log-pattern",
        default="*.log*",
        help="Glob pattern for --oom-log-dir entries (default: *.log*).",
    )
    parser.add_argument(
        "--oom-min-hours",
        type=float,
        default=48.0,
        help="Minimum hours required for the OOM-free check (default: 48).",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit non-zero if acceptance criteria are not met.",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    samples = load_samples_from_inputs(args.input)
    samples = analyze_memory.filter_samples(samples, args.pid)
    if not samples:
        raise SystemExit("No samples found for the requested filters")

    oom_log_paths = resolve_oom_log_paths(args.oom_log, args.oom_log_dir, args.oom_log_pattern)

    status = evaluate_acceptance(
        samples,
        min_hours=args.min_hours,
        warmup_hours=args.warmup_hours,
        max_slope_kb_per_hour=args.max_slope_kb_per_hour,
        min_coverage_ratio=args.min_coverage_ratio,
        oom_log_paths=oom_log_paths,
        oom_min_hours=args.oom_min_hours,
    )

    print(render_report(status))

    if args.strict:
        failures = []
        if not status.coverage_ready:
            failures.append("insufficient sample coverage for acceptance")
        if not status.stable_ready:
            failures.append("insufficient duration for 24-hour stability")
        if not status.stable_after_warmup:
            failures.append("memory did not stabilize after warmup")
        if status.oom_ready is None:
            failures.append("no OOM logs provided")
        elif status.oom_ready is False:
            failures.append("insufficient duration for 48-hour OOM scan")
        elif status.oom_check_passed is False:
            failures.append("OOM events detected")
        if failures:
            raise SystemExit("Acceptance check failed: " + "; ".join(failures))


if __name__ == "__main__":
    main()

# Commit-message checklist:
# - [ ] type is accurate (feat, fix, test)
# - [ ] scope is clear (memory)
# - [ ] summary is concise and imperative
