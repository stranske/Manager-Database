"""Verify memory stability and OOM-free criteria from monitoring data."""

from __future__ import annotations

import argparse
import importlib.util
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

_ANALYZE_MEMORY_PATH = Path(__file__).resolve().parent / "analyze_memory.py"
_PREPARE_REVIEW_PATH = Path(__file__).resolve().parent / "prepare_memory_review.py"


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
    stable_ready: bool
    stable_after_warmup: bool
    oom_ready: bool | None
    oom_events_total: int
    oom_check_passed: bool | None
    acceptance_met: bool


def evaluate_acceptance(
    samples: list[analyze_memory.MemorySample],
    *,
    min_hours: float,
    warmup_hours: float,
    max_slope_kb_per_hour: float,
    oom_log_paths: Sequence[Path],
    oom_min_hours: float,
) -> AcceptanceStatus:
    if not samples:
        raise ValueError("No memory samples available for acceptance verification")

    summary = analyze_memory.summarize_samples(samples)
    window_hours = summary.duration_s / 3600
    stable, _, _ = analyze_memory.evaluate_stability(
        samples,
        warmup_hours=warmup_hours,
        max_slope_kb_per_hour=max_slope_kb_per_hour,
    )
    stable_ready = window_hours >= min_hours
    stable_after_warmup = stable_ready and stable

    if oom_log_paths:
        counts = prepare_memory_review.scan_oom_logs(oom_log_paths)
        oom_events_total = sum(counts.values())
        oom_ready = window_hours >= oom_min_hours
        oom_check_passed = oom_ready and oom_events_total == 0
    else:
        oom_events_total = 0
        oom_ready = None
        oom_check_passed = None

    acceptance_met = stable_after_warmup and oom_check_passed is True

    return AcceptanceStatus(
        window_hours=window_hours,
        stable_ready=stable_ready,
        stable_after_warmup=stable_after_warmup,
        oom_ready=oom_ready,
        oom_events_total=oom_events_total,
        oom_check_passed=oom_check_passed,
        acceptance_met=acceptance_met,
    )


def render_report(status: AcceptanceStatus) -> str:
    def _format_optional(value: bool | None) -> str:
        if value is None:
            return "skipped"
        return str(value).lower()

    return "\n".join(
        [
            "# Memory Acceptance Check",
            f"window_hours: {status.window_hours:.2f}",
            f"stable_ready_24h: {str(status.stable_ready).lower()}",
            f"stable_after_warmup: {str(status.stable_after_warmup).lower()}",
            f"oom_ready_48h: {_format_optional(status.oom_ready)}",
            f"oom_events_total: {status.oom_events_total}",
            f"oom_check_passed: {_format_optional(status.oom_check_passed)}",
            f"acceptance_criteria_met: {str(status.acceptance_met).lower()}",
        ]
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Verify memory stability and OOM-free acceptance criteria."
    )
    parser.add_argument(
        "--input",
        default="monitoring/memory_usage.csv",
        help="CSV path to analyze (default: monitoring/memory_usage.csv).",
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
        "--oom-log",
        action="append",
        default=[],
        help="Log file path to scan for OOM markers (repeatable).",
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

    samples = analyze_memory.load_samples(args.input)
    samples = analyze_memory.filter_samples(samples, args.pid)
    if not samples:
        raise SystemExit("No samples found for the requested filters")

    status = evaluate_acceptance(
        samples,
        min_hours=args.min_hours,
        warmup_hours=args.warmup_hours,
        max_slope_kb_per_hour=args.max_slope_kb_per_hour,
        oom_log_paths=[Path(path) for path in args.oom_log],
        oom_min_hours=args.oom_min_hours,
    )

    print(render_report(status))

    if args.strict:
        failures = []
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
