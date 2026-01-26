"""Run memory monitoring and verify acceptance criteria in one pass."""

from __future__ import annotations

import argparse
import importlib.util
from pathlib import Path

_COLLECT_PATH = Path(__file__).resolve().parent / "collect_memory_24h.py"
_VERIFY_PATH = Path(__file__).resolve().parent / "verify_memory_acceptance.py"


def _load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Failed to load module {name} from {path}")
    module = importlib.util.module_from_spec(spec)
    import sys

    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


collect_memory = _load_module("collect_memory_24h", _COLLECT_PATH)
verify_memory_acceptance = _load_module("verify_memory_acceptance", _VERIFY_PATH)


def run_monitoring(
    *,
    pid: int,
    interval_s: int,
    duration_hours: float,
    output: str | None,
    output_dir: str,
    max_samples: int | None,
) -> str:
    output_path = collect_memory.resolve_output_path(
        output=output,
        output_dir=output_dir,
        now=collect_memory.dt.datetime.utcnow(),
    )
    collect_memory.run_collection(
        pid=pid,
        interval_s=interval_s,
        duration_hours=duration_hours,
        output_path=output_path,
        max_samples=max_samples,
    )
    return output_path


def run_acceptance_check(
    sample_paths: list[str],
    *,
    pid: int | None,
    min_hours: float,
    warmup_hours: float,
    max_slope_kb_per_hour: float,
    oom_log_paths: list[str],
    oom_log_dirs: list[str],
    oom_log_pattern: str,
    oom_min_hours: float,
) -> verify_memory_acceptance.AcceptanceStatus:
    samples = verify_memory_acceptance.load_samples_from_inputs(sample_paths)
    samples = verify_memory_acceptance.analyze_memory.filter_samples(samples, pid)
    if not samples:
        raise SystemExit("No samples found for the requested filters")

    resolved_oom_logs = verify_memory_acceptance.resolve_oom_log_paths(
        oom_log_paths, oom_log_dirs, oom_log_pattern
    )

    return verify_memory_acceptance.evaluate_acceptance(
        samples,
        min_hours=min_hours,
        warmup_hours=warmup_hours,
        max_slope_kb_per_hour=max_slope_kb_per_hour,
        oom_log_paths=resolved_oom_logs,
        oom_min_hours=oom_min_hours,
    )


def write_report(report: str, output_path: str | None) -> None:
    if not output_path:
        return
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(report + "\n", encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Collect memory samples and verify acceptance criteria in one run."
    )
    parser.add_argument(
        "--pid",
        type=int,
        default=None,
        help="Process ID to monitor (default: resolve from --process-name).",
    )
    parser.add_argument(
        "--process-name",
        default=None,
        help="Substring of the process command line to monitor.",
    )
    parser.add_argument(
        "--interval-seconds",
        type=int,
        default=60,
        help="Sampling interval in seconds (default: 60).",
    )
    parser.add_argument(
        "--duration-hours",
        type=float,
        default=24.0,
        help="Total sampling duration in hours (default: 24).",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Explicit output CSV path (default: timestamped file).",
    )
    parser.add_argument(
        "--output-dir",
        default="monitoring",
        help="Directory for timestamped outputs (default: monitoring).",
    )
    parser.add_argument(
        "--max-samples",
        type=int,
        default=None,
        help="Stop after this many samples (default: unlimited).",
    )
    parser.add_argument(
        "--skip-collection",
        action="store_true",
        help="Skip collection and only run the acceptance check.",
    )
    parser.add_argument(
        "--input",
        action="append",
        default=[],
        help=("CSV path to analyze (repeatable, default: monitoring/memory_usage.csv)."),
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
        "--report-output",
        default=None,
        help="Optional path to write the acceptance report.",
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

    collected_path = None
    pid = args.pid

    if args.skip_collection:
        if args.process_name and args.pid is None:
            raise SystemExit("Provide --pid when using --process-name with --skip-collection")
    else:
        pid = collect_memory.resolve_pid(args.pid, args.process_name)
        collected_path = run_monitoring(
            pid=pid,
            interval_s=args.interval_seconds,
            duration_hours=args.duration_hours,
            output=args.output,
            output_dir=args.output_dir,
            max_samples=args.max_samples,
        )

    sample_paths = list(args.input)
    if not sample_paths and collected_path is not None:
        sample_paths = [collected_path]

    status = run_acceptance_check(
        sample_paths,
        pid=pid,
        min_hours=args.min_hours,
        warmup_hours=args.warmup_hours,
        max_slope_kb_per_hour=args.max_slope_kb_per_hour,
        oom_log_paths=list(args.oom_log),
        oom_log_dirs=list(args.oom_log_dir),
        oom_log_pattern=args.oom_log_pattern,
        oom_min_hours=args.oom_min_hours,
    )

    report = verify_memory_acceptance.render_report(status)
    print(report)
    write_report(report, args.report_output)

    if args.strict and not status.acceptance_met:
        raise SystemExit("Acceptance criteria not met")


if __name__ == "__main__":
    main()

# Commit-message checklist:
# - [ ] type is accurate (feat, fix, test)
# - [ ] scope is clear (memory)
# - [ ] summary is concise and imperative
