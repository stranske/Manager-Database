"""Generate a 24-hour memory usage report from monitoring CSV data."""

from __future__ import annotations

import argparse
import datetime as dt
import importlib.util
from pathlib import Path

_ANALYZE_MEMORY_PATH = Path(__file__).resolve().parent / "analyze_memory.py"


def load_analyze_memory():
    """Load analyze_memory without requiring scripts to be a package."""
    spec = importlib.util.spec_from_file_location("analyze_memory", _ANALYZE_MEMORY_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError("Failed to load analyze_memory module")
    module = importlib.util.module_from_spec(spec)
    import sys

    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


analyze_memory = load_analyze_memory()


def ensure_min_duration(samples: list[analyze_memory.MemorySample], min_hours: float) -> float:
    """Ensure the dataset covers at least the requested window."""
    summary = analyze_memory.summarize_samples(samples)
    window_hours = summary.duration_s / 3600
    # Guard rail to avoid mislabeling shorter runs as 24-hour investigations.
    if window_hours < min_hours:
        raise ValueError(
            f"Insufficient duration: {window_hours:.2f}h < {min_hours:.2f}h minimum"
        )
    return window_hours


def count_anomaly_reasons(
    anomalies: list[analyze_memory.MemoryAnomaly],
) -> dict[str, int]:
    counts: dict[str, int] = {}
    for anomaly in anomalies:
        counts[anomaly.reason] = counts.get(anomaly.reason, 0) + 1
    return counts


def build_report(
    samples: list[analyze_memory.MemorySample],
    *,
    min_hours: float,
    pid: int | None,
) -> str:
    if not samples:
        raise ValueError("No memory samples available for report")

    summary = analyze_memory.summarize_samples(samples)
    slope = analyze_memory.rss_slope_kb_per_hour(samples)
    anomalies = analyze_memory.detect_anomalies(samples)
    window_hours = ensure_min_duration(samples, min_hours)
    ordered = sorted(samples, key=lambda sample: sample.timestamp)
    window_start = ordered[0].timestamp.isoformat()
    window_end = ordered[-1].timestamp.isoformat()
    anomaly_counts = count_anomaly_reasons(anomalies)
    pid_label = "all" if pid is None else str(pid)

    lines = [
        f"pid: {pid_label}",
        f"window_start: {window_start}",
        f"window_end: {window_end}",
        f"window_hours: {window_hours:.2f}",
    ]
    lines.extend(analyze_memory.format_summary(summary, slope).splitlines())
    lines.append(f"anomalies_total: {len(anomalies)}")
    for reason, count in sorted(anomaly_counts.items()):
        lines.append(f"anomalies_{reason}: {count}")
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate a 24-hour memory usage report from monitoring CSV data."
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
        help="Minimum hours required to label this a 24-hour report.",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Optional path to write the report (default: stdout only).",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    samples = analyze_memory.load_samples(args.input)
    samples = analyze_memory.filter_samples(samples, args.pid)
    if not samples:
        raise SystemExit("No samples found for the requested filters")

    report = build_report(samples, min_hours=args.min_hours, pid=args.pid)
    print(report)

    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(report + "\n", encoding="utf-8")
        print(f"report_written: {output_path}")


if __name__ == "__main__":
    main()

# Commit-message checklist:
# - [ ] type is accurate (feat, fix, test)
# - [ ] scope is clear (memory)
# - [ ] summary is concise and imperative
