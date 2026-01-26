"""Prepare a review-ready memory stability report from monitoring CSV data."""

from __future__ import annotations

import argparse
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
    # Guard rail to avoid mislabeling shorter runs as review-ready windows.
    if window_hours < min_hours:
        raise ValueError(f"Insufficient duration: {window_hours:.2f}h < {min_hours:.2f}h minimum")
    return window_hours


def build_review(
    samples: list[analyze_memory.MemorySample],
    *,
    min_hours: float,
    warmup_hours: float,
    max_slope_kb_per_hour: float,
    pid: int | None,
) -> str:
    if not samples:
        raise ValueError("No memory samples available for review")

    summary = analyze_memory.summarize_samples(samples)
    slope = analyze_memory.rss_slope_kb_per_hour(samples)
    anomalies = analyze_memory.detect_anomalies(samples)
    window_hours = ensure_min_duration(samples, min_hours)
    ordered = sorted(samples, key=lambda sample: sample.timestamp)
    window_start = ordered[0].timestamp.isoformat()
    window_end = ordered[-1].timestamp.isoformat()
    pid_label = "all" if pid is None else str(pid)

    # Evaluate stability after warmup to match the acceptance criteria signal.
    stable, post_warmup_slope, post_warmup_count = analyze_memory.evaluate_stability(
        samples,
        warmup_hours=warmup_hours,
        max_slope_kb_per_hour=max_slope_kb_per_hour,
    )

    lines = [
        "# Memory Leak Fix Review",
        "",
        "## Window",
        f"- pid: {pid_label}",
        f"- window_start: {window_start}",
        f"- window_end: {window_end}",
        f"- window_hours: {window_hours:.2f}",
        "",
        "## Summary",
        f"- samples: {summary.count}",
        f"- rss_kb_min: {summary.rss_min}",
        f"- rss_kb_avg: {summary.rss_avg:.1f}",
        f"- rss_kb_max: {summary.rss_max}",
        f"- vms_kb_min: {summary.vms_min}",
        f"- vms_kb_avg: {summary.vms_avg:.1f}",
        f"- vms_kb_max: {summary.vms_max}",
        f"- rss_slope_kb_per_hour: {slope:.2f}",
        "",
        "## Stability After Warmup",
        f"- warmup_hours: {warmup_hours:.2f}",
        f"- post_warmup_samples: {post_warmup_count}",
        f"- post_warmup_rss_slope_kb_per_hour: {post_warmup_slope:.2f}",
        f"- max_slope_kb_per_hour: {max_slope_kb_per_hour:.2f}",
        f"- stable_after_warmup: {str(stable).lower()}",
        "",
        "## Anomalies",
        f"- anomalies_total: {len(anomalies)}",
    ]
    if anomalies:
        counts: dict[str, int] = {}
        for anomaly in anomalies:
            counts[anomaly.reason] = counts.get(anomaly.reason, 0) + 1
        for reason, count in sorted(counts.items()):
            lines.append(f"- anomalies_{reason}: {count}")

    lines.extend(
        [
            "",
            "## Review Checklist",
            "- [ ] Confirm memory usage stabilizes after warmup.",
            "- [ ] Confirm no OOM errors occur after 48 hours.",
            "- [ ] Review anomaly counts and investigate spikes.",
        ]
    )

    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Prepare a review-ready memory stability report from monitoring CSV data."
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
        help="Minimum hours required to label this review-ready (default: 24).",
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

    report = build_review(
        samples,
        min_hours=args.min_hours,
        warmup_hours=args.warmup_hours,
        max_slope_kb_per_hour=args.max_slope_kb_per_hour,
        pid=args.pid,
    )
    print(report)

    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(report + "\n", encoding="utf-8")
        print(f"review_written: {output_path}")


if __name__ == "__main__":
    main()

# Commit-message checklist:
# - [ ] type is accurate (feat, fix, test)
# - [ ] scope is clear (memory)
# - [ ] summary is concise and imperative
