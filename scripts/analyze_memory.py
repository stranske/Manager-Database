"""Analyze memory usage samples collected by monitor_memory.py."""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import statistics
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class MemorySample:
    timestamp: dt.datetime
    rss_kb: int
    vms_kb: int
    pid: int


@dataclass(frozen=True)
class MemorySummary:
    count: int
    duration_s: float
    rss_min: int
    rss_avg: float
    rss_max: int
    vms_min: int
    vms_avg: float
    vms_max: int


@dataclass(frozen=True)
class MemoryAnomaly:
    sample: MemorySample
    reason: str
    delta_kb: int | None = None


def parse_timestamp(value: str) -> dt.datetime:
    """Parse ISO-8601 timestamps produced by the monitor script."""
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return dt.datetime.fromisoformat(value)


def load_samples(path: str) -> list[MemorySample]:
    """Load memory samples from a CSV file."""
    samples: list[MemorySample] = []
    with open(path, newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if not row:
                continue
            samples.append(
                MemorySample(
                    timestamp=parse_timestamp(row["timestamp"]),
                    rss_kb=int(row["rss_kb"]),
                    vms_kb=int(row["vms_kb"]),
                    pid=int(row["pid"]),
                )
            )
    return samples


def filter_samples(samples: list[MemorySample], pid: int | None) -> list[MemorySample]:
    if pid is None:
        return samples
    return [sample for sample in samples if sample.pid == pid]


def summarize_samples(samples: list[MemorySample]) -> MemorySummary:
    if not samples:
        raise ValueError("No samples available to summarize")

    ordered = sorted(samples, key=lambda sample: sample.timestamp)
    rss_values = [sample.rss_kb for sample in ordered]
    vms_values = [sample.vms_kb for sample in ordered]
    duration_s = (ordered[-1].timestamp - ordered[0].timestamp).total_seconds()

    return MemorySummary(
        count=len(ordered),
        duration_s=duration_s,
        rss_min=min(rss_values),
        rss_avg=statistics.fmean(rss_values),
        rss_max=max(rss_values),
        vms_min=min(vms_values),
        vms_avg=statistics.fmean(vms_values),
        vms_max=max(vms_values),
    )


def linear_regression_slope(points: list[tuple[float, float]]) -> float:
    """Return slope for y over x using least squares."""
    if len(points) < 2:
        return 0.0

    xs = [point[0] for point in points]
    ys = [point[1] for point in points]
    mean_x = statistics.fmean(xs)
    mean_y = statistics.fmean(ys)
    numerator = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys, strict=True))
    denominator = sum((x - mean_x) ** 2 for x in xs)
    if denominator == 0:
        return 0.0
    return numerator / denominator


def rss_slope_kb_per_hour(samples: list[MemorySample]) -> float:
    if len(samples) < 2:
        return 0.0
    ordered = sorted(samples, key=lambda sample: sample.timestamp)
    start = ordered[0].timestamp
    points = [((sample.timestamp - start).total_seconds(), sample.rss_kb) for sample in ordered]
    slope_kb_per_sec = linear_regression_slope(points)
    return slope_kb_per_sec * 3600


def detect_anomalies(
    samples: list[MemorySample],
    *,
    rss_sigma: float = 3.0,
    delta_sigma: float = 3.0,
    min_delta_kb: int = 1024,
) -> list[MemoryAnomaly]:
    if len(samples) < 3:
        return []

    ordered = sorted(samples, key=lambda sample: sample.timestamp)
    rss_values = [sample.rss_kb for sample in ordered]
    rss_mean = statistics.fmean(rss_values)
    rss_stdev = statistics.pstdev(rss_values)
    deltas = [curr.rss_kb - prev.rss_kb for prev, curr in zip(ordered, ordered[1:])]
    delta_mean = statistics.fmean(deltas)
    delta_stdev = statistics.pstdev(deltas)

    anomalies: list[MemoryAnomaly] = []

    # Flag extreme RSS spikes that exceed a configurable sigma threshold.
    if rss_stdev > 0:
        rss_threshold = rss_mean + rss_sigma * rss_stdev
        for sample in ordered:
            if sample.rss_kb >= rss_threshold:
                anomalies.append(MemoryAnomaly(sample=sample, reason="rss_spike"))

    # Flag large consecutive jumps that exceed both sigma and absolute thresholds.
    if delta_stdev > 0:
        delta_threshold = delta_mean + delta_sigma * delta_stdev
        for prev, curr in zip(ordered, ordered[1:]):
            delta = curr.rss_kb - prev.rss_kb
            if delta >= min_delta_kb and delta >= delta_threshold:
                anomalies.append(MemoryAnomaly(sample=curr, reason="rss_jump", delta_kb=delta))

    return anomalies


def format_summary(summary: MemorySummary, slope_kb_per_hour: float) -> str:
    duration = int(summary.duration_s)
    return "\n".join(
        [
            f"samples: {summary.count}",
            f"duration_seconds: {duration}",
            ("rss_kb: " f"min={summary.rss_min} avg={summary.rss_avg:.1f} max={summary.rss_max}"),
            ("vms_kb: " f"min={summary.vms_min} avg={summary.vms_avg:.1f} max={summary.vms_max}"),
            f"rss_slope_kb_per_hour: {slope_kb_per_hour:.2f}",
        ]
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Summarize memory usage samples from monitoring CSV data."
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
        "--anomalies",
        action="store_true",
        help="Include detected RSS anomalies in the output.",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    input_path = Path(args.input)
    samples = load_samples(str(input_path))
    samples = filter_samples(samples, args.pid)
    if not samples:
        raise SystemExit("No samples found for the requested filters")

    summary = summarize_samples(samples)
    slope = rss_slope_kb_per_hour(samples)
    print(format_summary(summary, slope))
    if args.anomalies:
        anomalies = detect_anomalies(samples)
        if not anomalies:
            print("anomalies: none")
        else:
            for anomaly in anomalies:
                timestamp = anomaly.sample.timestamp.isoformat()
                details = (
                    f"delta_kb={anomaly.delta_kb}"
                    if anomaly.delta_kb is not None
                    else "delta_kb=n/a"
                )
                print(
                    "anomaly: "
                    f"timestamp={timestamp} rss_kb={anomaly.sample.rss_kb} "
                    f"pid={anomaly.sample.pid} reason={anomaly.reason} {details}"
                )


if __name__ == "__main__":
    main()

# Commit-message checklist:
# - [ ] type is accurate (feat, fix, test)
# - [ ] scope is clear (memory)
# - [ ] summary is concise and imperative
