import datetime as dt
import importlib.util
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts" / "analyze_memory.py"


def _load_analyze_memory():
    spec = importlib.util.spec_from_file_location("analyze_memory", SCRIPT_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError("Failed to load analyze_memory module")
    module = importlib.util.module_from_spec(spec)
    import sys

    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


analyze_memory = _load_analyze_memory()


def _write_csv(path: Path, rows: list[tuple[str, int, int, int]]) -> None:
    content = ["timestamp,rss_kb,vms_kb,pid"]
    for timestamp, rss, vms, pid in rows:
        content.append(f"{timestamp},{rss},{vms},{pid}")
    path.write_text("\n".join(content) + "\n", encoding="utf-8")


def _build_stabilization_samples(base_time: dt.datetime, *, pid: int = 42) -> list[Any]:
    # Scope: 6h warmup growth, then 24+ hours of stabilized memory with dynamic jitter.
    samples: list[Any] = []
    warmup_hours = 6
    for hour in range(warmup_hours):
        samples.append(
            analyze_memory.MemorySample(
                timestamp=base_time + dt.timedelta(hours=hour),
                rss_kb=1000 + hour * 100,
                vms_kb=3000 + hour * 150,
                pid=pid,
            )
        )

    stable_rss_base = 1600
    stable_vms_base = 3400
    jitter_pattern = [0, 20, -15, 10, -5, 15, -10, 5]
    day_cycle = [12, 18, 8, -6, -12, -4, 6, 14]
    # Deterministic jitter + a small day-cycle keeps the data dynamic while settling.
    for index, hour in enumerate(range(warmup_hours, 32)):
        scale = 1.0 - min(index, 12) * 0.025
        jitter = int(jitter_pattern[index % len(jitter_pattern)] * scale)
        cycle = day_cycle[index % len(day_cycle)]
        rss_value = stable_rss_base + jitter + cycle
        vms_value = stable_vms_base + jitter * 2 + cycle
        samples.append(
            analyze_memory.MemorySample(
                timestamp=base_time + dt.timedelta(hours=hour),
                rss_kb=rss_value,
                vms_kb=vms_value,
                pid=pid,
            )
        )

    return samples


def test_load_samples_parses_csv(tmp_path: Path) -> None:
    csv_path = tmp_path / "memory.csv"
    _write_csv(
        csv_path,
        [
            ("2026-01-25T00:00:00Z", 100, 200, 42),
            ("2026-01-25T00:01:00Z", 110, 210, 42),
        ],
    )

    samples = analyze_memory.load_samples(str(csv_path))

    assert len(samples) == 2
    assert samples[0].rss_kb == 100
    assert samples[0].vms_kb == 200
    assert samples[0].pid == 42
    assert samples[0].timestamp == dt.datetime(2026, 1, 25, 0, 0, tzinfo=dt.UTC)


def test_summarize_samples_calculates_stats() -> None:
    samples = [
        analyze_memory.MemorySample(
            timestamp=dt.datetime(2026, 1, 25, 0, 0, tzinfo=dt.UTC),
            rss_kb=100,
            vms_kb=300,
            pid=1,
        ),
        analyze_memory.MemorySample(
            timestamp=dt.datetime(2026, 1, 25, 1, 0, tzinfo=dt.UTC),
            rss_kb=200,
            vms_kb=500,
            pid=1,
        ),
    ]

    summary = analyze_memory.summarize_samples(samples)

    assert summary.count == 2
    assert summary.duration_s == 3600
    assert summary.observed_duration_s == 3600
    assert summary.sample_interval_s == 3600
    assert summary.gap_count == 0
    assert summary.rss_min == 100
    assert summary.rss_max == 200
    assert summary.rss_avg == 150
    assert summary.vms_min == 300
    assert summary.vms_max == 500
    assert summary.vms_avg == 400


def test_summarize_samples_reports_gaps() -> None:
    base_time = dt.datetime(2026, 1, 25, 0, 0, tzinfo=dt.UTC)
    samples = [
        analyze_memory.MemorySample(
            timestamp=base_time,
            rss_kb=100,
            vms_kb=300,
            pid=1,
        ),
        analyze_memory.MemorySample(
            timestamp=base_time + dt.timedelta(minutes=1),
            rss_kb=120,
            vms_kb=320,
            pid=1,
        ),
        analyze_memory.MemorySample(
            timestamp=base_time + dt.timedelta(hours=2),
            rss_kb=140,
            vms_kb=340,
            pid=1,
        ),
    ]

    summary = analyze_memory.summarize_samples(samples)

    assert summary.duration_s == 7200
    assert summary.observed_duration_s == 60
    assert summary.sample_interval_s == 60
    assert summary.gap_count == 1


def test_rss_slope_kb_per_hour_constant_usage() -> None:
    samples = [
        analyze_memory.MemorySample(
            timestamp=dt.datetime(2026, 1, 25, 0, 0, tzinfo=dt.UTC),
            rss_kb=150,
            vms_kb=300,
            pid=1,
        ),
        analyze_memory.MemorySample(
            timestamp=dt.datetime(2026, 1, 25, 1, 0, tzinfo=dt.UTC),
            rss_kb=150,
            vms_kb=320,
            pid=1,
        ),
        analyze_memory.MemorySample(
            timestamp=dt.datetime(2026, 1, 25, 2, 0, tzinfo=dt.UTC),
            rss_kb=150,
            vms_kb=340,
            pid=1,
        ),
    ]

    slope = analyze_memory.rss_slope_kb_per_hour(samples)

    assert slope == 0


def test_detect_anomalies_flags_spike_and_jump() -> None:
    base_time = dt.datetime(2026, 1, 25, 0, 0, tzinfo=dt.UTC)
    samples = [
        analyze_memory.MemorySample(
            timestamp=base_time,
            rss_kb=1000,
            vms_kb=3000,
            pid=7,
        ),
        analyze_memory.MemorySample(
            timestamp=base_time + dt.timedelta(minutes=5),
            rss_kb=1050,
            vms_kb=3100,
            pid=7,
        ),
        analyze_memory.MemorySample(
            timestamp=base_time + dt.timedelta(minutes=10),
            rss_kb=1100,
            vms_kb=3200,
            pid=7,
        ),
        analyze_memory.MemorySample(
            timestamp=base_time + dt.timedelta(minutes=15),
            rss_kb=6000,
            vms_kb=8000,
            pid=7,
        ),
    ]

    # Use a low sigma to make the spike deterministic for the test.
    anomalies = analyze_memory.detect_anomalies(samples, rss_sigma=1.0, delta_sigma=1.0)

    reasons = {anomaly.reason for anomaly in anomalies}
    assert "rss_spike" in reasons
    assert "rss_jump" in reasons


def test_filter_after_warmup_discards_initial_window() -> None:
    base_time = dt.datetime(2026, 1, 25, 0, 0, tzinfo=dt.UTC)
    samples = [
        analyze_memory.MemorySample(
            timestamp=base_time,
            rss_kb=100,
            vms_kb=300,
            pid=1,
        ),
        analyze_memory.MemorySample(
            timestamp=base_time + dt.timedelta(hours=1),
            rss_kb=150,
            vms_kb=320,
            pid=1,
        ),
        analyze_memory.MemorySample(
            timestamp=base_time + dt.timedelta(hours=2),
            rss_kb=150,
            vms_kb=340,
            pid=1,
        ),
    ]

    # Warmup window should drop the first two samples (0h and 1h).
    filtered = analyze_memory.filter_after_warmup(samples, warmup_hours=1.5)

    assert len(filtered) == 1
    assert filtered[0].timestamp == base_time + dt.timedelta(hours=2)


def test_evaluate_stability_checks_post_warmup_slope() -> None:
    base_time = dt.datetime(2026, 1, 25, 0, 0, tzinfo=dt.UTC)
    samples = [
        analyze_memory.MemorySample(
            timestamp=base_time,
            rss_kb=100,
            vms_kb=300,
            pid=1,
        ),
        analyze_memory.MemorySample(
            timestamp=base_time + dt.timedelta(hours=1),
            rss_kb=200,
            vms_kb=320,
            pid=1,
        ),
        analyze_memory.MemorySample(
            timestamp=base_time + dt.timedelta(hours=2),
            rss_kb=200,
            vms_kb=340,
            pid=1,
        ),
        analyze_memory.MemorySample(
            timestamp=base_time + dt.timedelta(hours=3),
            rss_kb=200,
            vms_kb=360,
            pid=1,
        ),
    ]

    stable, slope, count = analyze_memory.evaluate_stability(
        samples,
        warmup_hours=1.5,
        max_slope_kb_per_hour=5.0,
    )

    assert count == 2
    assert slope == 0.0
    assert stable is True


def test_memory_stabilization_over_24h_variance_below_threshold() -> None:
    base_time = dt.datetime(2026, 1, 25, 0, 0, tzinfo=dt.UTC)
    samples = _build_stabilization_samples(base_time)

    warmup_hours = 6.0
    summary = analyze_memory.summarize_samples(samples)
    monitored_duration_s = summary.duration_s - (warmup_hours * 3600)
    assert monitored_duration_s / 3600 >= 24
    warmup_samples = analyze_memory.filter_after_warmup(samples, warmup_hours=warmup_hours)
    monitored_summary = analyze_memory.summarize_samples(warmup_samples)
    assert monitored_summary.duration_s == monitored_duration_s
    assert monitored_summary.duration_s / 3600 >= 24
    assert monitored_summary.observed_duration_s / 3600 >= 24
    rss_values = [sample.rss_kb for sample in warmup_samples]
    rss_avg = sum(rss_values) / len(rss_values)
    variance_ratio = (max(rss_values) - min(rss_values)) / rss_avg

    # Stabilized data should still exhibit some deterministic movement.
    assert len(set(rss_values)) > 1
    assert variance_ratio <= 0.05


# Commit-message checklist:
# - [ ] type is accurate (feat, fix, test)
# - [ ] scope is clear (memory)
# - [ ] summary is concise and imperative
