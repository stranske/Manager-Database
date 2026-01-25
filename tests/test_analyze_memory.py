import datetime as dt
import importlib.util
from pathlib import Path

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
    assert summary.rss_min == 100
    assert summary.rss_max == 200
    assert summary.rss_avg == 150
    assert summary.vms_min == 300
    assert summary.vms_max == 500
    assert summary.vms_avg == 400


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
