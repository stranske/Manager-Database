import datetime as dt
import importlib.util
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts" / "report_memory_24h.py"


def _load_report_memory():
    spec = importlib.util.spec_from_file_location("report_memory_24h", SCRIPT_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError("Failed to load report_memory_24h module")
    module = importlib.util.module_from_spec(spec)
    import sys

    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


report_memory = _load_report_memory()


def _write_csv(path: Path, rows: list[tuple[str, int, int, int]]) -> None:
    content = ["timestamp,rss_kb,vms_kb,pid"]
    for timestamp, rss, vms, pid in rows:
        content.append(f"{timestamp},{rss},{vms},{pid}")
    path.write_text("\n".join(content) + "\n", encoding="utf-8")


def test_build_report_requires_minimum_duration(tmp_path: Path) -> None:
    csv_path = tmp_path / "memory.csv"
    _write_csv(
        csv_path,
        [
            ("2026-01-25T00:00:00Z", 100, 200, 7),
            ("2026-01-25T03:00:00Z", 120, 220, 7),
        ],
    )

    samples = report_memory.analyze_memory.load_samples(str(csv_path))
    # Guard against labeling short samples as a 24-hour investigation.
    message = "Insufficient duration"
    try:
        report_memory.build_report(samples, min_hours=24.0, pid=7)
    except ValueError as exc:
        assert message in str(exc)
    else:
        raise AssertionError("Expected ValueError for insufficient duration")


def test_build_report_includes_window_and_anomalies(tmp_path: Path) -> None:
    csv_path = tmp_path / "memory.csv"
    _write_csv(
        csv_path,
        [
            ("2026-01-25T00:00:00Z", 500, 1000, 9),
            ("2026-01-25T12:00:00Z", 500, 1100, 9),
            ("2026-01-26T00:00:00Z", 500, 1200, 9),
        ],
    )

    samples = report_memory.analyze_memory.load_samples(str(csv_path))
    report = report_memory.build_report(samples, min_hours=24.0, pid=9)

    assert "window_hours: 24.00" in report
    assert "rss_slope_kb_per_hour: 0.00" in report
    assert "anomalies_total: 0" in report


# Commit-message checklist:
# - [ ] type is accurate (feat, fix, test)
# - [ ] scope is clear (memory)
# - [ ] summary is concise and imperative
