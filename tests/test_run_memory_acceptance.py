import datetime as dt
import importlib.util
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts" / "run_memory_acceptance.py"


def _load_runner():
    spec = importlib.util.spec_from_file_location("run_memory_acceptance", SCRIPT_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError("Failed to load run_memory_acceptance module")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


run_memory_acceptance = _load_runner()


def _write_csv(path: Path, rows: list[tuple[str, int, int, int]]) -> None:
    content = ["timestamp,rss_kb,vms_kb,pid"]
    for timestamp, rss, vms, pid in rows:
        content.append(f"{timestamp},{rss},{vms},{pid}")
    path.write_text("\n".join(content) + "\n", encoding="utf-8")


def _build_rows(pid: int) -> list[tuple[str, int, int, int]]:
    base_time = dt.datetime(2024, 1, 2, tzinfo=dt.UTC)
    rows = []
    for hour in range(3):
        timestamp = (base_time + dt.timedelta(hours=hour)).isoformat().replace("+00:00", "Z")
        rows.append((timestamp, 200, 400, pid))
    return rows


def test_run_acceptance_check_filters_pid_and_passes(tmp_path: Path) -> None:
    csv_path = tmp_path / "memory.csv"
    rows = _build_rows(11) + _build_rows(22)
    _write_csv(csv_path, rows)

    log_path = tmp_path / "app.log"
    log_path.write_text("INFO ok\n", encoding="utf-8")

    status = run_memory_acceptance.run_acceptance_check(
        [str(csv_path)],
        pid=11,
        min_hours=1.0,
        warmup_hours=0.0,
        max_slope_kb_per_hour=1.0,
        oom_log_paths=[str(log_path)],
        oom_min_hours=1.0,
    )

    assert status.stable_after_warmup is True
    assert status.oom_check_passed is True
    assert status.acceptance_met is True


def test_run_acceptance_check_requires_samples(tmp_path: Path) -> None:
    csv_path = tmp_path / "memory.csv"
    _write_csv(csv_path, _build_rows(11))

    with pytest.raises(SystemExit, match="No samples found"):
        run_memory_acceptance.run_acceptance_check(
            [str(csv_path)],
            pid=999,
            min_hours=1.0,
            warmup_hours=0.0,
            max_slope_kb_per_hour=1.0,
            oom_log_paths=[],
            oom_min_hours=1.0,
        )


def test_write_report_creates_file(tmp_path: Path) -> None:
    report_path = tmp_path / "report.txt"

    run_memory_acceptance.write_report("ok", str(report_path))

    assert report_path.read_text(encoding="utf-8") == "ok\n"


# Commit-message checklist:
# - [ ] type is accurate (feat, fix, test)
# - [ ] scope is clear (memory)
# - [ ] summary is concise and imperative
