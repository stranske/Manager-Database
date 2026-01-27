import datetime as dt
import importlib.util
from pathlib import Path
from typing import Any, TypeAlias

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts" / "verify_memory_acceptance.py"


def _load_verify_memory_acceptance():
    spec = importlib.util.spec_from_file_location("verify_memory_acceptance", SCRIPT_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError("Failed to load verify_memory_acceptance module")
    module = importlib.util.module_from_spec(spec)
    import sys

    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


verify_memory_acceptance = _load_verify_memory_acceptance()
MemorySample: TypeAlias = Any


def _write_csv(path: Path, rows: list[tuple[str, int, int, int]]) -> None:
    content = ["timestamp,rss_kb,vms_kb,pid"]
    for timestamp, rss, vms, pid in rows:
        content.append(f"{timestamp},{rss},{vms},{pid}")
    path.write_text("\n".join(content) + "\n", encoding="utf-8")


def _build_samples(hours: int) -> list[MemorySample]:
    base_time = dt.datetime(2024, 1, 2, tzinfo=dt.UTC)
    return [
        verify_memory_acceptance.analyze_memory.MemorySample(
            timestamp=base_time + dt.timedelta(hours=hour),
            rss_kb=200,
            vms_kb=400,
            pid=11,
        )
        for hour in range(hours + 1)
    ]


def _build_samples_with_gap(hours: list[int]) -> list[MemorySample]:
    base_time = dt.datetime(2024, 1, 2, tzinfo=dt.UTC)
    return [
        verify_memory_acceptance.analyze_memory.MemorySample(
            timestamp=base_time + dt.timedelta(hours=hour),
            rss_kb=200,
            vms_kb=400,
            pid=11,
        )
        for hour in hours
    ]


def test_acceptance_passes_with_stable_50h_and_no_oom(tmp_path: Path) -> None:
    samples = _build_samples(50)
    log_path = tmp_path / "app.log"
    log_path.write_text("INFO ok\n", encoding="utf-8")

    status = verify_memory_acceptance.evaluate_acceptance(
        samples,
        min_hours=24.0,
        warmup_hours=1.0,
        max_slope_kb_per_hour=1.0,
        min_coverage_ratio=0.9,
        oom_log_paths=[log_path],
        oom_min_hours=48.0,
    )

    assert status.stable_after_warmup is True
    assert status.stable_remaining_hours == 0.0
    assert status.oom_check_passed is True
    assert status.oom_remaining_hours == 0.0
    assert status.acceptance_met is True


def test_acceptance_fails_when_window_too_short(tmp_path: Path) -> None:
    samples = _build_samples(20)
    log_path = tmp_path / "app.log"
    log_path.write_text("INFO ok\n", encoding="utf-8")

    status = verify_memory_acceptance.evaluate_acceptance(
        samples,
        min_hours=24.0,
        warmup_hours=1.0,
        max_slope_kb_per_hour=1.0,
        min_coverage_ratio=0.9,
        oom_log_paths=[log_path],
        oom_min_hours=48.0,
    )

    assert status.stable_ready is False
    assert status.stable_remaining_hours > 0.0
    assert status.oom_ready is False
    assert status.oom_remaining_hours > 0.0
    assert status.acceptance_met is False


def test_acceptance_requires_observed_duration(tmp_path: Path) -> None:
    base_time = dt.datetime(2024, 1, 2, tzinfo=dt.UTC)
    hours = [0, 1, 2, 3, 33, 34]
    samples = [
        verify_memory_acceptance.analyze_memory.MemorySample(
            timestamp=base_time + dt.timedelta(hours=hour),
            rss_kb=200,
            vms_kb=400,
            pid=11,
        )
        for hour in hours
    ]
    log_path = tmp_path / "app.log"
    log_path.write_text("INFO ok\n", encoding="utf-8")

    status = verify_memory_acceptance.evaluate_acceptance(
        samples,
        min_hours=24.0,
        warmup_hours=1.0,
        max_slope_kb_per_hour=1.0,
        min_coverage_ratio=0.9,
        oom_log_paths=[log_path],
        oom_min_hours=48.0,
    )

    assert status.window_hours > 24
    assert status.observed_hours < 24
    assert status.stable_ready is False


def test_acceptance_requires_coverage_ratio(tmp_path: Path) -> None:
    hours = [hour for hour in range(0, 41) if hour not in range(10, 22)]
    samples = _build_samples_with_gap(hours)
    log_path = tmp_path / "app.log"
    log_path.write_text("INFO ok\n", encoding="utf-8")

    status = verify_memory_acceptance.evaluate_acceptance(
        samples,
        min_hours=24.0,
        warmup_hours=1.0,
        max_slope_kb_per_hour=1.0,
        min_coverage_ratio=0.9,
        oom_log_paths=[log_path],
        oom_min_hours=48.0,
    )

    assert status.observed_hours > 24
    assert status.coverage_ratio < 0.9
    assert status.coverage_ready is False
    assert status.stable_ready is False


def test_acceptance_fails_with_oom_event(tmp_path: Path) -> None:
    samples = _build_samples(50)
    log_path = tmp_path / "app.log"
    log_path.write_text("ERROR Out of memory\n", encoding="utf-8")

    status = verify_memory_acceptance.evaluate_acceptance(
        samples,
        min_hours=24.0,
        warmup_hours=1.0,
        max_slope_kb_per_hour=1.0,
        min_coverage_ratio=0.9,
        oom_log_paths=[log_path],
        oom_min_hours=48.0,
    )

    assert status.oom_check_passed is False
    assert status.oom_remaining_hours == 0.0
    assert status.acceptance_met is False


def test_acceptance_passes_without_oom_logs() -> None:
    samples = _build_samples(50)

    status = verify_memory_acceptance.evaluate_acceptance(
        samples,
        min_hours=24.0,
        warmup_hours=1.0,
        max_slope_kb_per_hour=1.0,
        min_coverage_ratio=0.9,
        oom_log_paths=[],
        oom_min_hours=48.0,
    )

    assert status.oom_check_passed is True
    assert status.oom_ready is True
    assert status.oom_remaining_hours == 0.0
    assert status.acceptance_met is True


def test_load_samples_from_inputs_merges_files(tmp_path: Path) -> None:
    csv_a = tmp_path / "memory_a.csv"
    csv_b = tmp_path / "memory_b.csv"
    _write_csv(
        csv_a,
        [
            ("2026-01-25T00:00:00Z", 120, 240, 3),
        ],
    )
    _write_csv(
        csv_b,
        [
            ("2026-01-25T01:00:00Z", 130, 260, 3),
        ],
    )

    samples = verify_memory_acceptance.load_samples_from_inputs([str(csv_a), str(csv_b)])

    assert len(samples) == 2
    assert {sample.rss_kb for sample in samples} == {120, 130}


def test_resolve_oom_log_paths_from_dir(tmp_path: Path) -> None:
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    log_a = log_dir / "app.log"
    log_b = log_dir / "notes.txt"
    log_a.write_text("INFO ok\n", encoding="utf-8")
    log_b.write_text("OOM warning\n", encoding="utf-8")

    resolved = verify_memory_acceptance.resolve_oom_log_paths([], [str(log_dir)], "*.log")

    assert [path.name for path in resolved] == ["app.log"]


def test_resolve_oom_log_paths_defaults_when_present(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monitoring_dir = tmp_path / "monitoring"
    monitoring_dir.mkdir()
    log_path = monitoring_dir / "oom_scan.log"
    log_path.write_text("INFO ok\n", encoding="utf-8")

    monkeypatch.chdir(tmp_path)

    resolved = verify_memory_acceptance.resolve_oom_log_paths([], [], "*.log")

    assert len(resolved) == 1
    assert resolved[0].resolve() == log_path.resolve()


# Commit-message checklist:
# - [ ] type is accurate (feat, fix, test)
# - [ ] scope is clear (memory)
# - [ ] summary is concise and imperative
