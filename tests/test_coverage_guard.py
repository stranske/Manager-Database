"""Unit tests for coverage guard pure helpers (issue #1266)."""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

import pytest

from tools.coverage_guard import (
    BaselineConfig,
    CoverageSnapshot,
    FileCoverage,
    _coverage_value_for_recovery,
    _normalize_labels,
    _recovery_window_satisfied,
    build_recovered_comment,
    build_update_comment,
    compute_top_files,
    load_baseline,
)


def test_load_baseline_uses_defaults_when_file_missing(tmp_path: Path) -> None:
    config = load_baseline(tmp_path / "missing-baseline.json")

    assert config == BaselineConfig(baseline=80.0, warn_drop=1.0, recovery_days=3)


def test_load_baseline_reads_line_and_recovery_days(tmp_path: Path) -> None:
    path = tmp_path / "baseline.json"
    path.write_text(
        json.dumps({"line": 92.5, "warn_drop": 0.5, "recovery_days": 5}),
        encoding="utf-8",
    )

    config = load_baseline(path)

    assert config == BaselineConfig(baseline=92.5, warn_drop=0.5, recovery_days=5)


def test_load_baseline_supports_legacy_keys(tmp_path: Path) -> None:
    path = tmp_path / "legacy-baseline.json"
    path.write_text(
        json.dumps({"coverage": 88.0, "recovery_window": 2, "recovery_runs": 1}),
        encoding="utf-8",
    )

    config = load_baseline(path)

    assert config.baseline == 88.0
    assert config.warn_drop == 1.0
    assert config.recovery_days == 3


def test_compute_top_files_returns_empty_for_invalid_input() -> None:
    assert compute_top_files({}, limit=0) == []
    assert compute_top_files({"files": "not-a-dict"}, limit=5) == []
    assert compute_top_files({"files": {}}, limit=-1) == []


def test_compute_top_files_skips_invalid_summaries_and_zero_statement_rows() -> None:
    coverage_data = {
        "files": {
            123: {"summary": {"percent_covered": 50.0}},
            "bad-summary.py": "not-a-dict",
            "missing-percent.py": {"summary": {"covered_lines": 1, "missing_lines": 1}},
            "invalid-percent.py": {"summary": {"percent_covered": "n/a", "missing_lines": 2}},
            "zero-statements.py": {
                "summary": {
                    "percent_covered": 0.0,
                    "covered_lines": 0,
                    "missing_lines": 0,
                    "num_statements": 0,
                }
            },
            "valid.py": {
                "summary": {
                    "percent_covered": 75.0,
                    "covered_lines": 3,
                    "missing_lines": 1,
                    "num_statements": 4,
                }
            },
        }
    }

    rows = compute_top_files(coverage_data, limit=10)

    assert [row.path for row in rows] == ["valid.py"]
    assert rows[0] == FileCoverage(
        path="valid.py",
        percent=75.0,
        covered=3,
        total=4,
        missing=1,
    )


def test_compute_top_files_sorts_by_missing_lines_and_applies_limit() -> None:
    coverage_data = {
        "files": {
            "b.py": {
                "summary": {
                    "percent_covered": 80.0,
                    "covered_lines": 8,
                    "missing_lines": 2,
                }
            },
            "a.py": {
                "summary": {
                    "percent_covered": 70.0,
                    "covered_lines": 7,
                    "missing_lines": 3,
                }
            },
            "c.py": {
                "summary": {
                    "percent_covered": 90.0,
                    "covered_lines": 9,
                    "missing_lines": 1,
                }
            },
        }
    }

    rows = compute_top_files(coverage_data, limit=2)

    assert [row.path for row in rows] == ["a.py", "b.py"]


def test_compute_top_files_sorts_by_total_when_no_missing_lines() -> None:
    coverage_data = {
        "files": {
            "small.py": {
                "summary": {
                    "percent_covered": 100.0,
                    "covered_lines": 2,
                    "missing_lines": 0,
                }
            },
            "large.py": {
                "summary": {
                    "percent_covered": 100.0,
                    "covered_lines": 10,
                    "missing_lines": 0,
                }
            },
        }
    }

    rows = compute_top_files(coverage_data, limit=5)

    assert [row.path for row in rows] == ["large.py", "small.py"]


def test_build_update_comment_includes_status_table_and_optional_fields() -> None:
    snapshot = CoverageSnapshot(current=78.5, baseline=80.0, delta=-1.5)
    config = BaselineConfig(baseline=80.0, warn_drop=1.0, recovery_days=3)
    top_files = [
        FileCoverage(path="src/a.py", percent=50.0, covered=5, total=10, missing=5),
    ]
    date = dt.date(2026, 6, 27)

    comment = build_update_comment(
        snapshot,
        config,
        below_baseline=True,
        date=date,
        run_url="https://example.com/run/1",
        recovery_progress="1/3 days above baseline",
        top_files=top_files,
    )

    assert comment.splitlines() == [
        "## Coverage guard update",
        "",
        "Date: 2026-06-27",
        "Status: Below baseline",
        "Current coverage: 78.50%",
        "Baseline coverage: 80.00%",
        "Delta vs baseline: -1.50 pts",
        "Warning threshold: 1.00 pts",
        "Recovery window: 3 days",
        "Recovery progress: 1/3 days above baseline",
        "Run: https://example.com/run/1",
        "",
        "### Top changed files",
        "",
        "| File | Coverage | Covered | Missing | Total |",
        "| --- | ---: | ---: | ---: | ---: |",
        "| `src/a.py` | 50.00% | 5 | 5 | 10 |",
    ]


def test_build_update_comment_handles_above_baseline_and_missing_top_files() -> None:
    snapshot = CoverageSnapshot(current=81.0, baseline=80.0, delta=1.0)
    config = BaselineConfig(baseline=80.0, warn_drop=1.0, recovery_days=3)

    comment = build_update_comment(
        snapshot,
        config,
        below_baseline=False,
        date=dt.date(2026, 1, 2),
        run_url="",
        recovery_progress=None,
        top_files=[],
    )

    assert comment.splitlines() == [
        "## Coverage guard update",
        "",
        "Date: 2026-01-02",
        "Status: At or above baseline",
        "Current coverage: 81.00%",
        "Baseline coverage: 80.00%",
        "Delta vs baseline: +1.00 pts",
        "Warning threshold: 1.00 pts",
        "Recovery window: 3 days",
        "",
        "### Top changed files",
        "",
        "Top changed files unavailable.",
    ]


def test_build_recovered_comment_formats_expected_text() -> None:
    snapshot = CoverageSnapshot(current=82.25, baseline=80.0, delta=2.25)
    config = BaselineConfig(baseline=80.0, warn_drop=1.0, recovery_days=4)

    comment = build_recovered_comment(snapshot, config, dt.date(2026, 6, 27))

    assert comment.splitlines() == [
        "Coverage recovered above baseline.",
        "",
        "Date: 2026-06-27",
        "Current coverage: 82.25%",
        "Baseline coverage: 80.00%",
        "Delta vs baseline: +2.25 pts",
        "Recovered for 4 consecutive days.",
        "",
        "Closing this issue.",
    ]


@pytest.mark.parametrize(
    ("labels", "expected"),
    [
        (None, []),
        ([], []),
        ([" coverage ", "coverage", "", "automated", " coverage "], ["coverage", "automated"]),
    ],
)
def test_normalize_labels_preserves_order_and_deduplicates(
    labels: list[str] | None,
    expected: list[str],
) -> None:
    assert _normalize_labels(labels) == expected


def test_recovery_window_satisfied_short_circuits_for_single_sample_window() -> None:
    trend_data = {"current": 70.0, "run_id": "below-baseline"}

    assert _recovery_window_satisfied(trend_data, baseline=80.0, recovery_window=1) is True


def test_recovery_window_satisfied_fails_with_insufficient_history() -> None:
    trend_data = {"current": 85.0, "run_id": "run-2"}

    assert (
        _recovery_window_satisfied(
            trend_data,
            baseline=80.0,
            recovery_window=3,
            history_records=[
                {"current": 84.0, "run_id": "run-1"},
                {"current": 85.0, "run_id": "run-2"},
            ],
        )
        is False
    )


def test_recovery_window_satisfied_fails_when_recent_sample_below_baseline() -> None:
    history_records = [
        {"current": 82.0, "run_id": "run-1"},
        {"current": 79.0, "run_id": "run-2"},
        {"current": 85.0, "run_id": "run-3"},
    ]
    trend_data = {"current": 85.0, "run_id": "run-3"}

    assert (
        _recovery_window_satisfied(
            trend_data, baseline=80.0, recovery_window=3, history_records=history_records
        )
        is False
    )


def test_recovery_window_satisfied_passes_for_consecutive_above_baseline_samples() -> None:
    history_records = [
        {"current": 81.0, "run_id": "run-1"},
        {"current": 82.0, "run_id": "run-2"},
    ]
    trend_data = {"current": 83.0, "run_id": "run-3"}

    assert (
        _recovery_window_satisfied(
            trend_data, baseline=80.0, recovery_window=3, history_records=history_records
        )
        is True
    )


@pytest.mark.parametrize(
    ("record", "expected"),
    [
        ({"current": 81.5}, 81.5),
        ({"line": 79.0, "current": 80.0}, 80.0),
        ({"lines": 77.0, "worst_job_coverage": 76.0}, 77.0),
        ({"worst_job_coverage": 75.5, "avg_coverage": 74.0}, 75.5),
        ({"coverage": 73.0, "percent_covered": 72.0}, 73.0),
        ({"percent_covered": 71.0}, 71.0),
        ({"current": True, "line": 70.0}, 70.0),
        ({"current": "not-a-number", "coverage": 69.0}, 69.0),
        ({}, None),
        ({"current": float("nan")}, None),
    ],
)
def test_coverage_value_for_recovery_uses_supported_keys(
    record: dict, expected: float | None
) -> None:
    assert _coverage_value_for_recovery(record) == expected
