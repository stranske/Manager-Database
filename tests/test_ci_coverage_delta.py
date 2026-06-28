from __future__ import annotations

import json
from datetime import UTC, datetime, tzinfo
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import ci_coverage_delta

_ENV_KEYS = (
    "COVERAGE_XML_PATH",
    "OUTPUT_PATH",
    "BASELINE_COVERAGE",
    "ALERT_DROP",
    "FAIL_ON_DROP",
)


@pytest.fixture
def fixed_timestamp(monkeypatch: pytest.MonkeyPatch) -> str:
    timestamp = datetime(2026, 6, 27, 12, 34, 56, 789123, tzinfo=UTC)

    class FixedDateTime:
        @classmethod
        def now(cls, tz: tzinfo | None = None) -> datetime:
            if tz is None:
                return timestamp
            return timestamp.astimezone(tz)

    monkeypatch.setattr(
        ci_coverage_delta,
        "_dt",
        SimpleNamespace(datetime=FixedDateTime, UTC=UTC),
    )
    return "2026-06-27T12:34:56Z"


def _clear_coverage_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in _ENV_KEYS:
        monkeypatch.delenv(key, raising=False)


def _write_coverage_xml(path: Path, line_rate: str) -> Path:
    path.write_text(f'<coverage line-rate="{line_rate}"></coverage>\n', encoding="utf-8")
    return path


def test_extract_line_rate_reads_valid_cobertura_xml(tmp_path: Path) -> None:
    xml_path = _write_coverage_xml(tmp_path / "coverage.xml", "0.876543")

    assert ci_coverage_delta._extract_line_rate(xml_path) == pytest.approx(87.6543)


@pytest.mark.parametrize(
    (
        "current",
        "baseline",
        "alert_drop",
        "fail_on_drop",
        "expected_payload",
        "expected_should_fail",
    ),
    [
        (
            91.23456,
            0.0,
            1.25,
            False,
            {
                "current": 91.2346,
                "baseline": 0.0,
                "delta": 91.2346,
                "drop": 0.0,
                "threshold": 1.25,
                "status": "no-baseline",
                "fail_on_drop": False,
            },
            False,
        ),
        (
            98.75,
            99.0,
            1.0,
            False,
            {
                "current": 98.75,
                "baseline": 99.0,
                "delta": -0.25,
                "drop": 0.25,
                "threshold": 1.0,
                "status": "ok",
                "fail_on_drop": False,
            },
            False,
        ),
        (
            99.87654,
            99.12345,
            1.0,
            False,
            {
                "current": 99.8765,
                "baseline": 99.1235,
                "delta": 0.7531,
                "drop": 0.0,
                "threshold": 1.0,
                "status": "ok",
                "fail_on_drop": False,
            },
            False,
        ),
        (
            94.87654,
            97.12345,
            2.0,
            False,
            {
                "current": 94.8765,
                "baseline": 97.1235,
                "delta": -2.2469,
                "drop": 2.2469,
                "threshold": 2.0,
                "status": "alert",
                "fail_on_drop": False,
            },
            False,
        ),
        (
            94.87654,
            97.12345,
            2.0,
            True,
            {
                "current": 94.8765,
                "baseline": 97.1235,
                "delta": -2.2469,
                "drop": 2.2469,
                "threshold": 2.0,
                "status": "fail",
                "fail_on_drop": True,
            },
            True,
        ),
    ],
)
def test_build_payload_sets_status_and_rounded_values(
    fixed_timestamp: str,
    current: float,
    baseline: float,
    alert_drop: float,
    fail_on_drop: bool,
    expected_payload: dict[str, object],
    expected_should_fail: bool,
) -> None:
    payload, should_fail = ci_coverage_delta._build_payload(
        current,
        baseline,
        alert_drop,
        fail_on_drop=fail_on_drop,
    )

    assert should_fail is expected_should_fail
    assert payload == {"timestamp": fixed_timestamp, **expected_payload}


def test_main_returns_failure_for_missing_coverage_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _clear_coverage_env(monkeypatch)
    missing_xml = tmp_path / "missing.xml"
    output_path = tmp_path / "coverage-delta.json"
    monkeypatch.setenv("COVERAGE_XML_PATH", str(missing_xml))
    monkeypatch.setenv("OUTPUT_PATH", str(output_path))

    assert ci_coverage_delta.main() == 1

    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == f"Coverage XML not found: {missing_xml}\n"
    assert not output_path.exists()


def test_main_writes_json_with_environment_overrides(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    fixed_timestamp: str,
) -> None:
    _clear_coverage_env(monkeypatch)
    xml_path = _write_coverage_xml(tmp_path / "custom-coverage.xml", "0.900006")
    output_path = tmp_path / "custom-delta.json"
    monkeypatch.setenv("COVERAGE_XML_PATH", str(xml_path))
    monkeypatch.setenv("OUTPUT_PATH", str(output_path))
    monkeypatch.setenv("BASELINE_COVERAGE", "92.34567")
    monkeypatch.setenv("ALERT_DROP", "1.5")
    monkeypatch.setenv("FAIL_ON_DROP", "true")

    assert ci_coverage_delta.main() == 1

    captured = capsys.readouterr()
    assert captured.out == f"Coverage delta written to {output_path}\n"
    assert captured.err == ""
    assert json.loads(output_path.read_text(encoding="utf-8")) == {
        "timestamp": fixed_timestamp,
        "current": 90.0006,
        "baseline": 92.3457,
        "delta": -2.3451,
        "drop": 2.3451,
        "threshold": 1.5,
        "status": "fail",
        "fail_on_drop": True,
    }
