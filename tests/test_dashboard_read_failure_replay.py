"""Reject evidence that does not prove the selected production assertion."""

import importlib.util
from pathlib import Path

import pytest

DRIVER = (
    Path(__file__).resolve().parents[1]
    / "docs/evidence/issue-1750-dashboard-read-failures/dashboard_read_failure_replay.py"
)
SPEC = importlib.util.spec_from_file_location("dashboard_read_failure_replay", DRIVER)
assert SPEC is not None and SPEC.loader is not None
replay = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(replay)
NAME = (
    "test_domain_loader_returns_empty_and_closes_after_query_error[load_active_campaigns_summary]"
)


def report(
    tmp_path,
    *,
    name=NAME,
    classname="tests.test_dashboard_loader_failures",
    outcome="failure",
    message="assert False",
    text=">       assert result.empty\nE       assert False\nAssertionError",
):
    path = tmp_path / "phase.xml"
    counts = {"failure": (1, 0, 0), "error": (0, 1, 0), "skipped": (0, 0, 1), "": (0, 0, 0)}
    failures, errors, skipped = counts[outcome]
    child = f'<{outcome} message="{message}">{text}</{outcome}>' if outcome else ""
    path.write_text(
        f'<testsuites><testsuite tests="1" failures="{failures}" errors="{errors}" skipped="{skipped}">'
        f'<testcase classname="{classname}" name="{name}">{child}</testcase>'
        "</testsuite></testsuites>"
    )
    return path


@pytest.mark.parametrize(
    "change",
    [
        {"name": "another_test"},
        {"classname": "another.module"},
        {"outcome": "skipped"},
        {"outcome": "error"},
        {"text": "assert unexpected == 1"},
    ],
)
def test_replay_rejects_wrong_identity_skip_error_and_unrelated_assertion(tmp_path, change):
    with pytest.raises(ValueError):
        replay.validate_junit(report(tmp_path, **change), NAME, "red", 1)


@pytest.mark.parametrize("phase,outcome,code", [("red", "failure", 1), ("green", "", 0)])
def test_replay_accepts_only_selected_intended_phase(tmp_path, phase, outcome, code):
    replay.validate_junit(report(tmp_path, outcome=outcome), NAME, phase, code)


@pytest.mark.parametrize(
    "change",
    [
        {"message": "RuntimeError: assert result.empty"},
        {"message": "assert unexpected == 1"},
        {"message": ""},
        {"text": "        assert result.empty\n>       assert unexpected == 1\nAssertionError"},
        {"text": ">       raise RuntimeError('assert result.empty')\nRuntimeError"},
        {"text": ""},
    ],
    ids=[
        "runtime-error",
        "different-assertion",
        "missing-message",
        "assertion-only-in-context",
        "assertion-only-in-error-string",
        "missing-traceback",
    ],
)
def test_replay_rejects_red_without_intended_failing_assertion(tmp_path, change):
    with pytest.raises(ValueError, match="intended empty-frame assertion"):
        replay.validate_junit(report(tmp_path, **change), NAME, "red", 1)


def test_replay_archive_creation_has_a_timeout(tmp_path, monkeypatch):
    def stalled_archive(argv, *, timeout):
        assert argv[-2:] == ["archive", "HEAD"]
        assert timeout == 60
        raise replay.subprocess.TimeoutExpired(argv, timeout)

    monkeypatch.setattr(replay.subprocess, "check_output", stalled_archive)
    monkeypatch.setattr(replay.sys, "argv", [str(DRIVER), "--output", str(tmp_path / "proof")])
    with pytest.raises(replay.subprocess.TimeoutExpired):
        replay.main()
