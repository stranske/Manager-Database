"""Reject invalid replay receipts without leaving a mutation or claiming success."""

import subprocess
from pathlib import Path

import pytest

from tests import test_manager_dialect_replay_guard as guards

# Reuse the existing isolated caller/archive fixtures without changing historical inputs.
replay = guards.replay
private_archive = guards.private_archive


@pytest.mark.parametrize("phase", ["red", "green"])
@pytest.mark.parametrize(
    "defect",
    [
        "missing-junit",
        "missing-case",
        "duplicate-case",
        "wrong-name",
        "error",
        "skipped",
        "wrong-failure",
        "wrong-exit",
    ],
)
def test_dialect_replay_rejects_invalid_phase_evidence(
    replay, private_archive, monkeypatch, tmp_path, capsys, phase, defect
):
    caller, tree, original, tests = private_archive
    calls = []
    observations = []
    name = replay.CASES[0][0]

    def child(argv, **kwargs):
        observations.append(
            (
                kwargs["cwd"],
                argv[3],
                (caller / replay.SOURCE).read_bytes(),
                (caller / replay.TEST).read_bytes(),
            )
        )
        current_phase = "green" if (tree / replay.SOURCE).read_bytes() == original else "red"
        calls.append(current_phase)
        invalid = current_phase == phase
        exit_code = 1 if current_phase == "red" else 0
        junit = Path(next(arg.split("=", 1)[1] for arg in argv if arg.startswith("--junitxml=")))

        if invalid and defect == "missing-junit":
            return subprocess.CompletedProcess(argv, exit_code)

        case_name = "unrelated_test" if invalid and defect == "wrong-name" else name
        failure = "<failure message='mutation detected'/>" if current_phase == "red" else ""
        if invalid and defect == "wrong-failure":
            failure = "" if current_phase == "red" else "<failure/>"
        if invalid and defect in ("error", "skipped"):
            failure += f"<{defect}/>"
        cases = f'<testcase name="{case_name}">{failure}</testcase>'
        if invalid and defect == "missing-case":
            cases = ""
        if invalid and defect == "duplicate-case":
            cases *= 2
        junit.write_text(f"<testsuites><testsuite>{cases}</testsuite></testsuites>")
        if invalid and defect == "wrong-exit":
            exit_code = 1 - exit_code
        return subprocess.CompletedProcess(argv, exit_code)

    monkeypatch.setattr(replay.subprocess, "run", child)
    output = tmp_path / "proof"
    monkeypatch.setattr(replay.sys, "argv", [str(guards.DRIVER), "--output", str(output)])
    error = FileNotFoundError if defect == "missing-junit" else AssertionError
    with pytest.raises(error):
        replay.main()

    assert calls == (["red"] if phase == "red" else ["red", "green"])
    assert observations == [(tree, f"{replay.TEST}::{name}", original, tests)] * len(calls)
    assert (tree / replay.SOURCE).read_bytes() == original
    assert (tree / replay.TEST).read_bytes() == tests
    assert (caller / replay.SOURCE).read_bytes() == original
    assert (caller / replay.TEST).read_bytes() == tests
    assert not (output / "controls.json").exists()
    assert "PASS:" not in capsys.readouterr().out
