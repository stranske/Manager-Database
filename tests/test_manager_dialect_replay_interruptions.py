"""Interrupted green phases must restore bytes and leave the whole replay incomplete."""

import subprocess
from pathlib import Path

import pytest

from tests import test_manager_dialect_replay_guard as guards

replay = guards.replay
private_archive = guards.private_archive


@pytest.mark.parametrize("case_index", range(6))
@pytest.mark.parametrize("failure", ["timeout", "malformed-junit", "interrupt"])
def test_dialect_replay_rejects_interrupted_green_phase(
    replay, private_archive, monkeypatch, tmp_path, capsys, case_index, failure
):
    caller, tree, original, tests = private_archive
    calls = []
    target = replay.CASES[case_index][0]

    def child(argv, **kwargs):
        name = argv[3].split("::", 1)[1]
        phase = "green" if (tree / replay.SOURCE).read_bytes() == original else "red"
        calls.append((name, phase))
        assert kwargs["cwd"] == tree
        assert (caller / replay.SOURCE).read_bytes() == original
        assert (caller / replay.TEST).read_bytes() == tests
        assert (tree / replay.TEST).read_bytes() == tests
        junit = Path(next(arg.split("=", 1)[1] for arg in argv if arg.startswith("--junitxml=")))
        if name == target and phase == "green":
            if failure == "timeout":
                raise subprocess.TimeoutExpired(argv, 120)
            if failure == "interrupt":
                raise KeyboardInterrupt
            junit.write_text("<broken")
        else:
            result = "<failure message='mutation detected'/>" if phase == "red" else ""
            junit.write_text(
                f'<testsuites><testsuite><testcase name="{name}">'
                f"{result}</testcase></testsuite></testsuites>"
            )
        return subprocess.CompletedProcess(argv, 1 if phase == "red" else 0)

    monkeypatch.setattr(replay.subprocess, "run", child)
    output = tmp_path / "proof"
    monkeypatch.setattr(replay.sys, "argv", [str(guards.DRIVER), "--output", str(output)])
    error = {
        "timeout": subprocess.TimeoutExpired,
        "malformed-junit": guards.ET.ParseError,
        "interrupt": KeyboardInterrupt,
    }[failure]
    with pytest.raises(error):
        replay.main()

    assert calls == [
        (case[0], phase) for case in replay.CASES[: case_index + 1] for phase in ("red", "green")
    ]
    assert (tree / replay.SOURCE).read_bytes() == original
    assert (tree / replay.TEST).read_bytes() == tests
    assert (caller / replay.SOURCE).read_bytes() == original
    assert (caller / replay.TEST).read_bytes() == tests
    assert not (output / "controls.json").exists()
    assert "PASS:" not in capsys.readouterr().out
