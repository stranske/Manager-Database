"""Invalid mutation plans and interrupted mutant children cannot complete a replay."""

import subprocess
from pathlib import Path

import pytest

from tests import test_manager_dialect_replay_guard as guards

replay = guards.replay
private_archive = guards.private_archive


@pytest.mark.parametrize("case_index", range(6))
@pytest.mark.parametrize("failure", ["launch-error", "interrupt"])
def test_dialect_replay_restores_mutant_source_after_child_interruption(
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
        if name == target and phase == "red":
            if failure == "launch-error":
                raise OSError("Child process could not start")
            raise KeyboardInterrupt
        junit = Path(next(arg.split("=", 1)[1] for arg in argv if arg.startswith("--junitxml=")))
        result = "<failure message='mutation detected'/>" if phase == "red" else ""
        junit.write_text(
            f'<testsuites><testsuite><testcase name="{name}">'
            f"{result}</testcase></testsuite></testsuites>"
        )
        return subprocess.CompletedProcess(argv, 1 if phase == "red" else 0)

    monkeypatch.setattr(replay.subprocess, "run", child)
    output = tmp_path / "proof"
    monkeypatch.setattr(replay.sys, "argv", [str(guards.DRIVER), "--output", str(output)])
    error = OSError if failure == "launch-error" else KeyboardInterrupt
    with pytest.raises(error):
        replay.main()

    assert calls == [
        (case[0], phase) for case in replay.CASES[:case_index] for phase in ("red", "green")
    ] + [(target, "red")]
    assert (tree / replay.SOURCE).read_bytes() == original
    assert (tree / replay.TEST).read_bytes() == tests
    assert (caller / replay.SOURCE).read_bytes() == original
    assert (caller / replay.TEST).read_bytes() == tests
    assert not (output / "controls.json").exists()
    assert not (output / f"{target}-red.json").exists()
    assert "PASS:" not in capsys.readouterr().out


@pytest.mark.parametrize("case_index", range(6))
@pytest.mark.parametrize("defect", ["missing-function", "missing-anchor", "ambiguous-anchor"])
def test_dialect_replay_rejects_invalid_mutation_plan_before_child(
    replay, private_archive, monkeypatch, tmp_path, capsys, case_index, defect
):
    caller, tree, original, tests = private_archive
    name, function, before, after = replay.CASES[case_index]
    if defect == "missing-function":
        function = "_nonexistent_adapter_function"
    elif defect == "missing-anchor":
        before = "# nonexistent mutation anchor"
    else:
        # Every target function has multiple assignments. An unscoped anchor
        # must not select an arbitrary occurrence and launch a purported proof.
        before = "="
    monkeypatch.setattr(replay, "CASES", [(name, function, before, after)])

    def unexpected_child(*args, **kwargs):
        pytest.fail("An invalid mutation plan must fail before starting a child")

    monkeypatch.setattr(replay.subprocess, "run", unexpected_child)
    output = tmp_path / "proof"
    monkeypatch.setattr(replay.sys, "argv", [str(guards.DRIVER), "--output", str(output)])
    error = StopIteration if defect == "missing-function" else AssertionError
    with pytest.raises(error):
        replay.main()

    assert (tree / replay.SOURCE).read_bytes() == original
    assert (tree / replay.TEST).read_bytes() == tests
    assert (caller / replay.SOURCE).read_bytes() == original
    assert (caller / replay.TEST).read_bytes() == tests
    assert list(output.iterdir()) == []
    assert "PASS:" not in capsys.readouterr().out
