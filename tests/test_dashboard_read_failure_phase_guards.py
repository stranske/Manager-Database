"""A plausible assertion receipt cannot override failed replay phase controls."""

import io
import tarfile
import xml.etree.ElementTree as ET
from contextlib import contextmanager
from pathlib import Path

import pytest

from tests.test_dashboard_read_failure_replay import NAME, replay, report


@pytest.mark.parametrize("phase,outcome", [("red", "failure"), ("green", "")])
@pytest.mark.parametrize("code", [2, 5, -9], ids=["interrupted", "no-tests", "killed"])
def test_replay_rejects_unsuccessful_process_even_with_valid_receipt(
    tmp_path, phase, outcome, code
):
    with pytest.raises(ValueError, match="Wrong phase exit"):
        replay.validate_junit(report(tmp_path, outcome=outcome), NAME, phase, code)


@pytest.mark.parametrize("phase,outcome,code", [("red", "failure", 0), ("green", "", 1)])
def test_replay_rejects_reversed_exit_status(tmp_path, phase, outcome, code):
    with pytest.raises(ValueError, match="Wrong phase exit"):
        replay.validate_junit(report(tmp_path, outcome=outcome), NAME, phase, code)


@pytest.mark.parametrize("phase,outcome,code", [("red", "failure", 1), ("green", "", 0)])
@pytest.mark.parametrize("field", ["tests", "failures", "errors", "skipped"])
def test_replay_rejects_suite_counts_that_disagree_with_case(tmp_path, phase, outcome, code, field):
    path = report(tmp_path, outcome=outcome)
    tree = ET.parse(path)
    suite = tree.getroot().find("testsuite")
    suite.set(field, str(int(suite.get(field)) + 1))
    tree.write(path)

    with pytest.raises(ValueError, match="Wrong suite outcomes"):
        replay.validate_junit(path, NAME, phase, code)


@pytest.mark.parametrize("phase,outcome,code", [("red", "failure", 1), ("green", "", 0)])
def test_replay_rejects_case_outside_counted_suite(tmp_path, phase, outcome, code):
    path = report(tmp_path, outcome=outcome)
    tree = ET.parse(path)
    root = tree.getroot()
    suite = root.find("testsuite")
    case = suite.find("testcase")
    suite.remove(case)
    root.append(case)
    tree.write(path)

    with pytest.raises(ValueError, match="Wrong test identity"):
        replay.validate_junit(path, NAME, phase, code)


@pytest.mark.parametrize("outcome", ["failure", "error", "skipped"])
def test_replay_rejects_outcomes_hidden_outside_green_case(tmp_path, outcome):
    path = report(tmp_path, outcome="")
    tree = ET.parse(path)
    ET.SubElement(tree.getroot(), outcome)
    tree.write(path)

    with pytest.raises(ValueError, match="Error, skip or misplaced outcome"):
        replay.validate_junit(path, NAME, "green", 0)


def test_replay_rejects_red_failure_outside_selected_case(tmp_path):
    path = report(tmp_path)
    tree = ET.parse(path)
    root = tree.getroot()
    case = root.find("testsuite/testcase")
    failure = case.find("failure")
    case.remove(failure)
    root.append(failure)
    tree.write(path)

    with pytest.raises(ValueError, match="Error, skip or misplaced outcome"):
        replay.validate_junit(path, NAME, "red", 1)


def test_replay_refuses_output_reuse_before_reading_or_mutating_production(
    tmp_path, monkeypatch, capsys
):
    root = Path(replay.__file__).resolve().parents[3]
    caller = {name: (root / name).read_bytes() for name in (replay.SOURCE, replay.TEST)}
    output = tmp_path / "proof"
    output.mkdir()
    receipt = output / "controls.json"
    receipt.write_bytes(b"prior evidence must remain unchanged\n")
    before = {path.name: path.read_bytes() for path in output.iterdir()}

    def forbidden(*args, **kwargs):
        pytest.fail("Output reuse must be refused before archive creation or mutation")

    monkeypatch.setattr(replay.subprocess, "check_output", forbidden)
    monkeypatch.setattr(replay.subprocess, "run", forbidden)
    monkeypatch.setattr(replay.tempfile, "TemporaryDirectory", forbidden)
    monkeypatch.setattr(replay.sys, "argv", [replay.__file__, "--output", str(output)])

    with pytest.raises(FileExistsError):
        replay.main()

    assert {path.name: path.read_bytes() for path in output.iterdir()} == before
    assert {name: (root / name).read_bytes() for name in caller} == caller
    assert "PASS:" not in capsys.readouterr().out


def test_replay_refuses_uncommitted_production_before_starting_any_phase(
    tmp_path, monkeypatch, capsys
):
    root = Path(replay.__file__).resolve().parents[3]
    caller = {name: (root / name).read_bytes() for name in (replay.SOURCE, replay.TEST)}
    committed = caller[replay.SOURCE] + b"\n# Different committed production bytes\n"
    archive = io.BytesIO()
    with tarfile.open(fileobj=archive, mode="w") as bundle:
        for name, raw in {**caller, replay.SOURCE: committed}.items():
            member = tarfile.TarInfo(name)
            member.size = len(raw)
            bundle.addfile(member, io.BytesIO(raw))

    def committed_archive(argv, *, timeout):
        assert argv == ["git", "-C", str(root), "archive", "HEAD"]
        assert timeout == 60
        return archive.getvalue()

    private = tmp_path / "private"
    private.mkdir()

    @contextmanager
    def private_tree(*, prefix):
        assert prefix == "dashboard-fallback-proof-"
        try:
            yield str(private)
        finally:
            assert (private / replay.SOURCE).read_bytes() == committed

    def forbidden(*args, **kwargs):
        pytest.fail("Uncommitted production must be refused before pytest runs")

    output = tmp_path / "proof"
    monkeypatch.setattr(replay.subprocess, "check_output", committed_archive)
    monkeypatch.setattr(replay.tempfile, "TemporaryDirectory", private_tree)
    monkeypatch.setattr(replay.subprocess, "run", forbidden)
    monkeypatch.setattr(replay.sys, "argv", [replay.__file__, "--output", str(output)])

    with pytest.raises(ValueError, match="Committed source differs from checkout"):
        replay.main()

    assert list(output.iterdir()) == []
    assert {name: (root / name).read_bytes() for name in caller} == caller
    assert "PASS:" not in capsys.readouterr().out


@pytest.mark.parametrize("phase", ["red", "green"])
@pytest.mark.parametrize("fault", ["timeout", "wrong-exit", "malformed-junit"])
def test_replay_restores_private_production_after_failed_phase(
    tmp_path, monkeypatch, capsys, phase, fault
):
    """An aborted proof must restore production and never publish completed controls."""
    root = Path(replay.__file__).resolve().parents[3]
    caller = {name: (root / name).read_bytes() for name in (replay.SOURCE, replay.TEST)}
    original = caller[replay.SOURCE]
    archive = io.BytesIO()
    with tarfile.open(fileobj=archive, mode="w") as bundle:
        for name, raw in caller.items():
            member = tarfile.TarInfo(name)
            member.size = len(raw)
            bundle.addfile(member, io.BytesIO(raw))

    def committed_archive(argv, *, timeout):
        assert argv == ["git", "-C", str(root), "archive", "HEAD"]
        assert timeout == 60
        return archive.getvalue()

    private = tmp_path / "private"
    private.mkdir()
    source = private / replay.SOURCE

    @contextmanager
    def private_tree(*, prefix):
        assert prefix == "dashboard-fallback-proof-"
        try:
            yield str(private)
        finally:
            # Check before temporary-tree cleanup can hide an unrestored mutant.
            assert source.read_bytes() == original

    output = tmp_path / "proof"
    phases = []

    def run_phase(argv, *, cwd, stdout, stderr, timeout, check):
        assert cwd == private
        assert timeout == 120
        assert check is False
        assert stderr == replay.subprocess.STDOUT
        xml = Path(next(arg.split("=", 1)[1] for arg in argv if arg.startswith("--junitxml=")))
        current_phase = xml.stem.rsplit("-", 1)[1]
        phases.append(current_phase)
        expected = (
            output / f"{replay.SQL[0]}-{'mutant' if current_phase == 'red' else 'original'}.py"
        )
        assert source.read_bytes() == expected.read_bytes()
        assert (source.read_bytes() == original) is (current_phase == "green")
        if current_phase == phase and fault == "timeout":
            raise replay.subprocess.TimeoutExpired(argv, timeout)
        name = argv[argv.index("pytest") + 1].split("::", 1)[1]
        report(tmp_path, name=name, outcome="failure" if current_phase == "red" else "").replace(
            xml
        )
        code = 1 if current_phase == "red" else 0
        if current_phase == phase:
            if fault == "wrong-exit":
                code = 5
            else:
                xml.write_text("<testsuites>")
        stdout.write("controlled phase output\n")
        return replay.subprocess.CompletedProcess(argv, code)

    monkeypatch.setattr(replay.subprocess, "check_output", committed_archive)
    monkeypatch.setattr(replay.tempfile, "TemporaryDirectory", private_tree)
    monkeypatch.setattr(replay.subprocess, "run", run_phase)
    monkeypatch.setattr(replay.sys, "argv", [replay.__file__, "--output", str(output)])
    exception = {
        "timeout": replay.subprocess.TimeoutExpired,
        "wrong-exit": ValueError,
        "malformed-junit": ET.ParseError,
    }[fault]
    with pytest.raises(exception):
        replay.main()

    assert phases == (["red"] if phase == "red" else ["red", "green"])
    assert source.read_bytes() == original
    assert {name: (root / name).read_bytes() for name in caller} == caller
    assert not (output / "controls.json").exists()
    assert "PASS:" not in capsys.readouterr().out
