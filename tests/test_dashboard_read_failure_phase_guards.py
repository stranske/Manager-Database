"""A plausible assertion receipt cannot override failed replay phase controls."""

import io
import json
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


@pytest.mark.parametrize(
    "phase,fault",
    [
        (phase, fault)
        for phase in ("red", "green")
        for fault in (
            "timeout",
            "spawn-error",
            "interrupted",
            "wrong-exit",
            "malformed-junit",
            "missing-junit",
            "wrong-identity",
            "skipped-junit",
            "duplicate-case",
            "teardown-error",
            "receipt-write-error",
            "partial-source-write",
            "silent-source-write",
        )
    ]
    + [("red", "wrong-assertion")],
)
@pytest.mark.parametrize("loader", [replay.SQL[0], replay.DOMAIN[-1]], ids=["first", "last"])
def test_replay_restores_private_production_after_failed_phase(
    tmp_path, monkeypatch, capsys, phase, fault, loader
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
    source_writes = []

    write_bytes = Path.write_bytes

    def write_source(path, data):
        if path == source:
            source_writes.append(data)
            # Each pair writes RED, restores, writes GREEN, then restores again.
            target_write = 4 * (replay.SQL + replay.DOMAIN).index(loader)
            target_write += 2 * (phase == "green")
            if (
                fault in {"partial-source-write", "silent-source-write"}
                and len(source_writes) == target_write + 1
            ):
                partial = data[: len(data) // 2]
                written = write_bytes(path, partial)
                assert path.read_bytes() == partial != original
                if fault == "silent-source-write":
                    return written
                raise OSError("Private production write interrupted")
        return write_bytes(path, data)

    write_text = Path.write_text

    def write_receipt(path, data, *args, **kwargs):
        if fault == "receipt-write-error" and path == output / f"{loader}-{phase}.json":
            raise OSError("Phase receipt could not be persisted")
        return write_text(path, data, *args, **kwargs)

    def run_phase(argv, *, cwd, stdout, stderr, timeout, check):
        assert cwd == private
        assert timeout == 120
        assert check is False
        assert stderr == replay.subprocess.STDOUT
        xml = Path(next(arg.split("=", 1)[1] for arg in argv if arg.startswith("--junitxml=")))
        current_loader, current_phase = xml.stem.rsplit("-", 1)
        phases.append((current_loader, current_phase))
        expected = (
            output / f"{current_loader}-{'mutant' if current_phase == 'red' else 'original'}.py"
        )
        assert source.read_bytes() == expected.read_bytes()
        assert (source.read_bytes() == original) is (current_phase == "green")
        inject_fault = current_loader == loader and current_phase == phase
        if inject_fault and fault == "timeout":
            raise replay.subprocess.TimeoutExpired(argv, timeout)
        if inject_fault and fault == "spawn-error":
            raise OSError("Pytest process could not start")
        if inject_fault and fault == "interrupted":
            raise KeyboardInterrupt("Replay interrupted during pytest")
        name = argv[argv.index("pytest") + 1].split("::", 1)[1]
        receipt_options = {}
        if inject_fault:
            if fault == "wrong-identity":
                receipt_options["classname"] = "tests.another_module"
            elif fault == "wrong-assertion":
                receipt_options["text"] = ">       assert unrelated == 1\nE       assert False"
        if not inject_fault or fault != "missing-junit":
            outcome = "failure" if current_phase == "red" else ""
            if inject_fault and fault == "skipped-junit":
                outcome = "skipped"
            report(
                tmp_path,
                name=name,
                outcome=outcome,
                **receipt_options,
            ).replace(xml)
            if inject_fault and fault == "duplicate-case":
                tree = ET.parse(xml)
                suite = tree.getroot().find("testsuite")
                case = suite.find("testcase")
                suite.append(ET.fromstring(ET.tostring(case)))
                tree.write(xml)
            if inject_fault and fault == "teardown-error":
                # Pytest can produce the expected assertion result and also fail teardown.
                tree = ET.parse(xml)
                suite = tree.getroot().find("testsuite")
                suite.set("errors", "1")
                case = suite.find("testcase")
                ET.SubElement(case, "error", message="failed on teardown").text = (
                    "RuntimeError: database cleanup failed"
                )
                tree.write(xml)
        code = 1 if current_phase == "red" else 0
        if inject_fault:
            if fault == "wrong-exit":
                code = 5
            elif fault == "malformed-junit":
                xml.write_text("<testsuites>")
        stdout.write("controlled phase output\n")
        return replay.subprocess.CompletedProcess(argv, code)

    monkeypatch.setattr(replay.subprocess, "check_output", committed_archive)
    monkeypatch.setattr(replay.tempfile, "TemporaryDirectory", private_tree)
    monkeypatch.setattr(replay.subprocess, "run", run_phase)
    monkeypatch.setattr(Path, "write_bytes", write_source)
    monkeypatch.setattr(Path, "write_text", write_receipt)
    monkeypatch.setattr(replay.sys, "argv", [replay.__file__, "--output", str(output)])
    exception = {
        "timeout": replay.subprocess.TimeoutExpired,
        "spawn-error": OSError,
        "interrupted": KeyboardInterrupt,
        "wrong-exit": ValueError,
        "malformed-junit": ET.ParseError,
        "missing-junit": FileNotFoundError,
        "wrong-identity": ValueError,
        "skipped-junit": ValueError,
        "duplicate-case": ValueError,
        "teardown-error": ValueError,
        "wrong-assertion": ValueError,
        "receipt-write-error": OSError,
        "partial-source-write": OSError,
        "silent-source-write": ValueError,
    }[fault]
    message = {
        "wrong-exit": "Wrong phase exit",
        "wrong-identity": "Wrong test identity",
        "skipped-junit": "Wrong suite outcomes",
        "duplicate-case": "Wrong test identity",
        "teardown-error": "Wrong suite outcomes",
        "wrong-assertion": "intended empty-frame assertion",
        "receipt-write-error": "Phase receipt could not be persisted",
        "partial-source-write": "Private production write interrupted",
        "silent-source-write": "Phase production differs from intended source",
    }.get(fault)
    with pytest.raises(exception, match=message):
        replay.main()

    preceding = (replay.SQL + replay.DOMAIN)[: (replay.SQL + replay.DOMAIN).index(loader)]
    expected_phases = [(name, step) for name in preceding for step in ("red", "green")]
    steps = ["red"] if phase == "red" else ["red", "green"]
    if fault in {"partial-source-write", "silent-source-write"}:
        steps = steps[:-1]  # Pytest cannot start after a failed production write.
    expected_phases += [(loader, step) for step in steps]
    assert phases == expected_phases
    for name in preceding:
        for step, code in (("red", 1), ("green", 0)):
            prefix = output / f"{name}-{step}"
            assert prefix.with_suffix(".json").is_file()
            assert json.loads(prefix.with_suffix(".json").read_text())["exit"] == code
            assert prefix.with_suffix(".xml").is_file()
            replay.validate_junit(
                prefix.with_suffix(".xml"),
                ET.parse(prefix.with_suffix(".xml")).find("testsuite/testcase").get("name"),
                step,
                code,
            )
    assert source.read_bytes() == original
    assert {name: (root / name).read_bytes() for name in caller} == caller
    assert not (output / "controls.json").exists()
    if fault in {"partial-source-write", "silent-source-write"}:
        assert source_writes[-1] == original
        prefix = output / f"{loader}-{phase}"
        for suffix in (".log", ".json", ".xml"):
            assert not prefix.with_suffix(suffix).exists()
    if fault in {"timeout", "spawn-error", "interrupted"}:
        # A process that never returns cannot supply a completed phase receipt.
        prefix = output / f"{loader}-{phase}"
        assert not prefix.with_suffix(".json").exists()
        assert not prefix.with_suffix(".xml").exists()
    if fault == "receipt-write-error":
        # Successful pytest output cannot complete proof without its command receipt.
        prefix = output / f"{loader}-{phase}"
        assert not prefix.with_suffix(".json").exists()
        replay.validate_junit(
            prefix.with_suffix(".xml"),
            ET.parse(prefix.with_suffix(".xml")).find("testsuite/testcase").get("name"),
            phase,
            1 if phase == "red" else 0,
        )
    if fault == "teardown-error":
        prefix = output / f"{loader}-{phase}"
        case = ET.parse(prefix.with_suffix(".xml")).find("testsuite/testcase")
        assert (case.find("failure") is not None) is (phase == "red")
        assert case.find("error").get("message") == "failed on teardown"
        # A phase exit and its intended assertion cannot override a teardown error.
        assert json.loads(prefix.with_suffix(".json").read_text())["exit"] == (
            1 if phase == "red" else 0
        )
    assert "PASS:" not in capsys.readouterr().out
