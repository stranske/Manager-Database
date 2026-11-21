"""Verify adapter replay safeguards and the archived evidence's input bindings."""

import importlib.util
import io
import json
import subprocess
import tarfile
from contextlib import contextmanager
from pathlib import Path

import pytest
from defusedxml import ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]
EVIDENCE = ROOT / "docs/evidence/issue-1750-dialect-boundaries"
DRIVER = EVIDENCE / "replay_dialect_boundaries.py"


@pytest.fixture
def replay():
    spec = importlib.util.spec_from_file_location("manager_dialect_replay_guard", DRIVER)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def private_archive(replay, monkeypatch, tmp_path):
    caller = tmp_path / "caller"
    original = (ROOT / replay.SOURCE).read_bytes()
    tests = (ROOT / replay.TEST).read_bytes()
    for name, data in [(replay.SOURCE, original), (replay.TEST, tests)]:
        path = caller / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w") as archive:
        for name, data in [(replay.SOURCE, original), (replay.TEST, tests)]:
            member = tarfile.TarInfo(name)
            member.size = len(data)
            archive.addfile(member, io.BytesIO(data))
    monkeypatch.setattr(replay, "__file__", str(caller / "docs/evidence/chunk/replay.py"))
    monkeypatch.setattr(replay.subprocess, "check_output", lambda argv: buffer.getvalue())
    tree = tmp_path / "private"
    tree.mkdir()

    @contextmanager
    def temporary_directory(**kwargs):
        yield str(tree)

    monkeypatch.setattr(replay.tempfile, "TemporaryDirectory", temporary_directory)
    return caller, tree, original, tests


def test_dialect_replay_rejects_output_reuse_before_archiving(replay, monkeypatch, tmp_path):
    output = tmp_path / "proof"
    output.mkdir()
    proof = output / "controls.json"
    proof.write_bytes(b"existing proof\n")

    def unexpected_archive(*args, **kwargs):
        pytest.fail("Output reuse must be rejected before reading the archive")

    monkeypatch.setattr(replay.subprocess, "check_output", unexpected_archive)
    monkeypatch.setattr(replay.sys, "argv", [str(DRIVER), "--output", str(output)])
    with pytest.raises(FileExistsError):
        replay.main()
    assert list(output.iterdir()) == [proof]
    assert proof.read_bytes() == b"existing proof\n"


def test_dialect_replay_rejects_dirty_source_before_running_tests(
    replay, private_archive, monkeypatch, tmp_path
):
    caller, tree, original, tests = private_archive
    dirty = original + b"\n# uncommitted source change\n"
    (caller / replay.SOURCE).write_bytes(dirty)

    def unexpected_child(*args, **kwargs):
        pytest.fail("Source mismatch must be rejected before running test children")

    monkeypatch.setattr(replay.subprocess, "run", unexpected_child)
    output = tmp_path / "proof"
    monkeypatch.setattr(replay.sys, "argv", [str(DRIVER), "--output", str(output)])
    with pytest.raises(ValueError, match="Archive source differs from checkout"):
        replay.main()
    assert (caller / replay.SOURCE).read_bytes() == dirty
    assert (caller / replay.TEST).read_bytes() == tests
    assert (tree / replay.SOURCE).read_bytes() == original
    assert list(output.iterdir()) == []


@pytest.mark.parametrize("failure", ["timeout", "malformed-junit"])
def test_dialect_replay_restores_private_source_after_failed_child(
    replay, private_archive, monkeypatch, tmp_path, capsys, failure
):
    caller, tree, original, tests = private_archive

    def failed_child(argv, **kwargs):
        assert (tree / replay.SOURCE).read_bytes() != original
        assert (caller / replay.SOURCE).read_bytes() == original
        if failure == "timeout":
            raise subprocess.TimeoutExpired(argv, 120)
        junit = Path(next(arg.split("=", 1)[1] for arg in argv if arg.startswith("--junitxml=")))
        junit.write_text("<broken")
        return subprocess.CompletedProcess(argv, 1)

    monkeypatch.setattr(replay.subprocess, "run", failed_child)
    output = tmp_path / "proof"
    monkeypatch.setattr(replay.sys, "argv", [str(DRIVER), "--output", str(output)])
    error = subprocess.TimeoutExpired if failure == "timeout" else ET.ParseError
    with pytest.raises(error):
        replay.main()
    assert (tree / replay.SOURCE).read_bytes() == original
    assert (caller / replay.SOURCE).read_bytes() == original
    assert (caller / replay.TEST).read_bytes() == tests
    assert not (output / "controls.json").exists()
    assert "PASS:" not in capsys.readouterr().out


def test_dialect_evidence_archive_and_current_inputs_match_manifest(replay):
    manifest = json.loads((EVIDENCE / "manifest.json").read_text())
    archive = EVIDENCE / "validation.tar.gz"
    assert replay.digest(archive.read_bytes()) == manifest["archive_sha256"]
    with tarfile.open(archive) as bundle:
        members = {member.name: member for member in bundle.getmembers() if member.isfile()}
        assert members.keys() == manifest["archive_members"].keys()
        for name, member in members.items():
            stream = bundle.extractfile(member)
            assert stream is not None
            assert replay.digest(stream.read()) == manifest["archive_members"][name], name
    for name, expected in manifest["current_inputs"].items():
        assert replay.digest((ROOT / name).read_bytes()) == expected, name
