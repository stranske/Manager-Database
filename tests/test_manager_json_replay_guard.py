"""Evidence replay must not bind archived results to a dirty caller source."""

import importlib.util
import io
import tarfile
from pathlib import Path

import pytest


def test_json_replay_rejects_source_archive_mismatch_before_running_tests(tmp_path, monkeypatch):
    driver = (
        Path(__file__).resolve().parents[1]
        / "docs/evidence/issue-1750-json-decoding/replay_json_boundaries.py"
    )
    spec = importlib.util.spec_from_file_location("manager_json_replay_guard", driver)
    assert spec is not None and spec.loader is not None
    replay = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(replay)

    root = tmp_path / "caller"
    source = root / replay.SOURCE
    test = root / replay.TEST
    source.parent.mkdir(parents=True)
    test.parent.mkdir(parents=True)
    source.write_bytes(b"dirty checkout source\n")
    test.write_bytes(b"test overlay\n")
    before = (source.read_bytes(), test.read_bytes())
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w") as archive:
        for name, data in [(replay.SOURCE, b"committed source\n"), (replay.TEST, before[1])]:
            member = tarfile.TarInfo(name)
            member.size = len(data)
            archive.addfile(member, io.BytesIO(data))

    monkeypatch.setattr(replay, "__file__", str(root / "docs/evidence/chunk/replay.py"))
    monkeypatch.setattr(replay.subprocess, "check_output", lambda argv: buffer.getvalue())

    def unexpected_child(*args, **kwargs):
        pytest.fail("A mismatched archive must be rejected before running a test child")

    monkeypatch.setattr(replay.subprocess, "run", unexpected_child)
    output = tmp_path / "proof"
    monkeypatch.setattr(replay.sys, "argv", [str(driver), "--output", str(output)])
    with pytest.raises(ValueError, match="Archive source differs from checkout"):
        replay.main()
    assert (source.read_bytes(), test.read_bytes()) == before
    assert list(output.iterdir()) == []
