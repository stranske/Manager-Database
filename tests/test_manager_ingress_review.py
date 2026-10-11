"""Exercise streaming resource limits and replay caller-integrity failure."""

import asyncio
import importlib.util
import json
import shutil

import pytest

from api import managers
from api.chat import app


@pytest.mark.parametrize("content_length", [None, b"bad", b"1"])
def test_app_stops_receiving_at_first_oversized_chunk(monkeypatch, content_length):
    monkeypatch.setenv("BULK_IMPORT_MAX_BYTES", "20")

    def forbid_storage():
        raise AssertionError("oversized request reached storage")

    monkeypatch.setattr(managers, "connect_db", forbid_storage)
    headers = [(b"content-type", b"application/json")]
    if content_length is not None:
        headers.append((b"content-length", content_length))
    chunks = [b"x" * 15, b"x" * 10, b"must not be consumed"]
    consumed = []
    messages = []

    async def receive():
        index = len(consumed)
        consumed.append(index)
        return {"type": "http.request", "body": chunks[index], "more_body": index < 2}

    async def send(message):
        messages.append(message)

    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "POST",
        "scheme": "http",
        "path": "/api/managers/bulk",
        "raw_path": b"/api/managers/bulk",
        "query_string": b"",
        "root_path": "",
        "headers": headers,
        "client": ("127.0.0.1", 123),
        "server": ("test", 80),
    }
    asyncio.run(app(scope, receive, send))
    assert messages[0]["status"] == 413
    assert consumed == [0, 1], "receiver consumed chunks after exceeding its size limit"


def test_replay_records_but_rejects_changed_caller(tmp_path, monkeypatch, capsys):
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    relative = Path("docs/evidence/issue-1750-ingress/replay_ingress_boundaries.py")
    replay = tmp_path / relative
    replay.parent.mkdir(parents=True)
    shutil.copyfile(root / relative, replay)
    shutil.copyfile(root / relative.parent / "source.json", replay.parent / "source.json")
    source = tmp_path / "api/managers.py"
    source.parent.mkdir()
    shutil.copyfile(root / "api/managers.py", source)
    original = source.read_bytes()
    caller = tmp_path / "tests/test_manager_ingress_boundaries.py"
    caller.parent.mkdir()
    shutil.copyfile(root / "tests/test_manager_ingress_boundaries.py", caller)
    spec = importlib.util.spec_from_file_location("ingress_replay_under_test", replay)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    def changed_caller(*args):
        caller.write_text(caller.read_text() + "\n# concurrent caller change\n")
        return {"valid": True}

    monkeypatch.setattr(module, "run_case", changed_caller)
    output = tmp_path / "proof"
    monkeypatch.setattr("sys.argv", [str(replay), "--output", str(output)])
    with pytest.raises(RuntimeError, match="Caller integrity"):
        module.main()
    receipt = json.loads((output / "receipts.json").read_text())
    assert receipt["callers_unchanged"] is False
    assert receipt["restored"] is True
    assert source.read_bytes() == original
    assert "Four production mutations" not in capsys.readouterr().out
