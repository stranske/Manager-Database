from __future__ import annotations

import asyncio
import importlib.util
import json
import sqlite3
import sys
from pathlib import Path

import pytest


def _load_module():
    module_path = Path(__file__).resolve().parents[1] / "scripts" / "resolve_aliases.py"
    spec = importlib.util.spec_from_file_location("resolve_aliases_module", module_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _seed_db(db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("""
            CREATE TABLE managers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                cik TEXT,
                aliases TEXT NOT NULL DEFAULT '[]',
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """)
        conn.execute(
            "INSERT INTO managers(name, cik, aliases) VALUES (?, ?, ?)",
            ("Bridgewater Associates", "0001350694", json.dumps(["Bridgewater"])),
        )
        conn.execute(
            "INSERT INTO managers(name, cik, aliases) VALUES (?, ?, ?)",
            ("Berkshire Hathaway", "0001067983", json.dumps([])),
        )
        conn.execute(
            "INSERT INTO managers(name, cik, aliases) VALUES (?, ?, ?)",
            ("No CIK Manager", "", json.dumps([])),
        )
        conn.commit()
    finally:
        conn.close()


class _DummyResponse:
    def __init__(self, status_code: int, payload: dict[str, object]) -> None:
        self.status_code = status_code
        self._payload = payload
        self.content = json.dumps(payload).encode("utf-8")

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError("request failed")

    def json(self) -> dict[str, object]:
        return self._payload


class _DummyClient:
    async def get(self, _url: str, headers: dict[str, str]):
        assert "User-Agent" in headers
        return _DummyResponse(200, {"name": "Bridgewater Associates, LP"})


@pytest.mark.asyncio
async def test_fetch_edgar_official_name_uses_tracked_call(monkeypatch):
    module = _load_module()
    calls: list[tuple[str, str]] = []
    logged_statuses: list[int] = []

    class _Tracked:
        async def __aenter__(self):
            def _log(resp):
                logged_statuses.append(resp.status_code)

            return _log

        async def __aexit__(self, exc_type, exc, tb):
            return False

    def _fake_tracked_call(source: str, endpoint: str):
        calls.append((source, endpoint))
        return _Tracked()

    monkeypatch.setattr(module, "tracked_call", _fake_tracked_call)

    name = await module.fetch_edgar_official_name(
        _DummyClient(), "0001350694", user_agent="ua-test"
    )
    assert name == "Bridgewater Associates, LP"
    assert calls == [("edgar", "https://data.sec.gov/submissions/CIK0001350694.json")]
    assert logged_statuses == [200]


def test_resolve_aliases_updates_aliases(tmp_path, monkeypatch, capsys):
    module = _load_module()
    db_path = tmp_path / "managers.db"
    _seed_db(db_path)

    async def _fake_fetch(_client, cik: str, *, user_agent: str):
        if cik == "0001350694":
            return "Bridgewater Associates, LP"
        if cik == "0001067983":
            return "Berkshire Hathaway"
        raise RuntimeError("missing fixture")

    monkeypatch.setattr(module, "fetch_edgar_official_name", _fake_fetch)

    checked, alias_additions, errors = asyncio.run(
        module.resolve_aliases(db_path=str(db_path), user_agent="ua-test")
    )

    assert (checked, alias_additions, errors) == (2, 1, 0)
    output = capsys.readouterr().out
    assert "Added alias for manager" in output
    assert "0001350694" in output

    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute("SELECT name, aliases FROM managers ORDER BY id").fetchall()
    finally:
        conn.close()

    assert json.loads(rows[0][1]) == ["Bridgewater", "Bridgewater Associates, LP"]
    assert json.loads(rows[1][1]) == []


def test_resolve_aliases_dry_run_does_not_write(tmp_path, monkeypatch):
    module = _load_module()
    db_path = tmp_path / "managers.db"
    _seed_db(db_path)

    async def _fake_fetch(_client, cik: str, *, user_agent: str):
        if cik == "0001350694":
            return "Bridgewater Associates, LP"
        return "Berkshire Hathaway"

    monkeypatch.setattr(module, "fetch_edgar_official_name", _fake_fetch)

    checked, alias_additions, errors = asyncio.run(
        module.resolve_aliases(db_path=str(db_path), dry_run=True, user_agent="ua-test")
    )
    assert (checked, alias_additions, errors) == (2, 1, 0)

    conn = sqlite3.connect(db_path)
    try:
        aliases = conn.execute("SELECT aliases FROM managers WHERE id = 1").fetchone()[0]
    finally:
        conn.close()
    assert json.loads(aliases) == ["Bridgewater"]
