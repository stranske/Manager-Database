"""Exercise stats through HTTP against current and legacy SQLite schemas."""

import asyncio
import sqlite3
from typing import Any, cast
from unittest.mock import Mock

import httpx
import pytest

from api import managers
from api.chat import app


def _stats():
    async def request():
        await cast(Any, app.router).startup()
        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=cast(Any, app)), base_url="http://test"
            ) as client:
                return await client.get("/managers/stats")
        finally:
            await cast(Any, app.router).shutdown()

    return asyncio.run(request())


def _database(tmp_path, monkeypatch, *, legacy=False):
    path = tmp_path / "stats.db"
    monkeypatch.setenv("DB_PATH", str(path))
    monkeypatch.delenv("DB_URL", raising=False)
    conn = sqlite3.connect(path)
    managers._ensure_manager_table(conn)
    if legacy:
        conn.execute("ALTER TABLE managers ADD COLUMN jurisdiction TEXT")
    return conn


def test_stats_current_schema_retries_missing_legacy_column(tmp_path, monkeypatch):
    conn = _database(tmp_path, monkeypatch)
    conn.executemany(
        "INSERT INTO managers(name, cik, lei, jurisdictions, tags) VALUES (?, ?, ?, ?, ?)",
        [
            ("A", " 123 ", " ", '["US", "us", " UK "]', '["Value", "value"]'),
            ("B", None, "LEI", '["uk"]', '["Growth", "value"]'),
        ],
    )
    conn.commit()
    conn.close()
    response = _stats()
    assert response.status_code == 200
    assert response.json() == {
        "total_managers": 2,
        "with_cik": 1,
        "with_lei": 1,
        "by_jurisdiction": {"uk": 2, "us": 1},
        "by_tag": {"growth": 1, "value": 2},
    }


@pytest.mark.parametrize("empty", ["[]", ""], ids=["empty-array", "empty-text"])
def test_stats_legacy_jurisdiction_only_fills_empty_array(tmp_path, monkeypatch, empty):
    conn = _database(tmp_path, monkeypatch, legacy=True)
    conn.executemany(
        "INSERT INTO managers(name, jurisdictions, jurisdiction) VALUES (?, ?, ?)",
        [("Fallback", empty, "  CA  "), ("Modern", '["UK"]', "US")],
    )
    conn.commit()
    conn.close()
    response = _stats()
    assert response.status_code == 200
    assert response.json() == {
        "total_managers": 2,
        "with_cik": 0,
        "with_lei": 0,
        "by_jurisdiction": {"ca": 1, "uk": 1},
        "by_tag": {},
    }


def test_stats_unrelated_database_error_is_not_retried(monkeypatch):
    conn = Mock(spec=sqlite3.Connection)
    conn.execute.side_effect = sqlite3.OperationalError("private /secret/stats.db unavailable")
    monkeypatch.setattr(managers, "connect_db", Mock(return_value=conn))
    monkeypatch.setattr(managers, "_ensure_manager_table", Mock())
    response = _stats()
    assert response.status_code == 503
    assert response.json() == {"detail": "Database unavailable"}
    conn.execute.assert_called_once_with(
        "SELECT cik, lei, jurisdictions, tags, jurisdiction FROM managers"
    )
    conn.close.assert_called_once_with()
