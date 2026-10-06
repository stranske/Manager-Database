"""HTTP regressions for write outages, connection cleanup and cache preservation."""

import asyncio
import sqlite3
from typing import Any, cast
from unittest.mock import Mock

import httpx
import pytest

from api import managers
from api.chat import app


@pytest.mark.parametrize(
    ("method", "path", "payload", "write_symbol"),
    [
        ("PATCH", "/managers/1", {"name": "Updated"}, "_update_manager"),
        ("PATCH", "/managers/1/tags", {"add": ["new"]}, "_update_manager"),
        ("DELETE", "/managers/1", None, "delete_manager_data"),
    ],
    ids=["update", "tags", "delete"],
)
@pytest.mark.parametrize("stage", ["connect", "schema", "write"])
def test_write_outage_returns_503_and_preserves_cache(
    tmp_path, monkeypatch, method, path, payload, write_symbol, stage
):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "outage.db"))
    monkeypatch.delenv("DB_URL", raising=False)
    connection = Mock(spec=sqlite3.Connection)
    invalidate = Mock()
    monkeypatch.setattr(managers, "invalidate_cache_prefix", invalidate)

    def fail(*args, **kwargs):
        raise sqlite3.OperationalError("private database location /secret/outage.db")

    monkeypatch.setattr(managers, "connect_db", fail if stage == "connect" else lambda: connection)
    monkeypatch.setattr(managers, "_ensure_manager_table", fail if stage == "schema" else Mock())
    monkeypatch.setattr(managers, write_symbol, fail)
    # The tags endpoint reads the current row before attempting its write.
    monkeypatch.setattr(
        managers,
        "_fetch_manager",
        lambda *args: (1, "Original", None, None, "[]", "[]", "[]", None, None),
    )

    async def request():
        await cast(Any, app.router).startup()
        try:
            transport = httpx.ASGITransport(app=cast(Any, app))
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                return await client.request(method, path, json=payload)
        finally:
            await cast(Any, app.router).shutdown()

    response = asyncio.run(request())
    assert response.status_code == 503
    assert response.json() == {"detail": "Database unavailable"}
    invalidate.assert_not_called()
    if stage == "connect":
        connection.close.assert_not_called()
    else:
        connection.close.assert_called_once_with()
