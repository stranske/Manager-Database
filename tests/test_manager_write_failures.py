"""HTTP regressions for write outages, connection cleanup and cache preservation."""

import asyncio
import sqlite3
from typing import Any, cast
from unittest.mock import Mock

import httpx
import pytest

from api import cache as cache_module
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
    db_path = str(tmp_path / "outage.db")
    monkeypatch.setenv("DB_PATH", db_path)
    monkeypatch.delenv("DB_URL", raising=False)
    # Isolate the real in-memory backend and restore the previous backend on teardown.
    monkeypatch.delenv("REDIS_URL", raising=False)
    monkeypatch.setenv("CACHE_MAX_ITEMS", "512")
    monkeypatch.setenv("CACHE_TTL_SECONDS", "60")
    monkeypatch.setattr(cache_module, "_CACHE_BACKEND", None)
    monkeypatch.setattr(cache_module, "_CACHE_METRICS", {})
    existing_row = (1, "Original", None, None, "[]", "[]", "[]", "{}", "[]", None, None)
    cached_values = {
        cache_module._make_cache_key("managers.item", (db_path, 1), {}): existing_row,
        cache_module._make_cache_key(
            "managers.list", (db_path, 10, 0, None, None, None, None), {}
        ): [existing_row],
        cache_module._make_cache_key("managers.count", (db_path, None, None, None, None), {}): 1,
    }
    for key, value in cached_values.items():
        cache_module.cache_set(key, value)
    backend = cache_module._get_backend()
    cached_before = {key: backend.get(key) for key in cached_values}
    assert all(value is not None for value in cached_before.values())

    connection = Mock(spec=sqlite3.Connection)
    invalidate = Mock(wraps=cache_module.invalidate_cache_prefix)
    monkeypatch.setattr(managers, "invalidate_cache_prefix", invalidate)

    def fail(*args, **kwargs):
        raise sqlite3.OperationalError("private database location /secret/outage.db")

    connect = Mock(side_effect=fail) if stage == "connect" else Mock(return_value=connection)
    ensure_schema = Mock(side_effect=fail) if stage == "schema" else Mock()
    write = Mock(side_effect=fail)
    monkeypatch.setattr(managers, "connect_db", connect)
    monkeypatch.setattr(managers, "_ensure_manager_table", ensure_schema)
    monkeypatch.setattr(managers, write_symbol, write)
    # The tags endpoint reads the real warmed item cache before attempting its write.

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
    assert {key: backend.get(key) for key in cached_values} == cached_before
    invalidate.assert_not_called()
    connect.assert_called_once_with()
    connection.execute.assert_not_called()
    if stage == "connect":
        ensure_schema.assert_not_called()
        write.assert_not_called()
        connection.close.assert_not_called()
    else:
        ensure_schema.assert_called_once_with(connection)
        if stage == "schema":
            write.assert_not_called()
        else:
            write.assert_called_once()
            assert write.call_args.args[:2] == (connection, 1)
        connection.close.assert_called_once_with()
