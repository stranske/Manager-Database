"""Protect HTTP similarity results from non-finite rows and database outages."""

import asyncio
import sqlite3
from typing import Any, cast
from unittest.mock import Mock

import httpx
import pytest

from api import managers
from api.chat import app


def _get(basis):
    async def request():
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=cast(Any, app)), base_url="http://test"
        ) as client:
            return await client.get("/managers/1/similar", params={"basis": basis, "limit": 1})

    return asyncio.run(request())


@pytest.mark.parametrize("basis", ["jaccard", "cosine"])
@pytest.mark.parametrize("invalid_column", ["jaccard", "cosine"])
def test_similarity_discards_nonfinite_rows_before_limit(
    tmp_path, monkeypatch, basis, invalid_column
):
    path = tmp_path / "similarity.db"
    monkeypatch.setenv("DB_PATH", str(path))
    monkeypatch.delenv("DB_URL", raising=False)
    with sqlite3.connect(path) as conn:
        managers._ensure_manager_table(conn)
        conn.executemany(
            "INSERT INTO managers(id, name) VALUES (?, ?)", [(1, "One"), (2, "Two"), (3, "Three")]
        )
        managers.ensure_manager_similarity_table(conn)
        invalid = {"jaccard": 0.99, "cosine": 0.99}
        invalid[invalid_column] = float("inf")
        conn.executemany(
            "INSERT INTO manager_similarity "
            "(manager_id_a, manager_id_b, jaccard, cosine, overlap_count, union_count) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            [(1, 2, invalid["jaccard"], invalid["cosine"], 9, 10), (1, 3, 0.5, 0.6, 3, 5)],
        )
    response = _get(basis)
    assert response.status_code == 200
    assert response.json() == {
        "items": [
            {
                "manager_id": 3,
                "basis": basis,
                "score": 0.6 if basis == "cosine" else 0.5,
                "jaccard": 0.5,
                "cosine": 0.6,
                "overlap_count": 3,
                "union_count": 5,
            }
        ]
    }


@pytest.mark.parametrize("stage", ["connect", "query"])
def test_similarity_database_outage_is_sanitized_and_connection_closed(monkeypatch, stage):
    failure = sqlite3.OperationalError("private /secret/manager.db unavailable")
    connect = Mock()
    conn = Mock(spec=sqlite3.Connection)
    if stage == "connect":
        connect.side_effect = failure
    else:
        connect.return_value = conn
        conn.execute.side_effect = failure
    monkeypatch.setattr(managers, "connect_db", connect)
    monkeypatch.setattr(managers, "_ensure_manager_table", Mock())
    monkeypatch.setattr(managers, "_manager_id_column", Mock(return_value="id"))
    response = _get("jaccard")
    assert response.status_code == 503
    assert response.json() == {"detail": "Database unavailable"}
    connect.assert_called_once_with()
    if stage == "connect":
        conn.close.assert_not_called()
    else:
        conn.execute.assert_called_once_with("SELECT 1 FROM managers WHERE id = ?", (1,))
        conn.close.assert_called_once_with()
