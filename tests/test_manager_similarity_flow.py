import asyncio
import sqlite3
from pathlib import Path
from typing import Any, cast

import httpx
import pytest

from api import managers as managers_module
from api.chat import app
from etl.manager_similarity_flow import compute_manager_similarity, cosine_similarity


def test_similarity_uses_union_denominator_and_latest_filings():
    conn = sqlite3.connect(":memory:")
    conn.executescript(
        """PRAGMA foreign_keys = ON;
    CREATE TABLE managers (id INTEGER PRIMARY KEY);
    CREATE TABLE filings (filing_id INTEGER PRIMARY KEY, manager_id INTEGER, period_end TEXT);
    CREATE TABLE holdings (filing_id INTEGER, cusip TEXT, resolved_ticker TEXT, superseded_at TEXT);
    INSERT INTO managers VALUES (1), (2), (3), (4);
    INSERT INTO filings VALUES (0, 1, '2024-12-31'), (1, 1, '2025-03-31'), (2, 2, '2025-03-31'),
      (3, 3, '2025-03-31'), (4, 4, '2025-03-31');
    INSERT INTO holdings VALUES (0,'OLD',NULL,NULL),(1,'CUSIP-A','A',NULL),(1,'B',NULL,NULL),(1,'C',NULL,NULL),(1,'D',NULL,NULL),
      (1,'STALE',NULL,'2025-04-01'),(2,'C',NULL,NULL),(2,'D',NULL,NULL),(2,'E',NULL,NULL),(2,'F',NULL,NULL),(3,'Z',NULL,NULL);"""
    )
    assert compute_manager_similarity(conn) == 6
    assert conn.execute(
        "SELECT jaccard, overlap_count, union_count FROM manager_similarity WHERE manager_id_a=1 AND manager_id_b=2"
    ).fetchone() == (2 / 6, 2, 6)
    assert conn.execute(
        "SELECT jaccard FROM manager_similarity WHERE manager_id_a=1 AND manager_id_b=3"
    ).fetchone() == (0.0,)
    assert conn.execute(
        "SELECT jaccard FROM manager_similarity WHERE manager_id_a=1 AND manager_id_b=4"
    ).fetchone() == (0.0,)
    assert {row[2] for row in conn.execute("PRAGMA foreign_key_list(manager_similarity)")} == {
        "managers"
    }


def test_cosine_similarity_matches_hand_computed_vectors():
    assert cosine_similarity([1.0, 1.0], [1.0, 0.0]) == pytest.approx(1 / 2**0.5)
    assert cosine_similarity([0.0, 0.0], [1.0, 0.0]) is None


def test_similarity_rebuild_rolls_back_on_insert_failure():
    conn = sqlite3.connect(":memory:")
    conn.executescript("""PRAGMA foreign_keys = ON;
    CREATE TABLE managers (id INTEGER PRIMARY KEY);
    CREATE TABLE filings (filing_id INTEGER PRIMARY KEY, manager_id INTEGER, period_end TEXT);
    CREATE TABLE holdings (filing_id INTEGER, cusip TEXT, resolved_ticker TEXT, superseded_at TEXT);
    INSERT INTO managers VALUES (1), (2);
    INSERT INTO filings VALUES (1, 1, '2025-03-31'), (2, 2, '2025-03-31');
    INSERT INTO holdings VALUES (1, 'A', NULL, NULL), (2, 'B', NULL, NULL);""")
    assert compute_manager_similarity(conn) == 1
    before = conn.execute("SELECT * FROM manager_similarity").fetchall()
    conn.execute("""CREATE TRIGGER fail_similarity_insert BEFORE INSERT ON manager_similarity
        BEGIN SELECT RAISE(ABORT, 'injected write failure'); END""")
    try:
        compute_manager_similarity(conn)
    except sqlite3.IntegrityError as error:
        assert "injected write failure" in str(error)
    else:
        raise AssertionError("expected injected manager similarity write failure")
    assert conn.execute("SELECT * FROM manager_similarity").fetchall() == before


async def _get_similar_manager(manager_id: int, limit: int, basis: str = "jaccard"):
    await cast(Any, app.router).startup()
    try:
        transport = httpx.ASGITransport(app=cast(Any, app))
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", timeout=5.0
        ) as client:
            return await client.get(
                f"/managers/{manager_id}/similar", params={"limit": limit, "basis": basis}
            )
    finally:
        await cast(Any, app.router).shutdown()


def test_similar_manager_endpoint_orders_canonical_pairs_and_returns_404(tmp_path, monkeypatch):
    db_path = Path(tmp_path) / "similarity.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    conn = sqlite3.connect(db_path)
    managers_module._ensure_manager_table(conn)
    conn.executemany(
        "INSERT INTO managers (id, name) VALUES (?, ?)", [(1, "One"), (2, "Two"), (3, "Three")]
    )
    conn.execute(
        "CREATE TABLE manager_similarity (manager_id_a INTEGER, manager_id_b INTEGER, jaccard REAL, cosine REAL, overlap_count INTEGER, union_count INTEGER)"
    )
    conn.executemany(
        "INSERT INTO manager_similarity VALUES (?, ?, ?, ?, ?, ?)",
        [(1, 2, 0.5, 0.8, 2, 4), (1, 3, 0.75, 0.6, 3, 4)],
    )
    conn.commit()
    conn.close()

    response = asyncio.run(_get_similar_manager(1, 1))
    assert response.status_code == 200
    assert response.json() == {
        "items": [
            {
                "manager_id": 3,
                "basis": "jaccard",
                "score": 0.75,
                "jaccard": 0.75,
                "cosine": 0.6,
                "overlap_count": 3,
                "union_count": 4,
            }
        ]
    }
    reverse_response = asyncio.run(_get_similar_manager(2, 10))
    assert reverse_response.status_code == 200
    assert reverse_response.json() == {
        "items": [
            {
                "manager_id": 1,
                "basis": "jaccard",
                "score": 0.5,
                "jaccard": 0.5,
                "cosine": 0.8,
                "overlap_count": 2,
                "union_count": 4,
            }
        ]
    }
    cosine_response = asyncio.run(_get_similar_manager(1, 10, basis="cosine"))
    assert cosine_response.json()["items"][0]["manager_id"] == 2
    assert cosine_response.json()["items"][0]["score"] == 0.8
    assert asyncio.run(_get_similar_manager(999, 10)).status_code == 404
