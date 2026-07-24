import asyncio
import sqlite3
from pathlib import Path
from typing import Any, cast

import httpx

from api import managers as managers_module
from api.chat import app
from etl.manager_similarity_flow import compute_manager_similarity


def test_similarity_uses_union_denominator_and_latest_filings():
    conn = sqlite3.connect(":memory:")
    conn.executescript(
        """CREATE TABLE filings (filing_id INTEGER PRIMARY KEY, manager_id INTEGER, period_end TEXT);
    CREATE TABLE holdings (filing_id INTEGER, cusip TEXT, resolved_ticker TEXT);
    INSERT INTO filings VALUES (0, 1, '2024-12-31'), (1, 1, '2025-03-31'), (2, 2, '2025-03-31'),
      (3, 3, '2025-03-31'), (4, 4, '2025-03-31');
    INSERT INTO holdings VALUES (0,'OLD',NULL),(1,'CUSIP-A','A'),(1,'B',NULL),(1,'C',NULL),(1,'D',NULL),
      (2,'C',NULL),(2,'D',NULL),(2,'E',NULL),(2,'F',NULL),(3,'Z',NULL);"""
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


async def _get_similar_manager(manager_id: int, limit: int):
    await cast(Any, app.router).startup()
    try:
        transport = httpx.ASGITransport(app=cast(Any, app))
        async with httpx.AsyncClient(transport=transport, base_url="http://test", timeout=5.0) as client:
            return await client.get(f"/managers/{manager_id}/similar", params={"limit": limit})
    finally:
        await cast(Any, app.router).shutdown()


def test_similar_manager_endpoint_orders_canonical_pairs_and_returns_404(tmp_path, monkeypatch):
    db_path = Path(tmp_path) / "similarity.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    conn = sqlite3.connect(db_path)
    managers_module._ensure_manager_table(conn)
    conn.executemany("INSERT INTO managers (id, name) VALUES (?, ?)", [(1, "One"), (2, "Two"), (3, "Three")])
    conn.execute(
        "CREATE TABLE manager_similarity (manager_id_a INTEGER, manager_id_b INTEGER, jaccard REAL, overlap_count INTEGER, union_count INTEGER)"
    )
    conn.executemany(
        "INSERT INTO manager_similarity VALUES (?, ?, ?, ?, ?)",
        [(1, 2, 0.5, 2, 4), (1, 3, 0.75, 3, 4)],
    )
    conn.commit()
    conn.close()

    response = asyncio.run(_get_similar_manager(1, 1))
    assert response.status_code == 200
    assert response.json() == {
        "items": [{"manager_id": 3, "jaccard": 0.75, "overlap_count": 3, "union_count": 4}]
    }
    assert asyncio.run(_get_similar_manager(999, 10)).status_code == 404
