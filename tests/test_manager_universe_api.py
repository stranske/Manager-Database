import asyncio
import sqlite3
import sys
from pathlib import Path

import httpx

sys.path.append(str(Path(__file__).resolve().parents[1]))

from api.chat import app


async def _post_universe(payload):
    await app.router.startup()
    try:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", timeout=5.0
        ) as client:
            return await client.post("/managers/import/universe", json=payload)
    finally:
        await app.router.shutdown()


def test_universe_import_creates_updates_and_skips_records(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    payload = [
        {"name": "Berkshire Hathaway", "cik": "0001067983", "jurisdiction": "US"},
        {"name": "Bridgewater Associates", "cik": "0001350694", "jurisdiction": "us"},
        {"name": "", "cik": "0001423053", "jurisdiction": "us"},
    ]

    first = asyncio.run(_post_universe(payload))
    assert first.status_code == 200
    assert first.json() == {"created": 2, "updated": 0, "skipped": 1}

    second = asyncio.run(
        _post_universe(
            [
                {"name": "Berkshire Hathaway Inc.", "cik": "1067983", "jurisdiction": "us"},
                {"name": "Citadel Advisors", "cik": "0001423053", "jurisdiction": "us"},
            ]
        )
    )
    assert second.status_code == 200
    assert second.json() == {"created": 1, "updated": 1, "skipped": 0}

    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute("SELECT name, cik, jurisdiction FROM managers ORDER BY cik").fetchall()
    finally:
        conn.close()

    assert rows == [
        ("Berkshire Hathaway Inc.", "0001067983", "us"),
        ("Bridgewater Associates", "0001350694", "us"),
        ("Citadel Advisors", "0001423053", "us"),
    ]


def test_universe_import_requires_array_body(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))

    response = asyncio.run(_post_universe({"name": "not-an-array"}))
    assert response.status_code == 400
    payload = response.json()
    assert payload["errors"][0]["field"] == "body"


def test_universe_import_skips_non_object_records(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))

    response = asyncio.run(
        _post_universe(
            [
                {
                    "name": "Pershing Square Capital Management, L.P.",
                    "cik": "0001336528",
                    "jurisdiction": "us",
                },
                "invalid-record",
                None,
                42,
            ]
        )
    )
    assert response.status_code == 200
    assert response.json() == {"created": 1, "updated": 0, "skipped": 3}

    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute("SELECT name, cik, jurisdiction FROM managers").fetchall()
    finally:
        conn.close()

    assert rows == [("Pershing Square Capital Management, L.P.", "0001336528", "us")]
