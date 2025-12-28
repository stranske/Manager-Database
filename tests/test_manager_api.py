import asyncio
import sqlite3
import sys
from pathlib import Path

import httpx

sys.path.append(str(Path(__file__).resolve().parents[1]))

from api.chat import app


async def _post_manager(payload: dict):
    # Use the ASGI transport to exercise validation without a live server.
    await app.router.startup()
    try:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test"
        ) as client:
            return await client.post("/managers", json=payload)
    finally:
        await app.router.shutdown()


def test_manager_empty_name_returns_400(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    resp = asyncio.run(
        _post_manager({"name": "", "email": "owner@example.com", "department": "Ops"})
    )
    assert resp.status_code == 400
    payload = resp.json()
    assert payload["errors"][0]["field"] == "name"
    assert "required" in payload["errors"][0]["message"].lower()


def test_manager_invalid_email_returns_400(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    resp = asyncio.run(
        _post_manager(
            {"name": "Ada Lovelace", "email": "not-an-email", "department": "Ops"}
        )
    )
    assert resp.status_code == 400
    payload = resp.json()
    assert payload["errors"][0]["field"] == "email"
    assert "valid" in payload["errors"][0]["message"].lower()


def test_manager_valid_record_is_stored(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    payload = {"name": "Grace Hopper", "email": "grace@example.com", "department": "Eng"}
    resp = asyncio.run(_post_manager(payload))
    assert resp.status_code == 201
    created = resp.json()
    assert created["id"] > 0
    conn = sqlite3.connect(db_path)
    try:
        # Verify the record was persisted with the expected values.
        row = conn.execute(
            "SELECT name, email, department FROM managers WHERE id = ?",
            (created["id"],),
        ).fetchone()
    finally:
        conn.close()
    assert row == (payload["name"], payload["email"], payload["department"])
