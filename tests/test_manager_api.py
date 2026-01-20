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
        # Timeout keeps stuck ASGI calls from stalling the test suite.
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", timeout=5.0
        ) as client:
            return await client.post("/managers", json=payload)
    finally:
        await app.router.shutdown()


async def _get_managers(params: dict | None = None):
    # Use the ASGI transport to exercise list behavior without a live server.
    await app.router.startup()
    try:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", timeout=5.0
        ) as client:
            return await client.get("/managers", params=params)
    finally:
        await app.router.shutdown()


async def _get_manager(manager_id: int):
    # Use the ASGI transport to exercise detail behavior without a live server.
    await app.router.startup()
    try:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", timeout=5.0
        ) as client:
            return await client.get(f"/managers/{manager_id}")
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
        _post_manager({"name": "Ada Lovelace", "email": "not-an-email", "department": "Ops"})
    )
    assert resp.status_code == 400
    payload = resp.json()
    assert payload["errors"][0]["field"] == "email"
    assert "valid" in payload["errors"][0]["message"].lower()


def test_manager_empty_department_returns_400(tmp_path, monkeypatch):
    # Validate that required department values are enforced.
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    resp = asyncio.run(
        _post_manager({"name": "Ada Lovelace", "email": "ada@example.com", "department": "   "})
    )
    assert resp.status_code == 400
    payload = resp.json()
    assert payload["errors"][0]["field"] == "department"
    assert "required" in payload["errors"][0]["message"].lower()


def test_manager_valid_record_is_stored(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    payload = {
        "name": "Grace Hopper",
        "email": "grace@example.com",
        "department": "Eng",
    }
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


def test_manager_list_returns_paginated_results(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    payloads = [
        {"name": "Grace Hopper", "email": "grace@example.com", "department": "Eng"},
        {"name": "Ada Lovelace", "email": "ada@example.com", "department": "R&D"},
        {"name": "Mary Jackson", "email": "mary@example.com", "department": "Ops"},
    ]
    for payload in payloads:
        resp = asyncio.run(_post_manager(payload))
        assert resp.status_code == 201

    resp = asyncio.run(_get_managers({"limit": 2, "offset": 1}))
    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 3
    assert body["limit"] == 2
    assert body["offset"] == 1
    names = [item["name"] for item in body["items"]]
    assert names == ["Ada Lovelace", "Mary Jackson"]


def test_manager_list_defaults_return_empty_page(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    resp = asyncio.run(_get_managers())
    assert resp.status_code == 200
    body = resp.json()
    assert body["items"] == []
    assert body["total"] == 0
    assert body["limit"] == 25
    assert body["offset"] == 0


def test_manager_list_invalid_limit_returns_400(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    resp = asyncio.run(_get_managers({"limit": 0}))
    assert resp.status_code == 400
    payload = resp.json()
    assert payload["errors"][0]["field"] == "limit"
    assert "greater" in payload["errors"][0]["message"].lower()


def test_manager_get_returns_single_manager(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    payload = {"name": "Linus Torvalds", "email": "linus@example.com", "department": "Core"}
    resp = asyncio.run(_post_manager(payload))
    created = resp.json()

    get_resp = asyncio.run(_get_manager(created["id"]))
    assert get_resp.status_code == 200
    fetched = get_resp.json()
    assert fetched["id"] == created["id"]
    assert fetched["name"] == payload["name"]
    assert fetched["email"] == payload["email"]
    assert fetched["department"] == payload["department"]


def test_manager_get_returns_404_for_missing_id(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    resp = asyncio.run(_get_manager(999))
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Manager not found"
