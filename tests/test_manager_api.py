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
    resp = asyncio.run(_post_manager({"name": "", "role": "Operations Lead"}))
    assert resp.status_code == 400
    payload = resp.json()
    assert payload["errors"][0]["field"] == "name"
    assert "required" in payload["errors"][0]["message"].lower()


def test_manager_empty_role_returns_400(tmp_path, monkeypatch):
    # Validate that required role values are enforced.
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    resp = asyncio.run(_post_manager({"name": "Ada Lovelace", "role": "   "}))
    assert resp.status_code == 400
    payload = resp.json()
    assert payload["errors"][0]["field"] == "role"
    assert "required" in payload["errors"][0]["message"].lower()


def test_manager_valid_record_is_stored(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    payload = {
        "name": "Grace Hopper",
        "role": "Engineering Director",
    }
    resp = asyncio.run(_post_manager(payload))
    assert resp.status_code == 201
    created = resp.json()
    assert created["id"] > 0
    conn = sqlite3.connect(db_path)
    try:
        # Verify the record was persisted with the expected values.
        row = conn.execute(
            "SELECT name, role FROM managers WHERE id = ?",
            (created["id"],),
        ).fetchone()
    finally:
        conn.close()
    assert row == (payload["name"], payload["role"])


def test_manager_list_returns_paginated_results(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    payloads = [
        {"name": "Grace Hopper", "role": "Engineering Director"},
        {"name": "Ada Lovelace", "role": "Research Lead"},
        {"name": "Mary Jackson", "role": "Operations Manager"},
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


def test_manager_list_limit_offset_zero_returns_all(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    payloads = [
        {"name": "Grace Hopper", "role": "Engineering Director"},
        {"name": "Ada Lovelace", "role": "Research Lead"},
    ]
    for payload in payloads:
        resp = asyncio.run(_post_manager(payload))
        assert resp.status_code == 201

    resp = asyncio.run(_get_managers({"limit": 10, "offset": 0}))
    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 2
    assert body["limit"] == 10
    assert body["offset"] == 0
    assert [item["name"] for item in body["items"]] == ["Grace Hopper", "Ada Lovelace"]


def test_manager_list_defaults_return_empty_page(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    resp = asyncio.run(_get_managers())
    assert resp.status_code == 200
    body = resp.json()
    assert body["items"] == []
    assert body["total"] == 0
    assert body["limit"] == 0
    assert body["offset"] == 0


def test_manager_list_defaults_return_all_items(tmp_path, monkeypatch):
    # Default list behavior should return all managers when limit is omitted.
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    payloads = [
        {"name": "Grace Hopper", "role": "Engineering Director"},
        {"name": "Ada Lovelace", "role": "Research Lead"},
    ]
    for payload in payloads:
        resp = asyncio.run(_post_manager(payload))
        assert resp.status_code == 201

    resp = asyncio.run(_get_managers())
    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 2
    assert body["limit"] == 2
    assert body["offset"] == 0
    assert [item["name"] for item in body["items"]] == ["Grace Hopper", "Ada Lovelace"]


def test_manager_list_filter_by_department_returns_subset(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    # Ensure list filtering includes only managers from the requested department.
    payloads = [
        {"name": "Grace Hopper", "role": "Engineering Director", "department": "Engineering"},
        {"name": "Ada Lovelace", "role": "Research Lead", "department": "Engineering"},
        {"name": "Mary Jackson", "role": "Operations Manager", "department": "Operations"},
    ]
    for payload in payloads:
        resp = asyncio.run(_post_manager(payload))
        assert resp.status_code == 201

    resp = asyncio.run(_get_managers({"department": "Engineering"}))
    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 2
    assert [item["name"] for item in body["items"]] == ["Grace Hopper", "Ada Lovelace"]
    assert {item["department"] for item in body["items"]} == {"Engineering"}


def test_manager_list_invalid_limit_returns_400(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    resp = asyncio.run(_get_managers({"limit": 0}))
    assert resp.status_code == 400
    payload = resp.json()
    assert payload["errors"][0]["field"] == "limit"
    assert "greater" in payload["errors"][0]["message"].lower()


def test_manager_list_limit_above_max_returns_400(tmp_path, monkeypatch):
    # Upper-bound validation keeps pagination requests predictable.
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    resp = asyncio.run(_get_managers({"limit": 101}))
    assert resp.status_code == 400
    payload = resp.json()
    assert payload["errors"][0]["field"] == "limit"
    assert "less" in payload["errors"][0]["message"].lower()


def test_manager_list_offset_beyond_total_returns_empty_page(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    payloads = [
        {"name": "Grace Hopper", "role": "Engineering Director"},
        {"name": "Ada Lovelace", "role": "Research Lead"},
    ]
    for payload in payloads:
        resp = asyncio.run(_post_manager(payload))
        assert resp.status_code == 201

    resp = asyncio.run(_get_managers({"limit": 5, "offset": 5}))
    assert resp.status_code == 200
    body = resp.json()
    assert body["items"] == []
    assert body["total"] == 2
    assert body["limit"] == 5
    assert body["offset"] == 5


def test_manager_get_returns_single_manager(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    payload = {"name": "Linus Torvalds", "role": "Chief Architect"}
    resp = asyncio.run(_post_manager(payload))
    created = resp.json()

    get_resp = asyncio.run(_get_manager(created["id"]))
    assert get_resp.status_code == 200
    fetched = get_resp.json()
    assert fetched["id"] == created["id"]
    assert fetched["name"] == payload["name"]
    assert fetched["role"] == payload["role"]


def test_manager_get_returns_404_for_missing_id(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    resp = asyncio.run(_get_manager(999))
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Manager not found"


def test_manager_get_invalid_id_returns_400(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    resp = asyncio.run(_get_manager(0))
    assert resp.status_code == 400
    payload = resp.json()
    assert payload["errors"][0]["field"] == "id"
