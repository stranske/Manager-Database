import asyncio
import sqlite3
import sys
from pathlib import Path
from typing import Any, cast

import httpx

sys.path.append(str(Path(__file__).resolve().parents[1]))

from api import managers as managers_module
from api.chat import app


async def _post_manager(payload: dict):
    # Use the ASGI transport to exercise validation without a live server.
    await app.router.startup()
    try:
        transport = httpx.ASGITransport(app=cast(Any, app))
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
        transport = httpx.ASGITransport(app=cast(Any, app))
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", timeout=5.0
        ) as client:
            return await client.get("/managers", params=params)
    finally:
        await app.router.shutdown()


async def _get_manager_stats():
    await app.router.startup()
    try:
        transport = httpx.ASGITransport(app=cast(Any, app))
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", timeout=5.0
        ) as client:
            return await client.get("/managers/stats")
    finally:
        await app.router.shutdown()


async def _get_manager(manager_id: int):
    # Use the ASGI transport to exercise detail behavior without a live server.
    await app.router.startup()
    try:
        transport = httpx.ASGITransport(app=cast(Any, app))
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", timeout=5.0
        ) as client:
            return await client.get(f"/managers/{manager_id}")
    finally:
        await app.router.shutdown()


async def _patch_manager(manager_id: int, payload: dict):
    await app.router.startup()
    try:
        transport = httpx.ASGITransport(app=cast(Any, app))
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", timeout=5.0
        ) as client:
            return await client.patch(f"/managers/{manager_id}", json=payload)
    finally:
        await app.router.shutdown()


async def _patch_manager_tags(manager_id: int, payload: dict):
    await app.router.startup()
    try:
        transport = httpx.ASGITransport(app=cast(Any, app))
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", timeout=5.0
        ) as client:
            return await client.patch(f"/managers/{manager_id}/tags", json=payload)
    finally:
        await app.router.shutdown()


async def _delete_manager(manager_id: int):
    await app.router.startup()
    try:
        transport = httpx.ASGITransport(app=cast(Any, app))
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", timeout=5.0
        ) as client:
            return await client.delete(f"/managers/{manager_id}")
    finally:
        await app.router.shutdown()


def test_manager_empty_name_returns_400(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    resp = asyncio.run(_post_manager({"name": ""}))
    assert resp.status_code == 400
    payload = resp.json()
    assert payload["errors"][0]["field"] == "name"
    assert "required" in payload["errors"][0]["message"].lower()


def test_required_field_errors_contains_name_only():
    assert managers_module.REQUIRED_FIELD_ERRORS == {"name": "Name is required."}


def test_manager_invalid_cik_returns_400(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    resp = asyncio.run(_post_manager({"name": "Ada Lovelace", "cik": "123"}))
    assert resp.status_code == 400
    payload = resp.json()
    assert payload["errors"][0]["field"] == "cik"
    assert "10-digit" in payload["errors"][0]["message"].lower()


def test_manager_valid_record_is_stored(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    payload = {
        "name": "Elliott Investment Management L.P.",
        "cik": "0001791786",
        "jurisdictions": ["us"],
        "tags": ["activist"],
    }
    resp = asyncio.run(_post_manager(payload))
    assert resp.status_code == 201
    created = resp.json()
    assert created["manager_id"] > 0
    assert created["created_at"]
    assert created["updated_at"]
    conn = sqlite3.connect(db_path)
    try:
        # Verify the record was persisted with the expected values.
        row = conn.execute(
            "SELECT name, cik, jurisdictions, tags FROM managers WHERE id = ?",
            (created["manager_id"],),
        ).fetchone()
    finally:
        conn.close()
    assert row == (
        payload["name"],
        payload["cik"],
        '["us"]',
        '["activist"]',
    )


def test_manager_create_accepts_investment_payload_shape(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    payload = {
        "name": "Elliott Investment Management L.P.",
        "cik": "0001791786",
        "jurisdictions": ["us"],
        "tags": ["activist"],
    }

    resp = asyncio.run(_post_manager(payload))
    assert resp.status_code == 201
    body = resp.json()
    assert body["name"] == payload["name"]
    assert body["cik"] == payload["cik"]
    assert body["jurisdictions"] == payload["jurisdictions"]
    assert body["tags"] == payload["tags"]


def test_manager_list_returns_paginated_results(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    payloads = [
        {"name": "Manager A", "jurisdictions": ["us"]},
        {"name": "Manager B", "jurisdictions": ["uk"]},
        {"name": "Manager C", "jurisdictions": ["ca"]},
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
    assert names == ["Manager B", "Manager C"]


def test_manager_list_limit_offset_zero_returns_all(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    payloads = [
        {"name": "Manager A", "jurisdictions": ["us"]},
        {"name": "Manager B", "jurisdictions": ["uk"]},
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
    assert [item["name"] for item in body["items"]] == ["Manager A", "Manager B"]


def test_manager_list_returns_ordered_by_id(tmp_path, monkeypatch):
    # Keep ordering deterministic by asserting the list follows insertion order (id ascending).
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    payloads = [
        {"name": "Zeta Manager", "jurisdictions": ["us"]},
        {"name": "Alpha Manager", "jurisdictions": ["uk"]},
        {"name": "Omega Manager", "jurisdictions": ["ca"]},
    ]
    for payload in payloads:
        resp = asyncio.run(_post_manager(payload))
        assert resp.status_code == 201

    resp = asyncio.run(_get_managers({"limit": 3, "offset": 0}))
    assert resp.status_code == 200
    body = resp.json()
    assert [item["name"] for item in body["items"]] == [payload["name"] for payload in payloads]


def test_manager_list_defaults_return_empty_page(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    resp = asyncio.run(_get_managers())
    assert resp.status_code == 200
    body = resp.json()
    assert body["items"] == []
    assert body["total"] == 0
    # Default limit should be reflected even when no data exists.
    assert body["limit"] == 25
    assert body["offset"] == 0


def test_manager_list_defaults_return_first_page(tmp_path, monkeypatch):
    # Default list behavior should return a 25-item page when limit is omitted.
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    for idx in range(30):
        resp = asyncio.run(
            _post_manager(
                {"name": f"Manager {idx}", "jurisdictions": ["us"]},
            )
        )
        assert resp.status_code == 201

    resp = asyncio.run(_get_managers())
    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 30
    assert body["limit"] == 25
    assert body["offset"] == 0
    assert [item["name"] for item in body["items"]] == [f"Manager {idx}" for idx in range(25)]


def test_manager_list_returns_new_manager_shape(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    resp = asyncio.run(
        _post_manager(
            {
                "name": "Elliott Investment Management L.P.",
                "cik": "0001791786",
                "jurisdictions": ["us"],
                "tags": ["activist"],
            }
        )
    )
    assert resp.status_code == 201

    listed = asyncio.run(_get_managers())
    assert listed.status_code == 200
    item = listed.json()["items"][0]
    assert set(item.keys()) == {
        "manager_id",
        "name",
        "cik",
        "lei",
        "aliases",
        "jurisdictions",
        "tags",
        "registry_ids",
        "created_at",
        "updated_at",
    }
    assert item["name"] == "Elliott Investment Management L.P."
    assert item["cik"] == "0001791786"
    assert item["lei"] is None
    assert item["aliases"] == []
    assert item["jurisdictions"] == ["us"]
    assert item["tags"] == ["activist"]
    assert item["registry_ids"] == {}


def test_manager_list_filter_by_jurisdiction_returns_subset(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    # Ensure list filtering includes only managers from the requested jurisdiction.
    payloads = [
        {"name": "Manager A", "jurisdictions": ["us"]},
        {"name": "Manager B", "jurisdictions": ["us"]},
        {"name": "Manager C", "jurisdictions": ["uk"]},
    ]
    for payload in payloads:
        resp = asyncio.run(_post_manager(payload))
        assert resp.status_code == 201

    resp = asyncio.run(_get_managers({"jurisdiction": "us"}))
    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 2
    assert [item["name"] for item in body["items"]] == ["Manager A", "Manager B"]
    assert {tuple(item["jurisdictions"]) for item in body["items"]} == {("us",)}


def test_manager_list_filter_by_tag_returns_subset(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    payloads = [
        {"name": "Manager A", "jurisdictions": ["us"], "tags": ["activist"]},
        {"name": "Manager B", "jurisdictions": ["uk"], "tags": ["quant"]},
        {"name": "Manager C", "jurisdictions": ["ca"], "tags": ["activist"]},
    ]
    for payload in payloads:
        resp = asyncio.run(_post_manager(payload))
        assert resp.status_code == 201

    resp = asyncio.run(_get_managers({"tag": "activist"}))
    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 2
    assert [item["name"] for item in body["items"]] == ["Manager A", "Manager C"]
    assert {tuple(item["tags"]) for item in body["items"]} == {("activist",)}


def test_manager_list_filter_by_jurisdiction_and_tag_returns_subset(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    payloads = [
        {"name": "Manager A", "jurisdictions": ["us"], "tags": ["activist"]},
        {"name": "Manager B", "jurisdictions": ["us"], "tags": ["quant"]},
        {"name": "Manager C", "jurisdictions": ["uk"], "tags": ["activist"]},
    ]
    for payload in payloads:
        resp = asyncio.run(_post_manager(payload))
        assert resp.status_code == 201

    resp = asyncio.run(_get_managers({"jurisdiction": "us", "tag": "activist"}))
    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 1
    assert [item["name"] for item in body["items"]] == ["Manager A"]
    assert body["items"][0]["jurisdictions"] == ["us"]
    assert body["items"][0]["tags"] == ["activist"]


def test_manager_list_filter_with_pagination_returns_expected_page(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    payloads = [
        {"name": "Manager A", "jurisdictions": ["us"], "tags": ["activist"]},
        {"name": "Manager B", "jurisdictions": ["us"], "tags": ["activist"]},
        {"name": "Manager C", "jurisdictions": ["us"], "tags": ["activist"]},
        {"name": "Manager D", "jurisdictions": ["uk"], "tags": ["activist"]},
    ]
    for payload in payloads:
        resp = asyncio.run(_post_manager(payload))
        assert resp.status_code == 201

    resp = asyncio.run(
        _get_managers({"jurisdiction": "us", "tag": "activist", "limit": 1, "offset": 1})
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 3
    assert body["limit"] == 1
    assert body["offset"] == 1
    assert [item["name"] for item in body["items"]] == ["Manager B"]


def test_manager_list_filter_trims_whitespace_values(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    payloads = [
        {"name": "Manager A", "jurisdictions": ["us"], "tags": ["activist"]},
        {"name": "Manager B", "jurisdictions": ["us"], "tags": ["quant"]},
        {"name": "Manager C", "jurisdictions": ["uk"], "tags": ["activist"]},
    ]
    for payload in payloads:
        resp = asyncio.run(_post_manager(payload))
        assert resp.status_code == 201

    resp = asyncio.run(_get_managers({"jurisdiction": " us ", "tag": " activist "}))
    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 1
    assert [item["name"] for item in body["items"]] == ["Manager A"]


def test_manager_list_filter_whitespace_only_treated_as_no_filter(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    payloads = [
        {"name": "Manager A", "jurisdictions": ["us"], "tags": ["activist"]},
        {"name": "Manager B", "jurisdictions": ["uk"], "tags": ["quant"]},
    ]
    for payload in payloads:
        resp = asyncio.run(_post_manager(payload))
        assert resp.status_code == 201

    resp = asyncio.run(_get_managers({"jurisdiction": "   ", "tag": "   "}))
    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 2
    assert [item["name"] for item in body["items"]] == ["Manager A", "Manager B"]


def test_manager_stats_returns_accurate_jurisdiction_and_tag_counts(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    payloads = [
        {
            "name": "Manager A",
            "cik": "0001067983",
            "jurisdictions": ["us"],
            "tags": ["activist", "hedge-fund"],
        },
        {
            "name": "Manager B",
            "lei": "549300U3N12T57QLOU60",
            "jurisdictions": ["us", "uk"],
            "tags": ["quant"],
        },
        {
            "name": "Manager C",
            "cik": "0001350694",
            "lei": "529900K8VCB4TCB4UQ57",
            "jurisdictions": ["uk"],
            "tags": ["activist"],
        },
        {"name": "Manager D"},
    ]
    for payload in payloads:
        resp = asyncio.run(_post_manager(payload))
        assert resp.status_code == 201

    stats_resp = asyncio.run(_get_manager_stats())
    assert stats_resp.status_code == 200
    assert stats_resp.json() == {
        "total_managers": 4,
        "by_jurisdiction": {"uk": 2, "us": 2},
        "by_tag": {"activist": 2, "hedge-fund": 1, "quant": 1},
        "with_cik": 2,
        "with_lei": 2,
    }


def test_manager_list_invalid_limit_returns_400(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    resp = asyncio.run(_get_managers({"limit": 0}))
    assert resp.status_code == 400
    payload = resp.json()
    assert payload["error"][0]["field"] == "limit"
    assert payload["errors"][0]["field"] == "limit"
    assert payload["error"] == payload["errors"]
    assert "greater" in payload["errors"][0]["message"].lower()


def test_manager_list_negative_limit_returns_400(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    resp = asyncio.run(_get_managers({"limit": -1}))
    assert resp.status_code == 400
    payload = resp.json()
    assert payload["error"][0]["field"] == "limit"
    assert payload["errors"][0]["field"] == "limit"
    assert payload["error"] == payload["errors"]
    assert "greater" in payload["errors"][0]["message"].lower()


def test_manager_list_limit_above_max_returns_400(tmp_path, monkeypatch):
    # Upper-bound validation keeps pagination requests predictable.
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    resp = asyncio.run(_get_managers({"limit": 101}))
    assert resp.status_code == 400
    payload = resp.json()
    assert payload["error"][0]["field"] == "limit"
    assert payload["errors"][0]["field"] == "limit"
    assert payload["error"] == payload["errors"]
    assert "less" in payload["errors"][0]["message"].lower()


def test_manager_list_offset_beyond_total_returns_empty_page(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    payloads = [
        {"name": "Manager A", "jurisdictions": ["us"]},
        {"name": "Manager B", "jurisdictions": ["uk"]},
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


def test_manager_list_invalid_offset_returns_400(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    resp = asyncio.run(_get_managers({"offset": -1}))
    assert resp.status_code == 400
    payload = resp.json()
    assert payload["error"][0]["field"] == "offset"
    assert payload["errors"][0]["field"] == "offset"
    assert payload["error"] == payload["errors"]


def test_manager_list_invalid_limit_and_offset_returns_400(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    resp = asyncio.run(_get_managers({"limit": 0, "offset": -1}))
    assert resp.status_code == 400
    payload = resp.json()
    fields = {entry["field"] for entry in payload["errors"]}
    assert fields == {"limit", "offset"}
    assert payload["error"] == payload["errors"]


def test_manager_get_returns_single_manager(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    payload = {"name": "Elliott Investment Management L.P.", "tags": ["activist"]}
    resp = asyncio.run(_post_manager(payload))
    created = resp.json()

    get_resp = asyncio.run(_get_manager(created["manager_id"]))
    assert get_resp.status_code == 200
    fetched = get_resp.json()
    assert fetched["manager_id"] == created["manager_id"]
    assert fetched["name"] == payload["name"]
    assert fetched["tags"] == payload["tags"]


def test_manager_get_returns_full_investment_manager_record(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    payload = {
        "name": "Elliott Investment Management L.P.",
        "cik": "0001791786",
        "lei": "549300U3N12T57QLOU60",
        "aliases": ["Elliott Management"],
        "jurisdictions": ["us"],
        "tags": ["activist"],
        "registry_ids": {"fca_frn": "122927"},
    }
    resp = asyncio.run(_post_manager(payload))
    assert resp.status_code == 201
    created = resp.json()

    get_resp = asyncio.run(_get_manager(created["manager_id"]))
    assert get_resp.status_code == 200
    fetched = get_resp.json()
    assert fetched["manager_id"] == created["manager_id"]
    assert fetched["name"] == payload["name"]
    assert fetched["cik"] == payload["cik"]
    assert fetched["lei"] == payload["lei"]
    assert fetched["aliases"] == payload["aliases"]
    assert fetched["jurisdictions"] == payload["jurisdictions"]
    assert fetched["tags"] == payload["tags"]
    assert fetched["registry_ids"] == payload["registry_ids"]
    assert fetched["created_at"]
    assert fetched["updated_at"]


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


def test_manager_create_db_unavailable_returns_503(monkeypatch):
    def _raise_db_error(*_args, **_kwargs):
        raise sqlite3.OperationalError("db down")

    monkeypatch.setattr("api.managers.connect_db", _raise_db_error)
    resp = asyncio.run(_post_manager({"name": "Fail"}))
    assert resp.status_code == 503
    payload = resp.json()
    assert payload["detail"] == "Database unavailable"
    assert "down" not in payload["detail"].lower()


def test_manager_list_db_unavailable_returns_503(monkeypatch):
    def _raise_db_error(*_args, **_kwargs):
        raise sqlite3.OperationalError("db down")

    monkeypatch.setattr("api.managers.connect_db", _raise_db_error)
    resp = asyncio.run(_get_managers())
    assert resp.status_code == 503
    payload = resp.json()
    assert payload["detail"] == "Database unavailable"
    assert "down" not in payload["detail"].lower()


def test_manager_get_db_unavailable_returns_503(monkeypatch):
    def _raise_db_error(*_args, **_kwargs):
        raise sqlite3.OperationalError("db down")

    monkeypatch.setattr("api.managers.connect_db", _raise_db_error)
    resp = asyncio.run(_get_manager(1))
    assert resp.status_code == 503
    payload = resp.json()
    assert payload["detail"] == "Database unavailable"
    assert "down" not in payload["detail"].lower()


def test_manager_patch_updates_fields(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    create_resp = asyncio.run(
        _post_manager(
            {
                "name": "Elliott Investment Management L.P.",
                "cik": "0001791786",
                "jurisdictions": ["us"],
                "tags": ["activist"],
            }
        )
    )
    assert create_resp.status_code == 201
    manager_id = create_resp.json()["manager_id"]

    patch_resp = asyncio.run(
        _patch_manager(
            manager_id,
            {
                "lei": "549300U3N12T57QLOU60",
                "aliases": ["Elliott Management"],
                "tags": ["event-driven"],
            },
        )
    )
    assert patch_resp.status_code == 200
    body = patch_resp.json()
    assert body["manager_id"] == manager_id
    assert body["name"] == "Elliott Investment Management L.P."
    assert body["cik"] == "0001791786"
    assert body["lei"] == "549300U3N12T57QLOU60"
    assert body["aliases"] == ["Elliott Management"]
    assert body["jurisdictions"] == ["us"]
    assert body["tags"] == ["event-driven"]


def test_manager_patch_rejects_invalid_cik(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    create_resp = asyncio.run(_post_manager({"name": "Elliott Investment Management L.P."}))
    assert create_resp.status_code == 201
    manager_id = create_resp.json()["manager_id"]

    patch_resp = asyncio.run(_patch_manager(manager_id, {"cik": "123"}))
    assert patch_resp.status_code == 400
    payload = patch_resp.json()
    assert payload["errors"][0]["field"] == "cik"
    assert "10-digit" in payload["errors"][0]["message"].lower()


def test_manager_patch_requires_at_least_one_field(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    create_resp = asyncio.run(_post_manager({"name": "Elliott Investment Management L.P."}))
    assert create_resp.status_code == 201
    manager_id = create_resp.json()["manager_id"]

    patch_resp = asyncio.run(_patch_manager(manager_id, {}))
    assert patch_resp.status_code == 400
    payload = patch_resp.json()
    assert payload["errors"][0]["field"] == "body"


def test_manager_patch_returns_404_for_missing_id(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    patch_resp = asyncio.run(_patch_manager(999, {"tags": ["activist"]}))
    assert patch_resp.status_code == 404
    assert patch_resp.json()["detail"] == "Manager not found"


def test_manager_tags_patch_adds_and_removes_without_replacing_record(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    create_resp = asyncio.run(
        _post_manager(
            {
                "name": "Elliott Investment Management L.P.",
                "cik": "0001791786",
                "jurisdictions": ["us"],
                "tags": ["activist", "quant"],
            }
        )
    )
    assert create_resp.status_code == 201
    manager_id = create_resp.json()["manager_id"]

    patch_resp = asyncio.run(
        _patch_manager_tags(
            manager_id,
            {
                "add": ["event-driven", "activist"],
                "remove": ["quant"],
            },
        )
    )
    assert patch_resp.status_code == 200
    body = patch_resp.json()
    assert body["manager_id"] == manager_id
    assert body["name"] == "Elliott Investment Management L.P."
    assert body["cik"] == "0001791786"
    assert body["jurisdictions"] == ["us"]
    assert body["tags"] == ["activist", "event-driven"]


def test_manager_tags_patch_requires_non_empty_add_or_remove(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    create_resp = asyncio.run(_post_manager({"name": "Elliott Investment Management L.P."}))
    assert create_resp.status_code == 201
    manager_id = create_resp.json()["manager_id"]

    patch_resp = asyncio.run(_patch_manager_tags(manager_id, {"add": ["   "], "remove": []}))
    assert patch_resp.status_code == 400
    payload = patch_resp.json()
    assert payload["errors"][0]["field"] == "body"


def test_manager_tags_patch_returns_404_for_missing_id(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))

    patch_resp = asyncio.run(_patch_manager_tags(999, {"add": ["activist"]}))
    assert patch_resp.status_code == 404
    assert patch_resp.json()["detail"] == "Manager not found"


def test_manager_tags_patch_supports_remove_only(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    create_resp = asyncio.run(
        _post_manager(
            {
                "name": "Elliott Investment Management L.P.",
                "cik": "0001791786",
                "jurisdictions": ["us"],
                "tags": ["activist", "quant"],
            }
        )
    )
    assert create_resp.status_code == 201
    manager_id = create_resp.json()["manager_id"]

    patch_resp = asyncio.run(_patch_manager_tags(manager_id, {"remove": ["quant"]}))
    assert patch_resp.status_code == 200
    body = patch_resp.json()
    assert body["manager_id"] == manager_id
    assert body["name"] == "Elliott Investment Management L.P."
    assert body["cik"] == "0001791786"
    assert body["jurisdictions"] == ["us"]
    assert body["tags"] == ["activist"]


def test_manager_tags_patch_supports_add_only_and_dedupes(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    create_resp = asyncio.run(
        _post_manager(
            {
                "name": "Elliott Investment Management L.P.",
                "cik": "0001791786",
                "jurisdictions": ["us"],
                "tags": ["activist"],
            }
        )
    )
    assert create_resp.status_code == 201
    manager_id = create_resp.json()["manager_id"]

    patch_resp = asyncio.run(
        _patch_manager_tags(manager_id, {"add": ["event-driven", "event-driven", "  "]})
    )
    assert patch_resp.status_code == 200
    body = patch_resp.json()
    assert body["manager_id"] == manager_id
    assert body["name"] == "Elliott Investment Management L.P."
    assert body["cik"] == "0001791786"
    assert body["jurisdictions"] == ["us"]
    assert body["tags"] == ["activist", "event-driven"]


def test_manager_tags_patch_noop_preserves_updated_at(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    create_resp = asyncio.run(
        _post_manager(
            {
                "name": "Elliott Investment Management L.P.",
                "cik": "0001791786",
                "jurisdictions": ["us"],
                "tags": ["activist"],
            }
        )
    )
    assert create_resp.status_code == 201
    created = create_resp.json()
    manager_id = created["manager_id"]
    original_updated_at = created["updated_at"]

    patch_resp = asyncio.run(
        _patch_manager_tags(
            manager_id,
            {"add": ["activist", "activist"], "remove": ["not-present"]},
        )
    )
    assert patch_resp.status_code == 200
    body = patch_resp.json()
    assert body["manager_id"] == manager_id
    assert body["tags"] == ["activist"]
    assert body["updated_at"] == original_updated_at


def test_manager_delete_removes_record(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    create_resp = asyncio.run(_post_manager({"name": "Elliott Investment Management L.P."}))
    assert create_resp.status_code == 201
    manager_id = create_resp.json()["manager_id"]

    delete_resp = asyncio.run(_delete_manager(manager_id))
    assert delete_resp.status_code == 204
    assert delete_resp.text == ""

    fetch_resp = asyncio.run(_get_manager(manager_id))
    assert fetch_resp.status_code == 404


def test_manager_delete_returns_404_for_missing_id(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    delete_resp = asyncio.run(_delete_manager(999))
    assert delete_resp.status_code == 404
    assert delete_resp.json()["detail"] == "Manager not found"
