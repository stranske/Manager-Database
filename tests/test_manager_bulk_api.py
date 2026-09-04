import asyncio
import logging
import sqlite3
import sys
from pathlib import Path
from typing import Any, cast

import httpx

sys.path.append(str(Path(__file__).resolve().parents[1]))

from api.chat import app


async def _post_bulk_json(payload: object | None):
    # Use ASGI transport to avoid spinning up a server for bulk import tests.
    await cast(Any, app.router).startup()
    try:
        transport = httpx.ASGITransport(app=cast(Any, app))
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", timeout=5.0
        ) as client:
            if payload is None:
                return await client.post("/api/managers/bulk")
            return await client.post("/api/managers/bulk", json=payload)
    finally:
        await cast(Any, app.router).shutdown()


async def _post_bulk_raw(
    contents: bytes,
    *,
    content_type: str,
    headers: dict[str, str] | None = None,
):
    """Post exact request bytes so malformed-input boundaries stay observable."""
    await cast(Any, app.router).startup()
    try:
        transport = httpx.ASGITransport(app=cast(Any, app))
        request_headers = {"content-type": content_type, **(headers or {})}
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", timeout=5.0
        ) as client:
            return await client.post(
                "/api/managers/bulk", content=contents, headers=request_headers
            )
    finally:
        await cast(Any, app.router).shutdown()


async def _post_bulk_csv(contents: str):
    # Post raw CSV payloads with the text/csv content type.
    await cast(Any, app.router).startup()
    try:
        transport = httpx.ASGITransport(app=cast(Any, app))
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", timeout=5.0
        ) as client:
            return await client.post(
                "/api/managers/bulk",
                content=contents,
                headers={"content-type": "text/csv"},
            )
    finally:
        await cast(Any, app.router).shutdown()


def test_bulk_json_imports_valid_records(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    payloads = [
        {"name": "Manager A", "jurisdictions": ["us"]},
        {"name": "", "jurisdictions": ["us"]},
        {"name": "Manager B", "jurisdictions": ["uk"], "tags": ["quant"]},
    ]

    resp = asyncio.run(_post_bulk_json(payloads))
    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 3
    assert body["succeeded"] == 2
    assert body["failed"] == 1
    assert {item["index"] for item in body["successes"]} == {0, 2}
    assert body["failures"][0]["index"] == 1

    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute("SELECT name, jurisdictions, tags FROM managers ORDER BY id").fetchall()
    finally:
        conn.close()
    assert rows == [
        ("Manager A", '["us"]', "[]"),
        ("Manager B", '["uk"]', '["quant"]'),
    ]


def test_bulk_json_import_persists_investment_manager_fields(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    payloads = [
        {
            "name": "Elliott Investment Management L.P.",
            "cik": "0001791786",
            "lei": "549300U3N12T57QLOU60",
            "aliases": ["Elliott Management"],
            "jurisdictions": ["us"],
            "tags": ["activist"],
            "registry_ids": {"fca_frn": "122927"},
        }
    ]

    resp = asyncio.run(_post_bulk_json(payloads))
    assert resp.status_code == 200
    body = resp.json()
    assert body["succeeded"] == 1
    assert body["failed"] == 0
    manager = body["successes"][0]["manager"]
    assert manager["name"] == payloads[0]["name"]
    assert manager["cik"] == payloads[0]["cik"]
    assert manager["lei"] == payloads[0]["lei"]
    assert manager["aliases"] == payloads[0]["aliases"]
    assert manager["jurisdictions"] == payloads[0]["jurisdictions"]
    assert manager["tags"] == payloads[0]["tags"]
    assert manager["registry_ids"] == payloads[0]["registry_ids"]

    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            "SELECT name, cik, lei, aliases, jurisdictions, tags, registry_ids FROM managers"
        ).fetchone()
    finally:
        conn.close()
    assert row == (
        "Elliott Investment Management L.P.",
        "0001791786",
        "549300U3N12T57QLOU60",
        '["Elliott Management"]',
        '["us"]',
        '["activist"]',
        '{"fca_frn": "122927"}',
    )


def test_bulk_csv_import_logs_invalid_rows(tmp_path, monkeypatch, caplog):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    csv_payload = "\n".join(
        [
            "name,cik,jurisdictions",
            'Manager A,0001791786,["us"]',
            "Missing Name,0001791787,[]",
            ',0001791786,["us"]',
        ]
    )

    with caplog.at_level(logging.WARNING, logger="api.managers"):
        resp = asyncio.run(_post_bulk_csv(csv_payload))

    assert resp.status_code == 200
    body = resp.json()
    assert body["succeeded"] == 2
    assert body["failed"] == 1
    assert "Bulk import CSV record missing required values" in caplog.text

    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute("SELECT name, cik, jurisdictions FROM managers ORDER BY id").fetchall()
    finally:
        conn.close()
    assert rows == [("Manager A", "0001791786", '["us"]'), ("Missing Name", "0001791787", "[]")]


def test_bulk_csv_import_parses_investment_manager_field_names(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    csv_payload = "\n".join(
        [
            "name,cik,lei,aliases,jurisdictions,tags,registry_ids",
            (
                "Elliott Investment Management L.P.,0001791786,549300U3N12T57QLOU60,"
                '"[""Elliott Management""]","[""us""]","[""activist""]","{""fca_frn"": ""122927""}"'
            ),
        ]
    )

    resp = asyncio.run(_post_bulk_csv(csv_payload))
    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 1
    assert body["succeeded"] == 1
    assert body["failed"] == 0
    manager = body["successes"][0]["manager"]
    assert manager["name"] == "Elliott Investment Management L.P."
    assert manager["cik"] == "0001791786"
    assert manager["lei"] == "549300U3N12T57QLOU60"
    assert manager["aliases"] == ["Elliott Management"]
    assert manager["jurisdictions"] == ["us"]
    assert manager["tags"] == ["activist"]
    assert manager["registry_ids"] == {"fca_frn": "122927"}


def test_bulk_csv_import_rejects_missing_headers(tmp_path, monkeypatch, caplog):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    csv_payload = "\n".join(
        [
            "cik,jurisdictions",
            '0001791786,["us"]',
        ]
    )

    with caplog.at_level(logging.WARNING, logger="api.managers"):
        resp = asyncio.run(_post_bulk_csv(csv_payload))

    assert resp.status_code == 400
    payload = resp.json()
    assert payload["errors"][0]["field"] == "body"
    assert "missing required headers" in payload["errors"][0]["message"].lower()
    assert "Bulk import CSV missing required headers" in caplog.text


def test_bulk_json_import_handles_large_batch(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    payloads = [{"name": f"Manager {idx}", "jurisdictions": ["us"]} for idx in range(105)]

    resp = asyncio.run(_post_bulk_json(payloads))
    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 105
    assert body["succeeded"] == 105
    assert body["failed"] == 0


def test_bulk_import_rejects_large_payload(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.setenv("BULK_IMPORT_MAX_BYTES", "50")
    payloads = [{"name": "X" * 100, "jurisdictions": ["us"]}]

    resp = asyncio.run(_post_bulk_json(payloads))

    assert resp.status_code == 413
    payload = resp.json()
    assert payload["errors"][0]["field"] == "body"
    assert "payload exceeds" in payload["errors"][0]["message"].lower()


def test_bulk_import_requires_payload(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))

    resp = asyncio.run(_post_bulk_json(None))
    assert resp.status_code == 400
    payload = resp.json()
    assert payload["errors"][0]["field"] == "body"


def test_bulk_json_rejects_invalid_utf8_as_bad_request(tmp_path, monkeypatch):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "dev.db"))

    resp = asyncio.run(_post_bulk_raw(b"\xff", content_type="application/json"))

    assert resp.status_code == 400
    assert resp.json()["errors"] == [
        {"field": "body", "message": "Request body must be valid JSON."}
    ]


def test_bulk_json_reports_non_object_and_invalid_records(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))

    resp = asyncio.run(
        _post_bulk_json(
            [
                "not-an-object",
                {"name": "Bad Types", "aliases": "not-a-list"},
                {"name": "Valid Manager", "jurisdictions": ["us"]},
            ]
        )
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 3
    assert body["succeeded"] == 1
    assert body["failed"] == 2
    assert [item["index"] for item in body["failures"]] == [0, 1]
    assert body["failures"][0]["errors"] == [
        {"field": "record", "message": "Record must be an object."}
    ]
    assert body["failures"][1]["errors"][0]["field"] == "aliases"

    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute("SELECT name FROM managers").fetchall()
    finally:
        conn.close()
    assert rows == [("Valid Manager",)]


def test_bulk_import_checks_actual_body_size_after_declared_length(tmp_path, monkeypatch):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "dev.db"))
    monkeypatch.setenv("BULK_IMPORT_MAX_BYTES", "50")
    contents = b'[{"name":"' + (b"X" * 100) + b'"}]'

    resp = asyncio.run(
        _post_bulk_raw(
            contents,
            content_type="application/json",
            headers={"content-length": "1"},
        )
    )

    assert resp.status_code == 413
    assert "payload exceeds 50 bytes" in resp.json()["errors"][0]["message"].lower()


def test_bulk_csv_rejects_invalid_utf8(tmp_path, monkeypatch):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "dev.db"))

    resp = asyncio.run(_post_bulk_raw(b"\xff", content_type="text/csv"))

    assert resp.status_code == 400
    assert resp.json()["errors"] == [
        {"field": "body", "message": "CSV payload must be UTF-8 encoded."}
    ]


def test_bulk_json_requires_an_array_body(tmp_path, monkeypatch):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "dev.db"))

    resp = asyncio.run(_post_bulk_json({"name": "not-an-array"}))

    assert resp.status_code == 400
    assert resp.json()["errors"] == [
        {"field": "body", "message": "Request body must be a JSON array."}
    ]


def test_bulk_csv_with_only_empty_records_is_rejected(tmp_path, monkeypatch):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "dev.db"))

    resp = asyncio.run(_post_bulk_csv("name,cik,tags\n,,\n"))

    assert resp.status_code == 400
    assert resp.json()["errors"] == [
        {"field": "body", "message": "No manager records were provided."}
    ]
