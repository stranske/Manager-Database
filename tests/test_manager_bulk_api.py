import asyncio
import logging
import sqlite3
import sys
from pathlib import Path

import httpx

sys.path.append(str(Path(__file__).resolve().parents[1]))

from api.chat import app


async def _post_bulk_json(payload: list[dict] | None):
    # Use ASGI transport to avoid spinning up a server for bulk import tests.
    await app.router.startup()
    try:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", timeout=5.0
        ) as client:
            if payload is None:
                return await client.post("/api/managers/bulk")
            return await client.post("/api/managers/bulk", json=payload)
    finally:
        await app.router.shutdown()


async def _post_bulk_csv(contents: str):
    # Post raw CSV payloads with the text/csv content type.
    await app.router.startup()
    try:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", timeout=5.0
        ) as client:
            return await client.post(
                "/api/managers/bulk",
                content=contents,
                headers={"content-type": "text/csv"},
            )
    finally:
        await app.router.shutdown()


def test_bulk_json_imports_valid_records(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    payloads = [
        {"name": "Grace Hopper", "role": "Engineering Director"},
        {"name": "", "role": "Operations Lead"},
        {"name": "Ada Lovelace", "role": "Research Lead", "department": "R&D"},
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
        rows = conn.execute("SELECT name, role FROM managers ORDER BY id").fetchall()
    finally:
        conn.close()
    assert rows == [
        ("Grace Hopper", "Engineering Director"),
        ("Ada Lovelace", "Research Lead"),
    ]


def test_bulk_csv_import_logs_invalid_rows(tmp_path, monkeypatch, caplog):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    csv_payload = "\n".join(
        [
            "name,role,department",
            "Grace Hopper,Engineering Director,Engineering",
            "Missing Role,,Operations",
        ]
    )

    with caplog.at_level(logging.WARNING, logger="api.managers"):
        resp = asyncio.run(_post_bulk_csv(csv_payload))

    assert resp.status_code == 200
    body = resp.json()
    assert body["succeeded"] == 1
    assert body["failed"] == 1
    assert "Bulk import validation failed" in caplog.text

    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute("SELECT name, role FROM managers ORDER BY id").fetchall()
    finally:
        conn.close()
    assert rows == [("Grace Hopper", "Engineering Director")]


def test_bulk_csv_import_rejects_missing_headers(tmp_path, monkeypatch, caplog):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    csv_payload = "\n".join(
        [
            "name,department",
            "Grace Hopper,Engineering",
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
    payloads = [{"name": f"Manager {idx}", "role": "Team Lead"} for idx in range(105)]

    resp = asyncio.run(_post_bulk_json(payloads))
    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 105
    assert body["succeeded"] == 105
    assert body["failed"] == 0


def test_bulk_import_requires_payload(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))

    resp = asyncio.run(_post_bulk_json(None))
    assert resp.status_code == 400
    payload = resp.json()
    assert payload["errors"][0]["field"] == "body"
