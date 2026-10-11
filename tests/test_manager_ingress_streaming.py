"""Verify bulk ingress byte limits across real ASGI request boundaries."""

import asyncio
import json
import sqlite3
from contextlib import closing

import pytest

from api import managers
from api.chat import app


def post_chunks(chunks, content_type, content_length=None):
    headers = [(b"content-type", content_type)]
    if content_length is not None:
        headers.append((b"content-length", content_length))
    consumed = []
    messages = []

    async def receive():
        index = len(consumed)
        assert index < len(chunks), "request read beyond the final body chunk"
        consumed.append(index)
        return {
            "type": "http.request",
            "body": chunks[index],
            "more_body": index < len(chunks) - 1,
        }

    async def send(message):
        messages.append(message)

    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "POST",
        "scheme": "http",
        "path": "/api/managers/bulk",
        "raw_path": b"/api/managers/bulk",
        "query_string": b"",
        "root_path": "",
        "headers": headers,
        "client": ("127.0.0.1", 123),
        "server": ("test", 80),
    }
    asyncio.run(app(scope, receive, send))
    status = next(
        message["status"] for message in messages if message["type"] == "http.response.start"
    )
    body = b"".join(
        message.get("body", b"") for message in messages if message["type"] == "http.response.body"
    )
    return status, json.loads(body), consumed


@pytest.fixture(params=[b"application/json", b"text/csv"], ids=["json", "csv"])
def payload(request):
    name = "Élan" * 10
    if request.param == b"text/csv":
        body = f"name\n{name}\n".encode()
    else:
        body = json.dumps([{"name": name}], ensure_ascii=False).encode()
    return request.param, body, name


@pytest.mark.parametrize("content_length", [None, b"bad", b"1"])
@pytest.mark.parametrize("chunk_sizes", [(21,), (15, 10), (10, 10, 1)])
def test_bulk_stream_stops_on_limit_crossing(monkeypatch, payload, content_length, chunk_sizes):
    monkeypatch.setenv("BULK_IMPORT_MAX_BYTES", "20")

    def forbid_storage():
        raise AssertionError("oversized streamed request reached storage")

    monkeypatch.setattr(managers, "connect_db", forbid_storage)
    content_type, body, _ = payload
    chunks = []
    offset = 0
    for size in chunk_sizes:
        chunks.append(body[offset : offset + size])
        offset += size
    chunks.append(body[offset:])

    status, response, consumed = post_chunks(chunks, content_type, content_length)

    assert status == 413
    assert "payload exceeds 20 bytes" in response["errors"][0]["message"].lower()
    assert consumed == list(range(len(chunk_sizes))), "read beyond the first oversized chunk"


def test_bulk_stream_accepts_exact_byte_limit(tmp_path, monkeypatch, payload):
    content_type, body, name = payload
    db_path = tmp_path / "managers.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.setenv("BULK_IMPORT_MAX_BYTES", str(len(body)))
    # UTF-8 makes the byte limit distinct from the character count.
    assert len(body) > len(body.decode())

    status, response, consumed = post_chunks([body[:10], body[10:]], content_type, b"bad")

    assert status == 200
    assert response["succeeded"] == 1
    assert consumed == [0, 1]
    with closing(sqlite3.connect(db_path)) as conn:
        assert conn.execute("SELECT name FROM managers").fetchall() == [(name,)]


def test_declared_oversize_rejects_before_first_receive(monkeypatch, payload):
    monkeypatch.setenv("BULK_IMPORT_MAX_BYTES", "20")

    def forbid_storage():
        raise AssertionError("declared oversized request reached storage")

    monkeypatch.setattr(managers, "connect_db", forbid_storage)
    content_type, body, _ = payload

    status, response, consumed = post_chunks([body], content_type, b"21")

    assert status == 413
    assert "payload exceeds 20 bytes" in response["errors"][0]["message"].lower()
    assert consumed == []
