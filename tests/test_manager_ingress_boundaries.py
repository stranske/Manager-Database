"""Protect ingress validation before malformed manager requests reach storage."""

import asyncio
import json
import logging

from starlette.requests import Request

from api import managers


def request_body(body, content_type, content_length=None):
    headers = [(b"content-type", content_type.encode())]
    if content_length is not None:
        headers.append((b"content-length", content_length.encode()))

    async def receive():
        return {"type": "http.request", "body": body, "more_body": False}

    return Request({"type": "http", "headers": headers}, receive)


def forbid_storage():
    raise AssertionError("invalid request reached database")


def test_invalid_bulk_limit_uses_safe_default_and_warns(monkeypatch, caplog):
    monkeypatch.setenv("BULK_IMPORT_MAX_BYTES", "not-an-integer")
    with caplog.at_level(logging.WARNING, logger=managers.__name__):
        actual = managers._bulk_import_max_bytes()
    assert actual == 2_000_000, "invalid limit lost safe default"
    assert "Invalid BULK_IMPORT_MAX_BYTES value: not-an-integer" in caplog.text


def test_empty_csv_reports_missing_name_header_before_storage(monkeypatch):
    monkeypatch.setattr(managers, "connect_db", forbid_storage)
    response = asyncio.run(managers.bulk_import_managers(request_body(b"", "text/csv")))
    assert response.status_code == 400
    assert json.loads(response.body)["errors"] == [
        {"field": "body", "message": "CSV payload missing required headers: name"}
    ], "empty CSV lost missing-header diagnosis"


def test_invalid_patch_cik_is_rejected_before_storage(monkeypatch):
    monkeypatch.setattr(managers, "connect_db", forbid_storage)
    response = asyncio.run(managers.patch_manager(managers.ManagerUpdate(cik="123"), id=7))
    assert response.status_code == 400
    body = json.loads(response.body)
    expected = [{"field": "cik", "message": "CIK must be a 10-digit zero-padded string."}]
    assert body["errors"] == expected
    assert body["error"] == expected


def test_invalid_content_length_still_bounds_actual_bytes_before_storage(monkeypatch):
    monkeypatch.setenv("BULK_IMPORT_MAX_BYTES", "20")
    monkeypatch.setattr(managers, "connect_db", forbid_storage)
    body = b'[{"name":"' + b"X" * 30 + b'"}]'
    response = asyncio.run(
        managers.bulk_import_managers(request_body(body, "application/json", "not-an-integer"))
    )
    assert response.status_code == 413, "actual bytes were not bounded"
    assert "payload exceeds 20 bytes" in json.loads(response.body)["errors"][0]["message"].lower()
