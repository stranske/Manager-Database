import asyncio
import json
import sqlite3
import sys
from pathlib import Path

import pytest
from fastapi import Request
from fastapi.exceptions import RequestValidationError
from pydantic import ValidationError

sys.path.append(str(Path(__file__).resolve().parents[1]))

from api.chat import ManagerCreate, _create_manager_record, _validation_exception_handler
from adapters.base import connect_db


def _run_validation(payload):
    """Exercise validation errors without invoking the ASGI stack."""
    try:
        ManagerCreate(**payload)
    except ValidationError as exc:
        request_error = RequestValidationError(exc.errors())
        response = asyncio.run(
            _validation_exception_handler(Request({"type": "http"}), request_error)
        )
        return response
    pytest.fail("Expected validation error")


def test_create_manager_rejects_empty_name(tmp_path, monkeypatch):
    # Point the API to a temporary SQLite DB so inserts are isolated per test.
    db_path = tmp_path / "managers.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.delenv("DB_URL", raising=False)

    response = _run_validation(
        {"name": " ", "email": "alex@example.com", "department": "Ops"}
    )
    assert response.status_code == 400
    payload = json.loads(response.body.decode("utf-8"))
    assert {"field": "name", "message": "Name is required."} in payload["errors"]


def test_create_manager_rejects_invalid_email(tmp_path, monkeypatch):
    db_path = tmp_path / "managers.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.delenv("DB_URL", raising=False)

    response = _run_validation(
        {"name": "Alex", "email": "not-an-email", "department": "Ops"}
    )
    assert response.status_code == 400
    payload = json.loads(response.body.decode("utf-8"))
    assert {"field": "email", "message": "Email must be a valid email address."} in payload[
        "errors"
    ]


def test_create_manager_persists_record(tmp_path, monkeypatch):
    db_path = tmp_path / "managers.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.delenv("DB_URL", raising=False)

    payload = _create_manager_record(
        ManagerCreate(name="Alex", email="alex@example.com", department="Ops")
    )
    assert payload["name"] == "Alex"
    assert payload["email"] == "alex@example.com"
    assert payload["department"] == "Ops"

    conn = connect_db(str(db_path))
    try:
        if isinstance(conn, sqlite3.Connection):
            row = conn.execute(
                "SELECT name, email, department FROM managers WHERE id = ?",
                (payload["id"],),
            ).fetchone()
        else:
            row = conn.execute(
                "SELECT name, email, department FROM managers WHERE id = %s",
                (payload["id"],),
            ).fetchone()
    finally:
        conn.close()

    assert row == ("Alex", "alex@example.com", "Ops")
