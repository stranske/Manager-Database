import sqlite3
import sys
from pathlib import Path

import pytest
from fastapi import HTTPException

sys.path.append(str(Path(__file__).resolve().parents[1]))

from api.chat import create_manager, validate_manager_payload


def test_create_manager_rejects_empty_name(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.delenv("DB_URL", raising=False)
    monkeypatch.setenv("DB_PATH", str(db_path))
    payload = {"name": "  ", "email": "valid@example.com", "department": "Research"}
    with pytest.raises(HTTPException) as excinfo:
        validate_manager_payload(payload)
    assert excinfo.value.status_code == 400
    errors = excinfo.value.detail
    assert any(err["field"] == "name" for err in errors)


def test_create_manager_rejects_invalid_email(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.delenv("DB_URL", raising=False)
    monkeypatch.setenv("DB_PATH", str(db_path))
    payload = {"name": "Ada Lovelace", "email": "invalid-email", "department": "Research"}
    with pytest.raises(HTTPException) as excinfo:
        validate_manager_payload(payload)
    assert excinfo.value.status_code == 400
    errors = excinfo.value.detail
    assert any(err["field"] == "email" for err in errors)


def test_create_manager_persists_valid_record(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.delenv("DB_URL", raising=False)
    monkeypatch.setenv("DB_PATH", str(db_path))
    payload = {"name": "Ada Lovelace", "email": "ada@example.com", "department": "Research"}
    manager = validate_manager_payload(payload)
    body = create_manager(manager)
    assert body["name"] == payload["name"]
    assert body["email"] == payload["email"]
    assert body["department"] == payload["department"]
    # Verify the row was persisted with the expected fields.
    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            "SELECT name, email, department FROM managers WHERE id = ?",
            (body["id"],),
        ).fetchone()
    finally:
        conn.close()
    assert row == (payload["name"], payload["email"], payload["department"])
