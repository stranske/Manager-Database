import sqlite3
import sys
from pathlib import Path

import pytest
from fastapi import HTTPException

# Keep the project root on the path for local imports.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

from adapters.base import connect_db
from api.managers import create_manager


def test_create_manager_rejects_empty_name(tmp_path, monkeypatch):
    db_path = tmp_path / "managers.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.delenv("DB_URL", raising=False)

    with pytest.raises(HTTPException) as exc_info:
        create_manager({"name": " ", "email": "valid@example.com", "department": "Ops"})

    assert exc_info.value.status_code == 400
    errors = exc_info.value.detail["errors"]
    assert {"field": "name", "message": "Name is required."} in errors


def test_create_manager_rejects_invalid_email(tmp_path, monkeypatch):
    db_path = tmp_path / "managers.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.delenv("DB_URL", raising=False)

    with pytest.raises(HTTPException) as exc_info:
        create_manager(
            {"name": "Valid Name", "email": "invalid-email", "department": "Ops"}
        )

    assert exc_info.value.status_code == 400
    errors = exc_info.value.detail["errors"]
    assert {"field": "email", "message": "Email format is invalid."} in errors


def test_create_manager_accepts_valid_record(tmp_path, monkeypatch):
    db_path = tmp_path / "managers.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.delenv("DB_URL", raising=False)

    payload = {"name": "Valid Name", "email": "valid@example.com", "department": "Ops"}
    response_json = create_manager(payload)

    assert response_json["name"] == payload["name"]
    assert response_json["email"] == payload["email"]
    assert response_json["department"] == payload["department"]
    assert isinstance(response_json["id"], int)

    # Confirm persistence in the database.
    conn = connect_db(str(db_path))
    try:
        cursor = conn.execute(
            "SELECT name, email, department FROM managers WHERE id = ?",
            (response_json["id"],),
        )
        row = cursor.fetchone()
    finally:
        conn.close()

    assert row == (payload["name"], payload["email"], payload["department"])
