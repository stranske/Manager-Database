import asyncio
import json
import sys
from pathlib import Path

from fastapi.exceptions import RequestValidationError
from pydantic import ValidationError

sys.path.append(str(Path(__file__).resolve().parents[1]))

from adapters.base import connect_db
from api.chat import ManagerCreate, create_manager, validation_exception_handler

# Avoid TestClient to keep tests synchronous and prevent threadpool hangs.


def test_empty_name_returns_400(tmp_path, monkeypatch):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "dev.db"))
    try:
        ManagerCreate(name=" ", email="manager@example.com", department="Research")
    except ValidationError as exc:
        response = asyncio.run(
            validation_exception_handler(None, RequestValidationError(exc.errors()))
        )
        payload = json.loads(response.body)
        assert response.status_code == 400
        assert {"field": "name", "message": "name must not be empty"} in payload["errors"]


def test_invalid_email_returns_400(tmp_path, monkeypatch):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "dev.db"))
    try:
        ManagerCreate(name="Alex Manager", email="not-an-email", department="Research")
    except ValidationError as exc:
        response = asyncio.run(
            validation_exception_handler(None, RequestValidationError(exc.errors()))
        )
        payload = json.loads(response.body)
        assert response.status_code == 400
        assert payload["errors"][0]["field"] == "email"
        assert "valid email" in payload["errors"][0]["message"]


def test_valid_manager_is_stored(tmp_path, monkeypatch):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "dev.db"))
    payload = create_manager(
        ManagerCreate(
            name="Alex Manager",
            email="alex.manager@example.com",
            department="Research",
        )
    )
    assert payload["name"] == "Alex Manager"

    conn = connect_db(db_path=tmp_path / "dev.db")
    cursor = conn.execute(
        "SELECT name, email, department FROM managers WHERE id = ?",
        (payload["id"],),
    )
    row = cursor.fetchone()
    conn.close()
    assert row == ("Alex Manager", "alex.manager@example.com", "Research")
