"""Manager record endpoints and validation helpers."""

from __future__ import annotations

import re
import sqlite3
from typing import Any

from fastapi import APIRouter, Body, HTTPException

from adapters.base import connect_db

router = APIRouter()

_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def _normalize(value: Any) -> str:
    """Coerce incoming values to trimmed strings for validation."""
    if value is None:
        return ""
    return str(value).strip()


def _validate_manager_payload(payload: dict[str, Any]) -> list[dict[str, str]]:
    """Return a list of validation errors for manager payloads."""
    errors: list[dict[str, str]] = []
    name = _normalize(payload.get("name"))
    email = _normalize(payload.get("email"))
    department = _normalize(payload.get("department"))

    if not name:
        errors.append({"field": "name", "message": "Name is required."})
    if not email:
        errors.append({"field": "email", "message": "Email is required."})
    elif not _EMAIL_RE.match(email):
        errors.append({"field": "email", "message": "Email format is invalid."})
    if not department:
        errors.append({"field": "department", "message": "Department is required."})

    return errors


def _ensure_manager_table(conn) -> None:
    """Create the managers table if it does not exist."""
    if isinstance(conn, sqlite3.Connection):
        conn.execute(
            "CREATE TABLE IF NOT EXISTS managers ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, "
            "name TEXT NOT NULL, "
            "email TEXT NOT NULL, "
            "department TEXT NOT NULL)"
        )
    else:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS managers ("
            "id bigserial PRIMARY KEY, "
            "name text NOT NULL, "
            "email text NOT NULL, "
            "department text NOT NULL)"
        )


@router.post("/managers", status_code=201)
def create_manager(payload: dict[str, Any] = Body(..., description="Manager record")):
    """Create a manager record after validating required fields."""
    errors = _validate_manager_payload(payload)
    if errors:
        # Return a consistent 400 payload for validation failures.
        raise HTTPException(status_code=400, detail={"errors": errors})

    name = _normalize(payload.get("name"))
    email = _normalize(payload.get("email"))
    department = _normalize(payload.get("department"))

    conn = connect_db()
    try:
        _ensure_manager_table(conn)
        is_sqlite = isinstance(conn, sqlite3.Connection)
        if is_sqlite:
            cursor = conn.execute(
                "INSERT INTO managers (name, email, department) VALUES (?, ?, ?)",
                (name, email, department),
            )
            manager_id = int(cursor.lastrowid)
        else:
            cursor = conn.execute(
                "INSERT INTO managers (name, email, department) "
                "VALUES (%s, %s, %s) RETURNING id",
                (name, email, department),
            )
            manager_id = int(cursor.fetchone()[0])
        conn.commit()
    finally:
        conn.close()

    return {"id": manager_id, "name": name, "email": email, "department": department}
