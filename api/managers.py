"""Manager API endpoint and supporting models."""

from __future__ import annotations

import re
import sqlite3
from functools import wraps
from typing import Annotated

from fastapi import APIRouter, Body
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field

from adapters.base import connect_db

router = APIRouter()

EMAIL_PATTERN = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
# Keep validation rules centralized so API docs/tests stay in sync with behavior.
REQUIRED_FIELD_ERRORS = {
    "name": "Name is required.",
    "department": "Department is required.",
}
EMAIL_ERROR_MESSAGE = "Email must be a valid address."


class ManagerCreate(BaseModel):
    """Payload for creating manager records."""

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "name": "Grace Hopper",
                    "email": "grace@example.com",
                    "department": "Engineering",
                }
            ]
        }
    )
    name: str = Field(..., description="Manager name")
    email: str = Field(..., description="Manager email address")
    department: str = Field(..., description="Manager department")


class ManagerResponse(BaseModel):
    """Response payload for manager records."""

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "id": 101,
                    "name": "Grace Hopper",
                    "email": "grace@example.com",
                    "department": "Engineering",
                }
            ]
        }
    )
    id: int = Field(..., description="Manager identifier")
    name: str = Field(..., description="Manager name")
    email: str = Field(..., description="Manager email address")
    department: str = Field(..., description="Manager department")


class ErrorDetail(BaseModel):
    """Single validation error detail."""

    field: str = Field(..., description="Field that failed validation")
    message: str = Field(..., description="Validation error message")


class ErrorResponse(BaseModel):
    """Response payload for validation errors."""

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {"errors": [{"field": "email", "message": "Email must be a valid address."}]}
            ]
        }
    )
    errors: list[ErrorDetail] = Field(..., description="List of validation errors")


def _ensure_manager_table(conn) -> None:
    """Create the managers table if it does not exist."""
    # Use dialect-specific schema to keep SQLite and Postgres aligned.
    if isinstance(conn, sqlite3.Connection):
        conn.execute("""CREATE TABLE IF NOT EXISTS managers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                email TEXT NOT NULL,
                department TEXT NOT NULL
            )""")
    else:
        conn.execute("""CREATE TABLE IF NOT EXISTS managers (
                id bigserial PRIMARY KEY,
                name text NOT NULL,
                email text NOT NULL,
                department text NOT NULL
            )""")


def _insert_manager(conn, payload: ManagerCreate) -> int:
    """Insert a manager record and return the generated id."""
    if isinstance(conn, sqlite3.Connection):
        cursor = conn.execute(
            "INSERT INTO managers(name, email, department) VALUES (?, ?, ?)",
            (payload.name, payload.email, payload.department),
        )
        conn.commit()
        return int(cursor.lastrowid)
    cursor = conn.execute(
        "INSERT INTO managers(name, email, department) VALUES (%s, %s, %s) RETURNING id",
        (payload.name, payload.email, payload.department),
    )
    row = cursor.fetchone()
    return int(row[0]) if row else 0


def _validate_manager_payload(payload: ManagerCreate) -> list[dict[str, str]]:
    """Apply required field and email format checks."""
    errors: list[dict[str, str]] = []
    if not payload.name.strip():
        errors.append({"field": "name", "message": REQUIRED_FIELD_ERRORS["name"]})
    if not payload.department.strip():
        errors.append({"field": "department", "message": REQUIRED_FIELD_ERRORS["department"]})
    if not EMAIL_PATTERN.match(payload.email.strip()):
        errors.append({"field": "email", "message": EMAIL_ERROR_MESSAGE})
    return errors


def _require_valid_manager(handler):
    """Decorator to guard manager writes with validation."""

    @wraps(handler)
    async def wrapper(payload: ManagerCreate, *args, **kwargs):
        errors = _validate_manager_payload(payload)
        if errors:
            # Short-circuit invalid payloads before touching the database.
            return JSONResponse(status_code=400, content={"errors": errors})
        return await handler(payload, *args, **kwargs)

    return wrapper


@router.post(
    "/managers",
    status_code=201,
    response_model=ManagerResponse,
    summary="Create a manager record",
    description=(
        "Validate the incoming manager details, store the record, and return the "
        "saved manager payload with its generated identifier."
    ),
    responses={
        400: {
            "model": ErrorResponse,
            "description": "Validation error",
            "content": {
                "application/json": {
                    "examples": {
                        "invalid-email": {
                            "summary": "Invalid email",
                            "value": {
                                "errors": [
                                    {
                                        "field": "email",
                                        "message": "Email must be a valid address.",
                                    }
                                ]
                            },
                        }
                    }
                }
            },
        }
    },
)
@_require_valid_manager
async def create_manager(
    payload: Annotated[
        ManagerCreate,
        Body(
            ...,
            examples={
                "basic": {
                    "summary": "New manager",
                    "value": {
                        "name": "Grace Hopper",
                        "email": "grace@example.com",
                        "department": "Engineering",
                    },
                }
            },
        ),
    ],
):
    """Create a manager record after validating required fields."""
    conn = connect_db()
    try:
        # Ensure schema exists before storing the record.
        _ensure_manager_table(conn)
        manager_id = _insert_manager(conn, payload)
    finally:
        conn.close()
    return {
        "id": manager_id,
        "name": payload.name,
        "email": payload.email,
        "department": payload.department,
    }
