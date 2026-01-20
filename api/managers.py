"""Manager API endpoint and supporting models."""

from __future__ import annotations

import re
import sqlite3
from functools import wraps
from typing import Annotated

from fastapi import APIRouter, Body, HTTPException, Path, Query
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


class ManagerListResponse(BaseModel):
    """Response payload for manager list requests."""

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "items": [
                        {
                            "id": 101,
                            "name": "Grace Hopper",
                            "email": "grace@example.com",
                            "department": "Engineering",
                        }
                    ],
                    "total": 1,
                    "limit": 25,
                    "offset": 0,
                }
            ]
        }
    )
    items: list[ManagerResponse] = Field(..., description="Managers in the requested page")
    total: int = Field(..., description="Total number of managers available")
    limit: int = Field(..., description="Maximum managers returned per page")
    offset: int = Field(..., description="Offset into the manager list")


class NotFoundResponse(BaseModel):
    """Response payload for missing resources."""

    detail: str = Field(..., description="Error detail message")


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


def _count_managers(conn) -> int:
    """Return the total number of managers."""
    cursor = conn.execute("SELECT COUNT(*) FROM managers")
    row = cursor.fetchone()
    return int(row[0]) if row else 0


def _fetch_managers(conn, limit: int, offset: int) -> list[tuple[int, str, str, str]]:
    """Return managers ordered by id with pagination applied."""
    placeholder = "?" if isinstance(conn, sqlite3.Connection) else "%s"
    cursor = conn.execute(
        "SELECT id, name, email, department FROM managers ORDER BY id "
        f"LIMIT {placeholder} OFFSET {placeholder}",
        (limit, offset),
    )
    return cursor.fetchall()


def _fetch_manager(conn, manager_id: int) -> tuple[int, str, str, str] | None:
    """Return a single manager row by id."""
    placeholder = "?" if isinstance(conn, sqlite3.Connection) else "%s"
    cursor = conn.execute(
        f"SELECT id, name, email, department FROM managers WHERE id = {placeholder}",
        (manager_id,),
    )
    return cursor.fetchone()


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


@router.get(
    "/managers",
    response_model=ManagerListResponse,
    summary="List managers",
    description="Return a paginated list of managers with total count metadata.",
    # Document query validation errors for OpenAPI consumers.
    responses={
        400: {
            "model": ErrorResponse,
            "description": "Validation error",
            "content": {
                "application/json": {
                    "examples": {
                        "invalid-limit": {
                            "summary": "Invalid limit",
                            "value": {
                                "errors": [
                                    {
                                        "field": "limit",
                                        "message": "ensure this value is greater than or equal to 1",
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
async def list_managers(
    limit: int = Query(25, ge=1, le=100, description="Maximum number of managers to return"),
    offset: int = Query(0, ge=0, description="Number of managers to skip"),
):
    """Return a paginated list of managers."""
    conn = connect_db()
    try:
        # Ensure the table exists so empty databases still return metadata.
        _ensure_manager_table(conn)
        total = _count_managers(conn)
        # Cap pagination to remaining rows to avoid unnecessary DB work.
        remaining = max(total - offset, 0)
        page_limit = min(limit, remaining)
        if page_limit:
            rows = _fetch_managers(conn, page_limit, offset)
        else:
            rows = []
    finally:
        conn.close()
    items = [
        ManagerResponse(id=row[0], name=row[1], email=row[2], department=row[3]) for row in rows
    ]
    return ManagerListResponse(items=items, total=total, limit=limit, offset=offset)


@router.get(
    "/managers/{id}",
    response_model=ManagerResponse,
    summary="Retrieve a manager",
    description="Return a single manager by id.",
    # Surface path validation errors alongside the 404 response.
    responses={
        400: {
            "model": ErrorResponse,
            "description": "Validation error",
            "content": {
                "application/json": {
                    "examples": {
                        "invalid-id": {
                            "summary": "Invalid id",
                            "value": {
                                "errors": [
                                    {
                                        "field": "id",
                                        "message": "ensure this value is greater than or equal to 1",
                                    }
                                ]
                            },
                        }
                    }
                }
            },
        },
        404: {
            "model": NotFoundResponse,
            "description": "Manager not found",
            "content": {
                "application/json": {
                    "examples": {
                        "missing": {
                            "summary": "Missing manager",
                            "value": {"detail": "Manager not found"},
                        }
                    }
                }
            },
        },
    },
)
async def get_manager(
    id: int = Path(..., ge=1, description="Manager identifier"),
):
    """Return a manager by id or raise 404."""
    conn = connect_db()
    try:
        # Ensure the table exists before attempting the lookup.
        _ensure_manager_table(conn)
        row = _fetch_manager(conn, id)
    finally:
        conn.close()
    if row is None:
        raise HTTPException(status_code=404, detail="Manager not found")
    return ManagerResponse(id=row[0], name=row[1], email=row[2], department=row[3])
