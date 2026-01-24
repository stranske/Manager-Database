"""Manager API endpoint and supporting models."""

from __future__ import annotations

import os
import sqlite3
from functools import wraps
from typing import Annotated

from fastapi import APIRouter, Body, HTTPException, Path, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field

from adapters.base import connect_db
from api.cache import cache_query, invalidate_cache_prefix
from api.models import ManagerListResponse, ManagerResponse

router = APIRouter()

# Keep validation rules centralized so API docs/tests stay in sync with behavior.
REQUIRED_FIELD_ERRORS = {
    "name": "Name is required.",
    "role": "Role is required.",
}


class ManagerCreate(BaseModel):
    """Payload for creating manager records."""

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "name": "Grace Hopper",
                    "role": "Engineering Director",
                    "department": "Engineering",
                }
            ]
        }
    )
    name: str = Field(..., description="Manager name")
    role: str = Field(..., description="Manager role")
    department: str | None = Field(None, description="Manager department")


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
            "examples": [{"errors": [{"field": "role", "message": "Role is required."}]}]
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
                role TEXT NOT NULL,
                department TEXT
            )""")
    else:
        conn.execute("""CREATE TABLE IF NOT EXISTS managers (
                id bigserial PRIMARY KEY,
                name text NOT NULL,
                role text NOT NULL,
                department text
            )""")
    _ensure_manager_department_column(conn)


def _ensure_manager_department_column(conn) -> None:
    """Backfill the department column for existing manager tables."""
    # Older databases may not include the department column yet.
    if isinstance(conn, sqlite3.Connection):
        cursor = conn.execute("PRAGMA table_info(managers)")
        columns = {row[1] for row in cursor.fetchall()}
        if "department" not in columns:
            conn.execute("ALTER TABLE managers ADD COLUMN department TEXT")
            conn.commit()
        return
    cursor = conn.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name = %s",
        ("managers",),
    )
    columns = {row[0] for row in cursor.fetchall()}
    if "department" not in columns:
        conn.execute("ALTER TABLE managers ADD COLUMN department text")


def _insert_manager(conn, payload: ManagerCreate) -> int:
    """Insert a manager record and return the generated id."""
    # Normalize empty department values to NULL for consistent filtering.
    department = payload.department.strip() if payload.department else None
    if isinstance(conn, sqlite3.Connection):
        cursor = conn.execute(
            "INSERT INTO managers(name, role, department) VALUES (?, ?, ?)",
            (payload.name, payload.role, department),
        )
        conn.commit()
        lastrowid = cursor.lastrowid
        return int(lastrowid) if lastrowid is not None else 0
    cursor = conn.execute(
        "INSERT INTO managers(name, role, department) VALUES (%s, %s, %s) RETURNING id",
        (payload.name, payload.role, department),
    )
    row = cursor.fetchone()
    if not row or row[0] is None:
        return 0
    return int(row[0])


@cache_query("managers.count", skip_args=1)
def _count_managers(conn, db_identity: str, department: str | None) -> int:
    """Return the total number of managers, optionally filtered by department."""
    if department:
        placeholder = "?" if isinstance(conn, sqlite3.Connection) else "%s"
        cursor = conn.execute(
            f"SELECT COUNT(*) FROM managers WHERE department = {placeholder}",
            (department,),
        )
    else:
        cursor = conn.execute("SELECT COUNT(*) FROM managers")
    row = cursor.fetchone()
    if not row or row[0] is None:
        return 0
    return int(row[0])


@cache_query("managers.list", skip_args=1)
def _fetch_managers(
    conn,
    db_identity: str,
    limit: int,
    offset: int,
    department: str | None,
) -> list[tuple[int, str, str, str | None]]:
    """Return managers ordered by id with pagination applied."""
    placeholder = "?" if isinstance(conn, sqlite3.Connection) else "%s"
    where_clause = ""
    params: list[object] = []
    if department:
        where_clause = f"WHERE department = {placeholder}"
        params.append(department)
    params.extend([limit, offset])
    cursor = conn.execute(
        f"SELECT id, name, role, department FROM managers {where_clause} "
        f"ORDER BY id LIMIT {placeholder} OFFSET {placeholder}",
        params,
    )
    return cursor.fetchall()


@cache_query("managers.item", skip_args=1)
def _fetch_manager(
    conn, db_identity: str, manager_id: int
) -> tuple[int, str, str, str | None] | None:
    """Return a single manager row by id."""
    placeholder = "?" if isinstance(conn, sqlite3.Connection) else "%s"
    cursor = conn.execute(
        f"SELECT id, name, role, department FROM managers WHERE id = {placeholder}",
        (manager_id,),
    )
    return cursor.fetchone()


def _validate_manager_payload(payload: ManagerCreate) -> list[dict[str, str]]:
    """Apply required field checks."""
    errors: list[dict[str, str]] = []
    if not payload.name.strip():
        errors.append({"field": "name", "message": REQUIRED_FIELD_ERRORS["name"]})
    if not payload.role.strip():
        errors.append({"field": "role", "message": REQUIRED_FIELD_ERRORS["role"]})
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
                        "missing-role": {
                            "summary": "Missing role",
                            "value": {
                                "errors": [
                                    {
                                        "field": "role",
                                        "message": "Role is required.",
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
                        "role": "Engineering Director",
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
        invalidate_cache_prefix("managers")
    finally:
        conn.close()
    return {
        "id": manager_id,
        "name": payload.name,
        "role": payload.role,
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
    department: str | None = Query(None, description="Filter managers by department"),
):
    """Return a paginated list of managers."""
    db_identity = os.getenv("DB_URL") or os.getenv("DB_PATH", "dev.db")
    conn = connect_db()
    try:
        # Ensure the table exists so empty databases still return metadata.
        _ensure_manager_table(conn)
        normalized_department = department.strip() if department else None
        total = _count_managers(conn, db_identity, normalized_department)
        # Default to a 25-row page while preserving the client-requested limit in metadata.
        remaining = max(total - offset, 0)
        page_limit = min(limit, remaining)
        if page_limit:
            rows = _fetch_managers(conn, db_identity, page_limit, offset, normalized_department)
        else:
            rows = []
        response_limit = limit
    finally:
        conn.close()
    items = [
        ManagerResponse(id=row[0], name=row[1], role=row[2], department=row[3]) for row in rows
    ]
    return ManagerListResponse(items=items, total=total, limit=response_limit, offset=offset)


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
    db_identity = os.getenv("DB_URL") or os.getenv("DB_PATH", "dev.db")
    conn = connect_db()
    try:
        # Ensure the table exists before attempting the lookup.
        _ensure_manager_table(conn)
        row = _fetch_manager(conn, db_identity, id)
    finally:
        conn.close()
    if row is None:
        raise HTTPException(status_code=404, detail="Manager not found")
    return ManagerResponse(id=row[0], name=row[1], role=row[2], department=row[3])
