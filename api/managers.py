"""Manager API endpoint and supporting models."""

from __future__ import annotations

import csv
import io
import json
import logging
import os
import sqlite3
from functools import wraps
from typing import Annotated, Any

from fastapi import APIRouter, Body, HTTPException, Path, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from adapters.base import connect_db
from api.cache import cache_query, invalidate_cache_prefix
from api.models import (
    BulkImportFailure,
    BulkImportItemError,
    BulkImportResponse,
    BulkImportSuccess,
    ManagerListResponse,
    ManagerResponse,
)

router = APIRouter()
logger = logging.getLogger(__name__)

try:  # pragma: no cover - optional dependency
    import psycopg as psycopg
except ImportError:  # pragma: no cover - psycopg not installed for SQLite-only tests
    psycopg = None  # type: ignore[assignment]

DB_ERROR_TYPES: tuple[type[BaseException], ...] = (sqlite3.Error,)
if psycopg is not None:
    DB_ERROR_TYPES = DB_ERROR_TYPES + (psycopg.Error,)

# Keep validation rules centralized so API docs/tests stay in sync with behavior.
REQUIRED_FIELD_ERRORS = {
    "name": "Name is required.",
    "role": "Role is required.",
}
DEFAULT_BULK_IMPORT_MAX_BYTES = 2_000_000


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
            "examples": [
                {
                    "errors": [{"field": "role", "message": "Role is required."}],
                    "error": [{"field": "role", "message": "Role is required."}],
                }
            ]
        }
    )
    errors: list[ErrorDetail] = Field(..., description="List of validation errors")
    error: list[ErrorDetail] | None = Field(None, description="Alias for validation errors")


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
            return JSONResponse(status_code=400, content={"errors": errors, "error": errors})
        return await handler(payload, *args, **kwargs)

    return wrapper


def _raise_db_unavailable(exc: BaseException) -> None:
    logger.exception("Database error in managers API.", exc_info=exc)
    raise HTTPException(status_code=503, detail="Database unavailable") from exc


def _format_bulk_validation_errors(exc: ValidationError) -> list[dict[str, str]]:
    """Normalize Pydantic validation errors for bulk responses."""
    errors: list[dict[str, str]] = []
    for err in exc.errors():
        loc = [str(part) for part in err.get("loc", [])]
        field = loc[-1] if loc else "unknown"
        errors.append({"field": field, "message": err.get("msg", "Invalid value")})
    return errors


def _as_bulk_item_errors(errors: list[dict[str, str]]) -> list[BulkImportItemError]:
    return [BulkImportItemError(**error) for error in errors]


def _bulk_import_max_bytes() -> int:
    """Return the maximum allowed bulk import payload size in bytes."""
    raw_value = os.getenv("BULK_IMPORT_MAX_BYTES")
    if raw_value is None:
        return DEFAULT_BULK_IMPORT_MAX_BYTES
    try:
        value = int(raw_value)
    except ValueError:
        logger.warning("Invalid BULK_IMPORT_MAX_BYTES value: %s", raw_value)
        return DEFAULT_BULK_IMPORT_MAX_BYTES
    return max(value, 1)


def _parse_bulk_csv_payloads(content: str) -> tuple[list[dict[str, object]], list[str]]:
    """Parse CSV content into raw payload dictionaries."""
    reader = csv.DictReader(io.StringIO(content))
    if reader.fieldnames is None:
        return [], ["name", "role"]
    normalized_fields = {
        (field or "").strip().lower(): field for field in reader.fieldnames if field
    }
    required_headers = ["name", "role"]
    missing_headers = [header for header in required_headers if header not in normalized_fields]
    if missing_headers:
        return [], missing_headers
    name_key = normalized_fields["name"]
    role_key = normalized_fields["role"]
    department_key = normalized_fields.get("department")
    payloads: list[dict[str, object]] = []
    for row in reader:
        # Skip rows that are entirely empty to avoid noise in error reports.
        if not any(value and str(value).strip() for value in row.values()):
            continue
        payloads.append(
            {
                "name": row.get(name_key, "") or "",
                "role": row.get(role_key, "") or "",
                "department": row.get(department_key) if department_key else None,
            }
        )
    return payloads, []


def _validate_bulk_records(
    raw_records: list[Any],
    source: str,
) -> tuple[list[tuple[int, ManagerCreate]], list[BulkImportFailure]]:
    """Validate bulk manager records and return valid payloads with failures."""
    valid_records: list[tuple[int, ManagerCreate]] = []
    failures: list[BulkImportFailure] = []
    for index, raw in enumerate(raw_records):
        if not isinstance(raw, dict):
            errors = [{"field": "record", "message": "Record must be an object."}]
            failures.append(BulkImportFailure(index=index, errors=_as_bulk_item_errors(errors)))
            logger.warning("Bulk import validation failed for record %s: %s", index, errors)
            continue
        try:
            payload = ManagerCreate(**raw)
        except ValidationError as exc:
            errors = _format_bulk_validation_errors(exc)
            failures.append(BulkImportFailure(index=index, errors=_as_bulk_item_errors(errors)))
            logger.warning("Bulk import validation failed for record %s: %s", index, errors)
            continue
        errors = _validate_manager_payload(payload)
        if errors:
            failures.append(BulkImportFailure(index=index, errors=_as_bulk_item_errors(errors)))
            if source == "csv":
                logger.warning(
                    "Bulk import CSV record missing required values for record %s: %s",
                    index,
                    errors,
                )
            else:
                logger.warning("Bulk import validation failed for record %s: %s", index, errors)
            continue
        valid_records.append((index, payload))
    return valid_records, failures


def _bulk_request_error(field: str, message: str) -> JSONResponse:
    """Return a consistent 400 payload for bulk requests."""
    errors = [{"field": field, "message": message}]
    return JSONResponse(status_code=400, content={"errors": errors, "error": errors})


def _bulk_request_payload_too_large(max_bytes: int) -> JSONResponse:
    """Return a consistent 413 payload for bulk requests."""
    return JSONResponse(
        status_code=413,
        content={
            "errors": [
                {
                    "field": "body",
                    "message": f"Bulk import payload exceeds {max_bytes} bytes.",
                }
            ]
        },
    )


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
    conn = None
    try:
        conn = connect_db()
        # Ensure schema exists before storing the record.
        _ensure_manager_table(conn)
        manager_id = _insert_manager(conn, payload)
        invalidate_cache_prefix("managers")
    except DB_ERROR_TYPES as exc:
        _raise_db_unavailable(exc)
    finally:
        if conn is not None:
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
    limit: int = Query(
        25,
        ge=1,
        le=100,
        description="Maximum number of managers to return",
    ),
    offset: int = Query(
        0,
        ge=0,
        description="Number of managers to skip",
    ),
    department: str | None = Query(None, description="Filter managers by department"),
):
    """Return a paginated list of managers."""
    db_identity = os.getenv("DB_URL") or os.getenv("DB_PATH", "dev.db")
    conn = None
    try:
        conn = connect_db()
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
    except DB_ERROR_TYPES as exc:
        _raise_db_unavailable(exc)
    finally:
        if conn is not None:
            conn.close()
    items = [
        ManagerResponse(id=row[0], name=row[1], role=row[2], department=row[3]) for row in rows
    ]
    return ManagerListResponse(items=items, total=total, limit=response_limit, offset=offset)


@router.post(
    "/api/managers/bulk",
    status_code=200,
    response_model=BulkImportResponse,
    summary="Bulk import managers",
    description=(
        "Import multiple managers in a single request, returning per-record successes "
        "and validation failures."
    ),
    openapi_extra={
        "requestBody": {
            "required": True,
            "content": {
                "application/json": {
                    "schema": {
                        "type": "array",
                        "items": {"$ref": "#/components/schemas/ManagerCreate"},
                    },
                    "examples": {
                        "basic": {
                            "value": [
                                {
                                    "name": "Grace Hopper",
                                    "role": "Engineering Director",
                                    "department": "Engineering",
                                }
                            ]
                        }
                    },
                },
                "text/csv": {"schema": {"type": "string", "format": "binary"}},
                "application/csv": {"schema": {"type": "string", "format": "binary"}},
            },
        }
    },
    responses={
        400: {
            "model": ErrorResponse,
            "description": "Validation error",
        }
    },
)
async def bulk_import_managers(
    request: Request,
):
    """Bulk import managers from JSON or CSV inputs."""
    content_type = (request.headers.get("content-type") or "").lower()
    max_bytes = _bulk_import_max_bytes()
    content_length = request.headers.get("content-length")
    if content_length:
        try:
            declared_length = int(content_length)
        except ValueError:
            declared_length = None
        # Check declared size up front to avoid loading oversized bodies into memory.
        if declared_length is not None and declared_length > max_bytes:
            logger.warning(
                "Bulk import payload too large: %s bytes (max %s).",
                declared_length,
                max_bytes,
            )
            return _bulk_request_payload_too_large(max_bytes)

    raw_bytes = await request.body()
    if len(raw_bytes) > max_bytes:
        logger.warning(
            "Bulk import payload too large: %s bytes (max %s).",
            len(raw_bytes),
            max_bytes,
        )
        return _bulk_request_payload_too_large(max_bytes)

    source = "csv" if "csv" in content_type else "json"
    if "csv" in content_type:
        try:
            decoded = raw_bytes.decode("utf-8-sig")
        except UnicodeDecodeError:
            return _bulk_request_error("body", "CSV payload must be UTF-8 encoded.")
        raw_records, missing_headers = _parse_bulk_csv_payloads(decoded)
        if missing_headers:
            message = "CSV payload missing required headers: " + ", ".join(missing_headers)
            logger.warning("Bulk import CSV missing required headers: %s", missing_headers)
            return _bulk_request_error("body", message)
    else:
        try:
            body = json.loads(raw_bytes)
        except json.JSONDecodeError:
            return _bulk_request_error("body", "Request body must be valid JSON.")
        if not isinstance(body, list):
            return _bulk_request_error("body", "Request body must be a JSON array.")
        raw_records = body

    if not raw_records:
        return _bulk_request_error("body", "No manager records were provided.")

    # Validate all records before inserting any to satisfy bulk import guarantees.
    valid_records, failures = _validate_bulk_records(raw_records, source)

    conn = None
    successes: list[BulkImportSuccess] = []
    try:
        if valid_records:
            conn = connect_db()
            _ensure_manager_table(conn)
            for index, payload in valid_records:
                manager_id = _insert_manager(conn, payload)
                successes.append(
                    BulkImportSuccess(
                        index=index,
                        manager=ManagerResponse(
                            id=manager_id,
                            name=payload.name,
                            role=payload.role,
                            department=payload.department,
                        ),
                    )
                )
            invalidate_cache_prefix("managers")
    except DB_ERROR_TYPES as exc:
        _raise_db_unavailable(exc)
    finally:
        if conn is not None:
            conn.close()

    return BulkImportResponse(
        total=len(raw_records),
        succeeded=len(successes),
        failed=len(failures),
        successes=successes,
        failures=failures,
    )


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
    conn = None
    try:
        conn = connect_db()
        # Ensure the table exists before attempting the lookup.
        _ensure_manager_table(conn)
        row = _fetch_manager(conn, db_identity, id)
    except DB_ERROR_TYPES as exc:
        _raise_db_unavailable(exc)
    finally:
        if conn is not None:
            conn.close()
    if row is None:
        raise HTTPException(status_code=404, detail="Manager not found")
    return ManagerResponse(id=row[0], name=row[1], role=row[2], department=row[3])


# Commit-message checklist:
# - [ ] type is accurate (fix, test, chore)
# - [ ] scope is clear (managers)
# - [ ] summary is concise and imperative
