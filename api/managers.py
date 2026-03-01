"""Manager API endpoint and supporting models."""

from __future__ import annotations

import csv
import io
import json
import logging
import os
import re
import sqlite3
from functools import wraps
from typing import Annotated, Any, cast

from fastapi import APIRouter, Body, HTTPException, Path, Query, Request, Response
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
    UniverseImportResponse,
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
}
DEFAULT_BULK_IMPORT_MAX_BYTES = 2_000_000
CIK_PATTERN = re.compile(r"^\d{10}$")


class ManagerCreate(BaseModel):
    """Payload for creating manager records."""

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "name": "Elliott Investment Management L.P.",
                    "cik": "0001791786",
                    "lei": "549300U3N12T57QLOU60",
                    "aliases": ["Elliott Management"],
                    "jurisdictions": ["us"],
                    "tags": ["activist"],
                    "registry_ids": {"fca_frn": "122927"},
                }
            ]
        }
    )
    name: str = Field(..., description="Legal name of the investment manager")
    cik: str | None = Field(None, description="SEC Central Index Key")
    lei: str | None = Field(None, description="Legal Entity Identifier")
    aliases: list[str] = Field(default_factory=list, description="Alternative names")
    jurisdictions: list[str] = Field(
        default_factory=list, description="Filing jurisdictions (us, uk, ca)"
    )
    tags: list[str] = Field(default_factory=list, description="Classification tags")
    registry_ids: dict[str, str] = Field(default_factory=dict, description="External registry IDs")


class ManagerUpdate(BaseModel):
    """Payload for partially updating manager records."""

    name: str | None = Field(None, description="Legal name of the investment manager")
    cik: str | None = Field(None, description="SEC Central Index Key")
    lei: str | None = Field(None, description="Legal Entity Identifier")
    aliases: list[str] | None = Field(None, description="Alternative names")
    jurisdictions: list[str] | None = Field(None, description="Filing jurisdictions (us, uk, ca)")
    tags: list[str] | None = Field(None, description="Classification tags")
    registry_ids: dict[str, str] | None = Field(None, description="External registry IDs")


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
                    "errors": [{"field": "name", "message": "Name is required."}],
                    "error": [{"field": "name", "message": "Name is required."}],
                }
            ]
        }
    )
    errors: list[ErrorDetail] = Field(..., description="List of validation errors")
    error: list[ErrorDetail] | None = Field(None, description="Alias for validation errors")


def _ensure_manager_table(conn) -> None:
    """Ensure the managers table is available."""
    if isinstance(conn, sqlite3.Connection):
        conn.execute("""CREATE TABLE IF NOT EXISTS managers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                cik TEXT,
                lei TEXT,
                aliases TEXT NOT NULL DEFAULT '[]',
                jurisdictions TEXT NOT NULL DEFAULT '[]',
                tags TEXT NOT NULL DEFAULT '[]',
                registry_ids TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )""")
        return
    conn.execute("SELECT 1 FROM managers LIMIT 1")


def _json_array(raw: object) -> list[str]:
    if raw is None:
        return []
    if isinstance(raw, list):
        return [str(item) for item in raw]
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            if ";" in text:
                return [part.strip() for part in text.split(";") if part.strip()]
            if "," in text:
                return [part.strip() for part in text.split(",") if part.strip()]
            return [text]
        if isinstance(parsed, list):
            return [str(item) for item in parsed]
    return []


def _json_dict(raw: object) -> dict[str, str]:
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return {str(key): str(value) for key, value in raw.items()}
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return {}
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            return {}
        if isinstance(parsed, dict):
            return {str(key): str(value) for key, value in parsed.items()}
    return {}


def _to_manager_response(row: tuple[object, ...]) -> ManagerResponse:
    manager_id_raw = row[0]
    if not isinstance(manager_id_raw, (int, str)):
        manager_id_raw = 0
    return ManagerResponse(
        manager_id=int(cast(int | str, manager_id_raw)),
        name=str(row[1]),
        cik=str(row[2]) if row[2] is not None else None,
        lei=str(row[3]) if row[3] is not None else None,
        aliases=_json_array(row[4]),
        jurisdictions=_json_array(row[5]),
        tags=_json_array(row[6]),
        registry_ids=_json_dict(row[7]),
        created_at=str(row[8]) if row[8] is not None else None,
        updated_at=str(row[9]) if row[9] is not None else None,
    )


def _normalize_cik(raw: Any) -> str:
    cik = "" if raw is None else str(raw).strip()
    if not cik:
        return ""
    digits = "".join(ch for ch in cik if ch.isdigit())
    if not digits:
        return ""
    return digits.zfill(10)


def _ensure_universe_schema(conn: Any) -> None:
    """Ensure managers table has the columns/index needed for universe imports."""
    _ensure_manager_table(conn)
    if isinstance(conn, sqlite3.Connection):
        columns = {row[1] for row in conn.execute("PRAGMA table_info(managers)").fetchall()}
        if "cik" not in columns:
            conn.execute("ALTER TABLE managers ADD COLUMN cik TEXT")
        if "jurisdiction" not in columns:
            conn.execute("ALTER TABLE managers ADD COLUMN jurisdiction TEXT")
        if "created_at" not in columns:
            conn.execute("ALTER TABLE managers ADD COLUMN created_at TIMESTAMP")
            conn.execute(
                "UPDATE managers SET created_at = CURRENT_TIMESTAMP WHERE created_at IS NULL"
            )
        if "updated_at" not in columns:
            conn.execute("ALTER TABLE managers ADD COLUMN updated_at TIMESTAMP")
            conn.execute(
                "UPDATE managers SET updated_at = CURRENT_TIMESTAMP WHERE updated_at IS NULL"
            )
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_managers_cik_unique ON managers(cik)")
        conn.commit()
        return

    conn.execute("ALTER TABLE managers ADD COLUMN IF NOT EXISTS cik text")
    conn.execute("ALTER TABLE managers ADD COLUMN IF NOT EXISTS jurisdiction text")
    conn.execute(
        "ALTER TABLE managers ADD COLUMN IF NOT EXISTS created_at timestamptz DEFAULT now()"
    )
    conn.execute(
        "ALTER TABLE managers ADD COLUMN IF NOT EXISTS updated_at timestamptz DEFAULT now()"
    )
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_managers_cik_unique ON managers(cik)")


def _existing_ciks(conn: Any) -> set[str]:
    rows = conn.execute(
        "SELECT cik FROM managers WHERE cik IS NOT NULL AND TRIM(cik) != ''"
    ).fetchall()
    return {str(row[0]).strip() for row in rows if row and row[0] is not None}


def _upsert_universe_record(conn: Any, name: str, cik: str, jurisdiction: str) -> None:
    if isinstance(conn, sqlite3.Connection):
        conn.execute(
            """
            INSERT INTO managers(name, cik, jurisdiction, updated_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(cik)
            DO UPDATE SET
                name = excluded.name,
                jurisdiction = excluded.jurisdiction,
                updated_at = CURRENT_TIMESTAMP
            """,
            (name, cik, jurisdiction),
        )
        return
    conn.execute(
        """
        INSERT INTO managers(name, cik, jurisdiction, updated_at)
        VALUES (%s, %s, %s, now())
        ON CONFLICT(cik)
        DO UPDATE SET
            name = EXCLUDED.name,
            jurisdiction = EXCLUDED.jurisdiction,
            updated_at = now()
        """,
        (name, cik, jurisdiction),
    )


def _insert_manager(conn, payload: ManagerCreate) -> int:
    """Insert a manager record and return the generated id."""
    if isinstance(conn, sqlite3.Connection):
        cursor = conn.execute(
            (
                "INSERT INTO managers(name, cik, lei, aliases, jurisdictions, tags, registry_ids) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)"
            ),
            (
                payload.name,
                payload.cik,
                payload.lei,
                json.dumps(payload.aliases),
                json.dumps(payload.jurisdictions),
                json.dumps(payload.tags),
                json.dumps(payload.registry_ids),
            ),
        )
        conn.commit()
        lastrowid = cursor.lastrowid
        return int(lastrowid) if lastrowid is not None else 0
    cursor = conn.execute(
        (
            "INSERT INTO managers(name, cik, lei, aliases, jurisdictions, tags, registry_ids) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb) RETURNING id"
        ),
        (
            payload.name,
            payload.cik,
            payload.lei,
            payload.aliases,
            payload.jurisdictions,
            payload.tags,
            json.dumps(payload.registry_ids),
        ),
    )
    row = cursor.fetchone()
    if not row or row[0] is None:
        return 0
    return int(row[0])


def _update_manager(conn, manager_id: int, payload: ManagerUpdate) -> bool:
    """Update a manager record and return whether a row changed."""
    fields = payload.model_dump(exclude_unset=True)
    if not fields:
        return False

    set_clauses: list[str] = []
    params: list[object] = []
    placeholder = "?" if isinstance(conn, sqlite3.Connection) else "%s"

    for field, value in fields.items():
        if field in {"aliases", "jurisdictions", "tags"}:
            if isinstance(conn, sqlite3.Connection):
                set_clauses.append(f"{field} = {placeholder}")
                params.append(json.dumps(value))
            else:
                set_clauses.append(f"{field} = {placeholder}")
                params.append(value)
            continue
        if field == "registry_ids":
            if isinstance(conn, sqlite3.Connection):
                set_clauses.append(f"{field} = {placeholder}")
                params.append(json.dumps(value))
            else:
                set_clauses.append(f"{field} = {placeholder}::jsonb")
                params.append(json.dumps(value))
            continue
        set_clauses.append(f"{field} = {placeholder}")
        params.append(value)

    set_clauses.append("updated_at = CURRENT_TIMESTAMP")
    params.append(manager_id)

    cursor = conn.execute(
        f"UPDATE managers SET {', '.join(set_clauses)} WHERE id = {placeholder}",
        params,
    )
    if isinstance(conn, sqlite3.Connection):
        conn.commit()
    return cursor.rowcount > 0


def _delete_manager(conn, manager_id: int) -> bool:
    """Delete a manager by id and return whether a row was removed."""
    placeholder = "?" if isinstance(conn, sqlite3.Connection) else "%s"
    cursor = conn.execute(f"DELETE FROM managers WHERE id = {placeholder}", (manager_id,))
    if isinstance(conn, sqlite3.Connection):
        conn.commit()
    return cursor.rowcount > 0


@cache_query("managers.count", skip_args=1)
def _count_managers(conn, db_identity: str, jurisdiction: str | None, tag: str | None) -> int:
    """Return the total number of managers with optional filters."""
    params: list[object] = []
    clauses: list[str] = []
    if jurisdiction:
        if isinstance(conn, sqlite3.Connection):
            clauses.append("EXISTS (SELECT 1 FROM json_each(jurisdictions) WHERE value = ?)")
        else:
            clauses.append("%s = ANY(jurisdictions)")
        params.append(jurisdiction)
    if tag:
        if isinstance(conn, sqlite3.Connection):
            clauses.append("EXISTS (SELECT 1 FROM json_each(tags) WHERE value = ?)")
        else:
            clauses.append("%s = ANY(tags)")
        params.append(tag)
    where_clause = f" WHERE {' AND '.join(clauses)}" if clauses else ""
    cursor = conn.execute(f"SELECT COUNT(*) FROM managers{where_clause}", params)
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
    jurisdiction: str | None,
    tag: str | None,
) -> list[tuple[object, ...]]:
    """Return managers ordered by id with pagination applied."""
    placeholder = "?" if isinstance(conn, sqlite3.Connection) else "%s"
    params: list[object] = []
    clauses: list[str] = []
    if jurisdiction:
        if isinstance(conn, sqlite3.Connection):
            clauses.append("EXISTS (SELECT 1 FROM json_each(jurisdictions) WHERE value = ?)")
        else:
            clauses.append("%s = ANY(jurisdictions)")
        params.append(jurisdiction)
    if tag:
        if isinstance(conn, sqlite3.Connection):
            clauses.append("EXISTS (SELECT 1 FROM json_each(tags) WHERE value = ?)")
        else:
            clauses.append("%s = ANY(tags)")
        params.append(tag)
    where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    params.extend([limit, offset])
    cursor = conn.execute(
        f"SELECT id, name, cik, lei, aliases, jurisdictions, tags, registry_ids, created_at, updated_at "
        f"FROM managers {where_clause} "
        f"ORDER BY id LIMIT {placeholder} OFFSET {placeholder}",
        params,
    )
    return cursor.fetchall()


@cache_query("managers.item", skip_args=1)
def _fetch_manager(conn, db_identity: str, manager_id: int) -> tuple[object, ...] | None:
    """Return a single manager row by id."""
    placeholder = "?" if isinstance(conn, sqlite3.Connection) else "%s"
    cursor = conn.execute(
        (
            f"SELECT id, name, cik, lei, aliases, jurisdictions, tags, registry_ids, created_at, updated_at "
            f"FROM managers WHERE id = {placeholder}"
        ),
        (manager_id,),
    )
    return cursor.fetchone()


def _validate_manager_payload(payload: ManagerCreate) -> list[dict[str, str]]:
    """Apply required field checks."""
    errors: list[dict[str, str]] = []
    if not payload.name.strip():
        errors.append({"field": "name", "message": REQUIRED_FIELD_ERRORS["name"]})
    if (
        payload.cik is not None
        and payload.cik.strip()
        and not CIK_PATTERN.match(payload.cik.strip())
    ):
        errors.append({"field": "cik", "message": "CIK must be a 10-digit zero-padded string."})
    return errors


def _validate_manager_update_payload(payload: ManagerUpdate) -> list[dict[str, str]]:
    """Apply validation checks for partial updates."""
    errors: list[dict[str, str]] = []
    provided_fields = payload.model_dump(exclude_unset=True)
    if not provided_fields:
        errors.append({"field": "body", "message": "At least one field must be provided."})
        return errors
    if payload.name is not None and not payload.name.strip():
        errors.append({"field": "name", "message": REQUIRED_FIELD_ERRORS["name"]})
    if (
        payload.cik is not None
        and payload.cik.strip()
        and not CIK_PATTERN.match(payload.cik.strip())
    ):
        errors.append({"field": "cik", "message": "CIK must be a 10-digit zero-padded string."})
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
        return [], ["name"]
    normalized_fields = {
        (field or "").strip().lower(): field for field in reader.fieldnames if field
    }
    required_headers = ["name"]
    missing_headers = [header for header in required_headers if header not in normalized_fields]
    if missing_headers:
        return [], missing_headers
    name_key = normalized_fields["name"]
    cik_key = normalized_fields.get("cik")
    lei_key = normalized_fields.get("lei")
    aliases_key = normalized_fields.get("aliases")
    jurisdictions_key = normalized_fields.get("jurisdictions")
    tags_key = normalized_fields.get("tags")
    registry_ids_key = normalized_fields.get("registry_ids")
    payloads: list[dict[str, object]] = []
    for row in reader:
        # Skip rows that are entirely empty to avoid noise in error reports.
        if not any(value and str(value).strip() for value in row.values()):
            continue
        payloads.append(
            {
                "name": row.get(name_key, "") or "",
                "cik": (row.get(cik_key, "") or "").strip() if cik_key else None,
                "lei": (row.get(lei_key, "") or "").strip() if lei_key else None,
                "aliases": _json_array(row.get(aliases_key)) if aliases_key else [],
                "jurisdictions": (
                    _json_array(row.get(jurisdictions_key)) if jurisdictions_key else []
                ),
                "tags": _json_array(row.get(tags_key)) if tags_key else [],
                "registry_ids": _json_dict(row.get(registry_ids_key)) if registry_ids_key else {},
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
                        "missing-name": {
                            "summary": "Missing name",
                            "value": {
                                "errors": [
                                    {
                                        "field": "name",
                                        "message": "Name is required.",
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
                        "name": "Elliott Investment Management L.P.",
                        "cik": "0001791786",
                        "lei": "549300U3N12T57QLOU60",
                        "aliases": ["Elliott Management"],
                        "jurisdictions": ["us"],
                        "tags": ["activist"],
                        "registry_ids": {"fca_frn": "122927"},
                    },
                }
            },
        ),
    ],
):
    """Create a manager record after validating required fields."""
    db_identity = os.getenv("DB_URL") or os.getenv("DB_PATH", "dev.db")
    conn = None
    try:
        conn = connect_db()
        # Ensure schema exists before storing the record.
        _ensure_manager_table(conn)
        manager_id = _insert_manager(conn, payload)
        row = _fetch_manager(conn, db_identity, manager_id)
        invalidate_cache_prefix("managers")
    except DB_ERROR_TYPES as exc:
        _raise_db_unavailable(exc)
    finally:
        if conn is not None:
            conn.close()
    if row is not None:
        return _to_manager_response(row)
    return ManagerResponse(
        manager_id=manager_id,
        name=payload.name,
        cik=payload.cik,
        lei=payload.lei,
        aliases=payload.aliases,
        jurisdictions=payload.jurisdictions,
        tags=payload.tags,
        registry_ids=payload.registry_ids,
        created_at=None,
        updated_at=None,
    )


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
    openapi_extra={
        "responses": {
            "200": {
                "content": {
                    "application/json": {
                        "examples": {
                            "investment-managers": {
                                "summary": "Investment manager page",
                                "value": {
                                    "items": [
                                        {
                                            "manager_id": 101,
                                            "name": "Elliott Investment Management L.P.",
                                            "cik": "0001791786",
                                            "lei": "549300U3N12T57QLOU60",
                                            "aliases": ["Elliott Management"],
                                            "jurisdictions": ["us"],
                                            "tags": ["activist"],
                                            "registry_ids": {"fca_frn": "122927"},
                                            "created_at": "2026-02-01T10:00:00Z",
                                            "updated_at": "2026-02-01T10:00:00Z",
                                        }
                                    ],
                                    "total": 1,
                                    "limit": 25,
                                    "offset": 0,
                                },
                            }
                        }
                    }
                }
            }
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
    jurisdiction: str | None = Query(None, description="Filter managers by jurisdiction"),
    tag: str | None = Query(None, description="Filter managers by tag"),
):
    """Return a paginated list of managers."""
    db_identity = os.getenv("DB_URL") or os.getenv("DB_PATH", "dev.db")
    conn = None
    try:
        conn = connect_db()
        # Ensure the table exists so empty databases still return metadata.
        _ensure_manager_table(conn)
        normalized_jurisdiction = jurisdiction.strip() or None if jurisdiction else None
        normalized_tag = tag.strip() or None if tag else None
        total = _count_managers(conn, db_identity, normalized_jurisdiction, normalized_tag)
        # Default to a 25-row page while preserving the client-requested limit in metadata.
        remaining = max(total - offset, 0)
        page_limit = min(limit, remaining)
        if page_limit:
            rows = _fetch_managers(
                conn,
                db_identity,
                page_limit,
                offset,
                normalized_jurisdiction,
                normalized_tag,
            )
        else:
            rows = []
        response_limit = limit
    except DB_ERROR_TYPES as exc:
        _raise_db_unavailable(exc)
    finally:
        if conn is not None:
            conn.close()
    items = [_to_manager_response(row) for row in rows]
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
                                    "name": "Elliott Investment Management L.P.",
                                    "cik": "0001791786",
                                    "jurisdictions": ["us"],
                                    "tags": ["activist"],
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
                            manager_id=manager_id,
                            name=payload.name,
                            cik=payload.cik,
                            lei=payload.lei,
                            aliases=payload.aliases,
                            jurisdictions=payload.jurisdictions,
                            tags=payload.tags,
                            registry_ids=payload.registry_ids,
                            created_at=None,
                            updated_at=None,
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


@router.post(
    "/managers/import/universe",
    status_code=200,
    response_model=UniverseImportResponse,
    summary="Import manager universe records",
    description=(
        "Import a JSON array of manager universe records with CIK-based upsert behavior. "
        "Valid records create or update managers by CIK; invalid records are skipped."
    ),
)
async def import_manager_universe(
    records: Annotated[Any, Body(..., description="Array of manager records")],
):
    """Upsert manager universe records using CIK as the unique key."""
    if not isinstance(records, list):
        return _bulk_request_error("body", "Request body must be a JSON array.")

    if not records:
        return UniverseImportResponse(created=0, updated=0, skipped=0)

    conn = None
    created = 0
    updated = 0
    skipped = 0
    try:
        conn = connect_db()
        _ensure_universe_schema(conn)
        known_ciks = _existing_ciks(conn)
        for index, record in enumerate(records):
            if not isinstance(record, dict):
                skipped += 1
                logger.warning("Universe import skipped record %s: record must be an object", index)
                continue
            name = str(record.get("name", "")).strip()
            cik = _normalize_cik(record.get("cik"))
            jurisdiction = str(record.get("jurisdiction", "")).strip().lower()
            if not name or not cik or not jurisdiction:
                skipped += 1
                logger.warning(
                    "Universe import skipped record %s: requires name, cik, jurisdiction",
                    index,
                )
                continue

            if cik in known_ciks:
                updated += 1
            else:
                created += 1
                known_ciks.add(cik)

            _upsert_universe_record(conn, name, cik, jurisdiction)

        conn.commit()
        invalidate_cache_prefix("managers")
    except DB_ERROR_TYPES as exc:
        _raise_db_unavailable(exc)
    finally:
        if conn is not None:
            conn.close()

    return UniverseImportResponse(created=created, updated=updated, skipped=skipped)


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
    openapi_extra={
        "responses": {
            "200": {
                "content": {
                    "application/json": {
                        "examples": {
                            "investment-manager": {
                                "summary": "Investment manager",
                                "value": {
                                    "manager_id": 101,
                                    "name": "Elliott Investment Management L.P.",
                                    "cik": "0001791786",
                                    "lei": "549300U3N12T57QLOU60",
                                    "aliases": ["Elliott Management"],
                                    "jurisdictions": ["us"],
                                    "tags": ["activist"],
                                    "registry_ids": {"fca_frn": "122927"},
                                    "created_at": "2026-02-01T10:00:00Z",
                                    "updated_at": "2026-02-01T10:00:00Z",
                                },
                            }
                        }
                    }
                }
            }
        }
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
    return _to_manager_response(row)


@router.patch(
    "/managers/{id}",
    response_model=ManagerResponse,
    summary="Update a manager",
    description="Partially update manager fields and return the updated record.",
    responses={
        400: {"model": ErrorResponse, "description": "Validation error"},
        404: {"model": NotFoundResponse, "description": "Manager not found"},
    },
    openapi_extra={
        "requestBody": {
            "content": {
                "application/json": {
                    "examples": {
                        "update-tags-and-lei": {
                            "summary": "Update manager metadata",
                            "value": {
                                "lei": "549300U3N12T57QLOU60",
                                "aliases": ["Elliott Management"],
                                "tags": ["event-driven"],
                            },
                        }
                    }
                }
            }
        }
    },
)
async def patch_manager(
    payload: ManagerUpdate,
    id: int = Path(..., ge=1, description="Manager identifier"),
):
    """Partially update a manager by id."""
    errors = _validate_manager_update_payload(payload)
    if errors:
        return JSONResponse(status_code=400, content={"errors": errors, "error": errors})

    db_identity = os.getenv("DB_URL") or os.getenv("DB_PATH", "dev.db")
    conn = None
    try:
        conn = connect_db()
        _ensure_manager_table(conn)
        updated = _update_manager(conn, id, payload)
        if not updated:
            raise HTTPException(status_code=404, detail="Manager not found")
        row = _fetch_manager(conn, db_identity, id)
        invalidate_cache_prefix("managers")
    except DB_ERROR_TYPES as exc:
        _raise_db_unavailable(exc)
    finally:
        if conn is not None:
            conn.close()

    if row is None:
        raise HTTPException(status_code=404, detail="Manager not found")
    return _to_manager_response(row)


@router.delete(
    "/managers/{id}",
    status_code=204,
    summary="Delete a manager",
    description="Delete a manager by id.",
    responses={404: {"model": NotFoundResponse, "description": "Manager not found"}},
)
async def delete_manager(
    id: int = Path(..., ge=1, description="Manager identifier"),
):
    """Delete a manager by id."""
    conn = None
    try:
        conn = connect_db()
        _ensure_manager_table(conn)
        deleted = _delete_manager(conn, id)
        if not deleted:
            raise HTTPException(status_code=404, detail="Manager not found")
        invalidate_cache_prefix("managers")
    except DB_ERROR_TYPES as exc:
        _raise_db_unavailable(exc)
    finally:
        if conn is not None:
            conn.close()
    return Response(status_code=204)


# Commit-message checklist:
# - [ ] type is accurate (fix, test, chore)
# - [ ] scope is clear (managers)
# - [ ] summary is concise and imperative
