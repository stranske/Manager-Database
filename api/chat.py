"""Minimal FastAPI app providing /chat and health endpoints."""

from __future__ import annotations

import asyncio
import json
import os
import re
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor
from functools import wraps

from fastapi import FastAPI, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from adapters.base import connect_db

app = FastAPI()
_APP_EXECUTOR = ThreadPoolExecutor(max_workers=4)
_HEALTH_EXECUTOR = ThreadPoolExecutor(max_workers=1)
APP_START_TIME = time.monotonic()


def get_health_executor():
    """Return a working health executor, creating a new one if needed."""
    global _HEALTH_EXECUTOR
    if _HEALTH_EXECUTOR._shutdown:
        _HEALTH_EXECUTOR = ThreadPoolExecutor(max_workers=1)
    return _HEALTH_EXECUTOR


# Keep APP_EXECUTOR for backwards compatibility
APP_EXECUTOR = _APP_EXECUTOR


EMAIL_PATTERN = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
# Keep validation rules centralized so API docs/tests stay in sync with behavior.
REQUIRED_FIELD_ERRORS = {
    "name": "Name is required.",
    "department": "Department is required.",
}
EMAIL_ERROR_MESSAGE = "Email must be a valid address."


class ManagerCreate(BaseModel):
    """Payload for creating manager records."""

    name: str = Field(..., description="Manager name")
    email: str = Field(..., description="Manager email address")
    department: str = Field(..., description="Manager department")


def _format_validation_errors(exc: RequestValidationError) -> list[dict[str, str]]:
    """Normalize validation errors into a concise field/message list."""
    errors: list[dict[str, str]] = []
    for err in exc.errors():
        loc = [str(part) for part in err.get("loc", []) if part != "body"]
        field = loc[-1] if loc else "unknown"
        errors.append({"field": field, "message": err.get("msg", "Invalid value")})
    return errors


@app.exception_handler(RequestValidationError)
async def _validation_exception_handler(
    _request: Request, exc: RequestValidationError
) -> JSONResponse:
    # Return 400s with field-level messages for API clients.
    return JSONResponse(status_code=400, content={"errors": _format_validation_errors(exc)})


def _ensure_manager_table(conn) -> None:
    """Create the managers table if it does not exist."""
    if isinstance(conn, sqlite3.Connection):
        conn.execute(
            """CREATE TABLE IF NOT EXISTS managers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                email TEXT NOT NULL,
                department TEXT NOT NULL
            )"""
        )
    else:
        conn.execute(
            """CREATE TABLE IF NOT EXISTS managers (
                id bigserial PRIMARY KEY,
                name text NOT NULL,
                email text NOT NULL,
                department text NOT NULL
            )"""
        )


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


@app.get("/chat")
def chat(q: str = Query(..., description="User question")):
    """Return a naive answer built from stored documents."""
    # Import here to avoid loading embedding models during unrelated endpoints/tests.
    from embeddings import search_documents

    hits = search_documents(q)
    if not hits:
        answer = "No documents found."
    else:
        answer = "Context: " + " ".join(h["content"] for h in hits)
    return {"answer": answer}


def _health_payload() -> dict[str, int | bool]:
    """Build the base app health payload."""
    # Use monotonic time to avoid issues if the system clock changes.
    uptime_s = int(time.monotonic() - APP_START_TIME)
    return {"healthy": True, "uptime_s": uptime_s}


@app.get("/health")
def health_app():
    """Return application liveness and uptime."""
    return _health_payload()


@app.get("/health/live")
def health_live():
    """Alias liveness endpoint for standard probes."""
    return _health_payload()


@app.post("/managers", status_code=201)
@_require_valid_manager
async def create_manager(payload: ManagerCreate):
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


@app.on_event("startup")
async def _configure_default_executor() -> None:
    """Install a known-good default executor for sync endpoints."""
    asyncio.get_running_loop().set_default_executor(_APP_EXECUTOR)


def _db_timeout_seconds() -> float:
    """Return the DB health timeout in seconds."""
    # Cap health checks to 5s so monitoring calls never stall longer.
    return min(float(os.getenv("DB_HEALTH_TIMEOUT_S", "5")), 5.0)


def _ping_db(timeout_seconds: float) -> None:
    """Run a lightweight DB query to verify connectivity."""
    # Pass a connect timeout so the ping doesn't hang on slow networks.
    conn = connect_db(connect_timeout=timeout_seconds)
    try:
        conn.execute("SELECT 1")
    finally:
        conn.close()


@app.get("/health/db")
async def health_db():
    """Return database connectivity status and latency."""
    start = time.perf_counter()
    timeout_seconds = _db_timeout_seconds()
    try:
        # Use a dedicated executor to avoid relying on the loop default executor.
        await asyncio.wait_for(
            asyncio.get_running_loop().run_in_executor(get_health_executor(), _ping_db, timeout_seconds),
            timeout=timeout_seconds,
        )
        latency_ms = int((time.perf_counter() - start) * 1000)
        payload = {"healthy": True, "latency_ms": latency_ms}
        return JSONResponse(status_code=200, content=payload)
    except TimeoutError:
        # Fail fast to keep the endpoint under the timeout budget.
        latency_ms = int(timeout_seconds * 1000)
        payload = {"healthy": False, "latency_ms": latency_ms}
        return JSONResponse(status_code=503, content=payload)
    except Exception:
        latency_ms = int((time.perf_counter() - start) * 1000)
        payload = {"healthy": False, "latency_ms": latency_ms}
        return JSONResponse(status_code=503, content=payload)


@app.get("/health/ready")
async def health_ready():
    """Return readiness status combining app and database checks."""
    # Reuse the existing health endpoints to keep readiness logic consistent.
    app_payload = _health_payload()
    db_response = await health_db()
    db_payload = json.loads(db_response.body)
    healthy = app_payload["healthy"] and db_payload["healthy"]
    payload = {
        "healthy": healthy,
        "uptime_s": app_payload["uptime_s"],
        "db_latency_ms": db_payload["latency_ms"],
    }
    status_code = 200 if healthy else 503
    return JSONResponse(status_code=status_code, content=payload)


@app.on_event("shutdown")
def _shutdown_executors() -> None:
    """Release the health check executors on app shutdown."""
    _APP_EXECUTOR.shutdown(wait=False, cancel_futures=True)
    _HEALTH_EXECUTOR.shutdown(wait=False, cancel_futures=True)
