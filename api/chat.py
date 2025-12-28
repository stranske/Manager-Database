"""Minimal FastAPI app providing /chat and health endpoints."""

from __future__ import annotations

import asyncio
import os
import re
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor

from fastapi import FastAPI, Query
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, field_validator

from adapters.base import connect_db
from embeddings import search_documents

app = FastAPI()
APP_EXECUTOR = ThreadPoolExecutor(max_workers=4)
HEALTH_EXECUTOR = ThreadPoolExecutor(max_workers=1)


class ManagerCreate(BaseModel):
    """Payload for creating a manager record."""

    name: str
    email: str
    department: str

    @field_validator("name", "department")
    @classmethod
    def non_empty(cls, value, info):
        """Reject blank strings so we don't store empty names/departments."""
        stripped = value.strip()
        # Normalize whitespace to keep required fields consistent.
        if not stripped:
            raise ValueError(f"{info.field_name} must not be empty")
        return stripped

    @field_validator("email")
    @classmethod
    def valid_email(cls, value):
        """Require a basic email shape to avoid malformed addresses."""
        cleaned = value.strip()
        # Use a trimmed value so blank emails get a clear error message.
        if not cleaned:
            raise ValueError("email must not be empty")
        if not EMAIL_PATTERN.match(cleaned):
            raise ValueError("email must be a valid email address")
        return cleaned


def _format_validation_errors(exc: RequestValidationError) -> list[dict[str, str]]:
    """Normalize validation errors to include the field and message."""
    errors: list[dict[str, str]] = []
    for err in exc.errors():
        loc = err.get("loc", ())
        field = ""
        if len(loc) >= 2 and loc[0] == "body":
            field = str(loc[1])
        else:
            field = ".".join(str(part) for part in loc)
        message = err.get("msg", "Invalid value")
        if message.startswith("Value error, "):
            message = message.replace("Value error, ", "", 1)
        errors.append({"field": field, "message": message})
    return errors


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(_, exc: RequestValidationError):
    """Return 400 responses so validation failures match API expectations."""
    return JSONResponse(
        status_code=400, content={"errors": _format_validation_errors(exc)}
    )


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
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT NOT NULL,
                department TEXT NOT NULL
            )"""
        )


def _store_manager(payload: ManagerCreate) -> int:
    """Persist the manager record and return the new id."""
    conn = connect_db()
    _ensure_manager_table(conn)
    if isinstance(conn, sqlite3.Connection):
        cursor = conn.execute(
            "INSERT INTO managers(name, email, department) VALUES (?, ?, ?)",
            (payload.name, str(payload.email), payload.department),
        )
        manager_id = int(cursor.lastrowid) if cursor.lastrowid is not None else 0
    else:
        cursor = conn.execute(
            "INSERT INTO managers(name, email, department) VALUES (%s, %s, %s) RETURNING id",
            (payload.name, str(payload.email), payload.department),
        )
        row = cursor.fetchone()
        manager_id = int(row[0]) if row else 0
    conn.commit()
    conn.close()
    return manager_id


@app.get("/chat")
def chat(q: str = Query(..., description="User question")):
    """Return a naive answer built from stored documents."""
    hits = search_documents(q)
    if not hits:
        answer = "No documents found."
    else:
        answer = "Context: " + " ".join(h["content"] for h in hits)
    return {"answer": answer}


@app.post("/managers")
def create_manager(payload: ManagerCreate):
    """Create a manager record with validated fields."""
    manager_id = _store_manager(payload)
    return {"id": manager_id, **payload.model_dump()}


@app.on_event("startup")
async def _configure_default_executor() -> None:
    """Install a known-good default executor for sync endpoints."""
    asyncio.get_running_loop().set_default_executor(APP_EXECUTOR)


def _db_timeout_seconds() -> float:
    """Return the DB health timeout in seconds."""
    return float(os.getenv("DB_HEALTH_TIMEOUT_S", "5"))


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
            asyncio.get_running_loop().run_in_executor(
                HEALTH_EXECUTOR, _ping_db, timeout_seconds
            ),
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


@app.on_event("shutdown")
def _shutdown_executors() -> None:
    """Release the health check executors on app shutdown."""
    APP_EXECUTOR.shutdown(wait=False, cancel_futures=True)
    HEALTH_EXECUTOR.shutdown(wait=False, cancel_futures=True)


EMAIL_PATTERN = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
