"""Minimal FastAPI app providing /chat and health endpoints."""

from __future__ import annotations

import asyncio
import os
import re
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor

from fastapi import Depends, FastAPI, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, validator

from adapters.base import connect_db
from embeddings import search_documents

app = FastAPI()
APP_EXECUTOR = ThreadPoolExecutor(max_workers=4)
HEALTH_EXECUTOR = ThreadPoolExecutor(max_workers=1)
EMAIL_PATTERN = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


class ManagerCreate(BaseModel):
    """Payload for manager creation."""

    name: str
    email: str
    department: str

    @validator("name")
    def _name_required(cls, value: str) -> str:
        trimmed = value.strip()
        if not trimmed:
            raise ValueError("Name is required.")
        return trimmed

    @validator("department")
    def _department_required(cls, value: str) -> str:
        trimmed = value.strip()
        if not trimmed:
            raise ValueError("Department is required.")
        return trimmed

    @validator("email")
    def _email_valid(cls, value: str) -> str:
        trimmed = value.strip()
        if not EMAIL_PATTERN.match(trimmed):
            raise ValueError("Email must be a valid email address.")
        return trimmed


def _validate_manager_payload(payload: ManagerCreate) -> ManagerCreate:
    """Dependency hook to normalize validated manager payloads."""
    # Centralize normalization so validation stays consistent across endpoints.
    return ManagerCreate(
        name=payload.name,
        email=payload.email.lower(),
        department=payload.department,
    )


def _ensure_managers_table(conn) -> None:
    """Create the managers table if it does not already exist."""
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


def _insert_manager(conn, manager: ManagerCreate) -> int:
    """Insert a manager row and return the generated id."""
    placeholder = "%s" if not isinstance(conn, sqlite3.Connection) else "?"
    values_clause = ",".join([placeholder] * 3)
    sql = f"INSERT INTO managers (name, email, department) VALUES ({values_clause})"
    if isinstance(conn, sqlite3.Connection):
        cursor = conn.execute(sql, (manager.name, manager.email, manager.department))
        conn.commit()
        return int(cursor.lastrowid)
    cursor = conn.execute(
        f"{sql} RETURNING id", (manager.name, manager.email, manager.department)
    )
    row = cursor.fetchone()
    return int(row[0])


def _clean_validation_message(message: str) -> str:
    """Strip pydantic wrapper text from validation error messages."""
    prefix = "Value error, "
    return message[len(prefix) :] if message.startswith(prefix) else message


@app.exception_handler(RequestValidationError)
async def _validation_exception_handler(
    request: Request, exc: RequestValidationError
):
    """Translate validation failures into 400 responses with field detail."""
    errors = []
    for error in exc.errors():
        loc = error.get("loc", [])
        field = str(loc[-1]) if loc else "body"
        message = error.get("msg", "Invalid value.")
        errors.append({"field": field, "message": _clean_validation_message(message)})
    return JSONResponse(status_code=400, content={"errors": errors})


@app.get("/chat")
def chat(q: str = Query(..., description="User question")):
    """Return a naive answer built from stored documents."""
    hits = search_documents(q)
    if not hits:
        answer = "No documents found."
    else:
        answer = "Context: " + " ".join(h["content"] for h in hits)
    return {"answer": answer}


@app.post("/managers", status_code=201)
async def create_manager(manager: ManagerCreate = Depends(_validate_manager_payload)):
    """Persist a manager record after validating required fields."""
    # Keep the handler async while doing minimal DB work for now.
    return _create_manager_record(manager)


def _create_manager_record(manager: ManagerCreate) -> dict[str, object]:
    """Insert and return a manager record dict."""
    conn = connect_db()
    try:
        _ensure_managers_table(conn)
        manager_id = _insert_manager(conn, manager)
    finally:
        conn.close()
    return {
        "id": manager_id,
        "name": manager.name,
        "email": manager.email,
        "department": manager.department,
    }


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
