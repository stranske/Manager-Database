"""Minimal FastAPI app providing /chat and health endpoints."""

from __future__ import annotations

import asyncio
import os
import re
import sqlite3
import time
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor

from fastapi import Body, Depends, FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

from adapters.base import connect_db
from embeddings import search_documents

app = FastAPI()
APP_EXECUTOR = ThreadPoolExecutor(max_workers=4)
HEALTH_EXECUTOR = ThreadPoolExecutor(max_workers=1)
EMAIL_PATTERN = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


@dataclass(frozen=True)
class ManagerPayload:
    name: str
    email: str
    department: str


@app.get("/chat")
def chat(q: str = Query(..., description="User question")):
    """Return a naive answer built from stored documents."""
    hits = search_documents(q)
    if not hits:
        answer = "No documents found."
    else:
        answer = "Context: " + " ".join(h["content"] for h in hits)
    return {"answer": answer}


def _normalize_text(value: object) -> str:
    """Normalize incoming text fields for validation."""
    return value.strip() if isinstance(value, str) else ""


def validate_manager_payload(payload: dict = Body(...)) -> ManagerPayload:
    """Validate manager payloads and raise 400s with field-level errors."""
    errors: list[dict[str, str]] = []
    name = _normalize_text(payload.get("name"))
    if not name:
        errors.append({"field": "name", "message": "name is required"})
    email = _normalize_text(payload.get("email"))
    if not email:
        errors.append({"field": "email", "message": "email is required"})
    elif not EMAIL_PATTERN.fullmatch(email):
        # Keep email validation lightweight while still rejecting obvious bad input.
        errors.append({"field": "email", "message": "email must be a valid email address"})
    department = _normalize_text(payload.get("department"))
    if not department:
        errors.append({"field": "department", "message": "department is required"})
    if errors:
        # Normalize all validation errors to a single 400 response for clients.
        raise HTTPException(status_code=400, detail=errors)
    return ManagerPayload(name=name, email=email, department=department)


def _ensure_managers_table(conn) -> None:
    """Ensure the managers table exists before inserts."""
    if isinstance(conn, sqlite3.Connection):
        conn.execute(
            """CREATE TABLE IF NOT EXISTS managers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                email TEXT NOT NULL,
                department TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )"""
        )
    else:
        conn.execute(
            """CREATE TABLE IF NOT EXISTS managers (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT NOT NULL,
                department TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )"""
        )


def _insert_manager(conn, manager: ManagerPayload) -> int:
    """Insert the manager row and return its new ID."""
    if isinstance(conn, sqlite3.Connection):
        cursor = conn.execute(
            "INSERT INTO managers(name, email, department) VALUES (?, ?, ?)",
            (manager.name, manager.email, manager.department),
        )
        manager_id = int(cursor.lastrowid)
    else:
        cursor = conn.execute(
            "INSERT INTO managers(name, email, department) VALUES (%s, %s, %s) RETURNING id",
            (manager.name, manager.email, manager.department),
        )
        manager_id = int(cursor.fetchone()[0])
    conn.commit()
    return manager_id


@app.post("/managers", status_code=201)
def create_manager(manager: ManagerPayload = Depends(validate_manager_payload)):
    """Create a manager record after validating required fields."""
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
