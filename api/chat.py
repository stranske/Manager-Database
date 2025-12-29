"""Minimal FastAPI app providing /chat and health endpoints."""

from __future__ import annotations

import asyncio
import os
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse

from adapters.base import connect_db
from embeddings import search_documents

app = FastAPI()
APP_EXECUTOR = ThreadPoolExecutor(max_workers=4)
HEALTH_EXECUTOR = ThreadPoolExecutor(max_workers=1)


@app.get("/chat")
def chat(q: str = Query(..., description="User question")):
    """Return a naive answer built from stored documents."""
    hits = search_documents(q)
    if not hits:
        answer = "No documents found."
    else:
        answer = "Context: " + " ".join(h["content"] for h in hits)
    return {"answer": answer}


@app.on_event("startup")
async def _configure_default_executor() -> None:
    """Install a known-good default executor for sync endpoints."""
    global APP_EXECUTOR
    global HEALTH_EXECUTOR
    # Recreate executors if a prior shutdown (e.g., tests) already closed them.
    if getattr(APP_EXECUTOR, "_shutdown", False):
        APP_EXECUTOR = ThreadPoolExecutor(max_workers=4)
    if getattr(HEALTH_EXECUTOR, "_shutdown", False):
        HEALTH_EXECUTOR = ThreadPoolExecutor(max_workers=1)
    asyncio.get_running_loop().set_default_executor(APP_EXECUTOR)


def _db_timeout_seconds() -> float:
    """Return the DB health timeout in seconds."""
    # Cap health checks to 5s so monitoring calls never stall longer.
    return min(float(os.getenv("DB_HEALTH_TIMEOUT_S", "5")), 5.0)


def _ping_db(timeout_seconds: float) -> None:
    """Run a lightweight DB query to verify connectivity."""
    # Pass a connect timeout so the ping doesn't hang on slow networks.
    conn = connect_db(connect_timeout=timeout_seconds)
    try:
        if isinstance(conn, sqlite3.Connection):
            # Align SQLite busy timeout with the health check deadline.
            conn.execute(f"PRAGMA busy_timeout = {int(timeout_seconds * 1000)}")
        else:
            # Best-effort statement timeout keeps slow DBs within the health budget.
            try:
                conn.execute(
                    "SET statement_timeout = %s", (int(timeout_seconds * 1000),)
                )
            except Exception:
                pass
        cursor = conn.execute("SELECT 1")
        # Force the database to return a result so the query truly runs.
        if hasattr(cursor, "fetchone"):
            cursor.fetchone()
    finally:
        conn.close()


def _health_payload(start: float, healthy: bool) -> dict[str, int | bool]:
    """Build the health response payload with a consistent latency measurement."""
    # Measure latency after each exit path to keep results comparable.
    latency_ms = int((time.perf_counter() - start) * 1000)
    return {"healthy": healthy, "latency_ms": latency_ms}


@app.get("/health/db")
async def health_db():
    """Return database connectivity status and latency."""
    start = time.perf_counter()
    timeout_seconds = _db_timeout_seconds()
    loop = asyncio.get_running_loop()
    future = loop.run_in_executor(HEALTH_EXECUTOR, _ping_db, timeout_seconds)
    try:
        # Use a dedicated executor to avoid relying on the loop default executor.
        await asyncio.wait_for(future, timeout=timeout_seconds)
        payload = _health_payload(start, True)
        return JSONResponse(status_code=200, content=payload)
    except TimeoutError:
        # Cancel the queued future to avoid piling up stale health checks.
        future.cancel()
        # Fail fast to keep the endpoint under the timeout budget.
        payload = _health_payload(start, False)
        return JSONResponse(status_code=503, content=payload)
    except Exception:
        payload = _health_payload(start, False)
        return JSONResponse(status_code=503, content=payload)


@app.on_event("shutdown")
def _shutdown_executors() -> None:
    """Release the health check executors on app shutdown."""
    APP_EXECUTOR.shutdown(wait=False, cancel_futures=True)
    HEALTH_EXECUTOR.shutdown(wait=False, cancel_futures=True)
