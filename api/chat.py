"""Minimal FastAPI app providing /chat and health endpoints."""

from __future__ import annotations

import os
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse

from adapters.base import connect_db
from embeddings import search_documents

app = FastAPI()


@app.get("/chat")
def chat(q: str = Query(..., description="User question")):
    """Return a naive answer built from stored documents."""
    hits = search_documents(q)
    if not hits:
        answer = "No documents found."
    else:
        answer = "Context: " + " ".join(h["content"] for h in hits)
    return {"answer": answer}


def _db_timeout_seconds() -> float:
    """Return the DB health timeout in seconds."""
    return float(os.getenv("DB_HEALTH_TIMEOUT_S", "5"))


def _ping_db() -> None:
    """Run a lightweight DB query to verify connectivity."""
    conn = connect_db()
    try:
        conn.execute("SELECT 1")
    finally:
        conn.close()


@app.get("/health/db")
def health_db():
    """Return database connectivity status and latency."""
    start = time.perf_counter()
    timeout_seconds = _db_timeout_seconds()
    executor = ThreadPoolExecutor(max_workers=1)
    future = None
    try:
        # Run in a worker thread so slow connections can be timed out.
        future = executor.submit(_ping_db)
        future.result(timeout=timeout_seconds)
        latency_ms = int((time.perf_counter() - start) * 1000)
        payload = {"healthy": True, "latency_ms": latency_ms}
        return JSONResponse(status_code=200, content=payload)
    except FutureTimeoutError:
        # Fail fast to keep the endpoint under the timeout budget.
        if future is not None:
            future.cancel()
        latency_ms = int(timeout_seconds * 1000)
        payload = {"healthy": False, "latency_ms": latency_ms}
        return JSONResponse(status_code=503, content=payload)
    except Exception:
        latency_ms = int((time.perf_counter() - start) * 1000)
        payload = {"healthy": False, "latency_ms": latency_ms}
        return JSONResponse(status_code=503, content=payload)
    finally:
        # Avoid waiting on slow threads so we can return promptly.
        executor.shutdown(wait=False, cancel_futures=True)
