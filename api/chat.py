"""Minimal FastAPI app providing /chat and health endpoints."""

from __future__ import annotations

import asyncio
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor

from fastapi import FastAPI, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field

from adapters.base import connect_db
from api.managers import router as managers_router

app = FastAPI()
# Register the managers router to keep /managers definitions in its own module.
app.include_router(managers_router)
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


class ChatResponse(BaseModel):
    """Response payload for chat responses."""

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {"answer": ("Context: The latest holdings update was filed on 2024-06-30.")}
            ]
        }
    )
    answer: str = Field(..., description="Generated answer based on stored documents")


class HealthDbResponse(BaseModel):
    """Response payload for database health checks."""

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {"healthy": True, "latency_ms": 42},
                {"healthy": False, "latency_ms": 5000},
            ]
        }
    )
    healthy: bool = Field(..., description="Whether the database is reachable")
    latency_ms: int = Field(..., description="Observed database ping latency in milliseconds")


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


# OpenAPI metadata keeps /docs clear about chat behavior.
@app.get(
    "/chat",
    response_model=ChatResponse,
    summary="Answer a chat query",
    description=(
        "Search stored documents using the provided question and return a concise "
        "answer composed from the matching context."
    ),
)
def chat(
    q: str = Query(
        ...,
        description="User question",
        examples=["What is the latest holdings update?"],
    )
):
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


@app.get("/healthz")
def healthz():
    """Alias liveness endpoint for common probe conventions."""
    # Keep probe aliases routed through the same liveness payload.
    return _health_payload()


@app.get("/livez")
def health_livez():
    """Alias live probe endpoint for common probe conventions."""
    return _health_payload()


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


@app.get(
    "/health/db",
    response_model=HealthDbResponse,
    summary="Check database connectivity",
    description=(
        "Run a lightweight database ping and return the health status with " "observed latency."
    ),
    responses={
        503: {
            "model": HealthDbResponse,
            "description": "Database unavailable",
            "content": {
                "application/json": {
                    "examples": {
                        "timeout": {
                            "summary": "Timed out ping",
                            "value": {"healthy": False, "latency_ms": 5000},
                        }
                    }
                }
            },
        }
    },
)
async def health_db():
    """Return database connectivity status and latency."""
    start = time.perf_counter()
    timeout_seconds = _db_timeout_seconds()
    try:
        # Use a dedicated executor to avoid relying on the loop default executor.
        await asyncio.wait_for(
            asyncio.get_running_loop().run_in_executor(
                get_health_executor(), _ping_db, timeout_seconds
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


@app.get("/readyz")
async def health_readyz():
    """Alias readiness endpoint for common probe conventions."""
    # Reuse the main readiness check so probes stay in sync.
    return await health_ready()


@app.on_event("shutdown")
def _shutdown_executors() -> None:
    """Release the health check executors on app shutdown."""
    _APP_EXECUTOR.shutdown(wait=False, cancel_futures=True)
    _HEALTH_EXECUTOR.shutdown(wait=False, cancel_futures=True)
