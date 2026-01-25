"""Minimal FastAPI app providing /chat and health endpoints."""

from __future__ import annotations

import asyncio
import json
import os
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from threading import Lock
from typing import Any, cast

import boto3
from botocore.config import Config as BotoConfig
from fastapi import FastAPI, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, Histogram, generate_latest
from pydantic import BaseModel, ConfigDict, Field

from adapters.base import connect_db
from api.managers import router as managers_router

app = FastAPI()
# Tag manager endpoints so they group clearly in the Swagger UI.
app.include_router(managers_router, tags=["Managers"])
_APP_EXECUTOR = ThreadPoolExecutor(max_workers=4)
# Allow concurrent health checks without serializing every dependency probe.
_HEALTH_EXECUTOR = ThreadPoolExecutor(max_workers=3)


@dataclass(frozen=True)
class HealthClock:
    """Centralize time sources to enable deterministic health check tests."""

    perf_counter: Callable[[], float]
    monotonic: Callable[[], float]
    sleep: Callable[[float], None]


HEALTH_CLOCK = HealthClock(time.perf_counter, time.monotonic, time.sleep)
APP_START_TIME = HEALTH_CLOCK.monotonic()
HEALTH_CHECK_DURATION = Histogram(
    "health_check_duration_seconds",
    "Health check latency by endpoint.",
    labelnames=("endpoint",),
)


def get_health_executor():
    """Return a working health executor, creating a new one if needed."""
    global _HEALTH_EXECUTOR
    if _HEALTH_EXECUTOR._shutdown:
        _HEALTH_EXECUTOR = ThreadPoolExecutor(max_workers=3)
    return _HEALTH_EXECUTOR


class CircuitBreaker:
    """Track consecutive failures and open after a threshold."""

    def __init__(
        self,
        failure_threshold: int = 3,
        reset_timeout_s: float = 30.0,
        monotonic_fn: Callable[[], float] | None = None,
    ) -> None:
        self._failure_threshold = failure_threshold
        self._reset_timeout_s = reset_timeout_s
        self._consecutive_failures = 0
        self._opened_at: float | None = None
        # Allow tests to inject a deterministic clock without patching globals.
        self._monotonic = monotonic_fn or HEALTH_CLOCK.monotonic
        self._lock = Lock()

    def is_open(self) -> bool:
        """Return True when the breaker is open and requests should be skipped."""
        with self._lock:
            if self._opened_at is None:
                return False
            if (self._monotonic() - self._opened_at) >= self._reset_timeout_s:
                self._opened_at = None
                self._consecutive_failures = 0
                return False
            return True

    def record_success(self) -> None:
        """Reset the breaker after a successful call."""
        with self._lock:
            self._consecutive_failures = 0
            self._opened_at = None

    def record_failure(self) -> None:
        """Track a failure and open the breaker if needed."""
        with self._lock:
            self._consecutive_failures += 1
            if self._consecutive_failures >= self._failure_threshold:
                if self._opened_at is None:
                    self._opened_at = self._monotonic()


def _circuit_breaker_reset_seconds() -> float:
    """Return the circuit breaker reset timeout in seconds."""
    return max(float(os.getenv("HEALTH_CIRCUIT_RESET_S", "30")), 1.0)


_MINIO_CIRCUIT = CircuitBreaker(reset_timeout_s=_circuit_breaker_reset_seconds())
_REDIS_CIRCUIT = CircuitBreaker(reset_timeout_s=_circuit_breaker_reset_seconds())
_HEALTH_RETRY_BACKOFFS = (0.1, 0.2, 0.4)


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


class HealthDetailedResponse(BaseModel):
    """Response payload for detailed health checks."""

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "healthy": True,
                    "uptime_s": 3600,
                    "components": {
                        "app": {"healthy": True, "uptime_s": 3600},
                        "database": {"healthy": True, "latency_ms": 42},
                        "minio": {"healthy": True, "latency_ms": 58},
                        "redis": {"healthy": True, "latency_ms": 12, "enabled": True},
                    },
                }
            ]
        }
    )
    healthy: bool = Field(..., description="Whether all components are healthy")
    uptime_s: int = Field(..., description="Application uptime in seconds")
    components: dict[str, dict[str, int | bool]] = Field(
        ..., description="Per-component health details"
    )


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
    errors = _format_validation_errors(exc)
    return JSONResponse(status_code=400, content={"errors": errors, "error": errors})


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
        examples=cast(
            Any,
            {
                "basic": {
                    "summary": "Holdings question",
                    "value": "What is the latest holdings update?",
                }
            },
        ),
    ),
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
    uptime_s = int(HEALTH_CLOCK.monotonic() - APP_START_TIME)
    return {"healthy": True, "uptime_s": uptime_s}


def _format_dependency_error(exc: Exception) -> str:
    """Return a short reason string for failed dependency checks."""
    message = str(exc).strip()
    return (message or exc.__class__.__name__)[:200]


def _health_summary_timeout_seconds() -> float:
    """Return the timeout budget for /health dependency checks."""
    timeout = float(os.getenv("HEALTH_SUMMARY_TIMEOUT_S", "0.2"))
    return max(min(timeout, 0.2), 0.05)


async def _run_dependency_check(
    func,
    timeout_seconds: float,
    *args,
    circuit_breaker: CircuitBreaker | None = None,
    enabled: bool = True,
    include_enabled: bool = False,
) -> tuple[dict[str, int | bool], str | None]:
    """Run a dependency check and return its payload plus failure reason."""
    if not enabled:
        payload = {"healthy": True, "latency_ms": 0}
        if include_enabled:
            payload["enabled"] = False
        return payload, None
    if circuit_breaker and circuit_breaker.is_open():
        payload = {"healthy": False, "latency_ms": 0, "circuit_open": True}
        if include_enabled:
            payload["enabled"] = True
        return payload, "circuit_open"
    start = HEALTH_CLOCK.perf_counter()
    try:
        # Keep retries inside the per-check timeout budget for responsiveness.
        await _run_health_check_with_retries(func, timeout_seconds, *args)
        latency_ms = int((HEALTH_CLOCK.perf_counter() - start) * 1000)
        if circuit_breaker:
            circuit_breaker.record_success()
        payload = {"healthy": True, "latency_ms": latency_ms}
        if include_enabled:
            payload["enabled"] = True
        return payload, None
    except TimeoutError:
        latency_ms = int(min(timeout_seconds, HEALTH_CLOCK.perf_counter() - start) * 1000)
        if circuit_breaker:
            circuit_breaker.record_failure()
        payload = {"healthy": False, "latency_ms": latency_ms}
        if include_enabled:
            payload["enabled"] = True
        return payload, "timeout"
    except Exception as exc:
        latency_ms = int((HEALTH_CLOCK.perf_counter() - start) * 1000)
        if circuit_breaker:
            circuit_breaker.record_failure()
        payload = {"healthy": False, "latency_ms": latency_ms}
        if include_enabled:
            payload["enabled"] = True
        return payload, _format_dependency_error(exc)


async def _run_health_summary_checks(
    timeout_budget: float,
    db_timeout_seconds: float,
    minio_timeout_seconds: float,
    redis_timeout_seconds: float,
    redis_url: str | None,
) -> dict[str, tuple[dict[str, int | bool], str | None]]:
    """Run summary dependency checks with an overall time budget."""
    tasks = {
        "database": asyncio.create_task(
            _run_dependency_check(_ping_db, db_timeout_seconds, db_timeout_seconds)
        ),
        "minio": asyncio.create_task(
            _run_dependency_check(
                _ping_minio,
                minio_timeout_seconds,
                minio_timeout_seconds,
                circuit_breaker=_MINIO_CIRCUIT,
            )
        ),
        "redis": asyncio.create_task(
            _run_dependency_check(
                _ping_redis,
                redis_timeout_seconds,
                redis_url,
                redis_timeout_seconds,
                circuit_breaker=_REDIS_CIRCUIT,
                enabled=bool(redis_url),
                include_enabled=True,
            )
        ),
    }
    task_names = {task: name for name, task in tasks.items()}
    start = HEALTH_CLOCK.perf_counter()
    # Allow a small buffer beyond the budget to avoid false timeouts in the executor.
    overall_timeout = min(timeout_budget + 0.05, 0.2)
    done, pending = await asyncio.wait(tasks.values(), timeout=overall_timeout)
    elapsed_ms = int(min(overall_timeout, HEALTH_CLOCK.perf_counter() - start) * 1000)
    for task in pending:
        task.cancel()
        name = task_names.get(task)
        if name == "minio":
            _MINIO_CIRCUIT.record_failure()
        elif name == "redis" and redis_url:
            _REDIS_CIRCUIT.record_failure()
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)
    results: dict[str, tuple[dict[str, int | bool], str | None]] = {}
    for name, task in tasks.items():
        if task in done:
            results[name] = task.result()
        else:
            payload = {"healthy": False, "latency_ms": elapsed_ms}
            if name == "redis":
                payload["enabled"] = bool(redis_url)
            results[name] = (payload, "timeout")
    return results


@app.get("/health")
async def health_app():
    """Return application health and dependency status."""
    start = HEALTH_CLOCK.perf_counter()
    try:
        app_payload = _health_payload()
        timeout_budget = _health_summary_timeout_seconds()
        db_timeout_seconds = min(_db_timeout_seconds(), timeout_budget)
        minio_timeout_seconds = min(_minio_timeout_seconds(), timeout_budget)
        redis_timeout_seconds = min(_redis_timeout_seconds(), timeout_budget)
        redis_url = os.getenv("REDIS_URL")
        results = await _run_health_summary_checks(
            timeout_budget,
            db_timeout_seconds,
            minio_timeout_seconds,
            redis_timeout_seconds,
            redis_url,
        )
        db_payload, db_reason = results["database"]
        minio_payload, minio_reason = results["minio"]
        redis_payload, redis_reason = results["redis"]
        components = {
            "app": {"healthy": app_payload["healthy"], "uptime_s": app_payload["uptime_s"]},
            "database": db_payload,
            "minio": minio_payload,
            "redis": redis_payload,
        }
        failed_checks = {}
        if not db_payload["healthy"]:
            failed_checks["database"] = db_reason or "unhealthy"
        if not minio_payload["healthy"]:
            failed_checks["minio"] = minio_reason or "unhealthy"
        if redis_payload.get("enabled", True) and not redis_payload["healthy"]:
            failed_checks["redis"] = redis_reason or "unhealthy"
        redis_healthy = redis_payload["healthy"] if redis_payload.get("enabled", True) else True
        healthy = (
            app_payload["healthy"]
            and db_payload["healthy"]
            and minio_payload["healthy"]
            and redis_healthy
        )
        payload = {
            "healthy": healthy,
            "uptime_s": app_payload["uptime_s"],
            "components": components,
            "failed_checks": failed_checks,
        }
        status_code = 200 if healthy else 503
        return JSONResponse(status_code=status_code, content=payload)
    finally:
        # Track summary health latency for dashboards and alerts.
        HEALTH_CHECK_DURATION.labels(endpoint="health").observe(HEALTH_CLOCK.perf_counter() - start)


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


def _minio_timeout_seconds() -> float:
    """Return the MinIO health timeout in seconds."""
    return min(float(os.getenv("MINIO_HEALTH_TIMEOUT_S", "5")), 5.0)


def _minio_client(timeout_seconds: float):
    """Create a MinIO client with aggressive timeouts for health checks."""
    config = BotoConfig(
        connect_timeout=timeout_seconds,
        read_timeout=timeout_seconds,
        retries={"max_attempts": 0},
    )
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
        aws_access_key_id=os.getenv("MINIO_ROOT_USER", "minio"),
        aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD", "minio123"),
        region_name=os.getenv("MINIO_REGION", "us-east-1"),
        config=config,
    )


def _redis_timeout_seconds() -> float:
    """Return the Redis health timeout in seconds."""
    return min(float(os.getenv("REDIS_HEALTH_TIMEOUT_S", "2")), 5.0)


def _ping_redis(redis_url: str, timeout_seconds: float) -> None:
    """Run a lightweight Redis ping to verify cache connectivity."""
    # Import lazily so Redis remains optional outside of cache deployments.
    import redis

    client = redis.Redis.from_url(redis_url, socket_timeout=timeout_seconds)
    client.ping()


def _ping_minio(timeout_seconds: float) -> None:
    """Run a lightweight MinIO API call to verify connectivity."""
    client = _minio_client(timeout_seconds)
    client.list_buckets()


async def _run_health_check_with_retries(
    func,
    timeout_seconds: float,
    *args,
    sleep_fn: Callable[[float], None] | None = None,
    perf_counter_fn: Callable[[], float] | None = None,
) -> None:
    """Run a health check with exponential backoff for flaky dependencies."""

    def _run_with_retries_sync() -> None:
        # Resolve injected timing functions once so retries stay consistent.
        resolved_sleep = sleep_fn or HEALTH_CLOCK.sleep
        resolved_perf_counter = perf_counter_fn or HEALTH_CLOCK.perf_counter
        deadline = resolved_perf_counter() + timeout_seconds
        for backoff in _HEALTH_RETRY_BACKOFFS:
            try:
                func(*args)
                return
            except Exception:
                if resolved_perf_counter() + backoff >= deadline:
                    raise
                resolved_sleep(backoff)
        func(*args)

    await asyncio.wait_for(
        asyncio.get_running_loop().run_in_executor(get_health_executor(), _run_with_retries_sync),
        timeout=timeout_seconds,
    )


@app.get(
    "/health/db",
    response_model=HealthDbResponse,
    summary="Check database connectivity",
    description=(
        "Run a lightweight database ping and return the health status with observed latency."
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
    start = HEALTH_CLOCK.perf_counter()
    timeout_seconds = _db_timeout_seconds()
    try:
        # Use a dedicated executor to avoid relying on the loop default executor.
        await _run_health_check_with_retries(_ping_db, timeout_seconds, timeout_seconds)
        latency_ms = int((HEALTH_CLOCK.perf_counter() - start) * 1000)
        payload = {"healthy": True, "latency_ms": latency_ms}
        return JSONResponse(status_code=200, content=payload)
    except TimeoutError:
        # Fail fast to keep the endpoint under the timeout budget.
        latency_ms = int(min(timeout_seconds, HEALTH_CLOCK.perf_counter() - start) * 1000)
        payload = {"healthy": False, "latency_ms": latency_ms}
        return JSONResponse(status_code=503, content=payload)
    except Exception:
        latency_ms = int((HEALTH_CLOCK.perf_counter() - start) * 1000)
        payload = {"healthy": False, "latency_ms": latency_ms}
        return JSONResponse(status_code=503, content=payload)
    finally:
        # Record total DB check duration even when the ping fails.
        HEALTH_CHECK_DURATION.labels(endpoint="health_db").observe(
            HEALTH_CLOCK.perf_counter() - start
        )


@app.get("/health/ready")
async def health_ready():
    """Return readiness status combining app and database checks."""
    start = HEALTH_CLOCK.perf_counter()
    # Reuse the existing health endpoints to keep readiness logic consistent.
    app_payload = _health_payload()
    try:
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
    finally:
        # Capture readiness latency for alerting dashboards.
        HEALTH_CHECK_DURATION.labels(endpoint="health_ready").observe(
            HEALTH_CLOCK.perf_counter() - start
        )


@app.get("/readyz")
async def health_readyz():
    """Alias readiness endpoint for common probe conventions."""
    # Reuse the main readiness check so probes stay in sync.
    return await health_ready()


@app.get(
    "/health/detailed",
    response_model=HealthDetailedResponse,
    summary="Return detailed health status",
    description="Return per-component health status, including database connectivity.",
    responses={
        503: {
            "model": HealthDetailedResponse,
            "description": "One or more components are unavailable",
        }
    },
)
async def health_detailed():
    """Return detailed health status for app and database components."""
    start = HEALTH_CLOCK.perf_counter()
    # Reuse existing health payloads to keep liveness logic consistent.
    app_payload = _health_payload()
    try:
        db_response = await health_db()
        db_payload = json.loads(db_response.body)
        if _MINIO_CIRCUIT.is_open():
            minio_payload = {"healthy": False, "latency_ms": 0, "circuit_open": True}
        else:
            minio_start = HEALTH_CLOCK.perf_counter()
            minio_timeout_seconds = _minio_timeout_seconds()
            try:
                await _run_health_check_with_retries(
                    _ping_minio, minio_timeout_seconds, minio_timeout_seconds
                )
                minio_latency_ms = int((HEALTH_CLOCK.perf_counter() - minio_start) * 1000)
                _MINIO_CIRCUIT.record_success()
                minio_payload = {"healthy": True, "latency_ms": minio_latency_ms}
            except TimeoutError:
                minio_latency_ms = int(minio_timeout_seconds * 1000)
                _MINIO_CIRCUIT.record_failure()
                minio_payload = {"healthy": False, "latency_ms": minio_latency_ms}
            except Exception:
                minio_latency_ms = int((HEALTH_CLOCK.perf_counter() - minio_start) * 1000)
                _MINIO_CIRCUIT.record_failure()
                minio_payload = {"healthy": False, "latency_ms": minio_latency_ms}
        redis_url = os.getenv("REDIS_URL")
        if redis_url:
            if _REDIS_CIRCUIT.is_open():
                redis_payload = {
                    "healthy": False,
                    "latency_ms": 0,
                    "enabled": True,
                    "circuit_open": True,
                }
            else:
                redis_start = HEALTH_CLOCK.perf_counter()
                redis_timeout_seconds = _redis_timeout_seconds()
                try:
                    await _run_health_check_with_retries(
                        _ping_redis, redis_timeout_seconds, redis_url, redis_timeout_seconds
                    )
                    redis_latency_ms = int((HEALTH_CLOCK.perf_counter() - redis_start) * 1000)
                    _REDIS_CIRCUIT.record_success()
                    redis_payload = {
                        "healthy": True,
                        "latency_ms": redis_latency_ms,
                        "enabled": True,
                    }
                except TimeoutError:
                    redis_latency_ms = int(redis_timeout_seconds * 1000)
                    _REDIS_CIRCUIT.record_failure()
                    redis_payload = {
                        "healthy": False,
                        "latency_ms": redis_latency_ms,
                        "enabled": True,
                    }
                except Exception:
                    redis_latency_ms = int((HEALTH_CLOCK.perf_counter() - redis_start) * 1000)
                    _REDIS_CIRCUIT.record_failure()
                    redis_payload = {
                        "healthy": False,
                        "latency_ms": redis_latency_ms,
                        "enabled": True,
                    }
        else:
            # Skip cache probes when Redis isn't configured in this environment.
            redis_payload = {"healthy": True, "latency_ms": 0, "enabled": False}
        components = {
            "app": {"healthy": app_payload["healthy"], "uptime_s": app_payload["uptime_s"]},
            "database": {
                "healthy": db_payload["healthy"],
                "latency_ms": db_payload["latency_ms"],
            },
            "minio": minio_payload,
            "redis": redis_payload,
        }
        redis_healthy = redis_payload["healthy"] if redis_payload["enabled"] else True
        healthy = (
            app_payload["healthy"]
            and db_payload["healthy"]
            and minio_payload["healthy"]
            and redis_healthy
        )
        payload = {
            "healthy": healthy,
            "uptime_s": app_payload["uptime_s"],
            "components": components,
        }
        status_code = 200 if healthy else 503
        return JSONResponse(status_code=status_code, content=payload)
    finally:
        # Track full detailed health check latency for dashboards.
        HEALTH_CHECK_DURATION.labels(endpoint="health_detailed").observe(
            HEALTH_CLOCK.perf_counter() - start
        )


@app.get("/metrics")
def metrics() -> Response:
    """Return Prometheus metrics for scraping."""
    # Prometheus expects the text exposition format on this endpoint.
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.on_event("shutdown")
def _shutdown_executors() -> None:
    """Release the health check executors on app shutdown."""
    _APP_EXECUTOR.shutdown(wait=False, cancel_futures=True)
    _HEALTH_EXECUTOR.shutdown(wait=False, cancel_futures=True)


# Commit-message checklist:
# - [ ] type is accurate (feat, fix, test)
# - [ ] scope is clear (health)
# - [ ] summary is concise and imperative
