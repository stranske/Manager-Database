"""Test database error handling and graceful degradation."""

from __future__ import annotations

import asyncio
import sqlite3
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

import httpx
import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from api.chat import app


async def _post_manager(payload: dict):
    """Make a POST request to /managers endpoint."""
    await app.router.startup()
    try:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", timeout=5.0
        ) as client:
            return await client.post("/managers", json=payload)
    finally:
        await app.router.shutdown()


async def _get_managers(params: dict | None = None):
    """Make a GET request to /managers endpoint."""
    await app.router.startup()
    try:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", timeout=5.0
        ) as client:
            return await client.get("/managers", params=params)
    finally:
        await app.router.shutdown()


async def _get_manager(manager_id: int):
    """Make a GET request to /managers/{id} endpoint."""
    await app.router.startup()
    try:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", timeout=5.0
        ) as client:
            return await client.get(f"/managers/{manager_id}")
    finally:
        await app.router.shutdown()


@pytest.mark.asyncio
async def test_create_manager_returns_503_on_db_connection_error(monkeypatch):
    """POST /managers returns 503 when database is unreachable."""
    
    def mock_connect_db(*args, **kwargs):
        raise sqlite3.OperationalError("connection to server failed")
    
    monkeypatch.setattr("api.managers.connect_db", mock_connect_db)
    
    response = await _post_manager({
        "name": "Grace Hopper",
        "email": "grace@example.com",
        "department": "Engineering",
    })
    
    assert response.status_code == 503
    data = response.json()
    assert "detail" in data
    assert "Database service temporarily unavailable" in data["detail"]
    # Ensure no internal error details are exposed
    assert "sqlite3" not in str(data).lower()
    assert "OperationalError" not in str(data)


@pytest.mark.asyncio
async def test_list_managers_returns_503_on_db_connection_error(monkeypatch):
    """GET /managers returns 503 when database is unreachable."""
    
    def mock_connect_db(*args, **kwargs):
        raise sqlite3.DatabaseError("database is locked")
    
    monkeypatch.setattr("api.managers.connect_db", mock_connect_db)
    
    response = await _get_managers()
    
    assert response.status_code == 503
    data = response.json()
    assert "detail" in data
    assert "Database service temporarily unavailable" in data["detail"]
    # Ensure no internal error details are exposed
    assert "sqlite3" not in str(data).lower()
    assert "DatabaseError" not in str(data)


@pytest.mark.asyncio
async def test_get_manager_returns_503_on_db_connection_error(monkeypatch):
    """GET /managers/{id} returns 503 when database is unreachable."""
    
    def mock_connect_db(*args, **kwargs):
        raise sqlite3.OperationalError("disk I/O error")
    
    monkeypatch.setattr("api.managers.connect_db", mock_connect_db)
    
    response = await _get_manager(1)
    
    assert response.status_code == 503
    data = response.json()
    assert "detail" in data
    assert "Database service temporarily unavailable" in data["detail"]
    # Ensure no internal error details are exposed
    assert "sqlite3" not in str(data).lower()
    assert "OperationalError" not in str(data)


@pytest.mark.asyncio
async def test_create_manager_returns_503_on_psycopg_error(monkeypatch):
    """POST /managers returns 503 on Postgres connection errors."""
    
    class MockPsycopgError(Exception):
        """Mock psycopg OperationalError."""
        pass
    
    def mock_connect_db(*args, **kwargs):
        exc = MockPsycopgError("connection to server at localhost failed")
        exc.__class__.__name__ = "OperationalError"
        raise exc
    
    monkeypatch.setattr("api.managers.connect_db", mock_connect_db)
    
    response = await _post_manager({
        "name": "Grace Hopper",
        "email": "grace@example.com",
        "department": "Engineering",
    })
    
    assert response.status_code == 503
    data = response.json()
    assert "Database service temporarily unavailable" in data["detail"]


@pytest.mark.asyncio
async def test_create_manager_non_db_errors_propagate(monkeypatch):
    """POST /managers propagates non-database exceptions."""
    
    def mock_connect_db(*args, **kwargs):
        raise ValueError("Invalid connection string")
    
    monkeypatch.setattr("api.managers.connect_db", mock_connect_db)
    
    with pytest.raises(ValueError, match="Invalid connection string"):
        await _post_manager({
            "name": "Grace Hopper",
            "email": "grace@example.com",
            "department": "Engineering",
        })


@pytest.mark.asyncio
async def test_list_managers_non_db_errors_propagate(monkeypatch):
    """GET /managers propagates non-database exceptions."""
    
    def mock_connect_db(*args, **kwargs):
        raise TypeError("Unexpected type")
    
    monkeypatch.setattr("api.managers.connect_db", mock_connect_db)
    
    with pytest.raises(TypeError, match="Unexpected type"):
        await _get_managers()


@pytest.mark.asyncio
async def test_get_manager_non_db_errors_propagate(monkeypatch):
    """GET /managers/{id} propagates non-database exceptions."""
    
    def mock_connect_db(*args, **kwargs):
        raise RuntimeError("System error")
    
    monkeypatch.setattr("api.managers.connect_db", mock_connect_db)
    
    with pytest.raises(RuntimeError, match="System error"):
        await _get_manager(1)


@pytest.mark.asyncio
async def test_create_manager_validation_runs_before_db_access(monkeypatch):
    """POST /managers validates input before attempting database access."""
    
    # Track whether connect_db was called
    connect_db_called = False
    
    def mock_connect_db(*args, **kwargs):
        nonlocal connect_db_called
        connect_db_called = True
        raise sqlite3.OperationalError("connection failed")
    
    monkeypatch.setattr("api.managers.connect_db", mock_connect_db)
    
    # Invalid email should fail validation before trying to connect
    response = await _post_manager({
        "name": "Grace Hopper",
        "email": "invalid-email",
        "department": "Engineering",
    })
    
    assert response.status_code == 400
    assert not connect_db_called
    data = response.json()
    assert "errors" in data
    assert any(e["field"] == "email" for e in data["errors"])


def test_connect_db_retries_on_postgres_failure(monkeypatch):
    """connect_db retries Postgres connections with exponential backoff."""
    from adapters.base import connect_db, _connect_postgres_with_retry
    
    # Mock psycopg
    class MockPsycopgError(Exception):
        pass
    
    mock_psycopg = MagicMock()
    mock_psycopg.OperationalError = MockPsycopgError
    mock_psycopg.connect = MagicMock(side_effect=MockPsycopgError("Connection refused"))
    
    monkeypatch.setattr("adapters.base.psycopg", mock_psycopg)
    monkeypatch.setenv("DB_URL", "postgres://localhost/test")
    
    # Should raise after retries
    with pytest.raises(MockPsycopgError):
        _connect_postgres_with_retry("postgres://localhost/test", max_retries=2)
    
    # Verify multiple attempts were made
    assert mock_psycopg.connect.call_count >= 2


def test_connect_db_succeeds_on_eventual_postgres_success(monkeypatch):
    """connect_db succeeds when Postgres recovers after transient failures."""
    from adapters.base import _connect_postgres_with_retry
    
    class MockPsycopgError(Exception):
        pass
    
    mock_psycopg = MagicMock()
    mock_psycopg.OperationalError = MockPsycopgError
    
    # Fail twice, then succeed
    mock_connection = MagicMock()
    mock_psycopg.connect = MagicMock(
        side_effect=[
            MockPsycopgError("Connection refused"),
            MockPsycopgError("Connection refused"),
            mock_connection,
        ]
    )
    
    monkeypatch.setattr("adapters.base.psycopg", mock_psycopg)
    
    result = _connect_postgres_with_retry("postgres://localhost/test", max_retries=4)
    
    assert result == mock_connection
    assert mock_psycopg.connect.call_count == 3


def test_connect_db_sqlite_raises_on_error(monkeypatch):
    """connect_db logs and re-raises SQLite connection errors."""
    from adapters.base import _connect_sqlite
    import tempfile
    
    # Use an invalid/inaccessible path
    invalid_path = "/invalid/nonexistent/path/db.sqlite"
    
    with pytest.raises(sqlite3.OperationalError):
        _connect_sqlite(invalid_path)


@pytest.mark.asyncio
async def test_error_response_does_not_expose_stack_trace(monkeypatch):
    """503 error responses don't leak stack traces or internal details."""
    
    def mock_connect_db(*args, **kwargs):
        raise sqlite3.OperationalError("connection to server at 192.168.1.1:5432 failed")
    
    monkeypatch.setattr("api.managers.connect_db", mock_connect_db)
    
    response = await _post_manager({
        "name": "Grace Hopper",
        "email": "grace@example.com",
        "department": "Engineering",
    })
    
    assert response.status_code == 503
    response_text = response.text
    
    # Ensure no sensitive details leak
    assert "192.168." not in response_text
    assert "5432" not in response_text
    assert "psycopg" not in response_text
    assert "Traceback" not in response_text
