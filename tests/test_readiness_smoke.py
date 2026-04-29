"""Tests for the local readiness smoke wrapper.

These tests do not require docker-compose to be running. They drive the
smoke-script logic against an in-process httpx ``MockTransport`` so each
probe and failure mode is verified deterministically.
"""

from __future__ import annotations

import sys
from pathlib import Path

import httpx
import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from scripts import readiness_smoke


def _client(handler):
    return httpx.Client(transport=httpx.MockTransport(handler), base_url="http://test")


def _ok_health() -> httpx.Response:
    return httpx.Response(
        200,
        json={
            "healthy": True,
            "uptime_s": 1,
            "components": {
                "app": {"healthy": True, "uptime_s": 1},
                "database": {"healthy": True, "latency_ms": 1},
                "minio": {"healthy": True, "latency_ms": 1},
                "redis": {"healthy": True, "latency_ms": 0, "enabled": False},
            },
        },
    )


def _ok_managers() -> httpx.Response:
    return httpx.Response(
        200,
        json={
            "items": [
                {
                    "manager_id": 1,
                    "name": "Elliott Investment Management L.P.",
                }
            ],
            "total": 1,
            "limit": 1,
            "offset": 0,
        },
    )


def _ok_chat() -> httpx.Response:
    return httpx.Response(
        200,
        json={
            "answer": "No documents found.",
            "latency_ms": 0,
            "chain_used": "legacy_search",
            "sources": [],
            "sql": None,
            "trace_url": None,
            "response_id": "smoke",
        },
    )


def test_run_passes_when_all_components_healthy():
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/health/detailed":
            return _ok_health()
        if request.url.path == "/managers":
            return _ok_managers()
        if request.url.path == "/chat":
            return _ok_chat()
        raise AssertionError(f"unexpected path {request.url.path!r}")

    with _client(handler) as client:
        assert readiness_smoke.check_health(client)["healthy"] is True
        assert readiness_smoke.check_managers(client)["items"]
        assert readiness_smoke.check_chat(client)["answer"]


def test_health_failure_raises_readiness_error():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            503,
            json={
                "healthy": False,
                "uptime_s": 1,
                "components": {
                    "app": {"healthy": True, "uptime_s": 1},
                    "database": {"healthy": False, "latency_ms": 1},
                    "minio": {"healthy": True, "latency_ms": 1},
                    "redis": {"healthy": True, "latency_ms": 0, "enabled": False},
                },
            },
        )

    with _client(handler) as client:
        with pytest.raises(readiness_smoke.ReadinessError):
            readiness_smoke.check_health(client)


def test_managers_empty_list_raises_with_seed_hint():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"items": [], "total": 0, "limit": 1, "offset": 0},
        )

    with _client(handler) as client:
        with pytest.raises(readiness_smoke.ReadinessError) as info:
            readiness_smoke.check_managers(client)
    assert "seed_managers" in str(info.value)


def test_chat_missing_answer_raises():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"sources": []})

    with _client(handler) as client:
        with pytest.raises(readiness_smoke.ReadinessError):
            readiness_smoke.check_chat(client)


def test_main_exit_codes(monkeypatch, capsys):
    def passing_run(base_url: str, timeout_s: float) -> int:
        return 0

    monkeypatch.setattr(readiness_smoke, "run", passing_run)
    assert readiness_smoke.main(["--base-url", "http://test"]) == 0
    out = capsys.readouterr().out
    assert "readiness smoke OK" in out

    def failing_run(base_url: str, timeout_s: float) -> int:
        raise readiness_smoke.ReadinessError("simulated failure")

    monkeypatch.setattr(readiness_smoke, "run", failing_run)
    assert readiness_smoke.main(["--base-url", "http://test"]) == 1
    err = capsys.readouterr().err
    assert "FAILED" in err
    assert "simulated failure" in err


def test_main_handles_transport_error(monkeypatch, capsys):
    def raise_transport(base_url: str, timeout_s: float) -> int:
        raise httpx.ConnectError("refused")

    monkeypatch.setattr(readiness_smoke, "run", raise_transport)
    assert readiness_smoke.main([]) == 1
    err = capsys.readouterr().err
    assert "transport" in err
