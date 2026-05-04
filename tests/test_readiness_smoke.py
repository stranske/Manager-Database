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
                    "name": readiness_smoke.EXPECTED_MANAGER_NAME,
                }
            ],
            "total": 1,
            "limit": readiness_smoke.DEFAULT_MANAGER_PAGE_SIZE,
            "offset": 0,
        },
    )


def _ok_chat() -> httpx.Response:
    return httpx.Response(
        200,
        json={
            "answer": "Context: Readiness smoke deterministic fact: manager universe bootstrap is healthy.",
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


def test_check_ui_requires_success_status(monkeypatch):
    responses = [httpx.Response(503, text="warming"), httpx.Response(200, text="streamlit")]
    monkeypatch.setattr(readiness_smoke.time, "sleep", lambda seconds: None)

    def fake_get(url: str, timeout: float) -> httpx.Response:
        assert url == "http://ui"
        assert timeout == 1.0
        return responses.pop(0)

    monkeypatch.setattr(readiness_smoke.httpx, "get", fake_get)
    readiness_smoke.check_ui("http://ui", 1.0)
    assert responses == []

    def failing_get(url: str, timeout: float) -> httpx.Response:
        return httpx.Response(503, text="warming")

    monkeypatch.setattr(readiness_smoke.httpx, "get", failing_get)
    with pytest.raises(readiness_smoke.ReadinessError):
        readiness_smoke.check_ui("http://ui", 1.0)


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


def test_chat_missing_expected_snippet_raises():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "answer": "The readiness smoke confirms manager bootstrap data is present.",
                "sources": [],
            },
        )

    with _client(handler) as client:
        with pytest.raises(readiness_smoke.ReadinessError):
            readiness_smoke.check_chat(client)


def test_managers_requires_expected_seeded_name():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "items": [{"manager_id": 99, "name": "Unexpected Manager"}],
                "total": 1,
                "limit": readiness_smoke.DEFAULT_MANAGER_PAGE_SIZE,
                "offset": 0,
            },
        )

    with _client(handler) as client:
        with pytest.raises(readiness_smoke.ReadinessError):
            readiness_smoke.check_managers(client)


def test_managers_pages_until_expected_seeded_name():
    requests: list[dict[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(dict(request.url.params))
        if request.url.params["offset"] == "0":
            return httpx.Response(
                200,
                json={
                    "items": [{"manager_id": i, "name": f"Manager {i}"} for i in range(100)],
                    "total": 101,
                    "limit": 100,
                    "offset": 0,
                },
            )
        return httpx.Response(
            200,
            json={
                "items": [{"manager_id": 101, "name": readiness_smoke.EXPECTED_MANAGER_NAME}],
                "total": 101,
                "limit": 100,
                "offset": 100,
            },
        )

    with _client(handler) as client:
        assert readiness_smoke.check_managers(client)["offset"] == 100
    assert [params["offset"] for params in requests] == ["0", "100"]


def test_main_exit_codes(monkeypatch, capsys):
    def passing_run(
        base_url: str,
        ui_url: str,
        timeout_s: float,
        start_stack: bool,
        reset_volumes: bool,
        compose_file: str,
    ) -> int:
        assert base_url == "http://test"
        assert ui_url == readiness_smoke.DEFAULT_UI_BASE
        assert start_stack is True
        assert reset_volumes in {True, False}
        assert compose_file == "docker-compose.yml"
        return 0

    monkeypatch.setattr(readiness_smoke, "run", passing_run)
    assert readiness_smoke.main(["--base-url", "http://test"]) == 0
    out = capsys.readouterr().out
    assert "readiness smoke OK" in out
    assert readiness_smoke.main(["--base-url", "http://test", "--reuse-stack"]) == 0

    def failing_run(
        base_url: str,
        ui_url: str,
        timeout_s: float,
        start_stack: bool,
        reset_volumes: bool,
        compose_file: str,
    ) -> int:
        raise readiness_smoke.ReadinessError("simulated failure")

    monkeypatch.setattr(readiness_smoke, "run", failing_run)
    assert readiness_smoke.main(["--base-url", "http://test"]) == 1
    err = capsys.readouterr().err
    assert "FAILED" in err
    assert "simulated failure" in err


def test_main_handles_transport_error(monkeypatch, capsys):
    def raise_transport(
        base_url: str,
        ui_url: str,
        timeout_s: float,
        start_stack: bool,
        reset_volumes: bool,
        compose_file: str,
    ) -> int:
        raise httpx.ConnectError("refused")

    monkeypatch.setattr(readiness_smoke, "run", raise_transport)
    assert readiness_smoke.main(["--skip-stack-start"]) == 1
    err = capsys.readouterr().err
    assert "transport" in err


def test_run_invokes_compose_seed_and_stack_start_by_default(monkeypatch):
    calls: list[str] = []
    original_client = httpx.Client

    def fake_stack(compose_file: str, *, reset_volumes: bool = False) -> None:
        calls.append(f"stack:{compose_file}:{reset_volumes}")

    def fake_seed(compose_file: str, *, in_compose: bool = False) -> None:
        calls.append(f"seed:{compose_file}:{in_compose}")

    def fake_check_ui(ui_url: str, timeout_s: float) -> None:
        calls.append(f"ui:{ui_url}:{timeout_s}")

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/health/detailed":
            return _ok_health()
        if request.url.path == "/managers":
            return _ok_managers()
        if request.url.path == "/chat":
            return _ok_chat()
        raise AssertionError(f"unexpected path {request.url.path!r}")

    monkeypatch.setattr(readiness_smoke, "bring_up_stack", fake_stack)
    monkeypatch.setattr(readiness_smoke, "seed_local_readiness_data", fake_seed)
    monkeypatch.setattr(readiness_smoke, "check_ui", fake_check_ui)
    monkeypatch.setattr(
        readiness_smoke.httpx,
        "Client",
        lambda *args, **kwargs: original_client(
            transport=httpx.MockTransport(handler), base_url="http://test"
        ),
    )

    assert readiness_smoke.run("http://test", "http://ui", 1.0, True, False, "compose.yml") == 0
    assert calls == ["stack:compose.yml:False", "seed:compose.yml:True", "ui:http://ui:1.0"]
    calls.clear()
    assert readiness_smoke.run("http://test", "http://ui", 1.0, True, True, "compose.yml") == 0
    assert calls == ["stack:compose.yml:True", "seed:compose.yml:True", "ui:http://ui:1.0"]
    calls.clear()
    assert readiness_smoke.run("http://test", "http://ui", 1.0, False, False, "compose.yml") == 0
    assert calls == ["seed:compose.yml:False", "ui:http://ui:1.0"]


def test_run_waits_for_api_health_before_in_compose_seed(monkeypatch):
    calls: list[str] = []
    original_client = httpx.Client
    health_attempts = [httpx.Response(503, json={"healthy": False}), _ok_health()]

    monkeypatch.setattr(
        readiness_smoke,
        "bring_up_stack",
        lambda compose_file, *, reset_volumes=False: calls.append(
            f"stack:{compose_file}:{reset_volumes}"
        ),
    )
    monkeypatch.setattr(
        readiness_smoke,
        "seed_local_readiness_data",
        lambda compose_file, *, in_compose=False: calls.append(f"seed:{compose_file}:{in_compose}"),
    )
    monkeypatch.setattr(
        readiness_smoke,
        "check_ui",
        lambda ui_url, timeout_s: calls.append(f"ui:{ui_url}:{timeout_s}"),
    )

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/health/detailed":
            calls.append("health")
            return health_attempts.pop(0) if health_attempts else _ok_health()
        if request.url.path == "/managers":
            return _ok_managers()
        if request.url.path == "/chat":
            return _ok_chat()
        raise AssertionError(f"unexpected path {request.url.path!r}")

    monkeypatch.setattr(
        readiness_smoke.httpx,
        "Client",
        lambda *args, **kwargs: original_client(
            transport=httpx.MockTransport(handler), base_url="http://test"
        ),
    )

    assert readiness_smoke.run("http://test", "http://ui", 1.0, True, False, "compose.yml") == 0
    assert calls[:4] == ["stack:compose.yml:False", "health", "health", "seed:compose.yml:True"]


def test_main_reuse_stack_disables_volume_reset(monkeypatch):
    captured: dict[str, bool] = {}

    def fake_run(
        base_url: str,
        ui_url: str,
        timeout_s: float,
        start_stack: bool,
        reset_volumes: bool,
        compose_file: str,
    ) -> int:
        captured["start_stack"] = start_stack
        captured["reset_volumes"] = reset_volumes
        return 0

    monkeypatch.setattr(readiness_smoke, "run", fake_run)
    assert readiness_smoke.main(["--reuse-stack"]) == 0
    assert captured == {"start_stack": True, "reset_volumes": False}
