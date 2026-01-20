import json
import sys
import threading
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

import pytest

from api import chat
from api.chat import health_app, health_live, health_livez, healthz


def _configure_health_env(monkeypatch, tmp_path):
    # Keep health checks local and fast for unit tests.
    monkeypatch.delenv("DB_URL", raising=False)
    monkeypatch.delenv("REDIS_URL", raising=False)
    monkeypatch.setenv("DB_PATH", str(tmp_path / "dev.db"))
    monkeypatch.setenv("HEALTH_SUMMARY_TIMEOUT_S", "0.18")
    monkeypatch.setattr(chat, "_ping_minio", lambda _timeout_seconds: None)


def _shutdown_health_executor():
    # Avoid lingering worker threads after async health checks.
    chat._HEALTH_EXECUTOR.shutdown(wait=False, cancel_futures=True)


class _FakeClock:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.now = 0.0

    def perf_counter(self) -> float:
        with self._lock:
            return self.now

    def monotonic(self) -> float:
        with self._lock:
            return self.now

    def sleep(self, seconds: float) -> None:
        self.advance(seconds)

    def advance(self, seconds: float) -> None:
        with self._lock:
            self.now += seconds


def _install_health_clock(monkeypatch, fake_clock: _FakeClock) -> chat.HealthClock:
    # Provide a deterministic clock for health endpoint timing assertions.
    clock = chat.HealthClock(
        perf_counter=fake_clock.perf_counter,
        monotonic=fake_clock.monotonic,
        sleep=fake_clock.sleep,
    )
    monkeypatch.setattr(chat, "HEALTH_CLOCK", clock)
    monkeypatch.setattr(chat, "APP_START_TIME", clock.monotonic())
    return clock


def _install_timeout_wait(monkeypatch, fake_clock: _FakeClock) -> None:
    async def _fake_wait(fs, timeout=None, return_when=chat.asyncio.ALL_COMPLETED):
        if timeout:
            fake_clock.advance(timeout)
        return set(), set(fs)

    monkeypatch.setattr(chat.asyncio, "wait", _fake_wait)


def _install_immediate_wait(monkeypatch) -> None:
    async def _fake_wait(fs, timeout=None, return_when=chat.asyncio.ALL_COMPLETED):
        await chat.asyncio.gather(*fs, return_exceptions=True)
        return set(fs), set()

    monkeypatch.setattr(chat.asyncio, "wait", _fake_wait)


@pytest.mark.asyncio
async def test_health_app_ok(tmp_path, monkeypatch):
    _configure_health_env(monkeypatch, tmp_path)
    resp = await health_app()
    _shutdown_health_executor()
    payload = json.loads(resp.body)
    assert resp.status_code == 200
    assert payload["healthy"] is True
    assert payload["uptime_s"] >= 0
    assert payload["failed_checks"] == {}


@pytest.mark.asyncio
async def test_health_app_uptime_uses_injected_clock(tmp_path, monkeypatch):
    _configure_health_env(monkeypatch, tmp_path)
    fake_clock = _FakeClock()
    _install_health_clock(monkeypatch, fake_clock)

    async def _fast_checks(*_args, **_kwargs):
        return {
            "database": ({"healthy": True, "latency_ms": 10}, None),
            "minio": ({"healthy": True, "latency_ms": 12}, None),
            "redis": ({"healthy": True, "latency_ms": 0, "enabled": False}, None),
        }

    monkeypatch.setattr(chat, "_run_health_summary_checks", _fast_checks)
    fake_clock.advance(12.3)
    resp = await health_app()
    _shutdown_health_executor()
    payload = json.loads(resp.body)
    assert payload["uptime_s"] == 12


def test_health_live_ok():
    payload = health_live()
    assert payload["healthy"] is True
    assert payload["uptime_s"] >= 0


def test_healthz_ok():
    # Probe aliases should mirror the base liveness payload.
    payload = healthz()
    assert payload["healthy"] is True
    assert payload["uptime_s"] >= 0


def test_health_livez_ok():
    payload = health_livez()
    assert payload["healthy"] is True
    assert payload["uptime_s"] >= 0


@pytest.mark.asyncio
async def test_health_app_reports_failed_dependencies(tmp_path, monkeypatch):
    _configure_health_env(monkeypatch, tmp_path)

    def _raise_minio(_timeout_seconds):
        raise RuntimeError("minio down")

    monkeypatch.setattr(chat, "_ping_minio", _raise_minio)
    resp = await health_app()
    _shutdown_health_executor()
    payload = json.loads(resp.body)
    assert resp.status_code == 503
    assert payload["failed_checks"]["minio"] == "minio down"


@pytest.mark.parametrize("budget_s", ["0.1"])
@pytest.mark.asyncio
async def test_health_app_responds_within_budget(tmp_path, monkeypatch, budget_s):
    _configure_health_env(monkeypatch, tmp_path)
    monkeypatch.setenv("HEALTH_SUMMARY_TIMEOUT_S", budget_s)
    fake_clock = _FakeClock()
    health_clock = _install_health_clock(monkeypatch, fake_clock)

    async def _fast_checks(*_args, **_kwargs):
        fake_clock.advance(0.08)
        return {
            "database": ({"healthy": True, "latency_ms": 10}, None),
            "minio": ({"healthy": True, "latency_ms": 12}, None),
            "redis": ({"healthy": True, "latency_ms": 0, "enabled": False}, None),
        }

    monkeypatch.setattr(chat, "_run_health_summary_checks", _fast_checks)

    start = health_clock.perf_counter()
    resp = await health_app()
    _shutdown_health_executor()
    elapsed = health_clock.perf_counter() - start
    assert resp.status_code == 200
    assert elapsed < 0.2


@pytest.mark.asyncio
async def test_health_app_performance_with_mocked_timing(tmp_path, monkeypatch):
    _configure_health_env(monkeypatch, tmp_path)
    fake_clock = _FakeClock()
    health_clock = _install_health_clock(monkeypatch, fake_clock)

    async def _fast_checks(*_args, **_kwargs):
        fake_clock.advance(0.15)
        return {
            "database": ({"healthy": True, "latency_ms": 10}, None),
            "minio": ({"healthy": True, "latency_ms": 12}, None),
            "redis": ({"healthy": True, "latency_ms": 8, "enabled": False}, None),
        }

    monkeypatch.setattr(chat, "_run_health_summary_checks", _fast_checks)

    start = health_clock.perf_counter()
    resp = await health_app()
    _shutdown_health_executor()
    elapsed = health_clock.perf_counter() - start
    assert resp.status_code == 200
    assert elapsed < 0.2


@pytest.mark.asyncio
async def test_health_app_parallel_dependency_checks(tmp_path, monkeypatch):
    _configure_health_env(monkeypatch, tmp_path)
    monkeypatch.setenv("HEALTH_SUMMARY_TIMEOUT_S", "0.18")
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
    fake_clock = _FakeClock()
    health_clock = _install_health_clock(monkeypatch, fake_clock)

    def _slow_db(_timeout_seconds):
        fake_clock.sleep(0.03)

    def _slow_minio(_timeout_seconds):
        fake_clock.sleep(0.03)

    def _slow_redis(_redis_url, _timeout_seconds):
        fake_clock.sleep(0.03)

    monkeypatch.setattr(chat, "_ping_db", _slow_db)
    monkeypatch.setattr(chat, "_ping_minio", _slow_minio)
    monkeypatch.setattr(chat, "_ping_redis", _slow_redis)
    # Warm executor threads so timing reflects steady-state performance.
    executor = chat.get_health_executor()
    for _ in range(3):
        executor.submit(lambda: None).result()
    start = health_clock.perf_counter()
    resp = await health_app()
    _shutdown_health_executor()
    elapsed = health_clock.perf_counter() - start
    assert resp.status_code == 200
    assert elapsed < 0.2


@pytest.mark.asyncio
async def test_health_app_timeout_budget_caps_total_time(tmp_path, monkeypatch):
    _configure_health_env(monkeypatch, tmp_path)
    monkeypatch.setenv("HEALTH_SUMMARY_TIMEOUT_S", "0.05")
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
    fake_clock = _FakeClock()
    health_clock = _install_health_clock(monkeypatch, fake_clock)
    _install_timeout_wait(monkeypatch, fake_clock)

    monkeypatch.setattr(chat, "_ping_db", lambda _timeout_seconds: None)
    monkeypatch.setattr(chat, "_ping_minio", lambda _timeout_seconds: None)
    monkeypatch.setattr(chat, "_ping_redis", lambda _redis_url, _timeout_seconds: None)
    start = health_clock.perf_counter()
    resp = await health_app()
    _shutdown_health_executor()
    elapsed = health_clock.perf_counter() - start
    payload = json.loads(resp.body)
    assert resp.status_code == 503
    assert elapsed < 0.2
    assert payload["failed_checks"]["database"] == "timeout"
    assert payload["failed_checks"]["minio"] == "timeout"
    assert payload["failed_checks"]["redis"] == "timeout"


@pytest.mark.asyncio
async def test_health_app_wait_timeout_respects_budget(tmp_path, monkeypatch):
    _configure_health_env(monkeypatch, tmp_path)
    monkeypatch.setenv("HEALTH_SUMMARY_TIMEOUT_S", "0.05")
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setattr(chat, "_ping_db", lambda _timeout_seconds: None)
    monkeypatch.setattr(chat, "_ping_minio", lambda _timeout_seconds: None)
    monkeypatch.setattr(chat, "_ping_redis", lambda _redis_url, _timeout_seconds: None)

    captured = {}
    original_wait = chat.asyncio.wait

    async def _capture_wait(*args, **kwargs):
        # Capture the timeout passed to asyncio.wait without changing behavior.
        captured["timeout"] = kwargs.get("timeout")
        return await original_wait(*args, **kwargs)

    monkeypatch.setattr(chat.asyncio, "wait", _capture_wait)
    resp = await health_app()
    _shutdown_health_executor()
    assert resp.status_code == 200
    assert captured["timeout"] == pytest.approx(0.1)


@pytest.mark.asyncio
async def test_health_app_circuit_breaker_opens_after_summary_timeouts(tmp_path, monkeypatch):
    _configure_health_env(monkeypatch, tmp_path)
    monkeypatch.setenv("HEALTH_SUMMARY_TIMEOUT_S", "0.05")
    fake_clock = _FakeClock()
    _install_health_clock(monkeypatch, fake_clock)
    monkeypatch.setattr(
        chat, "_MINIO_CIRCUIT", chat.CircuitBreaker(failure_threshold=3, reset_timeout_s=60.0)
    )
    _install_timeout_wait(monkeypatch, fake_clock)

    for _ in range(3):
        resp = await health_app()
        assert resp.status_code == 503

    _install_immediate_wait(monkeypatch)

    def _unexpected_minio(_timeout_seconds):
        raise AssertionError("minio should not be called when circuit is open")

    monkeypatch.setattr(chat, "_ping_minio", _unexpected_minio)
    resp = await health_app()
    payload = json.loads(resp.body)
    _shutdown_health_executor()
    assert resp.status_code == 503
    assert payload["failed_checks"]["minio"] == "circuit_open"
    assert payload["components"]["minio"]["circuit_open"] is True


@pytest.mark.asyncio
async def test_circuit_breaker_opens_after_three_failures(monkeypatch):
    monkeypatch.setattr(chat, "_HEALTH_RETRY_BACKOFFS", ())

    def _always_fail(_timeout_seconds):
        raise RuntimeError("minio down")

    circuit = chat.CircuitBreaker(failure_threshold=3, reset_timeout_s=60.0)
    for _ in range(2):
        payload, _reason = await chat._run_dependency_check(
            _always_fail,
            0.05,
            0.05,
            circuit_breaker=circuit,
        )
        assert payload["healthy"] is False
        assert circuit.is_open() is False

    payload, _reason = await chat._run_dependency_check(
        _always_fail,
        0.05,
        0.05,
        circuit_breaker=circuit,
    )
    assert payload["healthy"] is False
    assert circuit.is_open() is True

    payload, reason = await chat._run_dependency_check(
        _always_fail,
        0.05,
        0.05,
        circuit_breaker=circuit,
    )
    assert payload["circuit_open"] is True
    assert reason == "circuit_open"
    _shutdown_health_executor()


@pytest.mark.asyncio
async def test_circuit_breaker_opens_after_three_timeouts(monkeypatch):
    monkeypatch.setattr(chat, "_HEALTH_RETRY_BACKOFFS", ())
    fake_clock = _FakeClock()
    _install_health_clock(monkeypatch, fake_clock)

    async def _timeout_run(*_args, **_kwargs):
        # Simulate elapsed time without sleeping so timeouts stay deterministic.
        fake_clock.advance(0.02)
        raise TimeoutError

    circuit = chat.CircuitBreaker(failure_threshold=3, reset_timeout_s=60.0)
    monkeypatch.setattr(chat, "_run_health_check_with_retries", _timeout_run)
    dependency = lambda _timeout_seconds: None
    for _ in range(2):
        payload, reason = await chat._run_dependency_check(
            dependency,
            0.01,
            0.01,
            circuit_breaker=circuit,
        )
        assert payload["healthy"] is False
        assert reason == "timeout"
        assert circuit.is_open() is False

    payload, reason = await chat._run_dependency_check(
        dependency,
        0.01,
        0.01,
        circuit_breaker=circuit,
    )
    assert payload["healthy"] is False
    assert reason == "timeout"
    assert circuit.is_open() is True

    payload, reason = await chat._run_dependency_check(
        dependency,
        0.01,
        0.01,
        circuit_breaker=circuit,
    )
    assert payload["circuit_open"] is True
    assert reason == "circuit_open"
    _shutdown_health_executor()


def test_circuit_breaker_resets_after_timeout(monkeypatch):
    fake_clock = _FakeClock()
    _install_health_clock(monkeypatch, fake_clock)
    circuit = chat.CircuitBreaker(failure_threshold=1, reset_timeout_s=10.0)
    circuit.record_failure()
    assert circuit.is_open() is True

    fake_clock.advance(10.5)
    assert circuit.is_open() is False

    circuit.record_failure()
    assert circuit.is_open() is True


def test_circuit_breaker_allows_calls_after_reset(monkeypatch):
    fake_clock = _FakeClock()
    _install_health_clock(monkeypatch, fake_clock)
    circuit = chat.CircuitBreaker(failure_threshold=2, reset_timeout_s=5.0)

    circuit.record_failure()
    circuit.record_failure()
    assert circuit.is_open() is True

    fake_clock.advance(6.0)
    assert circuit.is_open() is False

    circuit.record_failure()
    assert circuit.is_open() is False
    circuit.record_failure()
    assert circuit.is_open() is True


@pytest.mark.asyncio
async def test_circuit_breaker_allows_dependency_after_cooldown(monkeypatch):
    fake_clock = _FakeClock()
    _install_health_clock(monkeypatch, fake_clock)
    monkeypatch.setattr(chat, "_HEALTH_RETRY_BACKOFFS", ())
    calls = {"count": 0}

    def _ok(_timeout_seconds):
        calls["count"] += 1

    circuit = chat.CircuitBreaker(failure_threshold=1, reset_timeout_s=5.0)
    circuit.record_failure()
    payload, reason = await chat._run_dependency_check(
        _ok,
        0.01,
        0.01,
        circuit_breaker=circuit,
    )
    assert payload["circuit_open"] is True
    assert reason == "circuit_open"
    assert calls["count"] == 0

    fake_clock.advance(5.1)
    payload, reason = await chat._run_dependency_check(
        _ok,
        0.01,
        0.01,
        circuit_breaker=circuit,
    )
    assert payload["healthy"] is True
    assert reason is None
    assert calls["count"] == 1
    _shutdown_health_executor()


@pytest.mark.asyncio
async def test_health_app_reports_circuit_breaker_open(tmp_path, monkeypatch):
    _configure_health_env(monkeypatch, tmp_path)
    monkeypatch.setattr(
        chat, "_MINIO_CIRCUIT", chat.CircuitBreaker(failure_threshold=1, reset_timeout_s=60.0)
    )

    def _raise_minio(_timeout_seconds):
        raise RuntimeError("minio down")

    monkeypatch.setattr(chat, "_ping_minio", _raise_minio)
    resp = await health_app()
    payload = json.loads(resp.body)
    assert resp.status_code == 503
    assert payload["failed_checks"]["minio"] == "minio down"

    def _unexpected_minio(_timeout_seconds):
        raise AssertionError("minio should not be called when circuit is open")

    monkeypatch.setattr(chat, "_ping_minio", _unexpected_minio)
    resp = await health_app()
    _shutdown_health_executor()
    payload = json.loads(resp.body)
    assert resp.status_code == 503
    assert payload["failed_checks"]["minio"] == "circuit_open"
    assert payload["components"]["minio"]["circuit_open"] is True


# Commit-message checklist:
# - [ ] type is accurate (feat, fix, test)
# - [ ] scope is clear (health)
# - [ ] summary is concise and imperative
