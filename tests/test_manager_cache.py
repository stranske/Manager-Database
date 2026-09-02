import asyncio
import sys
from pathlib import Path
from typing import Any, cast

import fakeredis
import httpx

sys.path.append(str(Path(__file__).resolve().parents[1]))

from api import cache as cache_module
from api.chat import app


async def _post_manager(payload: dict):
    await cast(Any, app.router).startup()
    try:
        transport = httpx.ASGITransport(app=cast(Any, app))
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", timeout=5.0
        ) as client:
            return await client.post("/managers", json=payload)
    finally:
        await cast(Any, app.router).shutdown()


async def _get_managers(params: dict | None = None):
    await cast(Any, app.router).startup()
    try:
        transport = httpx.ASGITransport(app=cast(Any, app))
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", timeout=5.0
        ) as client:
            return await client.get("/managers", params=params)
    finally:
        await cast(Any, app.router).shutdown()


async def _get_manager(manager_id: int):
    await cast(Any, app.router).startup()
    try:
        transport = httpx.ASGITransport(app=cast(Any, app))
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", timeout=5.0
        ) as client:
            return await client.get(f"/managers/{manager_id}")
    finally:
        await cast(Any, app.router).shutdown()


def _configure_cache(monkeypatch):
    fake_redis = fakeredis.FakeRedis()
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setattr(cache_module, "_build_redis_client", lambda _url: fake_redis)
    cache_module.reset_cache_backend()
    cache_module.reset_cache_stats()
    return fake_redis


def test_cache_get_counts_stored_null_as_hit(monkeypatch):
    _configure_cache(monkeypatch)
    cache_module.cache_set("compat:null", None)
    cache_module.reset_cache_stats()

    assert cache_module.cache_get("compat", "compat:null") is None
    assert cache_module.get_cache_stats("compat") == {
        "hits": 1,
        "misses": 0,
        "hit_ratio": 1.0,
    }


def test_cache_get_preserves_legacy_non_json_payload(monkeypatch):
    fake_redis = _configure_cache(monkeypatch)
    fake_redis.set("compat:legacy", "legacy-value")

    assert cache_module.cache_get("compat", "compat:legacy") == "legacy-value"
    assert cache_module.get_cache_stats("compat") == {
        "hits": 1,
        "misses": 0,
        "hit_ratio": 1.0,
    }


def test_manager_list_cache_hits(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    _configure_cache(monkeypatch)
    resp = asyncio.run(_post_manager({"name": "Manager A", "jurisdictions": ["us"]}))
    assert resp.status_code == 201

    params = {"limit": 10, "offset": 0}
    first = asyncio.run(_get_managers(params))
    assert first.status_code == 200
    second = asyncio.run(_get_managers(params))
    assert second.status_code == 200

    stats = cache_module.get_cache_stats("managers.list")
    assert stats["hits"] >= 1
    assert stats["misses"] >= 1
    assert stats["hit_ratio"] > 0


def test_manager_cache_invalidation_on_write(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    _configure_cache(monkeypatch)
    resp = asyncio.run(_post_manager({"name": "Manager A", "jurisdictions": ["us"]}))
    assert resp.status_code == 201

    params = {"limit": 10, "offset": 0}
    asyncio.run(_get_managers(params))
    asyncio.run(_get_managers(params))
    before = cache_module.get_cache_stats("managers.list")

    resp = asyncio.run(_post_manager({"name": "Manager B", "jurisdictions": ["uk"]}))
    assert resp.status_code == 201
    asyncio.run(_get_managers(params))

    after = cache_module.get_cache_stats("managers.list")
    assert after["misses"] > before["misses"]


def test_missing_manager_result_is_cached(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    _configure_cache(monkeypatch)
    created = asyncio.run(_post_manager({"name": "Manager A", "jurisdictions": ["us"]}))
    assert created.status_code == 201
    cache_module.reset_cache_stats()

    first = asyncio.run(_get_manager(999))
    second = asyncio.run(_get_manager(999))

    assert first.status_code == 404
    assert second.status_code == 404
    assert cache_module.get_cache_stats("managers.item") == {
        "hits": 1,
        "misses": 1,
        "hit_ratio": 0.5,
    }


def test_cached_missing_manager_is_invalidated_before_create_refetch(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    _configure_cache(monkeypatch)

    missing = asyncio.run(_get_manager(1))
    assert missing.status_code == 404

    created = asyncio.run(_post_manager({"name": "Manager A", "jurisdictions": ["us"]}))
    fetched = asyncio.run(_get_manager(1))

    assert created.status_code == 201
    assert fetched.status_code == 200
    assert created.json() == fetched.json()
    assert created.json()["created_at"] is not None
