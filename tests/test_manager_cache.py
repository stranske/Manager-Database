import asyncio
import sys
from pathlib import Path

import fakeredis
import httpx

sys.path.append(str(Path(__file__).resolve().parents[1]))

from api import cache as cache_module
from api.chat import app


async def _post_manager(payload: dict):
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
    await app.router.startup()
    try:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", timeout=5.0
        ) as client:
            return await client.get("/managers", params=params)
    finally:
        await app.router.shutdown()


def _configure_cache(monkeypatch) -> None:
    fake_redis = fakeredis.FakeRedis()
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setattr(cache_module, "_build_redis_client", lambda _url: fake_redis)
    cache_module.reset_cache_backend()
    cache_module.reset_cache_stats()


def test_manager_list_cache_hits(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    _configure_cache(monkeypatch)
    resp = asyncio.run(
        _post_manager({"name": "Grace Hopper", "role": "Engineering Director"})
    )
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
    resp = asyncio.run(
        _post_manager({"name": "Ada Lovelace", "role": "Research Lead"})
    )
    assert resp.status_code == 201

    params = {"limit": 10, "offset": 0}
    asyncio.run(_get_managers(params))
    asyncio.run(_get_managers(params))
    before = cache_module.get_cache_stats("managers.list")

    resp = asyncio.run(
        _post_manager({"name": "Mary Jackson", "role": "Operations Manager"})
    )
    assert resp.status_code == 201
    asyncio.run(_get_managers(params))

    after = cache_module.get_cache_stats("managers.list")
    assert after["misses"] > before["misses"]
