"""App-specific adapter for the Manager-Database ``/managers`` HTTP surface.

This is the ONLY app-specific glue the shared ``baseline_kit`` needs for the
**api-snapshot** modality: a way to turn a scenario (an endpoint + query/path
params, run against a named seed variant) into

  * a JSON-able *payload* (``{"status_code": ..., "json": <body>}``) for golden
    snapshotting via ``baseline_kit.check_snapshot``, and
  * a flat ``dict[str, float | int]`` of reduced *metrics* for invariants and
    directional ("metamorphic") checks.

Everything else -- snapshot normalization, directional engine, invariant
assertion, coverage manifest -- is generic and lives in ``baseline_kit``.

Determinism
-----------
The app's data layer (``adapters.base.connect_db``) reads ``DB_PATH`` from the
environment at call time and opens a plain ``sqlite3`` connection. We exploit
that: each scenario seeds a *fresh* sqlite file (under a per-test temp dir) from
a committed, fixed seed variant, points ``DB_PATH`` at it, resets the process
cache backend, and drives the endpoint through a Starlette ``TestClient`` over a
minimal FastAPI app that mounts only ``api.managers.router``. Same seed + same
request => byte-identical JSON (modulo the volatile fields the golden layer
redacts: ``created_at``/``updated_at``, which we also pin explicitly in the seed
so they never wobble).

We mount a *minimal* app rather than importing ``api.chat.app`` so the kit does
not drag in boto3 / langsmith / prometheus just to snapshot three GET handlers.
"""

from __future__ import annotations

import json
import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]

# A fixed timestamp baked into every seeded row. The golden layer redacts
# created_at/updated_at anyway, but pinning them keeps the raw DB deterministic
# and makes invariant/debug output stable.
SEED_TIMESTAMP = "2026-01-01T00:00:00Z"


def _create_schema(conn: sqlite3.Connection) -> None:
    """Create the managers table with the universe-import column superset.

    This is the union of what ``_ensure_manager_table`` and
    ``_ensure_universe_schema`` produce: the columns the ``/managers`` SELECTs
    name explicitly (``... registry_ids, quality_flags, created_at, updated_at``)
    plus the scalar ``jurisdiction`` fallback column ``GET /managers/stats``
    reads. Seeding the full column set means the endpoints never have to ALTER
    the table at request time, so the snapshot is what the read path produces
    against a complete schema.
    """
    conn.execute("""
        CREATE TABLE managers (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            cik TEXT,
            lei TEXT,
            jurisdiction TEXT,
            aliases TEXT NOT NULL DEFAULT '[]',
            jurisdictions TEXT NOT NULL DEFAULT '[]',
            tags TEXT NOT NULL DEFAULT '[]',
            registry_ids TEXT NOT NULL DEFAULT '{}',
            quality_flags TEXT NOT NULL DEFAULT '[]',
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """)


def _insert_seed_rows(conn: sqlite3.Connection, rows: list[dict[str, Any]]) -> None:
    """Insert seed manager rows with explicit, deterministic ids and timestamps.

    JSON columns (``aliases``/``jurisdictions``/``tags``/``registry_ids``) are
    stored as JSON text, exactly as the production write path does, so the read
    path's ``json_each``/``json.loads`` handling is exercised faithfully.
    """
    for row in rows:
        conn.execute(
            """
            INSERT INTO managers
                (id, name, cik, lei, jurisdiction, aliases, jurisdictions,
                 tags, registry_ids, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(row["id"]),
                str(row["name"]),
                row.get("cik"),
                row.get("lei"),
                # jurisdiction (scalar fallback column) mirrors first jurisdiction.
                (row.get("jurisdictions") or [None])[0],
                json.dumps(list(row.get("aliases", []))),
                json.dumps(list(row.get("jurisdictions", []))),
                json.dumps(list(row.get("tags", []))),
                json.dumps(dict(row.get("registry_ids", {}))),
                SEED_TIMESTAMP,
                SEED_TIMESTAMP,
            ),
        )
    conn.commit()


def seed_database(db_path: Path, rows: list[dict[str, Any]]) -> None:
    """Create + seed a fresh sqlite DB at ``db_path`` from ``rows``."""
    if db_path.exists():
        db_path.unlink()
    conn = sqlite3.connect(str(db_path))
    try:
        _create_schema(conn)
        _insert_seed_rows(conn, rows)
    finally:
        conn.close()


@contextmanager
def _seeded_client(db_path: Path):
    """Yield a TestClient for a minimal managers-only app bound to ``db_path``.

    Reads of ``DB_PATH`` happen inside ``connect_db`` per request, so setting the
    env var around the client covers every endpoint call. The process cache
    backend is reset so a prior scenario's cached query results (keyed partly on
    the DB identity) can never leak into this one.
    """
    # Imported lazily so importing this module never pulls FastAPI/app deps until
    # a scenario actually runs.
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from api.cache import reset_cache_backend, reset_cache_stats
    from api.managers import router as managers_router

    app = FastAPI()
    app.include_router(managers_router)

    prev_db_path = os.environ.get("DB_PATH")
    prev_db_url = os.environ.get("DB_URL")
    # DB_URL must be unset/non-postgres so connect_db falls through to sqlite.
    os.environ.pop("DB_URL", None)
    os.environ["DB_PATH"] = str(db_path)
    reset_cache_backend()
    reset_cache_stats()
    try:
        with TestClient(app) as client:
            yield client
    finally:
        reset_cache_backend()
        reset_cache_stats()
        if prev_db_path is None:
            os.environ.pop("DB_PATH", None)
        else:
            os.environ["DB_PATH"] = prev_db_path
        if prev_db_url is not None:
            os.environ["DB_URL"] = prev_db_url


def _build_request(scenario: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    """Translate a catalog scenario into (path, query-params).

    Scenario shape:
        endpoint: "list" | "detail" | "stats"
        params:   {limit, offset, jurisdiction, tag}   # list filters
        manager_id: int                                  # detail target
    """
    endpoint = scenario["endpoint"]
    if endpoint == "list":
        return "/managers", dict(scenario.get("params") or {})
    if endpoint == "detail":
        return f"/managers/{scenario['manager_id']}", {}
    if endpoint == "stats":
        return "/managers/stats", {}
    raise ValueError(f"unknown endpoint {endpoint!r}")  # pragma: no cover - catalog typo guard


def _list_metrics(body: dict[str, Any]) -> dict[str, float | int]:
    """Reduce a list-endpoint body to flat scalar metrics."""
    items = body.get("items", []) or []
    names = [it.get("name", "") for it in items]
    ids = [it.get("manager_id") for it in items]
    return {
        "count": len(items),
        "total": int(body.get("total", 0)),
        "limit": int(body.get("limit", 0)),
        "offset": int(body.get("offset", 0)),
        "distinct_ids": len(set(ids)),
        "ids_sorted_ascending": int(ids == sorted(i for i in ids if i is not None)),
        "names_nonempty": int(all(bool(n) for n in names)),
        "items_with_cik": sum(1 for it in items if it.get("cik")),
    }


def _detail_metrics(status_code: int, body: dict[str, Any]) -> dict[str, float | int]:
    """Reduce a detail-endpoint body to flat scalar metrics."""
    found = int(status_code == 200 and "manager_id" in body)
    return {
        "found": found,
        "has_name": int(bool(body.get("name"))) if found else 0,
        "n_aliases": len(body.get("aliases", []) or []) if found else 0,
        "n_jurisdictions": len(body.get("jurisdictions", []) or []) if found else 0,
        "n_tags": len(body.get("tags", []) or []) if found else 0,
    }


def _stats_metrics(body: dict[str, Any]) -> dict[str, float | int]:
    """Reduce a stats-endpoint body to flat scalar metrics."""
    by_jur = body.get("by_jurisdiction", {}) or {}
    by_tag = body.get("by_tag", {}) or {}
    return {
        "total_managers": int(body.get("total_managers", 0)),
        "n_jurisdiction_keys": len(by_jur),
        "n_tag_keys": len(by_tag),
        "jurisdiction_count_sum": sum(int(v) for v in by_jur.values()),
        "tag_count_sum": sum(int(v) for v in by_tag.values()),
        "with_cik": int(body.get("with_cik", 0)),
        "with_lei": int(body.get("with_lei", 0)),
    }


def reduce_metrics(scenario: dict[str, Any], payload: dict[str, Any]) -> dict[str, float | int]:
    """Reduce a response payload to a flat metrics dict for the given endpoint."""
    endpoint = scenario["endpoint"]
    status_code = int(payload["status_code"])
    body = payload["json"] if isinstance(payload["json"], dict) else {}
    base: dict[str, float | int] = {"status_code": status_code}
    if endpoint == "list":
        base.update(_list_metrics(body))
    elif endpoint == "detail":
        base.update(_detail_metrics(status_code, body))
    elif endpoint == "stats":
        base.update(_stats_metrics(body))
    return base


def run_scenario(
    scenario: dict[str, Any],
    seed_rows: list[dict[str, Any]],
    db_path: Path,
) -> tuple[dict[str, Any], dict[str, float | int]]:
    """Seed a fresh DB, call the scenario's endpoint, return (payload, metrics).

    ``payload`` is ``{"status_code", "json"}`` (via ``response_to_payload``) for
    snapshotting; ``metrics`` is a flat ``dict[str, float | int]`` for invariants
    and directional checks.
    """
    from baseline_kit import response_to_payload

    seed_database(db_path, seed_rows)
    path, params = _build_request(scenario)
    with _seeded_client(db_path) as client:
        response = client.get(path, params=params)
    payload = response_to_payload(response)
    metrics = reduce_metrics(scenario, payload)
    return payload, metrics
