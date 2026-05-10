from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any

from llm.cost_tracking import log_llm_usage
from tests._pg_fakes import StrictPostgresConn

REPO_ROOT = Path(__file__).resolve().parents[1]


class _Rows(list[tuple[Any, ...]]):
    def fetchall(self) -> _Rows:
        return self


class _PostgresConn(StrictPostgresConn):
    def execute(self, sql: str, params: Any = None) -> _Rows:
        super().execute(sql, params)
        normalized = " ".join(sql.split()).lower()
        if "information_schema.columns" in normalized:
            return _Rows(
                [
                    ("id",),
                    ("name",),
                    ("role",),
                    ("cik",),
                    ("jurisdiction",),
                    ("jurisdictions",),
                    ("updated_at",),
                ]
            )
        return _Rows()


def _load_script_module(name: str):
    module_path = REPO_ROOT / "scripts" / f"{name}.py"
    spec = importlib.util.spec_from_file_location(f"{name}_module", module_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_log_llm_usage_postgres_path_avoids_sqlite_tokens() -> None:
    conn = _PostgresConn()

    log_llm_usage(
        conn,
        provider="openai",
        model="gpt-4o-mini",
        tokens_in=1000,
        tokens_out=500,
        latency_ms=123,
    )

    statements = " ".join(conn.statements)
    assert "CREATE TABLE IF NOT EXISTS api_usage" in statements
    assert "bigserial PRIMARY KEY" in statements
    assert conn.params[-1] == ("langchain", "openai/gpt-4o-mini", 200, 0, 123, 0.00045)


def test_seed_universe_postgres_schema_and_upsert_are_dialect_aware() -> None:
    module = _load_script_module("seed_universe")
    conn = _PostgresConn()

    module._ensure_universe_schema(conn)
    assert "role" in module._manager_columns(conn)
    module._upsert_universe_record(
        conn,
        name="Bridgewater Associates",
        cik="0001350694",
        jurisdiction="us",
        include_role=True,
    )

    statements = " ".join(conn.statements)
    assert "CREATE TABLE IF NOT EXISTS managers" in statements
    assert "ON CONFLICT(cik)" in statements
    assert conn.params[-1] == (
        "Bridgewater Associates",
        "Manager",
        "0001350694",
        "us",
        ["us"],
    )


def test_resolve_aliases_postgres_id_detection_and_update_are_dialect_aware() -> None:
    module = _load_script_module("resolve_aliases")
    conn = _PostgresConn()

    assert module._manager_id_column(conn) == "id"
    module._update_aliases(conn, "id", 7, ["Bridgewater Associates, LP"])

    statements = " ".join(conn.statements)
    assert "information_schema.columns" in statements
    assert "UPDATE managers SET aliases = %s" in statements
    assert conn.params[-1] == (["Bridgewater Associates, LP"], 7)
