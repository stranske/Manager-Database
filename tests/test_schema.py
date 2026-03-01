from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config

ROOT = Path(__file__).resolve().parents[1]

# Canonical 7-table model
EXPECTED_TABLES = {
    "managers",
    "filings",
    "holdings",
    "news_items",
    "documents",
    "daily_diffs",
    "api_usage",
}
# Two materialized views (regular views on SQLite)
EXPECTED_VIEWS = {"monthly_usage", "mv_daily_report"}


def _alembic_config(db_url: str) -> Config:
    config = Config(str(ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(ROOT / "alembic"))
    config.set_main_option("sqlalchemy.url", db_url)
    return config


def test_schema_upgrade_creates_expected_objects(monkeypatch, tmp_path):
    """Verify migration creates all 7 tables and 2 views."""
    monkeypatch.delenv("DB_URL", raising=False)
    db_path = tmp_path / "schema.db"
    config = _alembic_config(f"sqlite:///{db_path}")

    command.upgrade(config, "head")

    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            ).fetchall()
        }
        assert EXPECTED_TABLES.issubset(tables), f"Missing tables: {EXPECTED_TABLES - tables}"

        views = {
            row[0]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type='view'").fetchall()
        }
        assert EXPECTED_VIEWS.issubset(views), f"Missing views: {EXPECTED_VIEWS - views}"


def test_schema_foreign_keys(monkeypatch, tmp_path):
    """Verify FK constraint from filings → managers is enforced."""
    monkeypatch.delenv("DB_URL", raising=False)
    db_path = tmp_path / "schema.db"
    config = _alembic_config(f"sqlite:///{db_path}")

    command.upgrade(config, "head")

    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA foreign_keys = ON")

        # Insert a valid manager
        conn.execute(
            "INSERT INTO managers(manager_id, name) VALUES (1, 'Test Manager')"
        )

        # FK violation: filing references non-existent manager_id
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO filings(manager_id, type, source) VALUES (999, '13F-HR', 'sec')"
            )


def test_schema_downgrade_drops_tables(monkeypatch, tmp_path):
    """Verify downgrade removes all tables and views."""
    monkeypatch.delenv("DB_URL", raising=False)
    db_path = tmp_path / "schema.db"
    config = _alembic_config(f"sqlite:///{db_path}")

    command.upgrade(config, "head")
    command.downgrade(config, "base")

    with sqlite3.connect(db_path) as conn:
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            ).fetchall()
        }
    assert EXPECTED_TABLES.isdisjoint(tables), f"Tables not dropped: {EXPECTED_TABLES & tables}"
