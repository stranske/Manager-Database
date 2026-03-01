from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config

ROOT = Path(__file__).resolve().parents[1]

EXPECTED_TABLES = {
    "managers",
    "manager_aliases",
    "manager_jurisdictions",
    "manager_tags",
    "filings",
    "holdings",
    "api_usage",
}
EXPECTED_VIEWS = {"monthly_usage", "manager_holdings_summary"}


def _alembic_config(db_url: str) -> Config:
    config = Config(str(ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(ROOT / "alembic"))
    config.set_main_option("sqlalchemy.url", db_url)
    return config


def test_schema_upgrade_creates_expected_objects_and_fk(monkeypatch, tmp_path):
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
        assert EXPECTED_TABLES.issubset(tables)

        views = {
            row[0]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type='view'").fetchall()
        }
        assert EXPECTED_VIEWS.issubset(views)

        conn.execute("INSERT INTO managers(name, cik) VALUES (?, ?)", ("FK Manager", "0000000001"))
        manager_id = conn.execute(
            "SELECT id FROM managers WHERE cik = ?", ("0000000001",)
        ).fetchone()[0]
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO holdings(filing_id, manager_id, cusip) VALUES (?, ?, ?)",
                (999_999, manager_id, "000000000"),
            )


def test_schema_downgrade_drops_tables(monkeypatch, tmp_path):
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
    assert EXPECTED_TABLES.isdisjoint(tables)
