from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from alembic.config import Config

from alembic import command

ROOT = Path(__file__).resolve().parents[1]

# Canonical schema tables
EXPECTED_TABLES = {
    "managers",
    "filings",
    "activism_filings",
    "activism_events",
    "holdings",
    "news_items",
    "documents",
    "daily_diffs",
    "api_usage",
    "conviction_scores",
    "crowded_trades",
    "contrarian_signals",
}
# Two materialized views (regular views on SQLite)
EXPECTED_VIEWS = {"monthly_usage", "mv_daily_report"}


def _alembic_config(db_url: str) -> Config:
    config = Config(str(ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(ROOT / "alembic"))
    config.set_main_option("sqlalchemy.url", db_url)
    return config


def test_schema_upgrade_creates_expected_objects(monkeypatch, tmp_path):
    """Verify migration creates all canonical tables and views."""
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
        conn.execute("INSERT INTO managers(manager_id, name) VALUES (1, 'Test Manager')")

        # FK violation: filing references non-existent manager_id
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO filings(manager_id, type, source) VALUES (999, '13F-HR', 'sec')"
            )


def test_filings_raw_key_unique_index(monkeypatch, tmp_path):
    """Verify migration 002 creates a unique index on filings.raw_key."""
    monkeypatch.delenv("DB_URL", raising=False)
    db_path = tmp_path / "schema.db"
    config = _alembic_config(f"sqlite:///{db_path}")

    command.upgrade(config, "head")

    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA foreign_keys = ON")

        # Insert a manager and a filing (explicit filing_id because
        # BigInteger PK does not auto-increment on SQLite).
        conn.execute("INSERT INTO managers(manager_id, name) VALUES (1, 'Test')")
        conn.execute(
            "INSERT INTO filings(filing_id, manager_id, type, source, raw_key) "
            "VALUES (1, 1, '13F-HR', 'edgar', 'raw/test_key.xml')"
        )

        # Duplicate raw_key must raise IntegrityError
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO filings(filing_id, manager_id, type, source, raw_key) "
                "VALUES (2, 1, '13F-HR', 'edgar', 'raw/test_key.xml')"
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


def test_conviction_scores_schema_objects(monkeypatch, tmp_path):
    """Verify migration 004 creates conviction_scores indexes and unique key."""
    monkeypatch.delenv("DB_URL", raising=False)
    db_path = tmp_path / "schema.db"
    config = _alembic_config(f"sqlite:///{db_path}")

    command.upgrade(config, "head")

    with sqlite3.connect(db_path) as conn:
        columns = {
            row[1]: row[2].upper()
            for row in conn.execute("PRAGMA table_info('conviction_scores')").fetchall()
        }
        assert columns["score_id"] == "BIGINT"
        assert columns["manager_id"] == "BIGINT"
        assert columns["filing_id"] == "BIGINT"
        assert columns["cusip"] == "TEXT"
        assert columns["conviction_pct"].startswith("NUMERIC")
        assert columns["portfolio_weight"].startswith("NUMERIC")

        indexes = {
            row[1] for row in conn.execute("PRAGMA index_list('conviction_scores')").fetchall()
        }
        assert "idx_conviction_manager" in indexes
        assert "idx_conviction_cusip" in indexes
        assert "idx_conviction_pct" in indexes


def test_analytics_indexes_exist(monkeypatch, tmp_path):
    """Verify analytics and activism table indexes are created."""
    monkeypatch.delenv("DB_URL", raising=False)
    db_path = tmp_path / "schema.db"
    config = _alembic_config(f"sqlite:///{db_path}")

    command.upgrade(config, "head")

    with sqlite3.connect(db_path) as conn:
        crowded_indexes = {
            row[1] for row in conn.execute("PRAGMA index_list('crowded_trades')").fetchall()
        }
        assert {"idx_crowded_date", "idx_crowded_count"}.issubset(crowded_indexes)

        contrarian_indexes = {
            row[1] for row in conn.execute("PRAGMA index_list('contrarian_signals')").fetchall()
        }
        assert {"idx_contrarian_manager", "idx_contrarian_date"}.issubset(contrarian_indexes)

        activism_indexes = {
            row[1] for row in conn.execute("PRAGMA index_list('activism_filings')").fetchall()
        }
        assert {"idx_activism_manager", "idx_activism_cusip", "idx_activism_date"}.issubset(
            activism_indexes
        )

        activism_event_indexes = {
            row[1] for row in conn.execute("PRAGMA index_list('activism_events')").fetchall()
        }
        assert {
            "idx_activism_events_manager",
            "idx_activism_events_type",
            "idx_activism_events_date",
            "idx_activism_events_cusip",
            "idx_activism_events_unique_base",
            "idx_activism_events_unique_threshold",
        }.issubset(activism_event_indexes)


def test_schema_sql_defines_activism_events_unique_indexes():
    """Verify schema.sql supports deduping reruns without blocking multiple thresholds."""
    schema_sql = (ROOT / "schema.sql").read_text()

    assert "CREATE TABLE IF NOT EXISTS activism_events (" in schema_sql
    assert "UNIQUE (manager_id, filing_id, event_type)" not in schema_sql
    assert "CREATE UNIQUE INDEX IF NOT EXISTS idx_activism_events_unique_base" in schema_sql
    assert "WHERE threshold_crossed IS NULL" in schema_sql
    assert "CREATE UNIQUE INDEX IF NOT EXISTS idx_activism_events_unique_threshold" in schema_sql
    assert "WHERE threshold_crossed IS NOT NULL" in schema_sql


def test_activism_events_migration_allows_multiple_threshold_crossings(monkeypatch, tmp_path):
    """Verify migration 006 deduplicates reruns while allowing multiple thresholds per filing."""
    monkeypatch.delenv("DB_URL", raising=False)
    db_path = tmp_path / "schema.db"
    config = _alembic_config(f"sqlite:///{db_path}")

    command.upgrade(config, "head")

    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("INSERT INTO managers(manager_id, name) VALUES (1, 'Test Manager')")
        conn.execute(
            """INSERT INTO activism_filings(
                filing_id, manager_id, filing_type, subject_company, subject_cusip,
                ownership_pct, filed_date, url
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (1, 1, "SC 13D", "Apple Inc.", "037833100", 11.0, "2024-05-02", "https://example.test"),
        )

        conn.execute(
            """INSERT INTO activism_events(
                event_id, manager_id, filing_id, event_type, subject_company, subject_cusip,
                ownership_pct, previous_pct, delta_pct, threshold_crossed
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (1, 1, 1, "threshold_crossing", "Apple Inc.", "037833100", 11.0, 4.0, 7.0, 5.0),
        )
        conn.execute(
            """INSERT INTO activism_events(
                event_id, manager_id, filing_id, event_type, subject_company, subject_cusip,
                ownership_pct, previous_pct, delta_pct, threshold_crossed
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (2, 1, 1, "threshold_crossing", "Apple Inc.", "037833100", 11.0, 4.0, 7.0, 10.0),
        )

        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """INSERT INTO activism_events(
                    event_id, manager_id, filing_id, event_type, subject_company, subject_cusip,
                    ownership_pct, previous_pct, delta_pct, threshold_crossed
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (3, 1, 1, "threshold_crossing", "Apple Inc.", "037833100", 11.0, 4.0, 7.0, 10.0),
            )
