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
    "alert_rules",
    "alert_history",
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

        alert_rule_indexes = {
            row[1] for row in conn.execute("PRAGMA index_list('alert_rules')").fetchall()
        }
        assert {"idx_alert_rules_event"}.issubset(alert_rule_indexes)

        alert_history_indexes = {
            row[1] for row in conn.execute("PRAGMA index_list('alert_history')").fetchall()
        }
        assert {"idx_alert_history_unack", "idx_alert_history_rule"}.issubset(alert_history_indexes)


def test_alert_tables_schema_contract(monkeypatch, tmp_path):
    """Verify alert tables expose the expected schema contract after migration."""
    monkeypatch.delenv("DB_URL", raising=False)
    db_path = tmp_path / "schema.db"
    config = _alembic_config(f"sqlite:///{db_path}")

    command.upgrade(config, "head")

    with sqlite3.connect(db_path) as conn:
        alert_rules = {
            row[1]: {"type": row[2].upper(), "notnull": bool(row[3]), "default": row[4]}
            for row in conn.execute("PRAGMA table_info('alert_rules')").fetchall()
        }
        assert set(alert_rules) == {
            "rule_id",
            "name",
            "description",
            "event_type",
            "condition_json",
            "channels",
            "enabled",
            "manager_id",
            "created_by",
            "created_at",
            "updated_at",
        }
        assert alert_rules["name"]["notnull"] is True
        assert alert_rules["event_type"]["notnull"] is True
        assert alert_rules["condition_json"]["notnull"] is True
        assert alert_rules["enabled"]["notnull"] is True
        assert alert_rules["created_at"]["notnull"] is True
        assert alert_rules["updated_at"]["notnull"] is True
        assert alert_rules["condition_json"]["default"] in ("'{}'", "{}")
        assert "streamlit" in str(alert_rules["channels"]["default"])

        alert_history = {
            row[1]: {"type": row[2].upper(), "notnull": bool(row[3]), "default": row[4]}
            for row in conn.execute("PRAGMA table_info('alert_history')").fetchall()
        }
        assert set(alert_history) == {
            "alert_id",
            "rule_id",
            "fired_at",
            "event_type",
            "payload_json",
            "delivered_channels",
            "delivery_errors",
            "acknowledged",
            "acknowledged_by",
            "acknowledged_at",
        }
        assert alert_history["rule_id"]["notnull"] is True
        assert alert_history["fired_at"]["notnull"] is True
        assert alert_history["event_type"]["notnull"] is True
        assert alert_history["payload_json"]["notnull"] is True
        assert alert_history["delivered_channels"]["notnull"] is True
        assert alert_history["acknowledged"]["notnull"] is True

        alert_rules_sql = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='alert_rules'"
        ).fetchone()[0]
        assert "CHECK" in alert_rules_sql
        assert "'new_filing'" in alert_rules_sql
        assert "'activism_event'" in alert_rules_sql

        alert_history_fk = conn.execute("PRAGMA foreign_key_list('alert_history')").fetchall()
        assert any(row[2] == "alert_rules" and row[3] == "rule_id" for row in alert_history_fk)


def test_alert_migration_upgrade_and_downgrade(monkeypatch, tmp_path):
    """Verify alert history schema revisions upgrade and downgrade cleanly."""
    monkeypatch.delenv("DB_URL", raising=False)
    db_path = tmp_path / "schema.db"
    config = _alembic_config(f"sqlite:///{db_path}")

    command.upgrade(config, "006")
    command.upgrade(config, "008")

    with sqlite3.connect(db_path) as conn:
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            ).fetchall()
        }
        assert {"alert_rules", "alert_history"}.issubset(tables)
        history_columns = {
            row[1] for row in conn.execute("PRAGMA table_info('alert_history')").fetchall()
        }
        assert "rule_name" not in history_columns

    command.downgrade(config, "006")

    with sqlite3.connect(db_path) as conn:
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            ).fetchall()
        }
        assert {"alert_rules", "alert_history"}.isdisjoint(tables)
