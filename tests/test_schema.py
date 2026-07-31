from __future__ import annotations

import re
import sqlite3
from pathlib import Path

import pytest
from alembic.config import Config

from adapters.prices import ensure_price_cache_table
from alembic import command
from etl.activism_campaign_flow import ensure_activism_campaign_tables
from etl.attribution_flow import ensure_manager_attribution_table
from etl.backtest_flow import ensure_backtest_tables

ROOT = Path(__file__).resolve().parents[1]

# Canonical schema tables
EXPECTED_TABLES = {
    "managers",
    "filings",
    "activism_filings",
    "activism_events",
    "activism_campaigns",
    "activism_campaign_timeline",
    "alert_rules",
    "alert_history",
    "chat_feedback",
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


def test_activism_campaign_migration_generates_sqlite_primary_keys(monkeypatch, tmp_path):
    """Migration 015 must assign campaign/timeline IDs without callers supplying them."""
    monkeypatch.delenv("DB_URL", raising=False)
    db_path = tmp_path / "schema.db"
    command.upgrade(_alembic_config(f"sqlite:///{db_path}"), "head")

    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("INSERT INTO managers(manager_id, name) VALUES (1, 'Activist')")
        conn.execute(
            "INSERT INTO activism_filings(filing_id, manager_id, filing_type, subject_company, "
            "filed_date, url) VALUES (1, 1, 'SC 13D', 'Example Corp', '2024-05-01', "
            "'https://sec.example/1')"
        )
        campaign = conn.execute(
            "INSERT INTO activism_campaigns(manager_id, target_identifier, target_company, "
            "first_filed, last_filed, status) VALUES (1, '123456789', 'Example Corp', "
            "'2024-05-01', '2024-05-01', 'active')"
        )
        assert campaign.lastrowid is not None
        timeline = conn.execute(
            "INSERT INTO activism_campaign_timeline(campaign_id, filing_id, event_date, "
            "event_type, form_type, summary) VALUES (?, 1, '2024-05-01', 'initial_filing', "
            "'SC 13D', 'Filed SC 13D for Example Corp.')",
            (campaign.lastrowid,),
        )
        assert timeline.lastrowid is not None


def test_activism_campaign_migration_constrains_status_and_filing_only_rows(monkeypatch, tmp_path):
    monkeypatch.delenv("DB_URL", raising=False)
    db_path = tmp_path / "schema.db"
    command.upgrade(_alembic_config(f"sqlite:///{db_path}"), "head")

    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("INSERT INTO managers(manager_id, name) VALUES (1, 'Activist')")
        conn.execute(
            "INSERT INTO activism_filings(filing_id, manager_id, filing_type, subject_company, "
            "filed_date, url) VALUES (1, 1, 'SC 13D', 'Example Corp', '2024-05-01', "
            "'https://sec.example/1')"
        )
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO activism_campaigns(manager_id, target_identifier, target_company, "
                "first_filed, last_filed, status) VALUES (1, '123456789', 'Example Corp', "
                "'2024-05-01', '2024-05-01', 'bogus')"
            )
        campaign = conn.execute(
            "INSERT INTO activism_campaigns(manager_id, target_identifier, target_company, "
            "first_filed, last_filed, status) VALUES (1, '123456789', 'Example Corp', "
            "'2024-05-01', '2024-05-01', 'active')"
        )
        insert_filing_only = (
            "INSERT INTO activism_campaign_timeline(campaign_id, filing_id, event_date, "
            "event_type, form_type, summary) VALUES (?, 1, '2024-05-01', 'initial_filing', "
            "'SC 13D', 'Filed SC 13D for Example Corp.')"
        )
        conn.execute(insert_filing_only, (campaign.lastrowid,))
        # event_id IS NULL rows are not deduplicated by the composite UNIQUE constraint.
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(insert_filing_only, (campaign.lastrowid,))


def test_activism_campaign_migration_uses_timezone_aware_computed_at():
    """schema.sql declares timestamptz; the migration must not drift to a naive type."""
    migration = (ROOT / "alembic" / "versions" / "016_activism_campaigns.py").read_text(
        encoding="utf-8"
    )
    computed_at = re.search(r'"computed_at",\s*(sa\.DateTime\([^)]*\))', migration)
    assert computed_at is not None
    assert computed_at.group(1) == "sa.DateTime(timezone=True)"


def test_activism_campaign_return_migration_uses_timezone_aware_timestamp():
    migration = (ROOT / "alembic" / "versions" / "019_activism_campaign_returns.py").read_text(
        encoding="utf-8"
    )
    assert '"return_computed_at", sa.DateTime(timezone=True)' in migration


def test_activism_campaign_definitions_stay_in_sync(monkeypatch, tmp_path):
    """Keep migration, runtime SQLite DDL, and canonical schema.sql column sets aligned."""
    monkeypatch.delenv("DB_URL", raising=False)
    db_path = tmp_path / "migrated.db"
    command.upgrade(_alembic_config(f"sqlite:///{db_path}"), "head")

    tables = {"activism_campaigns", "activism_campaign_timeline"}
    with sqlite3.connect(db_path) as migrated, sqlite3.connect(":memory:") as runtime:
        ensure_activism_campaign_tables(runtime)
        migrated_columns = {
            table: {row[1] for row in migrated.execute(f"PRAGMA table_info({table})")}
            for table in tables
        }
        runtime_columns = {
            table: {row[1] for row in runtime.execute(f"PRAGMA table_info({table})")}
            for table in tables
        }

    schema = (ROOT / "schema.sql").read_text(encoding="utf-8")
    schema_columns = {}
    for table in tables:
        definition = re.search(
            rf"CREATE TABLE IF NOT EXISTS {table} \((.*?)\n\);", schema, flags=re.DOTALL
        )
        assert definition is not None
        schema_columns[table] = {
            line.strip().split()[0]
            for line in definition.group(1).splitlines()
            if line.strip()
            and not line.lstrip().startswith(
                ("PRIMARY", "FOREIGN", "UNIQUE", "CONSTRAINT", "CHECK")
            )
        }

    assert runtime_columns == migrated_columns == schema_columns


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


def test_backtest_migration_uses_sqlite_autoincrement_primary_keys(monkeypatch, tmp_path):
    """Migration 015 must generate IDs without callers supplying SQLite BIGINT keys."""
    monkeypatch.delenv("DB_URL", raising=False)
    db_path = tmp_path / "schema.db"
    command.upgrade(_alembic_config(f"sqlite:///{db_path}"), "head")

    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        run = conn.execute(
            "INSERT INTO backtest_runs(strategy, start_date, end_date) VALUES (?, ?, ?)",
            ("test", "2024-01-01", "2024-03-31"),
        )
        assert run.lastrowid is not None
        result = conn.execute(
            "INSERT INTO backtest_results(run_id, decision_date, entry_date, exit_date) "
            "VALUES (?, ?, ?, ?)",
            (run.lastrowid, "2024-01-01", "2024-01-02", "2024-04-02"),
        )
        assert result.lastrowid is not None


def test_short_interest_migration_uses_sqlite_autoincrement_primary_key(monkeypatch, tmp_path):
    """Migration 018 must generate metric_id without callers supplying SQLite BIGINT keys."""
    monkeypatch.delenv("DB_URL", raising=False)
    db_path = tmp_path / "schema.db"
    command.upgrade(_alembic_config(f"sqlite:///{db_path}"), "head")

    with sqlite3.connect(db_path) as conn:
        inserted = conn.execute(
            "INSERT INTO short_interest(ticker, report_date, source) VALUES (?, ?, ?)",
            ("AAPL", "2026-07-15", "finra"),
        )
        assert inserted.lastrowid is not None
        assert conn.execute("SELECT metric_id FROM short_interest").fetchone()[0] is not None


def test_backtest_schema_contract_stays_in_sync_across_all_three_definitions(monkeypatch, tmp_path):
    """Keep migration, runtime SQLite DDL, and canonical schema.sql column contracts aligned."""
    monkeypatch.delenv("DB_URL", raising=False)
    db_path = tmp_path / "migrated.db"
    command.upgrade(_alembic_config(f"sqlite:///{db_path}"), "head")

    tables = {"price_cache", "backtest_runs", "backtest_results"}
    with sqlite3.connect(db_path) as migrated, sqlite3.connect(":memory:") as runtime:
        ensure_price_cache_table(runtime)
        ensure_backtest_tables(runtime)
        migrated_columns = {
            table: {row[1] for row in migrated.execute(f"PRAGMA table_info({table})")}
            for table in tables
        }
        runtime_columns = {
            table: {row[1] for row in runtime.execute(f"PRAGMA table_info({table})")}
            for table in tables
        }

    schema = (ROOT / "schema.sql").read_text(encoding="utf-8")
    schema_columns = {}
    for table in tables:
        definition = re.search(
            rf"CREATE TABLE IF NOT EXISTS {table} \((.*?)\n\);", schema, flags=re.DOTALL
        )
        assert definition is not None
        schema_columns[table] = {
            line.strip().split()[0]
            for line in definition.group(1).splitlines()
            if line.strip()
            and not line.lstrip().startswith(("PRIMARY", "FOREIGN", "UNIQUE", "CONSTRAINT"))
        }

    assert runtime_columns == migrated_columns == schema_columns


def test_manager_attribution_migration_uses_sqlite_autoincrement_primary_key(monkeypatch, tmp_path):
    """Migration 019 must generate attribution_id without callers supplying BIGINT keys."""
    monkeypatch.delenv("DB_URL", raising=False)
    db_path = tmp_path / "schema.db"
    command.upgrade(_alembic_config(f"sqlite:///{db_path}"), "head")

    with sqlite3.connect(db_path) as conn:
        inserted = conn.execute(
            "INSERT INTO manager_attribution("
            "manager_id, filing_id, disclosure_date, as_of_date, security_key) "
            "VALUES (?, ?, ?, ?, ?)",
            (1, 10, "2024-05-01", "2024-08-01", "AAA"),
        )
        assert inserted.lastrowid is not None
        assert (
            conn.execute("SELECT attribution_id FROM manager_attribution").fetchone()[0] is not None
        )


def test_manager_attribution_schema_contract_stays_in_sync(monkeypatch, tmp_path):
    """Keep migration, runtime SQLite DDL, and schema.sql column contracts aligned."""
    monkeypatch.delenv("DB_URL", raising=False)
    db_path = tmp_path / "migrated.db"
    command.upgrade(_alembic_config(f"sqlite:///{db_path}"), "head")

    tables = {"manager_attribution"}
    with sqlite3.connect(db_path) as migrated, sqlite3.connect(":memory:") as runtime:
        ensure_manager_attribution_table(runtime)
        migrated_columns = {
            table: {row[1] for row in migrated.execute(f"PRAGMA table_info({table})")}
            for table in tables
        }
        runtime_columns = {
            table: {row[1] for row in runtime.execute(f"PRAGMA table_info({table})")}
            for table in tables
        }

    schema = (ROOT / "schema.sql").read_text(encoding="utf-8")
    schema_columns = {}
    for table in tables:
        definition = re.search(
            rf"CREATE TABLE IF NOT EXISTS {table} \((.*?)\n\);", schema, flags=re.DOTALL
        )
        assert definition is not None
        schema_columns[table] = {
            line.strip().split()[0]
            for line in definition.group(1).splitlines()
            if line.strip()
            and not line.lstrip().startswith(("PRIMARY", "FOREIGN", "UNIQUE", "CONSTRAINT"))
        }

    assert runtime_columns == migrated_columns == schema_columns


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

        chat_feedback_indexes = {
            row[1] for row in conn.execute("PRAGMA index_list('chat_feedback')").fetchall()
        }
        assert {"idx_chat_feedback_response_id"}.issubset(chat_feedback_indexes)


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

        chat_feedback = {
            row[1]: {"type": row[2].upper(), "notnull": bool(row[3]), "default": row[4]}
            for row in conn.execute("PRAGMA table_info('chat_feedback')").fetchall()
        }
        assert set(chat_feedback) == {
            "feedback_id",
            "response_id",
            "rating",
            "comment",
            "created_at",
        }
        assert chat_feedback["response_id"]["notnull"] is True
        assert chat_feedback["rating"]["notnull"] is True
        assert chat_feedback["created_at"]["notnull"] is True
        assert chat_feedback["created_at"]["default"] in ("CURRENT_TIMESTAMP", "current_timestamp")


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
