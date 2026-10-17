import sqlite3
import sys
from contextlib import closing
from datetime import date
from pathlib import Path
from unittest.mock import Mock

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from alerts.db import ensure_alert_tables
from etl.daily_diff_flow import compute_manager_diffs, daily_diff_flow
from tools.run_contract import RunResult


class FixedDate(date):
    @classmethod
    def today(cls):
        return cls(2024, 5, 2)


def _setup_db(tmp_path: Path) -> str:
    """Create a SQLite DB with canonical schema and sample data for two managers."""
    db_path = tmp_path / "dev.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE managers (manager_id INTEGER PRIMARY KEY, name TEXT, cik TEXT UNIQUE)"
    )
    conn.execute(
        "CREATE TABLE filings ("
        "filing_id INTEGER PRIMARY KEY, manager_id INTEGER, "
        "type TEXT, filed_date TEXT, source TEXT, raw_key TEXT)"
    )
    conn.execute(
        "CREATE TABLE holdings ("
        "holding_id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "filing_id INTEGER, cusip TEXT, name_of_issuer TEXT, "
        "shares INTEGER, value_usd REAL)"
    )
    conn.execute("""CREATE TABLE daily_diffs (
            diff_id INTEGER PRIMARY KEY AUTOINCREMENT,
            manager_id INTEGER NOT NULL,
            report_date TEXT NOT NULL,
            cusip TEXT NOT NULL,
            name_of_issuer TEXT,
            delta_type TEXT NOT NULL,
            shares_prev INTEGER,
            shares_curr INTEGER,
            value_prev REAL,
            value_curr REAL
        )""")
    conn.executemany(
        "INSERT INTO managers(manager_id, name, cik) VALUES (?, ?, ?)",
        [
            (1, "Manager One", "0000000000"),
            (2, "Manager Two", "0000000001"),
        ],
    )
    conn.executemany(
        "INSERT INTO filings(filing_id, manager_id, type, filed_date, source) "
        "VALUES (?, ?, ?, ?, ?)",
        [
            (101, 1, "13F-HR", "2024-01-01", "edgar"),
            (102, 1, "13F-HR", "2024-04-01", "edgar"),
            (201, 2, "13F-HR", "2024-01-01", "edgar"),
            (202, 2, "13F-HR", "2024-04-01", "edgar"),
        ],
    )
    conn.executemany(
        "INSERT INTO holdings(filing_id, cusip, name_of_issuer, shares, value_usd) "
        "VALUES (?, ?, ?, ?, ?)",
        [
            (101, "AAA", "CorpA", 100, 1000),
            (101, "BBB", "CorpB", 30, 300),
            (101, "EEE", "CorpE", 10, 100),
            (102, "AAA", "CorpA", 120, 1200),
            (102, "CCC", "CorpC", 40, 400),
            (102, "EEE", "CorpE", 8, 80),
            (201, "XXX", "CorpX", 10, 100),
            (202, "XXX", "CorpX", 10, 100),
            (202, "YYY", "CorpY", 5, 50),
        ],
    )
    conn.commit()
    conn.close()
    return str(db_path)


def _setup_db_with_fk(tmp_path: Path) -> str:
    """Create a SQLite DB with foreign key constraint on daily_diffs."""
    db_path = tmp_path / "dev_fk.db"
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute(
        "CREATE TABLE managers (manager_id INTEGER PRIMARY KEY, name TEXT, cik TEXT UNIQUE)"
    )
    conn.execute(
        "CREATE TABLE filings ("
        "filing_id INTEGER PRIMARY KEY, manager_id INTEGER, "
        "type TEXT, filed_date TEXT, source TEXT, raw_key TEXT)"
    )
    conn.execute(
        "CREATE TABLE holdings ("
        "holding_id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "filing_id INTEGER, cusip TEXT, name_of_issuer TEXT, "
        "shares INTEGER, value_usd REAL)"
    )
    conn.execute("""CREATE TABLE daily_diffs (
            diff_id INTEGER PRIMARY KEY AUTOINCREMENT,
            manager_id INTEGER NOT NULL REFERENCES managers(manager_id),
            report_date TEXT NOT NULL,
            cusip TEXT NOT NULL,
            name_of_issuer TEXT,
            delta_type TEXT NOT NULL,
            shares_prev INTEGER,
            shares_curr INTEGER,
            value_prev REAL,
            value_curr REAL
        )""")
    conn.executemany(
        "INSERT INTO managers(manager_id, name, cik) VALUES (?, ?, ?)",
        [
            (1, "Manager One", "0000000000"),
            (2, "Manager Two", "0000000001"),
        ],
    )
    conn.executemany(
        "INSERT INTO filings(filing_id, manager_id, type, filed_date, source) "
        "VALUES (?, ?, ?, ?, ?)",
        [
            (101, 1, "13F-HR", "2024-01-01", "edgar"),
            (102, 1, "13F-HR", "2024-04-01", "edgar"),
            (201, 2, "13F-HR", "2024-01-01", "edgar"),
            (202, 2, "13F-HR", "2024-04-01", "edgar"),
        ],
    )
    conn.executemany(
        "INSERT INTO holdings(filing_id, cusip, name_of_issuer, shares, value_usd) "
        "VALUES (?, ?, ?, ?, ?)",
        [
            (101, "AAA", "CorpA", 100, 1000),
            (101, "BBB", "CorpB", 30, 300),
            (101, "EEE", "CorpE", 10, 100),
            (102, "AAA", "CorpA", 120, 1200),
            (102, "CCC", "CorpC", 40, 400),
            (102, "EEE", "CorpE", 8, 80),
            (201, "XXX", "CorpX", 10, 100),
            (202, "XXX", "CorpX", 10, 100),
            (202, "YYY", "CorpY", 5, 50),
        ],
    )
    conn.commit()
    conn.close()
    return str(db_path)


def test_etl_failure_dispatch_uses_real_entrypoint_and_serialized_replay(tmp_path, monkeypatch):
    import json

    import alerts.integration as integration
    from alerts.channels import DeliveryResult, NotificationChannel
    from etl.evaluation_flow import fire_quality_alerts

    db_path = _setup_db(tmp_path)
    conn = sqlite3.connect(db_path)
    ensure_alert_tables(conn)
    conn.execute(
        "INSERT INTO alert_rules(name,event_type,condition_json,channels,enabled) VALUES (?,?,?,?,?)",
        ("ETL failure", "etl_failure", "{}", '["email","slack"]', 1),
    )
    conn.execute("CREATE TABLE evaluation_observations(id INTEGER PRIMARY KEY)")
    conn.commit()
    conn.execute("INSERT INTO evaluation_observations VALUES (1)")
    sends = []

    class Channel(NotificationChannel):
        def __init__(self, name):
            self.channel_name = name

        async def deliver(self, alert):
            with sqlite3.connect(db_path) as observer:
                assert (
                    observer.execute("SELECT COUNT(*) FROM evaluation_observations").fetchone()[0]
                    == 1
                )
                assert (
                    observer.execute(
                        "SELECT COUNT(*) FROM alert_delivery_attempts WHERE channel=?",
                        (self.channel_name,),
                    ).fetchone()[0]
                    >= 1
                )
            sends.append(self.channel_name)
            return DeliveryResult(channel=self.channel_name, success=True)

    monkeypatch.setattr(
        integration,
        "build_configured_channels",
        lambda: {name: Channel(name) for name in ["email", "slack"]},
    )
    summary = {"failures": {"sql_safety": 0.0}}
    try:
        assert fire_quality_alerts.fn(summary, db_conn=conn) == 1
        replay = json.loads(json.dumps(summary))
        assert replay["alert_observed_at"] == summary["alert_observed_at"]
        assert fire_quality_alerts.fn(replay, db_conn=conn) == 1
        assert sends == ["email", "slack"]
        row = conn.execute(
            "SELECT delivered_channels FROM alert_history WHERE event_type='etl_failure'"
        ).fetchone()
        assert json.loads(row[0]) == ["email", "slack"]
        assert conn.execute("SELECT COUNT(*) FROM alert_delivery_attempts").fetchone()[0] == 2
        assert fire_quality_alerts.fn({"failures": {"sql_safety": 0.0}}, db_conn=conn) == 1
        assert sends == ["email", "slack", "email", "slack"]
    finally:
        conn.close()


def test_compute_manager_diffs_persists_all_four_delta_types_with_values(tmp_path):
    """compute_manager_diffs writes all 4 delta types with correct prev/curr values."""
    db_path = _setup_db(tmp_path)
    conn = sqlite3.connect(db_path)
    count = compute_manager_diffs.fn(1, "2024-05-01", conn)
    conn.commit()

    rows = conn.execute(
        "SELECT cusip, delta_type, shares_prev, shares_curr, value_prev, value_curr "
        "FROM daily_diffs WHERE manager_id = 1 ORDER BY cusip"
    ).fetchall()
    conn.close()

    assert count == 4
    assert rows == [
        ("AAA", "INCREASE", 100, 120, 1000.0, 1200.0),
        ("BBB", "EXIT", 30, None, 300.0, None),
        ("CCC", "ADD", None, 40, None, 400.0),
        ("EEE", "DECREASE", 10, 8, 100.0, 80.0),
    ]


def test_compute_manager_diffs_valid_manager_fk(tmp_path):
    """All daily_diffs rows reference valid manager_ids (FK integrity)."""
    db_path = _setup_db_with_fk(tmp_path)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    compute_manager_diffs.fn(1, "2024-05-01", conn)
    conn.commit()

    orphan_count = conn.execute("""
        SELECT COUNT(*)
        FROM daily_diffs d
        LEFT JOIN managers m ON m.manager_id = d.manager_id
        WHERE m.manager_id IS NULL
    """).fetchone()[0]
    conn.close()
    assert orphan_count == 0


def test_compute_manager_diffs_is_idempotent(tmp_path):
    """Running compute_manager_diffs twice doesn't create duplicates."""
    db_path = _setup_db(tmp_path)
    conn = sqlite3.connect(db_path)
    compute_manager_diffs.fn(1, "2024-05-01", conn)
    conn.commit()
    compute_manager_diffs.fn(1, "2024-05-01", conn)
    conn.commit()

    count = conn.execute("SELECT COUNT(*) FROM daily_diffs").fetchone()[0]
    conn.close()
    assert count == 4  # Not 8 (duplicated)


def test_daily_diff_flow_processes_multiple_managers(tmp_path, monkeypatch):
    """Flow iterates all managers and writes diffs for each."""
    db_path = _setup_db(tmp_path)
    monkeypatch.setenv("DB_PATH", db_path)
    monkeypatch.delenv("DB_URL", raising=False)

    daily_diff_flow.fn(date="2024-05-01")

    conn = sqlite3.connect(db_path)
    rows = conn.execute(
        "SELECT manager_id, report_date, cusip, delta_type "
        "FROM daily_diffs ORDER BY manager_id, cusip"
    ).fetchall()
    conn.close()
    assert rows == [
        (1, "2024-05-01", "AAA", "INCREASE"),
        (1, "2024-05-01", "BBB", "EXIT"),
        (1, "2024-05-01", "CCC", "ADD"),
        (1, "2024-05-01", "EEE", "DECREASE"),
        (2, "2024-05-01", "YYY", "ADD"),
    ]


def test_daily_diff_flow_records_large_delta_alerts(tmp_path, monkeypatch):
    db_path = _setup_db(tmp_path)
    conn = sqlite3.connect(db_path)
    ensure_alert_tables(conn)
    conn.execute(
        """INSERT INTO alert_rules(
            name, event_type, condition_json, channels, enabled, manager_id
        ) VALUES (?, ?, ?, ?, ?, ?)""",
        (
            "Large buy",
            "large_delta",
            '{"delta_type":"buy","value_usd_gt":350}',
            '["streamlit"]',
            1,
            1,
        ),
    )
    conn.commit()
    conn.close()

    monkeypatch.setenv("DB_PATH", db_path)
    monkeypatch.delenv("DB_URL", raising=False)

    daily_diff_flow.fn(date="2024-05-01")

    conn = sqlite3.connect(db_path)
    rows = conn.execute("""SELECT ah.event_type, ah.payload_json, ah.delivered_channels
           FROM alert_history ah
           JOIN alert_rules ar ON ar.rule_id = ah.rule_id
           WHERE ar.name = 'Large buy'""").fetchall()
    conn.close()

    assert rows == [
        (
            "large_delta",
            (
                '{"cusip":"CCC","delta_type":"buy","manager_id":1,'
                '"name_of_issuer":"CorpC","raw_delta_type":"ADD",'
                '"report_date":"2024-05-01","shares_curr":40,"shares_prev":null,'
                '"value_curr":400.0,"value_prev":null,"value_usd":400.0}'
            ),
            '["streamlit"]',
        )
    ]


def test_daily_diff_flow_defaults_to_yesterday(tmp_path, monkeypatch):
    """When date=None, the flow uses yesterday's date."""
    db_path = _setup_db(tmp_path)
    monkeypatch.setenv("DB_PATH", db_path)
    monkeypatch.delenv("DB_URL", raising=False)

    import etl.daily_diff_flow as ddf

    monkeypatch.setattr(ddf.dt, "date", FixedDate)

    daily_diff_flow.fn()

    conn = sqlite3.connect(db_path)
    dates = conn.execute("SELECT DISTINCT report_date FROM daily_diffs").fetchall()
    conn.close()
    assert dates == [("2024-05-01",)]


def test_daily_diff_flow_refreshes_matview_on_postgres(monkeypatch):
    """Flow calls REFRESH MATERIALIZED VIEW for non-SQLite (Postgres) connections."""
    executed_sql = []

    class FakePostgresConn:
        """Mimics a Postgres connection (not sqlite3.Connection)."""

        def execute(self, sql, params=None):
            executed_sql.append(sql)
            return self

        def fetchall(self):
            return [(1,)]

        def close(self):
            pass

    import etl.daily_diff_flow as ddf

    monkeypatch.setattr(ddf, "connect_db", lambda: FakePostgresConn())
    monkeypatch.setattr(
        ddf, "diff_holdings", lambda mid, conn: RunResult(tool="diff_holdings", outputs=[])
    )

    daily_diff_flow.fn(date="2024-01-01")

    assert "SELECT 1 FROM daily_diffs LIMIT 1" in executed_sql
    assert "BEGIN" in executed_sql
    assert "COMMIT" in executed_sql
    assert (
        "CREATE UNIQUE INDEX IF NOT EXISTS mv_daily_report_idx "
        "ON mv_daily_report (report_date, manager_id, cusip, delta_type)"
    ) in executed_sql
    assert "REFRESH MATERIALIZED VIEW CONCURRENTLY mv_daily_report" in executed_sql
    assert all("AUTOINCREMENT" not in sql for sql in executed_sql)
    assert all("CREATE TABLE IF NOT EXISTS daily_diffs" not in sql for sql in executed_sql)


def test_daily_diff_empty_database_emits_empty_artifact(monkeypatch):
    """No-manager runs stay replayable with a header-only CSV."""
    import etl.daily_diff_flow as ddf

    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE managers (manager_id INTEGER PRIMARY KEY)")
    captured: dict[str, object] = {}
    monkeypatch.setattr(ddf, "connect_db", lambda: conn)
    monkeypatch.setattr(ddf, "new_run_id", lambda: "run-empty")
    monkeypatch.setattr(
        ddf,
        "write_artifact_bundle",
        lambda run_id, tool, files, *, inputs: captured.update(
            {"run_id": run_id, "tool": tool, "files": files, "inputs": inputs}
        )
        or [{"name": "deltas.csv"}],
    )

    result = daily_diff_flow.fn(date="2024-05-01")

    assert result.outputs == {
        "managers_processed": 0,
        "managers_skipped": 0,
        "total_changes": 0,
        "data_quality": {"missing_fields": [], "conflicts": [], "confidence": "unknown"},
    }
    assert result.warnings == ["No managers found in database"]
    assert captured == {
        "run_id": "run-empty",
        "tool": "daily_diff",
        "files": {
            "deltas.csv": (
                "cusip,name_of_issuer,delta_type,shares_prev,shares_curr,value_prev,value_curr\n"
            )
        },
        "inputs": {"date": "2024-05-01"},
    }

    assert result.run_id == "run-empty"
    assert result.artifacts == [{"name": "deltas.csv"}]
    assert result.status == "success"
    with pytest.raises(sqlite3.ProgrammingError, match="closed database"):
        conn.execute("SELECT 1")


@pytest.mark.parametrize(
    ("delta_type", "value_prev", "value_curr", "normalized", "value_usd"),
    [
        ("add", None, -8, "buy", 8.0),
        ("INCREASE", 4, 10, "buy", 6.0),
        ("EXIT", -9, None, "sell", 9.0),
        ("DECREASE", 10, 4, "sell", 6.0),
        ("UNCHANGED", 9, 4, "unchanged", 5.0),
        ("OTHER", None, None, "other", None),
    ],
)
def test_daily_diff_normalizes_alerts(delta_type, value_prev, value_curr, normalized, value_usd):
    import etl.daily_diff_flow as ddf

    row = {
        "manager_id": 7,
        "report_date": "2024-05-01",
        "cusip": "AAA",
        "name_of_issuer": "CorpA",
        "delta_type": delta_type,
        "shares_prev": 3,
        "shares_curr": 5,
        "value_prev": value_prev,
        "value_curr": value_curr,
    }
    event = ddf._large_delta_event_from_diff(row)

    assert event.event_type == "large_delta"
    assert event.manager_id == 7
    assert event.payload == {
        **row,
        "raw_delta_type": delta_type.upper(),
        "delta_type": normalized,
        "value_usd": value_usd,
    }


@pytest.mark.parametrize("error_kind", ["message", "pgcode", "class_name"])
def test_daily_diff_missing_table_requires_migrations(error_kind):
    import etl.daily_diff_flow as ddf

    if error_kind == "pgcode":

        class MissingTableError(Exception):
            pgcode = "42P01"

        error = MissingTableError("missing")
    elif error_kind == "class_name":

        class UndefinedTable(Exception):
            pass

        error = UndefinedTable("missing")
    else:
        error = RuntimeError("relation daily_diffs does not exist")
    conn = Mock()
    conn.execute.side_effect = error

    with pytest.raises(RuntimeError, match="daily_diffs table is missing") as caught:
        ddf._ensure_daily_diffs_table(conn)

    assert caught.value.__cause__ is error
    conn.execute.assert_called_once_with("SELECT 1 FROM daily_diffs LIMIT 1")


def test_daily_diff_table_check_preserves_other_database_errors():
    import etl.daily_diff_flow as ddf

    error = RuntimeError("permission denied")
    conn = Mock()
    conn.execute.side_effect = error
    with pytest.raises(RuntimeError) as caught:
        ddf._ensure_daily_diffs_table(conn)
    assert caught.value is error


@pytest.mark.parametrize("missing", [False, True], ids=["permission-error", "missing-view"])
@pytest.mark.parametrize("statement", [1, 2], ids=["index", "refresh"])
def test_daily_diff_view_refresh_only_suppresses_missing_view(missing, statement):
    import etl.daily_diff_flow as ddf

    error = RuntimeError("relation DOES NOT EXIST" if missing else "permission denied")
    conn = Mock()
    conn.execute.side_effect = [None] * (statement - 1) + [error]

    if missing:
        ddf._refresh_matview(conn)
    else:
        with pytest.raises(RuntimeError) as caught:
            ddf._refresh_matview(conn)
        assert caught.value is error
    assert conn.execute.call_count == statement


def test_daily_diff_manager_failure_propagates_and_closes_connection(monkeypatch):
    import etl.daily_diff_flow as ddf

    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE managers (manager_id INTEGER PRIMARY KEY)")
    conn.executemany("INSERT INTO managers VALUES (?)", [(1,), (2,)])
    monkeypatch.setattr(ddf, "connect_db", lambda: conn)
    error = RuntimeError("diff computation failed")
    compute = Mock(side_effect=error)
    monkeypatch.setattr(ddf, "compute_manager_diffs", type("Task", (), {"fn": compute})())
    artifacts = Mock()
    monkeypatch.setattr(ddf, "write_artifact_bundle", artifacts)

    with pytest.raises(RuntimeError) as caught:
        daily_diff_flow.fn(date="2024-05-01")

    assert caught.value is error
    compute.assert_called_once_with(1, "2024-05-01", conn)
    artifacts.assert_not_called()
    with pytest.raises(sqlite3.ProgrammingError, match="closed database"):
        conn.execute("SELECT 1")


def test_daily_diff_continues_after_manager_with_insufficient_filings(tmp_path, monkeypatch):
    import etl.daily_diff_flow as ddf

    db_path = _setup_db(tmp_path)
    with closing(sqlite3.connect(db_path)) as conn:
        # Manager 1 is skipped; manager 2 must still be processed.
        conn.execute("DELETE FROM holdings WHERE filing_id = 101")
        conn.execute("DELETE FROM filings WHERE filing_id = 101")
        conn.commit()
    monkeypatch.setenv("DB_PATH", db_path)
    monkeypatch.delenv("DB_URL", raising=False)
    monkeypatch.setattr(ddf, "write_artifact_bundle", Mock(return_value=[]))

    result = daily_diff_flow.fn(date="2024-05-01")

    assert result.status == "success"
    assert result.warnings == ["manager 1 skipped (< 2 filings)"]
    assert result.outputs["managers_skipped"] == 1
    assert result.outputs["managers_processed"] == 1
    assert result.outputs["total_changes"] == 1
    with closing(sqlite3.connect(db_path)) as conn:
        assert conn.execute("SELECT manager_id, cusip FROM daily_diffs").fetchall() == [(2, "YYY")]


def test_daily_diff_timezone_falls_back_to_utc(monkeypatch):
    import etl.daily_diff_flow as ddf

    class NoTimezoneDateTime:
        @classmethod
        def now(cls):
            return cls()

        def astimezone(self):
            return type("Local", (), {"tzinfo": None})()

    monkeypatch.delenv("TZ", raising=False)
    monkeypatch.setattr(ddf.dt, "datetime", NoTimezoneDateTime)
    monkeypatch.setattr(ddf.os.path, "realpath", lambda _path: "/etc/localtime")
    assert ddf._resolve_local_timezone() == "UTC"
