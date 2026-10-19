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


@pytest.mark.parametrize("failure", ["none", "channel", "outcome", "unconfigured"])
def test_large_delta_dispatch_commits_before_send_and_suppresses_replay(
    tmp_path, monkeypatch, failure
):
    import json

    import alerts.integration as integration
    import etl.daily_diff_flow as flow_module
    from alerts.channels import DeliveryResult, NotificationChannel

    db_path = _setup_db(tmp_path)
    with sqlite3.connect(db_path) as conn:
        ensure_alert_tables(conn)
        conn.execute(
            "INSERT INTO alert_rules(name,event_type,condition_json,channels,enabled,manager_id) VALUES (?,?,?,?,?,?)",
            (
                "Dispatch buy",
                "large_delta",
                '{"delta_type":"buy","value_usd_gt":350}',
                '["email","slack"]',
                1,
                1,
            ),
        )
    sends = []
    connections = []
    injected = False

    class Connection(sqlite3.Connection):
        armed = False

        def commit(self):
            if self.armed:
                self.armed = False
                raise RuntimeError("injected outcome commit failure")
            super().commit()

    def connect():
        conn = sqlite3.connect(db_path, factory=Connection)
        connections.append(conn)
        return conn

    class Channel(NotificationChannel):
        def __init__(self, name):
            self.channel_name = name

        async def deliver(self, alert):
            nonlocal injected
            with sqlite3.connect(db_path) as observer:
                assert (
                    observer.execute(
                        "SELECT COUNT(*) FROM daily_diffs WHERE report_date=?", ("2024-05-01",)
                    ).fetchone()[0]
                    > 0
                )
                assert (
                    observer.execute(
                        "SELECT COUNT(*) FROM alert_delivery_attempts WHERE channel=?",
                        (self.channel_name,),
                    ).fetchone()[0]
                    >= 1
                )
            sends.append(self.channel_name)
            if not injected and failure in ["channel", "outcome"]:
                injected = True
                if failure == "channel":
                    raise RuntimeError("injected channel failure")
                connections[-1].armed = True
            return DeliveryResult(channel=self.channel_name, success=True)

    channels = {
        name: Channel(name)
        for name in (["email"] if failure == "unconfigured" else ["email", "slack"])
    }
    monkeypatch.setattr(flow_module, "connect_db", connect)
    monkeypatch.setattr(integration, "build_configured_channels", lambda: channels)
    first = daily_diff_flow.fn(date="2024-05-01")
    daily_diff_flow.fn(date="2024-05-01")
    daily_diff_flow.fn(date="2024-05-01")
    assert sends.count("email") == 1
    assert sends.count("slack") == (0 if failure == "unconfigured" else 1)
    with sqlite3.connect(db_path) as observer:
        delivered = json.loads(
            observer.execute(
                "SELECT delivered_channels FROM alert_history WHERE event_type='large_delta'"
            ).fetchone()[0]
        )
        expected = (
            ["email", "slack"]
            if failure == "none"
            else ["email"] if failure == "unconfigured" else ["slack"]
        )
        assert delivered == expected
        assert observer.execute("SELECT COUNT(*) FROM daily_diffs").fetchone()[0] > 0
        if failure == "none":
            observer.execute("UPDATE holdings SET value_usd=500 WHERE cusip='CCC'")
    if failure == "none":
        daily_diff_flow.fn(date="2024-05-01")
        assert sends.count("email") == sends.count("slack") == 2
    if failure in ["channel", "outcome"]:
        assert first.warnings


def _dispatch_inventory_errors(event_types=None, overrides=None, inventory_override=None):
    """Audit declared coverage and real authored call edges; never execute ETL."""
    import ast
    import json

    from alerts.models import ALERT_EVENT_TYPES, UNSUPPORTED_EVENT_TYPES

    root = Path(__file__).resolve().parents[1]
    inventory = (
        inventory_override
        if inventory_override is not None
        else json.loads((root / "docs/reports/alert-dispatch-inventory.json").read_text())
    )
    errors = []
    overrides = overrides or {}
    trees = {}

    def tree(path):
        if path not in trees:
            trees[path] = ast.parse(overrides.get(path, (root / path).read_text()))
        return trees[path]

    def authored_nodes(node):
        yield node
        for child in ast.iter_child_nodes(node):
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Lambda)):
                continue
            yield from authored_nodes(child)

    def disabled(nodes):
        return any(
            isinstance(node, ast.Assign)
            and isinstance(node.value, ast.Constant)
            and node.value.value is False
            and any(isinstance(t, ast.Name) and t.id == "__test__" for t in node.targets)
            for node in nodes
        )

    def discoverable_tests(path):
        module = tree(path)
        if disabled(module.body):
            return set()
        names = set()
        for node in module.body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name.startswith(
                "test_"
            ):
                names.add(node.name)
            elif isinstance(node, ast.ClassDef) and node.name.startswith("Test"):
                methods = [
                    n for n in node.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
                ]
                if disabled(node.body) or any(n.name in {"__init__", "__new__"} for n in methods):
                    continue
                for method in methods:
                    if method.name.startswith("test_"):
                        names.update((method.name, node.name + "." + method.name))
        return names

    def literal(path, value, seen=frozenset()):
        if isinstance(value, ast.Constant) and isinstance(value.value, str):
            return value.value
        if not isinstance(value, ast.Name) or (path, value.id) in seen:
            return None
        seen = seen | {(path, value.id)}
        for node in tree(path).body:
            if isinstance(node, ast.Assign) and any(
                isinstance(t, ast.Name) and t.id == value.id for t in node.targets
            ):
                return literal(path, node.value, seen)
            if isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
                for alias in node.names:
                    if (alias.asname or alias.name) == value.id:
                        imported = node.module.replace(".", "/") + ".py"
                        if (root / imported).is_file():
                            return literal(imported, ast.Name(id=alias.name), seen)
        return None

    def emitted_types(site):
        emitted = set()
        for function in tree(site["path"]).body:
            if (
                isinstance(function, (ast.FunctionDef, ast.AsyncFunctionDef))
                and function.name == site["function"]
            ):
                for node in authored_nodes(function):
                    if (
                        isinstance(node, ast.Call)
                        and isinstance(node.func, ast.Name)
                        and node.func.id == "AlertEvent"
                    ):
                        for keyword in node.keywords:
                            if keyword.arg == "event_type":
                                emitted.add(literal(site["path"], keyword.value))
        return emitted

    declared = set(ALERT_EVENT_TYPES if event_types is None else event_types)
    if set(inventory["events"]) != declared:
        errors.append("event inventory differs from declared ALERT_EVENT_TYPES")
    checks = {
        n.name
        for n in tree("alerts/data_quality.py").body
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name.startswith("check_")
    }
    if set(inventory["standing_checks"]) != checks:
        errors.append("standing check inventory differs from declared check functions")

    def function_exists(path, function):
        return any(
            isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name == function
            for n in ast.walk(tree(path))
        )

    for name, row in [*inventory["events"].items(), *inventory["standing_checks"].items()]:
        if row.get("disposition") not in {"connected", "intentional-exception"}:
            errors.append(
                f"{name}: disposition {row.get('disposition')!r} needs human review; "
                "only connected or intentional-exception may pass the gate"
            )
        if not isinstance(row.get("owners"), list) or not row.get("evidence_kind"):
            errors.append(f"{name}: owners list and evidence_kind required")
        if row.get("disposition") == "connected":
            producer = row.get("producer")
            if not isinstance(producer, str) or not producer:
                errors.append(f"{name}: connected row requires a production producer")
            else:
                for segment in producer.split("; "):
                    path, _, function = segment.rpartition(":")
                    if not path or not function or not function_exists(path, function):
                        errors.append(f"{name}: producer does not resolve: {segment}")
                if name in inventory["standing_checks"] and producer != (
                    f"alerts/data_quality.py:{name}"
                ):
                    errors.append(f"{name}: standing-check producer must be its check function")
            for site in row.get("emission_sites") or []:
                if not function_exists(site["path"], site["function"]):
                    errors.append(f"{name}: emission site does not exist: {site}")
        if not row.get("replay") or not row.get("tests"):
            errors.append(f"{name}: replay contract and executable evidence required")
        for ref in row["tests"]:
            path, function = ref.split(":")
            if function not in discoverable_tests(path):
                errors.append(f"{name}: missing named test {ref}")
        if row["disposition"] == "intentional-exception":
            if name not in UNSUPPORTED_EVENT_TYPES or not row.get("exception"):
                errors.append(f"{name}: exception needs actual retirement and justification")
            if row.get("producer") is not None or row.get("edges"):
                errors.append(f"{name}: intentional exception must not claim a dispatch path")
            continue
        if row["disposition"] != "connected" or not row.get("edges"):
            errors.append(f"{name}: dispatch evidence or justified exception required")
        if name in inventory["events"]:
            sites = row.get("emission_sites", [])
            if not sites or any(name not in emitted_types(site) for site in sites):
                errors.append(f"{name}: producer does not emit the declared event")
        elif not any(
            edge["path"] == "alerts/data_quality.py"
            and edge["function"] == "evaluate_daily_quality"
            and edge["calls"] == name
            for edge in row["edges"]
        ):
            errors.append(f"{name}: missing evaluate_daily_quality binding")
        for edge in row["edges"]:
            functions = [
                n
                for n in ast.walk(tree(edge["path"]))
                if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
                and n.name == edge["function"]
            ]
            calls = set()
            for function in functions:
                for node in authored_nodes(function):
                    if isinstance(node, ast.Call):
                        target = node.func
                        if isinstance(target, ast.Attribute) and target.attr == "fn":
                            target = target.value
                        if isinstance(target, ast.Name):
                            calls.add(target.id)
                        elif isinstance(target, ast.Attribute):
                            calls.add(target.attr)
            if edge.get("callback") and not any(
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id == edge["calls"]
                and any(
                    k.arg == edge["callback"]
                    and isinstance(k.value, ast.Name)
                    and k.value.id == edge["callback_value"]
                    for k in node.keywords
                )
                for function in functions
                for node in authored_nodes(function)
            ):
                errors.append(f"{name}: disconnected callback {edge['callback']}")
            if edge["calls"] not in calls:
                errors.append(
                    f"{name}: disconnected {edge['path']}:{edge['function']} -> {edge['calls']}"
                )
    return errors


def test_alert_dispatch_inventory_covers_declared_events_and_standing_checks():
    assert _dispatch_inventory_errors() == []


def test_alert_dispatch_inventory_rejects_new_unconnected_declaration():
    from alerts.models import ALERT_EVENT_TYPES

    assert any(
        "ALERT_EVENT_TYPES" in error
        for error in _dispatch_inventory_errors((*ALERT_EVENT_TYPES, "unwired_future_event"))
    )


def test_alert_dispatch_inventory_detects_removed_real_dispatch_edge():
    root = Path(__file__).resolve().parents[1]
    source = (root / "etl/daily_diff_flow.py").read_text()
    broken = source.replace(
        "fire_alerts_for_event_sync(conn, _large_delta_event_from_diff(row))", "[]"
    )
    assert broken != source
    assert any(
        "large_delta: disconnected" in error
        for error in _dispatch_inventory_errors(overrides={"etl/daily_diff_flow.py": broken})
    )


def test_alert_dispatch_inventory_preserves_justified_missing_filing_retirement():
    from alerts.models import CREATABLE_EVENT_TYPES

    assert "missing_filing" not in CREATABLE_EVENT_TYPES
    assert _dispatch_inventory_errors() == []


def test_inventory_news_producer_retry_changes_delivery_identity(monkeypatch):
    """Evidence for review: producer retries lack a retained timestamp/key."""
    import asyncio
    from datetime import UTC, datetime, timedelta

    import alerts.models as models
    import etl.news_flow as news
    from alerts.db import _delivery_event_key
    from alerts.models import AlertRule, FiredAlert

    events = []
    clock = iter(
        [datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 1, 1, tzinfo=UTC) + timedelta(seconds=1)]
    )

    class Clock:
        @staticmethod
        def now(tz):
            return next(clock)

    async def capture(conn, event):
        events.append(event)
        return []

    monkeypatch.setattr(models, "datetime", Clock)
    monkeypatch.setattr(news, "fire_alerts_for_event", capture)
    items = [{"manager_id": 1, "headline": "Same retained item", "source": "fixture"}]
    asyncio.run(news.emit_news_spike_alerts(items, object()))
    asyncio.run(news.emit_news_spike_alerts(items, object()))
    assert events[0].payload == events[1].payload
    rule = AlertRule(
        rule_id=1,
        name="audit",
        event_type="news_spike",
        condition_json={},
        channels=["streamlit"],
        enabled=True,
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
        updated_at=datetime(2026, 1, 1, tzinfo=UTC),
    )
    keys = [
        _delivery_event_key(FiredAlert(rule=rule, event=event, channels=["streamlit"]))
        for event in events
    ]
    assert keys[0] != keys[1], "update the audit if the producer acquires a stable retry identity"


def test_alert_dispatch_inventory_rejects_unconnected_standing_check():
    root = Path(__file__).resolve().parents[1]
    path = "alerts/data_quality.py"
    source = (root / path).read_text() + "\ndef check_unwired_future():\n    return None\n"
    assert any(
        "standing check inventory" in error
        for error in _dispatch_inventory_errors(overrides={path: source})
    )


def _inventory_copy():
    import json

    return json.loads(
        (
            Path(__file__).resolve().parents[1] / "docs/reports/alert-dispatch-inventory.json"
        ).read_text()
    )


def test_inventory_rejects_nested_named_test_evidence():
    path = "tests/test_news_flow.py"
    root = Path(__file__).resolve().parents[1]
    source = (
        (root / path)
        .read_text()
        .replace(
            "async def test_emit_news_spike_alerts_groups_items_by_manager(",
            "async def renamed_uncollected_evidence(",
        )
    )
    source += "\ndef container():\n    async def test_emit_news_spike_alerts_groups_items_by_manager():\n        pass\n"
    assert any(
        "missing named test" in e for e in _dispatch_inventory_errors(overrides={path: source})
    )


def test_inventory_rejects_complete_unrelated_event_row():
    import copy

    from alerts.models import ALERT_EVENT_TYPES

    inventory = _inventory_copy()
    inventory["events"]["unwired_future_event"] = copy.deepcopy(inventory["events"]["news_spike"])
    errors = _dispatch_inventory_errors(
        (*ALERT_EVENT_TYPES, "unwired_future_event"), inventory_override=inventory
    )
    assert any("unwired_future_event: producer does not emit" in e for e in errors)


def test_inventory_rejects_copied_standing_evaluator_edge():
    import copy

    inventory = _inventory_copy()
    inventory["standing_checks"]["check_unwired_future"] = copy.deepcopy(
        inventory["standing_checks"]["check_news_freshness"]
    )
    path = "alerts/data_quality.py"
    source = (
        Path(__file__).resolve().parents[1] / path
    ).read_text() + "\ndef check_unwired_future():\n    return None\n"
    errors = _dispatch_inventory_errors(overrides={path: source}, inventory_override=inventory)
    assert any("check_unwired_future: missing evaluate_daily_quality binding" in e for e in errors)


def test_inventory_rejects_unknown_disposition_for_human_review():
    inventory = _inventory_copy()
    inventory["events"]["news_spike"]["disposition"] = "unknown"
    errors = _dispatch_inventory_errors(inventory_override=inventory)
    assert any("news_spike: disposition 'unknown' needs human review" in e for e in errors)


def test_inventory_rejects_unresolvable_producer():
    inventory = _inventory_copy()
    inventory["events"]["large_delta"]["producer"] = "etl/daily_diff_flow.py:no_such_producer"
    errors = _dispatch_inventory_errors(inventory_override=inventory)
    assert any("large_delta: producer does not resolve" in e for e in errors)


def test_inventory_rejects_exception_claiming_dispatch_path():
    inventory = _inventory_copy()
    inventory["events"]["missing_filing"]["producer"] = "etl/edgar_flow.py:fetch_and_store"
    errors = _dispatch_inventory_errors(inventory_override=inventory)
    assert any(
        "missing_filing: intentional exception must not claim a dispatch path" in e for e in errors
    )


def test_inventory_accepts_discoverable_test_class_evidence():
    path = "tests/test_news_flow.py"
    root = Path(__file__).resolve().parents[1]
    source = (
        (root / path)
        .read_text()
        .replace(
            "async def test_emit_news_spike_alerts_groups_items_by_manager(",
            "async def renamed_class_evidence(",
        )
    )
    source += "\nclass TestDeliveryEvidence:\n    async def test_emit_news_spike_alerts_groups_items_by_manager(self):\n        pass\n"
    assert _dispatch_inventory_errors(overrides={path: source}) == []
