import importlib
import json
import sqlite3

import etl.conviction_flow as conviction_module
from etl.conviction_flow import conviction_flow, detect_contrarian_signals, detect_crowded_trades


def _setup_db(tmp_path, manager_count: int) -> str:
    db_path = tmp_path / "conviction.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE managers (manager_id INTEGER PRIMARY KEY, name TEXT)")
    conn.execute(
        "CREATE TABLE filings ("
        "filing_id INTEGER PRIMARY KEY, "
        "manager_id INTEGER NOT NULL, "
        "type TEXT, "
        "period_end TEXT, "
        "filed_date TEXT, "
        "source TEXT)"
    )
    conn.execute(
        "CREATE TABLE holdings ("
        "holding_id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "filing_id INTEGER NOT NULL, "
        "cusip TEXT, "
        "name_of_issuer TEXT, "
        "shares INTEGER, "
        "value_usd REAL)"
    )
    conn.execute(
        "CREATE TABLE crowded_trades ("
        "crowd_id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "cusip TEXT NOT NULL, "
        "name_of_issuer TEXT, "
        "manager_count INTEGER NOT NULL, "
        "manager_ids TEXT NOT NULL, "
        "total_value_usd REAL, "
        "avg_conviction_pct REAL, "
        "max_conviction_pct REAL, "
        "report_date TEXT NOT NULL, "
        "computed_at TEXT DEFAULT CURRENT_TIMESTAMP, "
        "UNIQUE(cusip, report_date))"
    )
    conn.execute(
        "CREATE TABLE daily_diffs ("
        "diff_id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "manager_id INTEGER NOT NULL, "
        "report_date TEXT NOT NULL, "
        "cusip TEXT NOT NULL, "
        "name_of_issuer TEXT, "
        "delta_type TEXT NOT NULL, "
        "shares_prev INTEGER, "
        "shares_curr INTEGER, "
        "value_prev REAL, "
        "value_curr REAL)"
    )
    conn.execute(
        "CREATE TABLE contrarian_signals ("
        "signal_id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "manager_id INTEGER NOT NULL, "
        "cusip TEXT NOT NULL, "
        "name_of_issuer TEXT, "
        "direction TEXT NOT NULL, "
        "consensus_direction TEXT NOT NULL, "
        "manager_delta_shares INTEGER, "
        "manager_delta_value REAL, "
        "consensus_count INTEGER, "
        "report_date TEXT NOT NULL, "
        "detected_at TEXT DEFAULT CURRENT_TIMESTAMP, "
        "UNIQUE(manager_id, cusip, report_date))"
    )

    for manager_id in range(1, manager_count + 1):
        conn.execute(
            "INSERT INTO managers(manager_id, name) VALUES (?, ?)",
            (manager_id, f"Manager {manager_id}"),
        )
        old_filing_id = manager_id * 10
        new_filing_id = old_filing_id + 1
        conn.execute(
            "INSERT INTO filings(filing_id, manager_id, type, period_end, filed_date, source) "
            "VALUES (?, ?, '13F-HR', '2023-12-31', '2024-01-15', 'edgar')",
            (old_filing_id, manager_id),
        )
        conn.execute(
            "INSERT INTO filings(filing_id, manager_id, type, period_end, filed_date, source) "
            "VALUES (?, ?, '13F-HR', '2024-03-31', '2024-04-15', 'edgar')",
            (new_filing_id, manager_id),
        )

        conn.execute(
            "INSERT INTO holdings(filing_id, cusip, name_of_issuer, shares, value_usd) "
            "VALUES (?, 'OLD000001', 'Old Co', 1, 999)",
            (old_filing_id,),
        )

        aapl_value = 300 if manager_id == manager_count else 100
        other_value = 100
        conn.execute(
            "INSERT INTO holdings(filing_id, cusip, name_of_issuer, shares, value_usd) "
            "VALUES (?, '037833100', 'Apple Inc', 10, ?)",
            (new_filing_id, aapl_value),
        )
        conn.execute(
            "INSERT INTO holdings(filing_id, cusip, name_of_issuer, shares, value_usd) "
            "VALUES (?, ?, ?, 10, ?)",
            (new_filing_id, f"OTHER{manager_id:03d}", f"Other {manager_id}", other_value),
        )

    conn.commit()
    conn.close()
    return str(db_path)


def test_detect_crowded_trades_identifies_aapl_across_latest_filings(tmp_path):
    db_path = _setup_db(tmp_path, manager_count=5)
    conn = sqlite3.connect(db_path)

    inserted = detect_crowded_trades.fn("2024-05-01", min_managers=3, conn=conn)

    row = conn.execute(
        "SELECT cusip, name_of_issuer, manager_count, manager_ids, total_value_usd, "
        "avg_conviction_pct, max_conviction_pct "
        "FROM crowded_trades WHERE report_date = '2024-05-01'"
    ).fetchone()
    conn.close()

    assert inserted == 1
    assert row[0] == "037833100"
    assert row[1] == "Apple Inc"
    assert row[2] == 5
    assert json.loads(row[3]) == [1, 2, 3, 4, 5]
    assert row[4] == 700.0
    assert round(row[5], 2) == 55.0
    assert row[6] == 75.0


def test_detect_crowded_trades_applies_threshold_filter(tmp_path):
    db_path = _setup_db(tmp_path, manager_count=2)
    conn = sqlite3.connect(db_path)

    inserted = detect_crowded_trades.fn("2024-05-01", min_managers=3, conn=conn)
    count = conn.execute("SELECT COUNT(*) FROM crowded_trades").fetchone()[0]
    conn.close()

    assert inserted == 0
    assert count == 0


def test_detect_crowded_trades_is_idempotent_for_same_report_date(tmp_path):
    db_path = _setup_db(tmp_path, manager_count=3)
    conn = sqlite3.connect(db_path)

    first = detect_crowded_trades.fn("2024-05-01", min_managers=3, conn=conn)

    conn.execute(
        "INSERT INTO managers(manager_id, name) VALUES (?, ?)",
        (4, "Manager 4"),
    )
    conn.execute(
        "INSERT INTO filings(filing_id, manager_id, type, period_end, filed_date, source) "
        "VALUES (40, 4, '13F-HR', '2023-12-31', '2024-01-15', 'edgar')"
    )
    conn.execute(
        "INSERT INTO filings(filing_id, manager_id, type, period_end, filed_date, source) "
        "VALUES (41, 4, '13F-HR', '2024-03-31', '2024-04-15', 'edgar')"
    )
    conn.execute(
        "INSERT INTO holdings(filing_id, cusip, name_of_issuer, shares, value_usd) "
        "VALUES (40, 'OLD000001', 'Old Co', 1, 999)"
    )
    conn.execute(
        "INSERT INTO holdings(filing_id, cusip, name_of_issuer, shares, value_usd) "
        "VALUES (41, '037833100', 'Apple Inc', 10, 100)"
    )
    conn.execute(
        "INSERT INTO holdings(filing_id, cusip, name_of_issuer, shares, value_usd) "
        "VALUES (41, 'OTHER004', 'Other 4', 10, 100)"
    )
    conn.commit()

    second = detect_crowded_trades.fn("2024-05-01", min_managers=3, conn=conn)

    rows = conn.execute(
        "SELECT cusip, manager_count, manager_ids FROM crowded_trades "
        "WHERE report_date = '2024-05-01'"
    ).fetchall()
    conn.close()

    assert first == 1
    assert second == 1
    assert len(rows) == 1
    assert rows[0][0] == "037833100"
    assert rows[0][1] == 4
    assert json.loads(rows[0][2]) == [1, 2, 3, 4]


def test_detect_crowded_trades_uses_env_threshold_when_unspecified(tmp_path, monkeypatch):
    db_path = _setup_db(tmp_path, manager_count=3)
    conn = sqlite3.connect(db_path)
    monkeypatch.setenv("CROWDED_TRADE_MIN_MANAGERS", "4")

    inserted = detect_crowded_trades.fn("2024-05-01", conn=conn)
    count = conn.execute("SELECT COUNT(*) FROM crowded_trades").fetchone()[0]
    conn.close()

    assert inserted == 0
    assert count == 0


def test_detect_crowded_trades_ignores_future_filings(tmp_path):
    db_path = _setup_db(tmp_path, manager_count=5)
    conn = sqlite3.connect(db_path)

    conn.execute(
        "INSERT INTO filings(filing_id, manager_id, type, period_end, filed_date, source) "
        "VALUES (12, 1, '13F-HR', '2024-06-30', '2024-07-15', 'edgar')"
    )
    conn.execute(
        "INSERT INTO holdings(filing_id, cusip, name_of_issuer, shares, value_usd) "
        "VALUES (12, '594918104', 'Microsoft Corp', 10, 100)"
    )
    conn.commit()

    inserted = detect_crowded_trades.fn("2024-05-01", min_managers=3, conn=conn)
    row = conn.execute(
        "SELECT manager_count, manager_ids FROM crowded_trades "
        "WHERE report_date = '2024-05-01' AND cusip = '037833100'"
    ).fetchone()
    conn.close()

    assert inserted == 1
    assert row[0] == 5
    assert json.loads(row[1]) == [1, 2, 3, 4, 5]


def test_detect_contrarian_signals_flags_seller_against_buy_consensus(tmp_path):
    db_path = _setup_db(tmp_path, manager_count=5)
    conn = sqlite3.connect(db_path)
    report_date = "2024-05-01"

    for manager_id in (1, 2, 3, 4):
        conn.execute(
            "INSERT INTO daily_diffs(manager_id, report_date, cusip, name_of_issuer, delta_type, "
            "shares_prev, shares_curr, value_prev, value_curr) "
            "VALUES (?, ?, '88160R101', 'Tesla Inc', 'BUY', NULL, 10, NULL, 100)",
            (manager_id, report_date),
        )
    conn.execute(
        "INSERT INTO daily_diffs(manager_id, report_date, cusip, name_of_issuer, delta_type, "
        "shares_prev, shares_curr, value_prev, value_curr) "
        "VALUES (5, ?, '88160R101', 'Tesla Inc', 'SELL', 10, NULL, 100, NULL)",
        (report_date,),
    )
    conn.commit()

    inserted = detect_contrarian_signals.fn(report_date, conn=conn)
    row = conn.execute(
        "SELECT manager_id, cusip, direction, consensus_direction, "
        "manager_delta_shares, manager_delta_value, consensus_count "
        "FROM contrarian_signals WHERE report_date = ?",
        (report_date,),
    ).fetchone()
    conn.close()

    assert inserted == 1
    assert row[0] == 5
    assert row[1] == "88160R101"
    assert row[2] == "SELL"
    assert row[3] == "BUY"
    assert row[4] == -10
    assert row[5] == -100.0
    assert row[6] == 4


def test_dispatch_conviction_alerts_emits_events_for_detected_rows(tmp_path, monkeypatch):
    db_path = _setup_db(tmp_path, manager_count=5)
    conn = sqlite3.connect(db_path)
    report_date = "2024-05-01"

    detect_crowded_trades.fn(report_date, min_managers=3, conn=conn)
    for manager_id in (1, 2, 3, 4):
        conn.execute(
            "INSERT INTO daily_diffs(manager_id, report_date, cusip, name_of_issuer, delta_type, "
            "shares_prev, shares_curr, value_prev, value_curr) "
            "VALUES (?, ?, '88160R101', 'Tesla Inc', 'BUY', NULL, 10, NULL, 100)",
            (manager_id, report_date),
        )
    conn.execute(
        "INSERT INTO daily_diffs(manager_id, report_date, cusip, name_of_issuer, delta_type, "
        "shares_prev, shares_curr, value_prev, value_curr) "
        "VALUES (5, ?, '88160R101', 'Tesla Inc', 'SELL', 10, NULL, 100, NULL)",
        (report_date,),
    )
    conn.commit()
    detect_contrarian_signals.fn(report_date, conn=conn)
    conn.close()

    events = []

    def fake_connect_db():
        return sqlite3.connect(db_path)

    def fake_fire_alerts_for_event_sync(db_conn, event):
        _ = db_conn
        events.append(event)
        return [1]

    monkeypatch.setattr(conviction_module, "connect_db", fake_connect_db)
    monkeypatch.setattr(
        conviction_module, "fire_alerts_for_event_sync", fake_fire_alerts_for_event_sync
    )

    total = conviction_module.dispatch_conviction_alerts.fn(
        report_date, crowded_trades=1, contrarian_signals=1
    )

    assert total == 2
    assert [event.event_type for event in events] == [
        "crowded_trade_change",
        "contrarian_signal",
    ]


def test_detect_contrarian_signals_skips_split_consensus(tmp_path):
    db_path = _setup_db(tmp_path, manager_count=4)
    conn = sqlite3.connect(db_path)
    report_date = "2024-05-01"

    for manager_id in (1, 2):
        conn.execute(
            "INSERT INTO daily_diffs(manager_id, report_date, cusip, name_of_issuer, delta_type, "
            "shares_prev, shares_curr, value_prev, value_curr) "
            "VALUES (?, ?, '88160R101', 'Tesla Inc', 'BUY', NULL, 10, NULL, 100)",
            (manager_id, report_date),
        )
    for manager_id in (3, 4):
        conn.execute(
            "INSERT INTO daily_diffs(manager_id, report_date, cusip, name_of_issuer, delta_type, "
            "shares_prev, shares_curr, value_prev, value_curr) "
            "VALUES (?, ?, '88160R101', 'Tesla Inc', 'SELL', 10, NULL, 100, NULL)",
            (manager_id, report_date),
        )
    conn.commit()

    inserted = detect_contrarian_signals.fn(report_date, conn=conn)
    count = conn.execute("SELECT COUNT(*) FROM contrarian_signals").fetchone()[0]
    conn.close()

    assert inserted == 0
    assert count == 0


def test_detect_contrarian_signals_is_idempotent_for_same_report_date(tmp_path):
    db_path = _setup_db(tmp_path, manager_count=5)
    conn = sqlite3.connect(db_path)
    report_date = "2024-05-01"

    for manager_id in (1, 2, 3, 4):
        conn.execute(
            "INSERT INTO daily_diffs(manager_id, report_date, cusip, name_of_issuer, delta_type, "
            "shares_prev, shares_curr, value_prev, value_curr) "
            "VALUES (?, ?, '88160R101', 'Tesla Inc', 'BUY', NULL, 10, NULL, 100)",
            (manager_id, report_date),
        )
    conn.execute(
        "INSERT INTO daily_diffs(manager_id, report_date, cusip, name_of_issuer, delta_type, "
        "shares_prev, shares_curr, value_prev, value_curr) "
        "VALUES (5, ?, '88160R101', 'Tesla Inc', 'SELL', 10, NULL, 100, NULL)",
        (report_date,),
    )
    conn.commit()

    first = detect_contrarian_signals.fn(report_date, conn=conn)

    conn.execute(
        "INSERT INTO daily_diffs(manager_id, report_date, cusip, name_of_issuer, delta_type, "
        "shares_prev, shares_curr, value_prev, value_curr) "
        "VALUES (6, ?, '88160R101', 'Tesla Inc', 'SELL', 20, NULL, 200, NULL)",
        (report_date,),
    )
    conn.commit()

    second = detect_contrarian_signals.fn(report_date, conn=conn)
    rows = conn.execute(
        "SELECT manager_id, direction, consensus_count "
        "FROM contrarian_signals WHERE report_date = ? ORDER BY manager_id",
        (report_date,),
    ).fetchall()
    conn.close()

    assert first == 1
    assert second == 2
    assert rows == [(5, "SELL", 4), (6, "SELL", 4)]


def test_conviction_flow_runs_scoring_then_crowded_then_contrarian_then_alerts(monkeypatch):
    calls = []

    def _fake_score(report_date, conn=None):
        del conn
        calls.append(("score", report_date))
        return 10

    def _fake_crowded(report_date, min_managers=None, conn=None):
        del conn
        calls.append(("crowded", report_date, min_managers))
        return 3

    def _fake_contrarian(report_date, conn=None):
        del conn
        calls.append(("contrarian", report_date))
        return 1

    def _fake_alerts(report_date, crowded_trades, contrarian_signals):
        calls.append(("alerts", report_date, crowded_trades, contrarian_signals))
        return crowded_trades + contrarian_signals

    monkeypatch.setattr("etl.conviction_flow.score_conviction_positions.fn", _fake_score)
    monkeypatch.setattr("etl.conviction_flow.detect_crowded_trades.fn", _fake_crowded)
    monkeypatch.setattr("etl.conviction_flow.detect_contrarian_signals.fn", _fake_contrarian)
    monkeypatch.setattr("etl.conviction_flow.dispatch_conviction_alerts.fn", _fake_alerts)

    result = conviction_flow.fn(report_date="2024-05-01", min_managers=4)

    assert calls == [
        ("score", "2024-05-01"),
        ("crowded", "2024-05-01", 4),
        ("contrarian", "2024-05-01"),
        ("alerts", "2024-05-01", 3, 1),
    ]
    assert result == {
        "scored_positions": 10,
        "crowded_trades": 3,
        "contrarian_signals": 1,
        "alerts_dispatched": 4,
    }


def test_conviction_flow_deployment_uses_nightly_defaults(monkeypatch):
    monkeypatch.delenv("CONVICTION_FLOW_CRON", raising=False)
    monkeypatch.delenv("CONVICTION_FLOW_TIMEZONE", raising=False)
    monkeypatch.setenv("TZ", "UTC")
    module = importlib.reload(conviction_module)

    assert module.CONVICTION_FLOW_NIGHTLY_CRON == "0 2 * * *"
    assert module.CONVICTION_FLOW_TIMEZONE == "UTC"
    schedule = module.conviction_flow_deployment.schedules[0].schedule
    assert schedule.cron == "0 2 * * *"
    assert schedule.timezone == "UTC"


def test_conviction_flow_deployment_allows_env_overrides(monkeypatch):
    monkeypatch.setenv("CONVICTION_FLOW_CRON", "15 3 * * *")
    monkeypatch.setenv("CONVICTION_FLOW_TIMEZONE", "America/New_York")
    module = importlib.reload(conviction_module)

    assert module.CONVICTION_FLOW_NIGHTLY_CRON == "15 3 * * *"
    assert module.CONVICTION_FLOW_TIMEZONE == "America/New_York"
    schedule = module.conviction_flow_deployment.schedules[0].schedule
    assert schedule.cron == "15 3 * * *"
    assert schedule.timezone == "America/New_York"
