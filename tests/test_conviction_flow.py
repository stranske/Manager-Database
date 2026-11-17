import json
import sqlite3

import pytest

import etl.conviction_flow as conviction_flow
from alerts.channels import DeliveryResult, NotificationChannel, StreamlitChannel
from alerts.db import ensure_alert_tables
from alerts.integration import fire_alerts_for_event_sync

DEFAULT_REPORT_DATE = "2026-01-15"


def _create_base_tables(conn: sqlite3.Connection) -> None:
    conn.execute("""CREATE TABLE managers (
            manager_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            cik TEXT
        )""")
    conn.execute("""CREATE TABLE filings (
            filing_id INTEGER PRIMARY KEY,
            manager_id INTEGER NOT NULL,
            type TEXT NOT NULL,
            filed_date DATE,
            source TEXT NOT NULL
        )""")
    conn.execute("""CREATE TABLE holdings (
            holding_id INTEGER PRIMARY KEY AUTOINCREMENT,
            filing_id INTEGER NOT NULL,
            cusip TEXT,
            name_of_issuer TEXT,
            shares INTEGER,
            value_usd REAL
        )""")


def _create_signal_tables(conn: sqlite3.Connection) -> None:
    """Inputs the default entry point's detectors read after scoring."""
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
    conn.execute("""CREATE TABLE manager_similarity (
            manager_id_a INTEGER NOT NULL,
            manager_id_b INTEGER NOT NULL,
            jaccard REAL NOT NULL,
            CHECK (manager_id_a < manager_id_b)
        )""")
    ensure_alert_tables(conn)


class _RecordingEmailChannel(NotificationChannel):
    channel_name = "email"

    def __init__(self) -> None:
        self.deliveries: list[str] = []

    async def deliver(self, alert):
        self.deliveries.append(alert.event.event_type)
        return DeliveryResult("email", success=True)


def _patch_default_entry_env(monkeypatch, db_path, email: _RecordingEmailChannel) -> None:
    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.delenv("CROWDED_TRADE_MIN_MANAGERS", raising=False)
    monkeypatch.setattr(conviction_flow, "_default_report_date", lambda: DEFAULT_REPORT_DATE)

    def dispatch_with_fake_channels(conn, event):
        return fire_alerts_for_event_sync(
            conn,
            event,
            channels={"email": email, "streamlit": StreamlitChannel()},
        )

    monkeypatch.setattr(conviction_flow, "fire_alerts_for_event_sync", dispatch_with_fake_channels)


def _seed_manager_and_filing(
    conn: sqlite3.Connection,
    *,
    manager_id: int,
    filing_id: int,
    filed_date: str,
    cik: str = "0000000000",
) -> None:
    conn.execute(
        "INSERT INTO managers(manager_id, name, cik) VALUES (?, ?, ?)",
        (manager_id, f"Manager {manager_id}", cik),
    )
    conn.execute(
        "INSERT INTO filings(filing_id, manager_id, type, filed_date, source) VALUES (?, ?, ?, ?, ?)",
        (filing_id, manager_id, "13F-HR", filed_date, "sec"),
    )


def test_similarity_crowding_counts_only_connected_managers():
    conn = sqlite3.connect(":memory:")
    conn.execute(
        "CREATE TABLE manager_similarity (manager_id_a INTEGER, manager_id_b INTEGER, jaccard REAL)"
    )
    conn.executemany(
        "INSERT INTO manager_similarity VALUES (?, ?, ?)",
        [(1, 2, 0.8), (1, 3, 0.7), (2, 3, 0.9), (3, 4, 0.2), (1, 4, 0.5)],
    )

    assert conviction_flow._similar_manager_ids(conn, [1, 2, 3, 4], 0.5) == [1, 2, 3, 4]
    assert conviction_flow._similar_manager_ids(conn, [1, 2, 3, 4], 0.8) == [1, 2, 3]
    assert conviction_flow._similar_manager_ids(conn, [1, 2, 3, 4], 0.95) == []


def test_compute_conviction_scores_known_portfolio(tmp_path):
    db_path = tmp_path / "conviction.db"
    conn = sqlite3.connect(db_path)
    _create_base_tables(conn)
    conviction_flow._ensure_conviction_scores_table(conn)

    _seed_manager_and_filing(conn, manager_id=1, filing_id=10, filed_date="2025-12-31")
    conn.executemany(
        "INSERT INTO holdings(filing_id, cusip, name_of_issuer, shares, value_usd) VALUES (?, ?, ?, ?, ?)",
        [
            (10, "AAA111", "Alpha Corp", 1000, 600000.0),
            (10, "BBB222", "Beta Corp", 500, 300000.0),
            (10, "CCC333", "Gamma Corp", 250, 100000.0),
        ],
    )

    inserted = conviction_flow.compute_conviction_scores.fn(10, conn)
    conn.commit()

    rows = conn.execute(
        "SELECT cusip, conviction_pct, portfolio_weight FROM conviction_scores ORDER BY cusip"
    ).fetchall()

    assert inserted == 3
    assert rows[0][0] == "AAA111"
    assert rows[0][1] == pytest.approx(60.0, rel=1e-4)
    assert rows[0][2] == pytest.approx(0.6, rel=1e-6)
    assert rows[1][1] == pytest.approx(30.0, rel=1e-4)
    assert rows[1][2] == pytest.approx(0.3, rel=1e-6)
    assert rows[2][1] == pytest.approx(10.0, rel=1e-4)
    assert rows[2][2] == pytest.approx(0.1, rel=1e-6)


@pytest.mark.parametrize(
    "holdings, expected_pct, expected_weight",
    [
        ([("ZERO01", "Zero", 10, 0.0)], 0.0, 0.0),
        ([("SOLO01", "Solo", 20, 250000.0)], 100.0, 1.0),
    ],
)
def test_compute_conviction_scores_edge_cases(tmp_path, holdings, expected_pct, expected_weight):
    db_path = tmp_path / "edges.db"
    conn = sqlite3.connect(db_path)
    _create_base_tables(conn)
    conviction_flow._ensure_conviction_scores_table(conn)

    _seed_manager_and_filing(conn, manager_id=2, filing_id=20, filed_date="2025-12-31")
    conn.executemany(
        "INSERT INTO holdings(filing_id, cusip, name_of_issuer, shares, value_usd) VALUES (?, ?, ?, ?, ?)",
        [(20, cusip, issuer, shares, value_usd) for cusip, issuer, shares, value_usd in holdings],
    )

    conviction_flow.compute_conviction_scores.fn(20, conn)
    row = conn.execute(
        "SELECT conviction_pct, portfolio_weight FROM conviction_scores WHERE filing_id = ?",
        (20,),
    ).fetchone()

    assert row is not None
    assert row[0] == pytest.approx(expected_pct, rel=1e-4)
    assert row[1] == pytest.approx(expected_weight, rel=1e-6)


def test_compute_conviction_scores_rejects_non_finite_values(tmp_path):
    db_path = tmp_path / "nonfinite.db"
    conn = sqlite3.connect(db_path)
    _create_base_tables(conn)
    conviction_flow._ensure_conviction_scores_table(conn)

    _seed_manager_and_filing(conn, manager_id=4, filing_id=40, filed_date="2025-12-31")
    conn.executemany(
        "INSERT INTO holdings(filing_id, cusip, name_of_issuer, shares, value_usd) VALUES (?, ?, ?, ?, ?)",
        [
            (40, "GOOD01", "Good Corp", 10, 100.0),
            (40, "BAD001", "Bad Corp", 10, "nan"),
        ],
    )

    with pytest.raises(ValueError, match="non-finite numeric value"):
        conviction_flow.compute_conviction_scores.fn(40, conn)

    assert conn.execute("SELECT COUNT(*) FROM conviction_scores").fetchone() == (0,)


def test_compute_conviction_scores_rejects_missing_values(tmp_path):
    db_path = tmp_path / "missing.db"
    conn = sqlite3.connect(db_path)
    try:
        _create_base_tables(conn)
        conviction_flow._ensure_conviction_scores_table(conn)

        _seed_manager_and_filing(conn, manager_id=4, filing_id=41, filed_date="2025-12-31")
        conn.executemany(
            "INSERT INTO holdings(filing_id, cusip, name_of_issuer, shares, value_usd) VALUES (?, ?, ?, ?, ?)",
            [
                (41, "GOOD01", "Good Corp", 10, 100.0),
                (41, "MISS01", "Missing Corp", 10, None),
            ],
        )

        with pytest.raises(ValueError, match="numeric value is required"):
            conviction_flow.compute_conviction_scores.fn(41, conn)

        assert conn.execute("SELECT COUNT(*) FROM conviction_scores").fetchone() == (0,)
    finally:
        conn.close()


def test_compute_conviction_scores_rejects_overflowed_total_value(tmp_path):
    db_path = tmp_path / "overflow.db"
    conn = sqlite3.connect(db_path)
    try:
        _create_base_tables(conn)
        conviction_flow._ensure_conviction_scores_table(conn)

        _seed_manager_and_filing(conn, manager_id=4, filing_id=42, filed_date="2025-12-31")
        conn.executemany(
            "INSERT INTO holdings(filing_id, cusip, name_of_issuer, shares, value_usd) VALUES (?, ?, ?, ?, ?)",
            [
                (42, "BIG001", "Big Corp", 10, 1e308),
                (42, "BIG002", "Bigger Corp", 10, 1e308),
            ],
        )

        with pytest.raises(ValueError, match="total holding value must be finite"):
            conviction_flow.compute_conviction_scores.fn(42, conn)

        assert conn.execute("SELECT COUNT(*) FROM conviction_scores").fetchone() == (0,)
    finally:
        conn.close()


def test_compute_conviction_scores_upsert_is_idempotent(tmp_path):
    db_path = tmp_path / "upsert.db"
    conn = sqlite3.connect(db_path)
    _create_base_tables(conn)
    conviction_flow._ensure_conviction_scores_table(conn)

    _seed_manager_and_filing(conn, manager_id=3, filing_id=30, filed_date="2025-12-31")
    conn.execute(
        "INSERT INTO holdings(filing_id, cusip, name_of_issuer, shares, value_usd) VALUES (?, ?, ?, ?, ?)",
        (30, "UPS001", "Upsert Corp", 10, 100.0),
    )

    conviction_flow.compute_conviction_scores.fn(30, conn)
    conn.execute("DELETE FROM holdings WHERE filing_id = ?", (30,))
    conn.execute(
        "INSERT INTO holdings(filing_id, cusip, name_of_issuer, shares, value_usd) VALUES (?, ?, ?, ?, ?)",
        (30, "UPS001", "Upsert Corp", 20, 200.0),
    )
    conviction_flow.compute_conviction_scores.fn(30, conn)

    rows = conn.execute(
        "SELECT cusip, shares, value_usd, conviction_pct, portfolio_weight FROM conviction_scores"
    ).fetchall()

    assert len(rows) == 1
    assert rows[0][0] == "UPS001"
    assert rows[0][1] == 20
    assert rows[0][2] == pytest.approx(200.0)
    assert rows[0][3] == pytest.approx(100.0)
    assert rows[0][4] == pytest.approx(1.0)


def test_score_all_latest_filings_only_scores_latest_per_manager(tmp_path):
    db_path = tmp_path / "latest.db"
    conn = sqlite3.connect(db_path)
    _create_base_tables(conn)
    conviction_flow._ensure_conviction_scores_table(conn)

    _seed_manager_and_filing(conn, manager_id=1, filing_id=101, filed_date="2025-06-30")
    conn.execute(
        "INSERT INTO filings(filing_id, manager_id, type, filed_date, source) VALUES (?, ?, ?, ?, ?)",
        (102, 1, "13F-HR", "2025-12-31", "sec"),
    )
    _seed_manager_and_filing(conn, manager_id=2, filing_id=201, filed_date="2025-11-30")

    conn.executemany(
        "INSERT INTO holdings(filing_id, cusip, name_of_issuer, shares, value_usd) VALUES (?, ?, ?, ?, ?)",
        [
            (101, "OLD001", "Old Position", 10, 1000.0),
            (102, "NEW001", "New Position", 20, 2000.0),
            (201, "M2P01", "Manager Two", 30, 3000.0),
        ],
    )

    result = conviction_flow.score_all_latest_filings.fn(conn)
    filing_ids = [
        row[0]
        for row in conn.execute(
            "SELECT DISTINCT filing_id FROM conviction_scores ORDER BY filing_id"
        )
    ]

    assert result == {"filings_scored": 2, "scores_computed": 2, "filings_failed": 0}
    assert filing_ids == [102, 201]


def test_score_all_latest_filings_skips_a_bad_filing_without_losing_others(tmp_path):
    """THE BUG: one manager's incomplete filing used to abort scoring for every manager.

    `compute_conviction_scores` deliberately REJECTS a single filing outright when a holding
    is missing its value_usd (see test_compute_conviction_scores_rejects_missing_values) -
    value_usd is nullable in the schema, and real 13F rows omit it for share classes or
    warrants with no reported dollar value. That per-filing rejection is fine on its own, but
    `score_all_latest_filings` loops over every manager's latest filing with no isolation
    between them, so manager B's one bad row used to raise straight out of the loop and deny
    manager A and manager C - whose filings were perfectly good - any conviction scores at
    all for that run.
    """
    db_path = tmp_path / "partial_failure.db"
    conn = sqlite3.connect(db_path)
    try:
        _create_base_tables(conn)
        conviction_flow._ensure_conviction_scores_table(conn)

        _seed_manager_and_filing(conn, manager_id=1, filing_id=301, filed_date="2025-12-31")
        _seed_manager_and_filing(conn, manager_id=2, filing_id=302, filed_date="2025-12-31")
        _seed_manager_and_filing(conn, manager_id=3, filing_id=303, filed_date="2025-12-31")
        conn.executemany(
            "INSERT INTO holdings(filing_id, cusip, name_of_issuer, shares, value_usd) "
            "VALUES (?, ?, ?, ?, ?)",
            [
                (301, "GOOD001", "Good Corp A", 10, 1000.0),
                (302, "MISS001", "Missing-Value Corp", 10, None),
                (303, "GOOD003", "Good Corp C", 10, 500.0),
            ],
        )
        conn.commit()

        result = conviction_flow.score_all_latest_filings.fn(conn)

        assert result == {"filings_scored": 2, "scores_computed": 2, "filings_failed": 1}
        scored_filings = {
            row[0] for row in conn.execute("SELECT DISTINCT filing_id FROM conviction_scores")
        }
        assert scored_filings == {301, 303}
    finally:
        conn.close()


def test_conviction_flow_logs_api_usage_and_supports_elliott_sample(monkeypatch, tmp_path):
    db_path = tmp_path / "flow.db"
    email = _RecordingEmailChannel()
    _patch_default_entry_env(monkeypatch, db_path, email)

    conn = sqlite3.connect(db_path)
    _create_base_tables(conn)
    _create_signal_tables(conn)
    _seed_manager_and_filing(
        conn,
        manager_id=7,
        filing_id=700,
        filed_date="2025-12-31",
        cik="0001791786",
    )
    conn.executemany(
        "INSERT INTO holdings(filing_id, cusip, name_of_issuer, shares, value_usd) VALUES (?, ?, ?, ?, ?)",
        [
            (700, "038222105", "Applied Materials", 10000, 520000.0),
            (700, "594918104", "Microsoft", 3000, 310000.0),
            (700, "64110L106", "Netflix", 1500, 170000.0),
        ],
    )
    conn.commit()
    conn.close()

    result = conviction_flow.conviction_flow.fn()

    verify_conn = sqlite3.connect(db_path)
    scores = verify_conn.execute(
        "SELECT conviction_pct FROM conviction_scores WHERE filing_id = ? ORDER BY conviction_pct DESC",
        (700,),
    ).fetchall()
    usage = verify_conn.execute(
        "SELECT source, endpoint, status FROM api_usage ORDER BY id DESC LIMIT 1"
    ).fetchone()
    verify_conn.close()

    total_pct = sum(float(row[0]) for row in scores)

    # A single manager cannot form a crowd and there are no daily diffs, so the detectors
    # and dispatch run but find nothing.
    assert result == {
        "filings_scored": 1,
        "scores_computed": 3,
        "filings_failed": 0,
        "crowded_trades": 0,
        "contrarian_signals": 0,
        "alerts_dispatched": 0,
    }
    assert email.deliveries == []
    assert len(scores) == 3
    assert total_pct == pytest.approx(100.0, rel=1e-4)
    assert usage == ("prefect", "flow:conviction-scoring", 200)


def test_conviction_flow_nightly_run_survives_one_managers_bad_filing(monkeypatch, tmp_path):
    """The consequence, asserted at the SAME entry point the nightly cron actually calls.

    Before the fix, `conviction_flow()` wrapped `score_all_latest_filings.fn(db)` in a bare
    try/except that only set status=500 and re-raised - it could not stop the exception from
    propagating. One manager with an incomplete filing (a null value_usd) meant the ENTIRE
    scheduled nightly run raised, recording a status-500 api_usage row and computing zero
    conviction scores for every manager that night, not only the one with bad data.
    """
    db_path = tmp_path / "nightly.db"
    email = _RecordingEmailChannel()
    _patch_default_entry_env(monkeypatch, db_path, email)

    conn = sqlite3.connect(db_path)
    _create_base_tables(conn)
    _create_signal_tables(conn)
    _seed_manager_and_filing(conn, manager_id=8, filing_id=800, filed_date="2025-12-31")
    _seed_manager_and_filing(conn, manager_id=9, filing_id=900, filed_date="2025-12-31")
    conn.executemany(
        "INSERT INTO holdings(filing_id, cusip, name_of_issuer, shares, value_usd) VALUES (?, ?, ?, ?, ?)",
        [
            (800, "GOOD800", "Reliable Corp", 10, 1000.0),
            (900, "BAD0900", "Incomplete Corp", 10, None),
        ],
    )
    conn.commit()
    conn.close()

    result = conviction_flow.conviction_flow.fn()

    verify_conn = sqlite3.connect(db_path)
    good_manager_scores = verify_conn.execute(
        "SELECT COUNT(*) FROM conviction_scores WHERE filing_id = 800"
    ).fetchone()[0]
    usage = verify_conn.execute(
        "SELECT source, endpoint, status FROM api_usage ORDER BY id DESC LIMIT 1"
    ).fetchone()
    verify_conn.close()

    assert result == {
        "filings_scored": 1,
        "scores_computed": 1,
        "filings_failed": 1,
        "crowded_trades": 0,
        "contrarian_signals": 0,
        "alerts_dispatched": 0,
    }
    assert good_manager_scores == 1
    # The money assertion: the nightly flow reports success, not the 500 it used to record
    # when any single manager's data was incomplete.
    assert usage == ("prefect", "flow:conviction-scoring", 200)


def _seed_default_entry_signal_inputs(conn: sqlite3.Connection) -> None:
    """Five managers share one CUSIP; four buy a second CUSIP that manager 5 sells."""
    for manager_id in range(1, 6):
        _seed_manager_and_filing(
            conn,
            manager_id=manager_id,
            filing_id=manager_id * 100,
            filed_date="2025-12-31",
        )
        conn.executemany(
            "INSERT INTO holdings(filing_id, cusip, name_of_issuer, shares, value_usd) "
            "VALUES (?, ?, ?, ?, ?)",
            [
                (manager_id * 100, "037833100", "Apple Inc", 10, 100.0),
                (manager_id * 100, f"OTHER{manager_id:03d}", f"Other {manager_id}", 10, 100.0),
            ],
        )
    conn.executemany(
        "INSERT INTO manager_similarity(manager_id_a, manager_id_b, jaccard) VALUES (?, ?, ?)",
        [(1, 2, 0.9), (1, 3, 0.9), (1, 4, 0.9), (1, 5, 0.9)],
    )
    for manager_id in (1, 2, 3, 4):
        conn.execute(
            "INSERT INTO daily_diffs(manager_id, report_date, cusip, name_of_issuer, delta_type, "
            "shares_prev, shares_curr, value_prev, value_curr) "
            "VALUES (?, ?, '88160R101', 'Tesla Inc', 'BUY', NULL, 10, NULL, 100)",
            (manager_id, DEFAULT_REPORT_DATE),
        )
    conn.execute(
        "INSERT INTO daily_diffs(manager_id, report_date, cusip, name_of_issuer, delta_type, "
        "shares_prev, shares_curr, value_prev, value_curr) "
        "VALUES (5, ?, '88160R101', 'Tesla Inc', 'SELL', 10, NULL, 100, NULL)",
        (DEFAULT_REPORT_DATE,),
    )
    conn.execute(
        "INSERT INTO alert_rules(name, event_type, condition_json, channels, enabled) "
        "VALUES (?, ?, ?, ?, ?)",
        (
            "Crowded trade",
            "crowded_trade_change",
            json.dumps({"similar_manager_count_gte": 3}),
            '["email"]',
            1,
        ),
    )
    conn.execute(
        "INSERT INTO alert_rules(name, event_type, condition_json, channels, enabled, manager_id) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        ("Contrarian signal", "contrarian_signal", "{}", '["email"]', 1, 5),
    )


def test_default_entry_point_runs_detection_and_dispatch(monkeypatch, tmp_path):
    """The nightly deployment calls conviction_flow() with no parameters.

    That default branch used to return straight after scoring, so the scheduled run never
    wrote crowded/contrarian signal rows and never dispatched an alert. A rerun for the same
    report date must reuse #1916's stable delivery identity and write no new history row.
    """
    db_path = tmp_path / "default_entry.db"
    email = _RecordingEmailChannel()
    _patch_default_entry_env(monkeypatch, db_path, email)

    conn = sqlite3.connect(db_path)
    _create_base_tables(conn)
    _create_signal_tables(conn)
    _seed_default_entry_signal_inputs(conn)
    conn.commit()
    conn.close()

    first = conviction_flow.conviction_flow.fn()

    def _counts() -> tuple[list[tuple], list[tuple], list[tuple], tuple]:
        verify_conn = sqlite3.connect(db_path)
        try:
            crowded = verify_conn.execute(
                "SELECT cusip, manager_count, report_date FROM crowded_trades ORDER BY cusip"
            ).fetchall()
            contrarian = verify_conn.execute(
                "SELECT manager_id, cusip, direction, consensus_direction, report_date "
                "FROM contrarian_signals"
            ).fetchall()
            history = verify_conn.execute(
                "SELECT r.name, a.channel FROM alert_history h "
                "JOIN alert_rules r ON r.rule_id = h.rule_id "
                "JOIN alert_delivery_attempts a ON a.alert_id = h.alert_id "
                "ORDER BY r.name, a.channel"
            ).fetchall()
            usage = verify_conn.execute(
                "SELECT source, endpoint, status FROM api_usage ORDER BY id DESC LIMIT 1"
            ).fetchone()
        finally:
            verify_conn.close()
        return crowded, contrarian, history, usage

    crowded, contrarian, history, usage = _counts()
    history_count_after_first = len(history)

    assert first == {
        "filings_scored": 5,
        "scores_computed": 10,
        "filings_failed": 0,
        "crowded_trades": 1,
        "contrarian_signals": 1,
        "alerts_dispatched": 2,
    }
    assert crowded == [("037833100", 5, DEFAULT_REPORT_DATE)]
    assert contrarian == [(5, "88160R101", "SELL", "BUY", DEFAULT_REPORT_DATE)]
    # One alert_history row per rule/channel.
    assert history == [("Contrarian signal", "email"), ("Crowded trade", "email")]
    assert email.deliveries == ["crowded_trade_change", "contrarian_signal"]
    assert usage == ("prefect", "flow:conviction-scoring", 200)

    conviction_flow.conviction_flow.fn()

    _, _, history_after_rerun, usage_after_rerun = _counts()
    assert len(history_after_rerun) == history_count_after_first
    assert history_after_rerun == history
    assert email.deliveries == ["crowded_trade_change", "contrarian_signal"]
    assert usage_after_rerun == ("prefect", "flow:conviction-scoring", 200)


def test_conviction_deployment_nightly_utc_schedule():
    schedule = conviction_flow.conviction_deployment.schedules[0].schedule
    assert schedule.cron == "0 2 * * *"
    assert schedule.timezone == "UTC"
