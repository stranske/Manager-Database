from __future__ import annotations

import sqlite3
from datetime import date

from etl.activism_campaign_flow import materialize_activism_campaigns


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE managers (manager_id INTEGER PRIMARY KEY, name TEXT)")
    conn.execute(
        "CREATE TABLE activism_filings (filing_id INTEGER PRIMARY KEY, manager_id INTEGER, "
        "filing_type TEXT, subject_company TEXT, subject_cusip TEXT, ownership_pct REAL, "
        "filed_date TEXT, url TEXT)"
    )
    conn.execute(
        "CREATE TABLE activism_events (event_id INTEGER PRIMARY KEY, filing_id INTEGER, "
        "event_type TEXT, detected_at TEXT)"
    )
    conn.execute("INSERT INTO managers VALUES (1, 'Activist')")
    return conn


def test_materialize_campaigns_groups_amendments_by_manager_and_target() -> None:
    conn = _conn()
    conn.executemany(
        "INSERT INTO activism_filings VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (1, 1, "SC 13D", "Example Corp", "123456789", 5.1, "2024-05-01", "https://sec/1"),
            (2, 1, "SC 13D/A", "Example Corp", "123456789", 9.4, "2024-05-03", "https://sec/2"),
            (3, 1, "SC 13G", "Other Corp", "987654321", 4.0, "2024-05-02", "https://sec/3"),
        ],
    )
    conn.execute(
        "INSERT INTO activism_events VALUES (10, 2, 'threshold_crossing', '2024-05-03T09:00:00')"
    )

    summary = materialize_activism_campaigns(conn)

    assert summary.campaigns_written == 2
    campaigns = conn.execute(
        "SELECT target_identifier, status, filing_count, peak_ownership_pct FROM activism_campaigns "
        "ORDER BY target_identifier"
    ).fetchall()
    assert campaigns == [("123456789", "active", 2, 9.4), ("987654321", "monitoring", 1, 4.0)]
    timeline = conn.execute(
        "SELECT event_date, event_type, form_type FROM activism_campaign_timeline "
        "WHERE campaign_id = (SELECT campaign_id FROM activism_campaigns WHERE target_identifier = '123456789') "
        "ORDER BY event_date, form_type, event_id"
    ).fetchall()
    assert timeline == [
        ("2024-05-01", "initial_filing", "SC 13D"),
        ("2024-05-03", "threshold_crossing", "SC 13D/A"),
    ]


def test_materialize_campaigns_orders_group_members_by_filing_date() -> None:
    """Case-only cusip differences group together but sort apart in raw SQL order."""
    conn = _conn()
    conn.executemany(
        "INSERT INTO activism_filings VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (1, 1, "SC 13D", "Tesla, Inc.", "88160r101", 5.1, "2024-05-01", "https://sec/1"),
            (2, 1, "SC 13G", "Tesla, Inc.", "88160R101", 3.2, "2024-05-05", "https://sec/2"),
        ],
    )
    conn.executemany(
        "INSERT INTO activism_events VALUES (?, ?, ?, ?)",
        [
            (10, 2, "stake_decrease", "2024-05-05T09:00:00"),
            (11, 1, "initial_stake", "2024-06-01T09:00:00"),
        ],
    )

    materialize_activism_campaigns(conn)

    row = conn.execute(
        "SELECT first_filed, last_filed, status, latest_ownership_pct, latest_event_type "
        "FROM activism_campaigns"
    ).fetchone()
    assert row == ("2024-05-01", "2024-05-05", "monitoring", 3.2, "initial_stake")


def test_materialize_campaigns_dates_timeline_by_filing_not_detection() -> None:
    conn = _conn()
    conn.execute(
        "INSERT INTO activism_filings VALUES "
        "(1, 1, 'SC 13D', 'Example Corp', '123456789', 5.1, '2024-05-01', 'https://sec/1')"
    )
    conn.execute(
        "INSERT INTO activism_events VALUES (10, 1, 'initial_stake', '2026-01-15T09:00:00')"
    )

    materialize_activism_campaigns(conn)

    assert conn.execute("SELECT event_date FROM activism_campaign_timeline").fetchall() == [
        ("2024-05-01",)
    ]


def test_materialize_campaigns_rerun_replaces_filing_only_timeline_rows() -> None:
    conn = _conn()
    conn.execute(
        "INSERT INTO activism_filings VALUES "
        "(1, 1, 'SC 13D', 'Example Corp', '123456789', 5.1, '2024-05-01', 'https://sec/1')"
    )

    first = materialize_activism_campaigns(conn)
    assert first.timeline_rows_written == 1

    # The filing gains an event only after its first materialization.
    conn.execute(
        "INSERT INTO activism_events VALUES (10, 1, 'initial_stake', '2024-05-02T09:00:00')"
    )
    second = materialize_activism_campaigns(conn)

    assert second.timeline_rows_written == 1
    rows = conn.execute("SELECT event_id, event_type FROM activism_campaign_timeline").fetchall()
    assert rows == [(10, "initial_stake")]

    # A third pass with no source change must stay append-safe.
    materialize_activism_campaigns(conn)
    assert conn.execute("SELECT COUNT(*) FROM activism_campaign_timeline").fetchone() == (1,)


def test_materialize_campaigns_since_recomputes_full_campaign_history() -> None:
    conn = _conn()
    conn.executemany(
        "INSERT INTO activism_filings VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (1, 1, "SC 13D", "Example Corp", "123456789", 5.1, "2024-05-01", "https://sec/1"),
            (2, 1, "SC 13D/A", "Example Corp", "123456789", 9.4, "2024-05-03", "https://sec/2"),
            (3, 2, "SC 13G", "Other Corp", "987654321", 4.0, "2024-01-02", "https://sec/3"),
        ],
    )
    materialize_activism_campaigns(conn)

    summary = materialize_activism_campaigns(conn, since=date(2024, 5, 3))

    # Only the campaign touched since the cutoff is refreshed ...
    assert summary.campaigns_written == 1
    # ... and it keeps the aggregates of its full history, not of the filtered slice.
    assert conn.execute(
        "SELECT filing_count, first_filed, peak_ownership_pct FROM activism_campaigns "
        "WHERE target_identifier = '123456789'"
    ).fetchone() == (2, "2024-05-01", 9.4)


def test_materialize_campaigns_keeps_missing_cusip_names_separate() -> None:
    conn = _conn()
    conn.executemany(
        "INSERT INTO activism_filings VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (1, 1, "SC 13D", "Alpha Holdings", None, 5.1, "2024-05-01", "https://sec/1"),
            (2, 1, "SC 13D", "Beta Holdings", None, 5.1, "2024-05-01", "https://sec/2"),
        ],
    )

    materialize_activism_campaigns(conn)

    rows = conn.execute(
        "SELECT target_identifier, data_quality_flags FROM activism_campaigns ORDER BY target_identifier"
    ).fetchall()
    assert rows == [
        ("name:ALPHA HOLDINGS", '["missing_cusip"]'),
        ("name:BETA HOLDINGS", '["missing_cusip"]'),
    ]


class _Prices:
    def __init__(self, prices: dict[date, float]) -> None:
        self.prices = prices

    def close_on_or_before(self, ticker: str | None, on: date) -> float | None:
        assert ticker == "ACME"
        available = [day for day in self.prices if day <= on]
        return self.prices[max(available)] if available else None


def test_materialize_campaigns_computes_window_return_and_holding_period() -> None:
    conn = _conn()
    conn.execute("CREATE TABLE identifier_resolution_cache (cusip TEXT PRIMARY KEY, ticker TEXT)")
    conn.execute("INSERT INTO identifier_resolution_cache VALUES ('123456789', 'acme')")
    conn.executemany(
        "INSERT INTO activism_filings VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (1, 1, "SC 13D", "Example Corp", "123456789", 5.1, "2024-05-01", "https://sec/1"),
            (2, 1, "SC 13D/A", "Example Corp", "123456789", 6.2, "2024-05-11", "https://sec/2"),
        ],
    )

    materialize_activism_campaigns(
        conn,
        price_adapter=_Prices({date(2024, 5, 1): 100.0, date(2024, 5, 10): 120.0}),
    )

    assert conn.execute(
        "SELECT target_ticker, window_return, holding_period_days FROM activism_campaigns"
    ).fetchone() == ("ACME", 0.2, 10)


def test_materialize_campaigns_missing_price_keeps_campaign_without_return() -> None:
    conn = _conn()
    conn.execute("CREATE TABLE identifier_resolution_cache (cusip TEXT PRIMARY KEY, ticker TEXT)")
    conn.execute("INSERT INTO identifier_resolution_cache VALUES ('123456789', 'ACME')")
    conn.execute(
        "INSERT INTO activism_filings VALUES "
        "(1, 1, 'SC 13D', 'Example Corp', '123456789', 5.1, '2024-05-01', 'https://sec/1')"
    )

    materialize_activism_campaigns(conn, price_adapter=_Prices({}))

    assert conn.execute(
        "SELECT window_return, holding_period_days FROM activism_campaigns"
    ).fetchone() == (
        None,
        0,
    )
