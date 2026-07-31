from __future__ import annotations

import sqlite3

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
