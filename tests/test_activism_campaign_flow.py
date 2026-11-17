from __future__ import annotations

import importlib.util
import sqlite3
from datetime import date
from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations

from api.activism import (
    query_activism_campaign_timeline,
    query_activism_campaigns,
    query_activism_documents,
)
from etl.activism_campaign_flow import materialize_activism_campaigns

ROOT = Path(__file__).resolve().parents[1]


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
    documents = conn.execute(
        "SELECT doc_type, source_url FROM activism_documents ORDER BY filing_id"
    ).fetchall()
    assert documents == [
        ("filing", "https://sec/1"),
        ("filing", "https://sec/2"),
        ("filing", "https://sec/3"),
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
            (1, 1, "SC 13D", "Example Corp", "123456789", 5.1, "2022-01-10", "https://sec/1"),
            (2, 1, "SC 13D/A", "Example Corp", "123456789", 7.2, "2022-03-15", "https://sec/2"),
            (3, 1, "SC 13D", "Example Corp", "123456789", 6.0, "2024-05-01", "https://sec/3"),
            (4, 1, "SC 13D/A", "Example Corp", "123456789", 9.4, "2024-05-03", "https://sec/4"),
            (5, 2, "SC 13G", "Other Corp", "987654321", 4.0, "2024-01-02", "https://sec/5"),
        ],
    )
    materialize_activism_campaigns(conn)
    before_ids = conn.execute(
        "SELECT campaign_id, opening_filing_id, first_filed, last_filed, filing_count "
        "FROM activism_campaigns WHERE target_identifier = '123456789' ORDER BY first_filed"
    ).fetchall()
    assert len(before_ids) == 2

    summary = materialize_activism_campaigns(conn, since=date(2024, 5, 3))

    # Full histories are partitioned into episodes before applying `since`, so only the
    # second episode touched on/after the cutoff is refreshed, and it keeps its full
    # episode aggregates rather than collapsing with the 2022 episode or shrinking.
    assert summary.campaigns_written == 1
    assert conn.execute(
        "SELECT campaign_id, opening_filing_id, filing_count, first_filed, last_filed, peak_ownership_pct "
        "FROM activism_campaigns WHERE target_identifier = '123456789' ORDER BY first_filed"
    ).fetchall() == [
        (before_ids[0][0], 1, 2, "2022-01-10", "2022-03-15", 7.2),
        (before_ids[1][0], 3, 2, "2024-05-01", "2024-05-03", 9.4),
    ]


def test_new_initial_after_inactivity_gap_opens_second_campaign() -> None:
    conn = _conn()
    conn.executemany(
        "INSERT INTO activism_filings VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (1, 1, "SC 13D", "Example Corp", "123456789", 5.1, "2022-01-10", "https://sec/1"),
            (2, 1, "SC 13D/A", "Example Corp", "123456789", 7.4, "2022-03-15", "https://sec/2"),
            (3, 1, "SC 13D", "Example Corp", "123456789", 6.0, "2024-05-01", "https://sec/3"),
            (4, 1, "SC 13D/A", "Example Corp", "123456789", 8.8, "2024-05-20", "https://sec/4"),
        ],
    )
    conn.executemany(
        "INSERT INTO activism_events VALUES (?, ?, ?, ?)",
        [
            (10, 1, "initial_stake", "2022-01-10T09:00:00"),
            (11, 2, "stake_increase", "2022-03-15T09:00:00"),
            (12, 3, "initial_stake", "2024-05-01T09:00:00"),
            (13, 4, "threshold_crossing", "2024-05-20T09:00:00"),
        ],
    )

    first_run = materialize_activism_campaigns(conn)
    assert first_run.campaigns_written == 2
    assert first_run.timeline_rows_written == 4

    rows = conn.execute(
        "SELECT campaign_id, opening_filing_id, first_filed, last_filed, filing_count, "
        "peak_ownership_pct, latest_ownership_pct, latest_event_type "
        "FROM activism_campaigns ORDER BY first_filed"
    ).fetchall()
    assert len(rows) == 2
    first_id, second_id = int(rows[0][0]), int(rows[1][0])
    assert first_id != second_id
    assert rows == [
        (first_id, 1, "2022-01-10", "2022-03-15", 2, 7.4, 7.4, "stake_increase"),
        (second_id, 3, "2024-05-01", "2024-05-20", 2, 8.8, 8.8, "threshold_crossing"),
    ]

    first_detail = query_activism_campaigns(conn, campaign_id=first_id)
    second_detail = query_activism_campaigns(conn, campaign_id=second_id)
    assert len(first_detail) == 1 and first_detail[0].first_filed == date(2022, 1, 10)
    assert first_detail[0].last_filed == date(2022, 3, 15)
    assert first_detail[0].filing_count == 2
    assert len(second_detail) == 1 and second_detail[0].first_filed == date(2024, 5, 1)
    assert second_detail[0].last_filed == date(2024, 5, 20)
    assert second_detail[0].filing_count == 2

    assert [row.filing_id for row in query_activism_campaign_timeline(conn, first_id)] == [1, 2]
    assert [row.filing_id for row in query_activism_campaign_timeline(conn, second_id)] == [3, 4]
    assert [doc.filing_id for doc in query_activism_documents(conn, first_id)] == [1, 2]
    assert [doc.filing_id for doc in query_activism_documents(conn, second_id)] == [3, 4]

    # Rerun stability: a second pass keeps the same episode IDs, anchors, and child rows.
    second_run = materialize_activism_campaigns(conn)
    assert second_run.campaigns_written == 2
    assert (
        conn.execute(
            "SELECT campaign_id, opening_filing_id, first_filed, last_filed, filing_count, "
            "peak_ownership_pct, latest_ownership_pct, latest_event_type "
            "FROM activism_campaigns ORDER BY first_filed"
        ).fetchall()
        == rows
    )
    assert [row.filing_id for row in query_activism_campaign_timeline(conn, first_id)] == [1, 2]
    assert [row.filing_id for row in query_activism_campaign_timeline(conn, second_id)] == [3, 4]


def test_late_amendment_stays_in_existing_episode_until_new_initial_filing() -> None:
    conn = _conn()
    conn.executemany(
        "INSERT INTO activism_filings VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (1, 1, "SC 13D", "Example Corp", "123456789", 5.1, "2022-01-10", "https://sec/1"),
            (2, 1, "SC 13D/A", "Example Corp", "123456789", 6.2, "2023-08-01", "https://sec/2"),
        ],
    )
    summary = materialize_activism_campaigns(conn)
    assert summary.campaigns_written == 1
    assert conn.execute(
        "SELECT opening_filing_id, first_filed, last_filed, filing_count FROM activism_campaigns"
    ).fetchall() == [(1, "2022-01-10", "2023-08-01", 2)]


def test_smaller_stale_after_days_splits_existing_campaign_and_moves_children() -> None:
    conn = _conn()
    conn.executemany(
        "INSERT INTO activism_filings VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (1, 1, "SC 13D", "Example Corp", "123456789", 5.1, "2024-01-01", "https://sec/1"),
            (2, 1, "SC 13D", "Example Corp", "123456789", 6.4, "2024-03-15", "https://sec/2"),
        ],
    )
    materialize_activism_campaigns(conn, stale_after_days=365)
    original_id = conn.execute("SELECT campaign_id FROM activism_campaigns").fetchone()[0]

    summary = materialize_activism_campaigns(conn, stale_after_days=30)
    assert summary.campaigns_written == 2
    rows = conn.execute(
        "SELECT campaign_id, opening_filing_id, filing_count FROM activism_campaigns ORDER BY first_filed"
    ).fetchall()
    assert rows[0] == (original_id, 1, 1)
    second_id = int(rows[1][0])
    assert second_id != original_id
    assert rows[1] == (second_id, 2, 1)
    assert [r.filing_id for r in query_activism_campaign_timeline(conn, original_id)] == [1]
    assert [r.filing_id for r in query_activism_campaign_timeline(conn, second_id)] == [2]


def test_migration_splits_collapsed_episodes_and_preserves_earliest_campaign_id(tmp_path) -> None:
    db_path = tmp_path / "legacy_episodes.db"
    engine = sa.create_engine(f"sqlite:///{db_path}")
    spec = importlib.util.spec_from_file_location(
        "campaign_episodes_migration",
        ROOT / "alembic/versions/024_activism_campaign_episodes.py",
    )
    assert spec is not None and spec.loader is not None
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)

    with engine.begin() as sa_conn:
        sa_conn.exec_driver_sql("CREATE TABLE managers (manager_id INTEGER PRIMARY KEY, name TEXT)")
        sa_conn.exec_driver_sql("INSERT INTO managers VALUES (1, 'Activist')")
        sa_conn.exec_driver_sql(
            "CREATE TABLE activism_filings (filing_id INTEGER PRIMARY KEY, manager_id INTEGER, "
            "filing_type TEXT, subject_company TEXT, subject_cusip TEXT, ownership_pct REAL, "
            "filed_date TEXT, url TEXT)"
        )
        sa_conn.exec_driver_sql(
            "CREATE TABLE activism_events (event_id INTEGER PRIMARY KEY, filing_id INTEGER, "
            "event_type TEXT, detected_at TEXT)"
        )
        sa_conn.exec_driver_sql(
            "INSERT INTO activism_filings VALUES "
            "(1, 1, 'SC 13D', 'Example Corp', '123456789', 5.1, '2022-01-10', 'https://sec/1'), "
            "(2, 1, 'SC 13D/A', 'Example Corp', '123456789', 7.4, '2022-03-15', 'https://sec/2'), "
            "(3, 1, 'SC 13D', 'Example Corp', '123456789', 6.0, '2024-05-01', 'https://sec/3'), "
            "(4, 1, 'SC 13D/A', 'Example Corp', '123456789', 8.8, '2024-05-20', 'https://sec/4')"
        )
        sa_conn.exec_driver_sql("""CREATE TABLE activism_campaigns (
                campaign_id INTEGER PRIMARY KEY,
                manager_id INTEGER NOT NULL,
                target_identifier TEXT NOT NULL,
                target_company TEXT NOT NULL,
                first_filed TEXT NOT NULL,
                last_filed TEXT NOT NULL,
                status TEXT NOT NULL,
                peak_ownership_pct REAL,
                latest_ownership_pct REAL,
                filing_count INTEGER NOT NULL DEFAULT 0,
                event_count INTEGER NOT NULL DEFAULT 0,
                latest_event_type TEXT,
                target_ticker TEXT,
                window_return REAL,
                holding_period_days INTEGER,
                return_computed_at TEXT,
                source_forms TEXT NOT NULL DEFAULT '[]',
                data_quality_flags TEXT NOT NULL DEFAULT '[]',
                computed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(manager_id, target_identifier),
                CHECK (status IN ('active', 'monitoring', 'closed', 'unknown'))
            )""")
        sa_conn.exec_driver_sql("""CREATE TABLE activism_campaign_timeline (
                timeline_id INTEGER PRIMARY KEY,
                campaign_id INTEGER NOT NULL REFERENCES activism_campaigns(campaign_id),
                filing_id INTEGER NOT NULL REFERENCES activism_filings(filing_id),
                event_id INTEGER REFERENCES activism_events(event_id),
                event_date TEXT NOT NULL,
                event_type TEXT NOT NULL,
                form_type TEXT NOT NULL,
                ownership_pct REAL,
                summary TEXT NOT NULL,
                source_url TEXT,
                UNIQUE(campaign_id, filing_id, event_id)
            )""")
        sa_conn.exec_driver_sql("""CREATE TABLE activism_documents (
                document_id INTEGER PRIMARY KEY,
                campaign_id INTEGER NOT NULL REFERENCES activism_campaigns(campaign_id),
                filing_id INTEGER NOT NULL REFERENCES activism_filings(filing_id),
                doc_type TEXT NOT NULL,
                source_url TEXT,
                raw_key TEXT,
                filed_date TEXT NOT NULL,
                UNIQUE(campaign_id, filing_id, doc_type, source_url)
            )""")
        sa_conn.exec_driver_sql(
            "INSERT INTO activism_campaigns(campaign_id, manager_id, target_identifier, "
            "target_company, first_filed, last_filed, status, peak_ownership_pct, "
            "latest_ownership_pct, filing_count, event_count) "
            "VALUES (7, 1, '123456789', 'Example Corp', '2022-01-10', '2024-05-20', 'active', 8.8, 8.8, 4, 0)"
        )
        for fid, fdate, ftype, pct in [
            (1, "2022-01-10", "SC 13D", 5.1),
            (2, "2022-03-15", "SC 13D/A", 7.4),
            (3, "2024-05-01", "SC 13D", 6.0),
            (4, "2024-05-20", "SC 13D/A", 8.8),
        ]:
            sa_conn.exec_driver_sql(
                "INSERT INTO activism_campaign_timeline(campaign_id, filing_id, event_date, "
                "event_type, form_type, ownership_pct, summary, source_url) "
                "VALUES (7, ?, ?, 'initial_filing', ?, ?, 'summary', 'https://sec')",
                (fid, fdate, ftype, pct),
            )
            sa_conn.exec_driver_sql(
                "INSERT INTO activism_documents(campaign_id, filing_id, doc_type, source_url, filed_date) "
                "VALUES (7, ?, 'filing', 'https://sec', ?)",
                (fid, fdate),
            )

        with Operations.context(MigrationContext.configure(sa_conn)):
            migration.upgrade()

    engine.dispose()

    with sqlite3.connect(db_path) as conn:
        campaigns = conn.execute(
            "SELECT campaign_id, opening_filing_id, first_filed, last_filed, filing_count, "
            "peak_ownership_pct FROM activism_campaigns ORDER BY first_filed"
        ).fetchall()
        assert len(campaigns) == 2
        assert campaigns[0] == (7, 1, "2022-01-10", "2022-03-15", 2, 7.4)
        later_id = int(campaigns[1][0])
        assert later_id != 7
        assert campaigns[1] == (later_id, 3, "2024-05-01", "2024-05-20", 2, 8.8)

        assert [row.filing_id for row in query_activism_campaign_timeline(conn, 7)] == [1, 2]
        assert [row.filing_id for row in query_activism_campaign_timeline(conn, later_id)] == [3, 4]
        assert [doc.filing_id for doc in query_activism_documents(conn, 7)] == [1, 2]
        assert [doc.filing_id for doc in query_activism_documents(conn, later_id)] == [3, 4]

        # Rerun and identifier correction preserve the migrated campaign IDs.
        materialize_activism_campaigns(conn)
        conn.execute("UPDATE activism_filings SET subject_cusip = '999999999' WHERE manager_id = 1")
        materialize_activism_campaigns(conn)
        assert conn.execute(
            "SELECT campaign_id, target_identifier, opening_filing_id, filing_count "
            "FROM activism_campaigns ORDER BY first_filed"
        ).fetchall() == [
            (7, "999999999", 1, 2),
            (later_id, "999999999", 3, 2),
        ]
        assert query_activism_campaigns(conn, campaign_id=7)[0].target_identifier == "999999999"
        assert (
            query_activism_campaigns(conn, campaign_id=later_id)[0].target_identifier == "999999999"
        )


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

    campaign = conn.execute(
        "SELECT target_ticker, window_return, holding_period_days, return_computed_at "
        "FROM activism_campaigns"
    ).fetchone()
    assert campaign[:3] == ("ACME", 0.2, 10)
    assert campaign[3] is not None


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


def test_materialize_campaigns_invalid_filing_date_keeps_campaign_without_metrics() -> None:
    conn = _conn()
    conn.execute("CREATE TABLE identifier_resolution_cache (cusip TEXT PRIMARY KEY, ticker TEXT)")
    conn.execute("INSERT INTO identifier_resolution_cache VALUES ('123456789', 'ACME')")
    conn.executemany(
        "INSERT INTO activism_filings VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (1, 1, "SC 13D", "Example Corp", "123456789", 5.1, "not-a-date", "https://sec/1"),
            (2, 1, "SC 13D/A", "Example Corp", "123456789", 6.2, "2024-05-11", "https://sec/2"),
        ],
    )

    summary = materialize_activism_campaigns(
        conn, price_adapter=_Prices({date(2024, 5, 10): 120.0})
    )

    assert summary.campaigns_written == 1
    assert conn.execute(
        "SELECT window_return, holding_period_days FROM activism_campaigns"
    ).fetchone() == (None, None)


@pytest.mark.parametrize("initial", [None, "OLD-CUSIP"])
def test_identifier_correction_preserves_id_and_derived_references(initial):
    conn = _conn()
    conn.execute(
        "INSERT INTO activism_filings VALUES (1,1,'SC 13D','Example Corp',?,5.1,'2024-05-01','https://sec/1')",
        (initial,),
    )
    materialize_activism_campaigns(conn)
    original = conn.execute("SELECT campaign_id FROM activism_campaigns").fetchone()[0]
    conn.execute("UPDATE activism_filings SET subject_cusip='123456789' WHERE filing_id=1")
    conn.execute(
        "INSERT INTO activism_filings VALUES (2,1,'SC 13D/A','Example Corp','123456789',7.0,'2024-05-02','https://sec/2')"
    )
    materialize_activism_campaigns(conn)
    materialize_activism_campaigns(conn)
    assert conn.execute(
        "SELECT campaign_id,target_identifier,filing_count FROM activism_campaigns"
    ).fetchall() == [(original, "123456789", 2)]
    for table in ["activism_campaign_timeline", "activism_documents"]:
        assert conn.execute(
            f"SELECT campaign_id,filing_id FROM {table} ORDER BY filing_id"
        ).fetchall() == [(original, 1), (original, 2)]
    detail = query_activism_campaigns(conn, campaign_id=original)
    assert len(detail) == 1 and detail[0].campaign_id == original
    assert len(query_activism_campaign_timeline(conn, campaign_id=original)) == 2


def test_identifier_correction_retains_shared_filing_membership():
    conn = _conn()
    conn.executemany(
        "INSERT INTO activism_filings VALUES (?,1,'SC 13D','Example Corp','OLD',5.1,?,'https://sec')",
        [(1, "2024-05-01"), (2, "2024-05-02")],
    )
    materialize_activism_campaigns(conn)
    original = conn.execute("SELECT campaign_id FROM activism_campaigns").fetchone()[0]
    conn.execute("UPDATE activism_filings SET subject_cusip='NEW' WHERE filing_id=2")
    materialize_activism_campaigns(conn)
    assert conn.execute(
        "SELECT campaign_id,target_identifier,filing_count FROM activism_campaigns"
    ).fetchall() == [(original, "NEW", 2)]


def test_identifier_correction_rolls_back_metadata_and_children_on_failure():
    conn = _conn()
    conn.execute(
        "INSERT INTO activism_filings VALUES (1,1,'SC 13D','Example Corp','OLD',5.1,'2024-05-01','https://sec/1')"
    )
    materialize_activism_campaigns(conn)
    tables = ["activism_campaigns", "activism_campaign_timeline", "activism_documents"]
    before = {t: conn.execute(f"SELECT * FROM {t}").fetchall() for t in tables}
    conn.execute("UPDATE activism_filings SET subject_cusip='NEW' WHERE filing_id=1")
    conn.commit()
    conn.execute(
        "CREATE TRIGGER fail_documents BEFORE INSERT ON activism_documents BEGIN SELECT RAISE(ABORT,'forced mid-update failure'); END"
    )
    with pytest.raises(sqlite3.IntegrityError, match="forced mid-update"):
        materialize_activism_campaigns(conn)
    assert {t: conn.execute(f"SELECT * FROM {t}").fetchall() for t in tables} == before


def test_identifier_collision_fails_without_renumbering_other_campaign():
    conn = _conn()
    conn.executemany(
        "INSERT INTO activism_filings VALUES (?,1,'SC 13D','Example Corp',?,5.1,?,'https://sec')",
        [(1, "FIRST", "2024-05-01"), (2, "SECOND", "2024-05-01"), (3, "EARLIER", "2022-01-10")],
    )
    materialize_activism_campaigns(conn)
    before = conn.execute(
        "SELECT campaign_id,target_identifier,opening_filing_id FROM activism_campaigns ORDER BY campaign_id"
    ).fetchall()
    conn.execute("UPDATE activism_filings SET subject_cusip='SECOND' WHERE filing_id=1")
    conn.commit()
    with pytest.raises(ValueError, match="ownership|another campaign"):
        materialize_activism_campaigns(conn)
    assert (
        conn.execute(
            "SELECT campaign_id,target_identifier,opening_filing_id FROM activism_campaigns ORDER BY campaign_id"
        ).fetchall()
        == before
    )

    # Restore filing 1 and correct the earlier episode (filing 3, >365 days prior)
    # to 'SECOND': episode-aware collision rules allow separate episodes to share a target.
    conn.execute("UPDATE activism_filings SET subject_cusip='FIRST' WHERE filing_id=1")
    conn.execute("UPDATE activism_filings SET subject_cusip='SECOND' WHERE filing_id=3")
    conn.commit()
    materialize_activism_campaigns(conn)
    assert conn.execute(
        "SELECT campaign_id,target_identifier,opening_filing_id FROM activism_campaigns ORDER BY campaign_id"
    ).fetchall() == [
        (before[0][0], "SECOND", 3),
        (before[1][0], "FIRST", 1),
        (before[2][0], "SECOND", 2),
    ]

    # Direct upsert collision check when another campaign row occupies the episode window
    # without child timeline/document rows: must also fail and roll back via savepoint.
    conn.execute("DELETE FROM activism_campaign_timeline WHERE campaign_id = ?", (before[2][0],))
    conn.execute("DELETE FROM activism_documents WHERE campaign_id = ?", (before[2][0],))
    conn.execute("DELETE FROM activism_filings WHERE filing_id = 2")
    conn.execute("UPDATE activism_filings SET subject_cusip='SECOND' WHERE filing_id=1")
    conn.commit()
    snapshot = conn.execute(
        "SELECT campaign_id,target_identifier,opening_filing_id FROM activism_campaigns ORDER BY campaign_id"
    ).fetchall()
    with pytest.raises(ValueError, match="another campaign"):
        materialize_activism_campaigns(conn)
    assert (
        conn.execute(
            "SELECT campaign_id,target_identifier,opening_filing_id FROM activism_campaigns ORDER BY campaign_id"
        ).fetchall()
        == snapshot
    )
