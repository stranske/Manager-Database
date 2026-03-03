import sqlite3

import pytest

import etl.conviction_flow as conviction_flow


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

    assert result == {"filings_scored": 2, "scores_computed": 2}
    assert filing_ids == [102, 201]


def test_conviction_flow_logs_api_usage_and_supports_elliott_sample(monkeypatch, tmp_path):
    db_path = tmp_path / "flow.db"
    monkeypatch.setenv("DB_PATH", str(db_path))

    conn = sqlite3.connect(db_path)
    _create_base_tables(conn)
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

    assert result == {"filings_scored": 1, "scores_computed": 3}
    assert len(scores) == 3
    assert total_pct == pytest.approx(100.0, rel=1e-4)
    assert usage == ("prefect", "flow:conviction-scoring", 200)


def test_conviction_deployment_nightly_utc_schedule():
    schedule = conviction_flow.conviction_deployment.schedules[0].schedule
    assert schedule.cron == "0 2 * * *"
    assert schedule.timezone == "UTC"
