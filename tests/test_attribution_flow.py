"""Acceptance tests for position-level performance attribution (#1465 / design #1402)."""

from __future__ import annotations

import sqlite3
from datetime import date
from pathlib import Path

import pytest

from adapters.prices import PriceAdapter
from etl.attribution_flow import (
    enforce_no_lookahead,
    query_manager_attribution,
    run_attribution,
    summarize_manager_attribution,
)

# Hand-computed paths: AAA 100 -> 130 = +30%; BBB 50 -> 40 = -20%.
# Equal-weight manager realized return = (0.30 + (-0.20)) / 2 = 0.05; hit-rate = 0.5.
PRICES: dict[str, dict[date, float]] = {
    "AAA": {
        date(2024, 5, 1): 100.0,
        date(2024, 4, 30): 90.0,  # pre-disclosure close used by the deliberate break
        date(2024, 8, 1): 130.0,
    },
    "BBB": {
        date(2024, 5, 1): 50.0,
        date(2024, 4, 30): 55.0,
        date(2024, 8, 1): 40.0,
    },
}

DISCLOSURE = date(2024, 5, 1)
AS_OF = date(2024, 8, 1)


def _make_fetcher(prices: dict[str, dict[date, float]]):
    def fetcher(ticker: str, start: date, end: date) -> dict[date, float]:
        return {d: p for d, p in prices.get(ticker, {}).items() if start <= d <= end}

    return fetcher


def _connect(tmp_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(tmp_path / "attribution.db")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE managers (manager_id INTEGER PRIMARY KEY, name TEXT)")
    conn.execute("""
        CREATE TABLE filings (
            filing_id INTEGER PRIMARY KEY,
            manager_id INTEGER NOT NULL,
            type TEXT NOT NULL,
            period_end TEXT,
            filed_date TEXT,
            source TEXT NOT NULL
        )
        """)
    conn.execute("""
        CREATE TABLE holdings (
            holding_id INTEGER PRIMARY KEY AUTOINCREMENT,
            filing_id INTEGER NOT NULL,
            cusip TEXT,
            resolved_ticker TEXT,
            name_of_issuer TEXT,
            shares INTEGER,
            value_usd REAL,
            knowledge_time TEXT NOT NULL,
            superseded_at TEXT,
            version INTEGER NOT NULL DEFAULT 1
        )
        """)
    conn.execute("INSERT INTO managers(manager_id, name) VALUES (1, 'Test Manager')")
    conn.commit()
    return conn


def _seed_two_positions(conn: sqlite3.Connection) -> None:
    conn.execute(
        "INSERT INTO filings(filing_id, manager_id, type, period_end, filed_date, source) "
        "VALUES (10, 1, '13F-HR', '2024-03-31', '2024-05-01', 'us')"
    )
    conn.execute(
        "INSERT INTO holdings(filing_id, cusip, resolved_ticker, name_of_issuer, shares, "
        "value_usd, knowledge_time) VALUES (10, 'AAA000000', 'AAA', 'AAA Corp', 1000, "
        "5000000.0, '2024-05-01T12:00:00Z')"
    )
    conn.execute(
        "INSERT INTO holdings(filing_id, cusip, resolved_ticker, name_of_issuer, shares, "
        "value_usd, knowledge_time) VALUES (10, 'BBB000000', 'BBB', 'BBB Corp', 2000, "
        "3000000.0, '2024-05-01T12:00:00Z')"
    )
    conn.commit()


def _adapter(conn: sqlite3.Connection) -> PriceAdapter:
    return PriceAdapter(conn, source="test", fetcher=_make_fetcher(PRICES))


def test_two_positions_yield_hand_computed_returns(tmp_path: Path) -> None:
    """Acceptance: known post-disclosure paths produce exact position + manager metrics."""
    conn = _connect(tmp_path)
    _seed_two_positions(conn)

    report = run_attribution(
        conn,
        manager_id=1,
        as_of_date=AS_OF,
        price_adapter=_adapter(conn),
        disclosure_dates=[DISCLOSURE],
    )

    by_ticker = {p.ticker: p for p in report.filled}
    assert set(by_ticker) == {"AAA", "BBB"}
    assert by_ticker["AAA"].position_return == pytest.approx(0.30)
    assert by_ticker["BBB"].position_return == pytest.approx(-0.20)
    assert report.realized_return == pytest.approx(0.05)
    assert report.hit_rate == pytest.approx(0.5)

    summary = summarize_manager_attribution(report.positions)
    assert summary["realized_return"] == pytest.approx(0.05)
    assert summary["hit_rate"] == pytest.approx(0.5)
    assert summary["positions"] == 2

    stored = query_manager_attribution(conn, 1, as_of_date=AS_OF)
    assert len(stored) == 2
    assert {row["ticker"] for row in stored} == {"AAA", "BBB"}


def test_no_lookahead_excludes_future_knowledge(tmp_path: Path) -> None:
    """Returns only use holdings knowable at disclosure — never a later amendment."""
    conn = _connect(tmp_path)
    _seed_two_positions(conn)
    # Amendment filed later must not enter the May 1 decision set.
    conn.execute(
        "INSERT INTO filings(filing_id, manager_id, type, period_end, filed_date, source) "
        "VALUES (11, 1, '13F-HR/A', '2024-03-31', '2024-05-15', 'us')"
    )
    conn.execute(
        "INSERT INTO holdings(filing_id, cusip, resolved_ticker, name_of_issuer, shares, "
        "value_usd, knowledge_time) VALUES (11, 'CCC000000', 'CCC', 'CCC Corp', 100, "
        "1000000.0, '2024-05-15T12:00:00Z')"
    )
    conn.commit()

    leaked = enforce_no_lookahead(
        [
            {
                "cusip": "CCC000000",
                "resolved_ticker": "CCC",
                "knowledge_time": "2024-05-15T12:00:00Z",
            }
        ],
        DISCLOSURE,
    )
    assert leaked == []

    report = run_attribution(
        conn,
        manager_id=1,
        as_of_date=AS_OF,
        price_adapter=_adapter(conn),
        disclosure_dates=[DISCLOSURE],
    )
    assert {p.ticker for p in report.filled} == {"AAA", "BBB"}
    assert all(p.disclosure_date == DISCLOSURE for p in report.positions)


def test_deliberate_break_starting_before_disclosure_fails(tmp_path: Path) -> None:
    """Deliberate-break: starting the return window before disclosure corrupts returns.

    Restoring the production offset (0) recovers the hand-computed path.
    """
    conn = _connect(tmp_path)
    _seed_two_positions(conn)
    adapter = _adapter(conn)

    broken = run_attribution(
        conn,
        manager_id=1,
        as_of_date=AS_OF,
        price_adapter=adapter,
        disclosure_dates=[DISCLOSURE],
        entry_date_offset_days=-1,
        persist=False,
    )
    by_ticker = {p.ticker: p for p in broken.filled}
    # AAA: 90 -> 130 = 40/90; BBB: 55 -> 40 = -15/55 — not the acceptance numbers.
    assert by_ticker["AAA"].position_return != pytest.approx(0.30)
    assert by_ticker["BBB"].position_return != pytest.approx(-0.20)
    assert broken.realized_return != pytest.approx(0.05)

    restored = run_attribution(
        conn,
        manager_id=1,
        as_of_date=AS_OF,
        price_adapter=adapter,
        disclosure_dates=[DISCLOSURE],
        entry_date_offset_days=0,
        persist=False,
    )
    assert restored.realized_return == pytest.approx(0.05)
    assert {p.ticker: p.position_return for p in restored.filled}["AAA"] == pytest.approx(0.30)
