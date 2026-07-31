"""Acceptance tests for the signal-alpha backtest harness (#1464 / design #1401)."""

from __future__ import annotations

import math
import sqlite3
import sys
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest

from adapters.prices import PriceAdapter, fetch_yfinance_prices
from etl import backtest_flow
from etl.backtest_flow import (
    BacktestReport,
    Position,
    enforce_no_lookahead,
    persist_backtest,
    run_backtest,
    select_new_buys,
)

# Fixture price series. Keys are (ticker) -> {date: close}.
PRICES: dict[str, dict[date, float]] = {
    "AAA": {date(2024, 5, 2): 100.0, date(2024, 8, 1): 120.0},
    "BBB": {date(2024, 8, 2): 200.0, date(2024, 11, 1): 220.0},
    "SPY": {
        date(2024, 5, 2): 100.0,
        date(2024, 8, 1): 105.0,
        date(2024, 8, 2): 105.0,
        date(2024, 11, 1): 107.1,
    },
}

DECISION_DATES = [date(2024, 5, 1), date(2024, 8, 1)]


def _make_fetcher(prices: dict[str, dict[date, float]]):
    calls: list[tuple[str, date, date]] = []

    def fetcher(ticker: str, start: date, end: date) -> dict[date, float]:
        calls.append((ticker, start, end))
        return {d: p for d, p in prices.get(ticker, {}).items() if start <= d <= end}

    fetcher.calls = calls  # type: ignore[attr-defined]
    return fetcher


def _connect(tmp_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(tmp_path / "backtest.db")
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


def _add_filing(conn: sqlite3.Connection, filing_id: int, period_end: str, filed_date: str) -> None:
    conn.execute(
        "INSERT INTO filings(filing_id, manager_id, type, period_end, filed_date, source) "
        "VALUES (?, 1, '13F-HR', ?, ?, 'us')",
        (filing_id, period_end, filed_date),
    )


def _add_holding(
    conn: sqlite3.Connection,
    filing_id: int,
    cusip: str,
    ticker: str | None,
    value_usd: float,
    knowledge_time: str,
) -> None:
    conn.execute(
        "INSERT INTO holdings(filing_id, cusip, resolved_ticker, name_of_issuer, shares, "
        "value_usd, knowledge_time) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (filing_id, cusip, ticker, f"{cusip} Corp", 1000, value_usd, knowledge_time),
    )


def _two_quarter_fixture(tmp_path: Path) -> sqlite3.Connection:
    """Q1 discloses AAA; Q2 additionally discloses BBB (the Q2 new buy)."""
    conn = _connect(tmp_path)
    _add_filing(conn, 10, "2024-03-31", "2024-05-01")
    _add_holding(conn, 10, "AAA000000", "AAA", 5_000_000.0, "2024-05-01T12:00:00Z")
    _add_filing(conn, 11, "2024-06-30", "2024-08-01")
    _add_holding(conn, 11, "AAA000000", "AAA", 5_500_000.0, "2024-08-01T12:00:00Z")
    _add_holding(conn, 11, "BBB000000", "BBB", 9_000_000.0, "2024-08-01T12:00:00Z")
    conn.commit()
    return conn


def _adapter(conn: sqlite3.Connection, prices=None) -> PriceAdapter:
    return PriceAdapter(conn, source="test", fetcher=_make_fetcher(prices or PRICES))


def test_two_quarter_backtest_matches_hand_computed_return_and_sharpe(tmp_path):
    """Stubbed prices over two quarters reproduce hand-computed metrics.

    Q1 buys AAA at 100 and exits at 120 (+20%); Q2 buys BBB at 200 and exits at
    220 (+10%). With one position per period the period returns are 0.20 and 0.10:
      total       = 1.20 * 1.10 - 1                       = 0.32
      annualized  = 1.32 ** (4/2) - 1                     = 0.7424
      sharpe      = mean/sample-stdev * sqrt(4)
                  = 0.15 / 0.0707106781 * 2               = 4.2426406871
      hit rate    = 2 filled positions, both positive     = 1.0
    """
    conn = _two_quarter_fixture(tmp_path)
    report = run_backtest(
        conn,
        manager_id=1,
        start_date=date(2024, 1, 1),
        end_date=date(2024, 12, 31),
        price_adapter=_adapter(conn),
        decision_dates=DECISION_DATES,
        entry_lag_days=1,
        holding_period_days=91,
        benchmark_ticker="SPY",
    )

    assert report.periods == 2
    assert report.period_returns == pytest.approx([0.20, 0.10])
    assert report.total_return == pytest.approx(0.32)
    assert report.annualized_return == pytest.approx(0.7424)
    assert report.sharpe == pytest.approx(0.15 / (0.1 / math.sqrt(2)) * 2.0)
    assert report.sharpe == pytest.approx(4.2426406871, rel=1e-9)
    assert report.hit_rate == pytest.approx(1.0)

    # Benchmark: +5% then +2% compounds to 7.1%; excess is strategy minus benchmark.
    assert report.benchmark_total_return == pytest.approx(0.071)
    assert report.excess_return == pytest.approx(0.32 - 0.071)

    tickers = sorted(p.ticker for p in report.filled)
    assert tickers == ["AAA", "BBB"]


def test_no_lookahead_ignores_a_later_known_holding(tmp_path):
    """A holding not knowable at the decision date must never enter the decision set.

    Deliberate-break check: allowing future-dated holdings in (e.g. sourcing the
    decision set from current holdings instead of the as-of snapshot) makes this
    test fail.
    """
    conn = _connect(tmp_path)
    _add_filing(conn, 10, "2024-03-31", "2024-05-01")
    _add_holding(conn, 10, "AAA000000", "AAA", 5_000_000.0, "2024-05-01T12:00:00Z")
    # Restated later: filed before the decision date, but only KNOWN afterwards.
    _add_holding(conn, 10, "ZZZ000000", "ZZZ", 9_999_999.0, "2024-07-15T12:00:00Z")
    conn.commit()

    decision_date = date(2024, 5, 1)
    selected = select_new_buys(conn, 1, decision_date, None, top_n=10)
    assert [row["resolved_ticker"] for row in selected] == [
        "AAA"
    ], "ZZZ was only knowable on 2024-07-15 and must not inform a 2024-05-01 decision"

    report = run_backtest(
        conn,
        manager_id=1,
        start_date=date(2024, 1, 1),
        end_date=date(2024, 12, 31),
        price_adapter=_adapter(conn),
        decision_dates=[decision_date],
        entry_lag_days=1,
        holding_period_days=91,
        benchmark_ticker="SPY",
    )
    assert "ZZZ" not in {p.ticker for p in report.positions}

    # The same holding IS visible once its knowledge time has passed.
    later = select_new_buys(conn, 1, date(2024, 7, 16), None, top_n=10)
    assert sorted(row["resolved_ticker"] for row in later) == ["AAA", "ZZZ"]


def test_enforce_no_lookahead_drops_future_knowledge_rows():
    """The harness-local guard drops future-known rows even if a query returns them."""
    rows = [
        {"cusip": "AAA000000", "knowledge_time": "2024-05-01T12:00:00Z"},
        {"cusip": "ZZZ000000", "knowledge_time": "2024-07-15T12:00:00Z"},
        {"cusip": "NNN000000", "knowledge_time": None},
        {"cusip": "BAD000000", "knowledge_time": "not-a-timestamp"},
    ]
    visible = enforce_no_lookahead(rows, date(2024, 5, 1))
    assert [row["cusip"] for row in visible] == ["AAA000000"]


def test_missing_price_skips_position_without_crashing(tmp_path, caplog):
    """A position with no obtainable price is excluded and logged, not fatal."""
    conn = _connect(tmp_path)
    _add_filing(conn, 10, "2024-03-31", "2024-05-01")
    _add_holding(conn, 10, "AAA000000", "AAA", 5_000_000.0, "2024-05-01T12:00:00Z")
    # No price series exists for MIA, and NOTICK never resolved to a ticker.
    _add_holding(conn, 10, "MIA000000", "MIA", 4_000_000.0, "2024-05-01T12:00:00Z")
    _add_holding(conn, 10, "NON000000", None, 3_000_000.0, "2024-05-01T12:00:00Z")
    conn.commit()

    with caplog.at_level("WARNING"):
        report = run_backtest(
            conn,
            manager_id=1,
            start_date=date(2024, 1, 1),
            end_date=date(2024, 12, 31),
            price_adapter=_adapter(conn),
            decision_dates=[date(2024, 5, 1)],
            entry_lag_days=1,
            holding_period_days=91,
            benchmark_ticker="SPY",
        )

    assert [p.ticker for p in report.filled] == ["AAA"]
    # The unresolved holding has no ticker at all, so it is identified by cusip.
    skipped = {p.cusip: (p.ticker, p.skip_reason) for p in report.skipped}
    assert skipped == {
        "MIA000000": ("MIA", "missing_price"),
        "NON000000": (None, "unresolved_ticker"),
    }
    # Metrics reflect only the filled position, and the run completed.
    assert report.period_returns == pytest.approx([0.20])
    assert report.hit_rate == pytest.approx(1.0)
    assert "Missing price for position" in caplog.text
    assert "no resolved ticker" in caplog.text


def test_backtest_persists_run_and_position_rows(tmp_path):
    conn = _two_quarter_fixture(tmp_path)
    report = run_backtest(
        conn,
        manager_id=1,
        start_date=date(2024, 1, 1),
        end_date=date(2024, 12, 31),
        price_adapter=_adapter(conn),
        decision_dates=DECISION_DATES,
        entry_lag_days=1,
        holding_period_days=91,
        benchmark_ticker="SPY",
    )

    assert report.run_id is not None
    run_row = conn.execute(
        "SELECT strategy, periods, positions, positions_skipped, total_return, hit_rate "
        "FROM backtest_runs WHERE run_id = ?",
        (report.run_id,),
    ).fetchone()
    assert run_row["strategy"] == "high_conviction_new_buys"
    assert run_row["periods"] == 2
    assert run_row["positions"] == 2
    assert run_row["positions_skipped"] == 0
    assert run_row["total_return"] == pytest.approx(0.32)

    result_rows = conn.execute(
        "SELECT ticker, entry_date, exit_date, position_return FROM backtest_results "
        "WHERE run_id = ? ORDER BY ticker",
        (report.run_id,),
    ).fetchall()
    assert [row["ticker"] for row in result_rows] == ["AAA", "BBB"]
    assert result_rows[0]["entry_date"] == "2024-05-02"
    assert result_rows[0]["exit_date"] == "2024-08-01"
    assert result_rows[0]["position_return"] == pytest.approx(0.20)

    runs = backtest_flow.query_backtest_runs(conn, manager_id=1)
    assert len(runs) == 1
    assert runs[0]["run_id"] == report.run_id
    assert runs[0]["sharpe"] == pytest.approx(4.2426406871, rel=1e-9)


def test_backtest_persistence_rolls_back_the_header_when_a_result_insert_fails(tmp_path):
    conn = _connect(tmp_path)
    report = BacktestReport(
        strategy="test",
        manager_id=1,
        start_date=date(2024, 1, 1),
        end_date=date(2024, 3, 31),
        positions=[
            Position(date(2024, 1, 1), date(2024, 1, 2), date(2024, 4, 2), "AAA", None),
            Position(
                date(2024, 1, 1),
                date(2024, 1, 2),
                date(2024, 4, 2),
                "BBB",
                None,
                status=None,  # type: ignore[arg-type]
            ),
        ],
    )

    with pytest.raises(sqlite3.IntegrityError):
        persist_backtest(
            conn,
            report,
            entry_lag_days=1,
            holding_period_days=91,
            benchmark_ticker="SPY",
            price_source="test",
            top_n=10,
        )

    assert conn.execute("SELECT COUNT(*) FROM backtest_runs").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM backtest_results").fetchone()[0] == 0


def test_decision_dates_derived_from_filing_disclosure_dates(tmp_path):
    conn = _two_quarter_fixture(tmp_path)
    dates = backtest_flow.derive_decision_dates(conn, 1, date(2024, 1, 1), date(2024, 12, 31))
    assert dates == DECISION_DATES


def test_price_adapter_caches_and_reports_missing(tmp_path):
    conn = _connect(tmp_path)
    fetcher = _make_fetcher(PRICES)
    adapter = PriceAdapter(conn, source="test", fetcher=fetcher)

    assert adapter.close_on_or_before("AAA", date(2024, 5, 2)) == pytest.approx(100.0)
    first_call_count = len(fetcher.calls)
    # Second read for the same date is served from price_cache.
    assert adapter.close_on_or_before("AAA", date(2024, 5, 2)) == pytest.approx(100.0)
    assert len(fetcher.calls) == first_call_count

    cached = conn.execute(
        "SELECT close_usd FROM price_cache WHERE ticker = ? AND price_date = ?",
        ("AAA", "2024-05-02"),
    ).fetchone()
    assert cached["close_usd"] == pytest.approx(100.0)

    # Unknown ticker and empty ticker degrade to None rather than raising.
    assert adapter.close_on_or_before("NOSUCH", date(2024, 5, 2)) is None
    assert adapter.close_on_or_before(None, date(2024, 5, 2)) is None


def test_price_adapter_uses_most_recent_close_within_staleness_window(tmp_path):
    conn = _connect(tmp_path)
    series = {"AAA": {date(2024, 5, 1): 90.0, date(2024, 5, 2): 100.0}}
    adapter = PriceAdapter(conn, source="test", fetcher=_make_fetcher(series))

    # 2024-05-04 has no print; the 05-02 close is the as-of price.
    assert adapter.close_on_or_before("AAA", date(2024, 5, 4)) == pytest.approx(100.0)
    # Beyond the staleness window there is no usable price.
    stale = PriceAdapter(
        conn,
        source="strict",
        fetcher=_make_fetcher(series),
        max_staleness_days=1,
    )
    assert stale.close_on_or_before("AAA", date(2024, 5, 10)) is None


def test_price_adapter_refreshes_a_partial_cache_for_a_newer_as_of_date(tmp_path):
    conn = _connect(tmp_path)
    prices = {"AAA": {date(2024, 5, 1): 90.0}}
    fetcher = _make_fetcher(prices)
    adapter = PriceAdapter(conn, source="test", fetcher=fetcher)

    assert adapter.close_on_or_before("AAA", date(2024, 5, 1)) == pytest.approx(90.0)
    prices["AAA"][date(2024, 5, 2)] = 100.0

    assert adapter.close_on_or_before("AAA", date(2024, 5, 2)) == pytest.approx(100.0)
    assert len(fetcher.calls) == 2


def test_fetch_yfinance_prices_handles_dates_empty_frames_and_missing_dependency(monkeypatch):
    calls: list[dict[str, object]] = []

    class Frame:
        Close = {date(2024, 5, 1): 12.5}

    class Ticker:
        def __init__(self, ticker: str) -> None:
            self.ticker = ticker

        def history(self, **kwargs):
            calls.append(kwargs)
            return Frame()

    monkeypatch.setitem(sys.modules, "yfinance", SimpleNamespace(Ticker=Ticker))
    assert fetch_yfinance_prices("AAA", date(2024, 5, 1), date(2024, 5, 2)) == {
        date(2024, 5, 1): 12.5
    }
    assert calls == [{"start": "2024-05-01", "end": "2024-05-03", "auto_adjust": True}]

    Frame.Close = {}
    assert fetch_yfinance_prices("AAA", date(2024, 5, 1), date(2024, 5, 2)) == {}
    monkeypatch.setitem(sys.modules, "yfinance", None)
    assert fetch_yfinance_prices("AAA", date(2024, 5, 1), date(2024, 5, 2)) == {}


def test_price_adapter_rejects_non_finite_quotes(tmp_path):
    conn = _connect(tmp_path)
    series = {"AAA": {date(2024, 5, 2): float("inf"), date(2024, 5, 1): 90.0}}
    adapter = PriceAdapter(conn, source="test", fetcher=_make_fetcher(series))
    # The infinite quote is discarded; the previous finite close is used instead.
    assert adapter.close_on_or_before("AAA", date(2024, 5, 2)) == pytest.approx(90.0)


def test_price_adapter_survives_a_raising_fetcher(tmp_path):
    conn = _connect(tmp_path)

    def broken(ticker: str, start: date, end: date) -> dict[date, float]:
        raise RuntimeError("provider exploded")

    adapter = PriceAdapter(conn, source="test", fetcher=broken)
    assert adapter.close_on_or_before("AAA", date(2024, 5, 2)) is None
