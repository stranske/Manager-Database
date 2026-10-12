"""Failed dashboard reads return empty results and always release their connection."""

import sqlite3

import pandas as pd
import pytest

from ui import dashboard


@pytest.mark.parametrize(
    ("loader", "columns"),
    [
        (
            "load_filing_timeline",
            ["filing_id", "type", "filed_date", "period_end", "source", "raw_key"],
        ),
        (
            "load_latest_holdings_snapshot",
            ["name_of_issuer", "cusip", "shares", "value_usd"],
        ),
        (
            "load_top_deltas",
            [
                "cusip",
                "name_of_issuer",
                "delta_type",
                "shares_prev",
                "shares_curr",
                "value_prev",
                "value_curr",
            ],
        ),
    ],
    ids=["load_filing_timeline", "load_latest_holdings_snapshot", "load_top_deltas"],
)
def test_sql_loader_returns_typed_empty_frame_and_closes_missing_schema(
    monkeypatch, loader, columns
):
    """Exercise real pandas/SQLite query failure, preserving the consumer schema."""
    closed = []

    class Connection(sqlite3.Connection):
        def close(self):
            closed.append(True)
            super().close()

    conn = sqlite3.connect(":memory:", factory=Connection)
    monkeypatch.setattr(dashboard, "connect_db", lambda: conn)

    result = getattr(dashboard, loader)(17)

    assert isinstance(result, pd.DataFrame)
    assert result.empty
    assert list(result.columns) == columns
    assert closed == [True]
    with pytest.raises(sqlite3.ProgrammingError, match="closed database"):
        conn.execute("SELECT 1")


@pytest.mark.parametrize(
    ("loader", "query", "args", "kwargs"),
    [
        (
            "load_manager_activism_filings",
            "query_activism_filings",
            (),
            {"manager_id": 17, "limit": 200},
        ),
        (
            "load_manager_activism_events",
            "query_activism_events",
            (),
            {"manager_id": 17, "limit": 200},
        ),
        ("load_manager_activism_timeline", "query_activism_timeline", (17,), {}),
        (
            "load_active_campaigns_summary",
            "query_active_campaigns",
            (),
            {"min_ownership_pct": 5.0, "limit": 10},
        ),
        (
            "load_manager_conviction_scores",
            "query_conviction_scores",
            (17,),
            {"filing_id": None, "min_conviction_pct": 0.0, "limit": 100},
        ),
        (
            "load_manager_crowded_trades",
            "query_crowded_trades",
            (),
            {"manager_id": 17, "min_managers": 3, "limit": 25},
        ),
        (
            "load_manager_contrarian_signals",
            "query_contrarian_signals",
            (),
            {"manager_id": 17, "limit": 25},
        ),
    ],
    ids=[
        "load_manager_activism_filings",
        "load_manager_activism_events",
        "load_manager_activism_timeline",
        "load_active_campaigns_summary",
        "load_manager_conviction_scores",
        "load_manager_crowded_trades",
        "load_manager_contrarian_signals",
    ],
)
def test_domain_loader_returns_empty_and_closes_after_query_error(
    monkeypatch, loader, query, args, kwargs
):
    events = []

    class Connection:
        def close(self):
            events.append("close")

    conn = Connection()

    def fail_query(connection, *actual_args, **actual_kwargs):
        assert connection is conn
        assert actual_args == args
        assert actual_kwargs == kwargs
        events.append("query")
        raise RuntimeError("database read unavailable")

    monkeypatch.setattr(dashboard, "connect_db", lambda: conn)
    monkeypatch.setattr(dashboard, query, fail_query)
    result = getattr(dashboard, loader)(
        **({} if loader == "load_active_campaigns_summary" else {"manager_id": 17})
    )

    assert isinstance(result, pd.DataFrame)
    assert result.empty
    assert list(result.columns) == []
    assert events == ["query", "close"]
