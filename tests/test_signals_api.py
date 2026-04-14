import asyncio
import sqlite3
import sys
from pathlib import Path
from typing import Any, cast

import httpx

sys.path.append(str(Path(__file__).resolve().parents[1]))

from api.chat import app


async def _request(path: str, params: dict[str, Any] | None = None):
    await cast(Any, app.router).startup()
    try:
        transport = httpx.ASGITransport(app=cast(Any, app))
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", timeout=5.0
        ) as client:
            return await client.get(path, params=params)
    finally:
        await cast(Any, app.router).shutdown()


def _seed_db(db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("CREATE TABLE managers (manager_id INTEGER PRIMARY KEY, name TEXT)")
        conn.execute(
            "CREATE TABLE filings (filing_id INTEGER PRIMARY KEY, manager_id INTEGER, type TEXT, filed_date TEXT, period_end TEXT, source TEXT)"
        )
        conn.execute(
            "CREATE TABLE holdings (holding_id INTEGER PRIMARY KEY, filing_id INTEGER, cusip TEXT, name_of_issuer TEXT, shares INTEGER, value_usd REAL)"
        )
        conn.execute(
            "CREATE TABLE conviction_scores (score_id INTEGER PRIMARY KEY, manager_id INTEGER, filing_id INTEGER, cusip TEXT, name_of_issuer TEXT, shares INTEGER, value_usd REAL, conviction_pct REAL, portfolio_weight REAL, computed_at TEXT)"
        )
        conn.execute(
            "CREATE TABLE crowded_trades (crowd_id INTEGER PRIMARY KEY, cusip TEXT, name_of_issuer TEXT, manager_count INTEGER, manager_ids TEXT, total_value_usd REAL, avg_conviction_pct REAL, max_conviction_pct REAL, report_date TEXT, computed_at TEXT)"
        )
        conn.execute(
            "CREATE TABLE contrarian_signals (signal_id INTEGER PRIMARY KEY, manager_id INTEGER, cusip TEXT, name_of_issuer TEXT, direction TEXT, consensus_direction TEXT, manager_delta_shares INTEGER, manager_delta_value REAL, consensus_count INTEGER, report_date TEXT, detected_at TEXT)"
        )
        conn.executemany(
            "INSERT INTO managers(manager_id, name) VALUES (?, ?)",
            [(1, "Alpha Partners"), (2, "Zulu Capital"), (3, "Gamma Capital")],
        )
        conn.executemany(
            "INSERT INTO filings(filing_id, manager_id, type, filed_date, period_end, source) VALUES (?, ?, ?, ?, ?, ?)",
            [
                (10, 1, "13F-HR", "2024-05-15", "2024-03-31", "edgar"),
                (11, 1, "13F-HR", "2024-02-15", "2023-12-31", "edgar"),
                (20, 2, "13F-HR", "2024-05-15", "2024-03-31", "edgar"),
            ],
        )
        conn.executemany(
            "INSERT INTO holdings(holding_id, filing_id, cusip, name_of_issuer, shares, value_usd) VALUES (?, ?, ?, ?, ?, ?)",
            [
                (1, 10, "AAA111111", "Example Corp", 100, 3000.0),
                (2, 10, "BBB222222", "Second Corp", 75, 1500.0),
                (3, 20, "AAA111111", "Example Corp", 40, 800.0),
            ],
        )
        conn.executemany(
            "INSERT INTO conviction_scores(score_id, manager_id, filing_id, cusip, name_of_issuer, shares, value_usd, conviction_pct, portfolio_weight, computed_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    101,
                    1,
                    10,
                    "AAA111111",
                    "Example Corp",
                    100,
                    3000.0,
                    66.67,
                    0.6667,
                    "2024-05-16T00:00:00",
                ),
                (
                    102,
                    1,
                    10,
                    "BBB222222",
                    "Second Corp",
                    75,
                    1500.0,
                    33.33,
                    0.3333,
                    "2024-05-16T00:00:00",
                ),
                (
                    103,
                    1,
                    11,
                    "OLD000001",
                    "Legacy Corp",
                    10,
                    200.0,
                    100.0,
                    1.0,
                    "2024-02-16T00:00:00",
                ),
            ],
        )
        conn.executemany(
            "INSERT INTO crowded_trades(crowd_id, cusip, name_of_issuer, manager_count, manager_ids, total_value_usd, avg_conviction_pct, max_conviction_pct, report_date, computed_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    201,
                    "AAA111111",
                    "Example Corp",
                    3,
                    "[1, 2, 3]",
                    4200.0,
                    22.25,
                    66.67,
                    "2024-05-01",
                    "2024-05-01T08:00:00",
                ),
                (
                    202,
                    "BBB222222",
                    "Second Corp",
                    2,
                    "[1, 2]",
                    2100.0,
                    19.10,
                    33.33,
                    "2024-05-01",
                    "2024-05-01T08:00:00",
                ),
                (
                    203,
                    "OLD000001",
                    "Legacy Corp",
                    4,
                    "[1, 2, 3, 4]",
                    1800.0,
                    14.00,
                    20.00,
                    "2024-02-01",
                    "2024-02-01T08:00:00",
                ),
            ],
        )
        conn.executemany(
            "INSERT INTO contrarian_signals(signal_id, manager_id, cusip, name_of_issuer, direction, consensus_direction, manager_delta_shares, manager_delta_value, consensus_count, report_date, detected_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    301,
                    1,
                    "AAA111111",
                    "Example Corp",
                    "SELL",
                    "BUY",
                    -100,
                    -2500.0,
                    4,
                    "2024-05-01",
                    "2024-05-01T09:00:00",
                ),
                (
                    302,
                    2,
                    "BBB222222",
                    "Second Corp",
                    "BUY",
                    "SELL",
                    80,
                    1200.0,
                    3,
                    "2024-05-01",
                    "2024-05-01T09:30:00",
                ),
            ],
        )
        conn.commit()
    finally:
        conn.close()


def test_signals_router_is_registered(tmp_path, monkeypatch):
    db_path = tmp_path / "signals.db"
    _seed_db(db_path)
    monkeypatch.setenv("DB_PATH", str(db_path))

    route_paths = {route.path for route in app.routes}
    expected_paths = {
        "/api/signals/crowded",
        "/api/signals/contrarian",
        "/api/signals/conviction/{manager_id}",
    }
    assert expected_paths.issubset(route_paths)

    openapi = app.openapi()
    for path in expected_paths:
        assert "Signals" in next(iter(openapi["paths"][path].values()))["tags"]


def test_get_crowded_trades_filters_by_report_date_and_threshold(tmp_path, monkeypatch):
    db_path = tmp_path / "signals.db"
    _seed_db(db_path)
    monkeypatch.setenv("DB_PATH", str(db_path))

    response = asyncio.run(
        _request(
            "/api/signals/crowded",
            params={"report_date": "2024-05-01", "min_managers": 3},
        )
    )
    assert response.status_code == 200
    payload = response.json()
    assert [item["cusip"] for item in payload] == ["AAA111111"]
    assert payload[0]["manager_names"] == ["Alpha Partners", "Zulu Capital", "Gamma Capital"]


def test_get_contrarian_signals_filters_by_manager(tmp_path, monkeypatch):
    db_path = tmp_path / "signals.db"
    _seed_db(db_path)
    monkeypatch.setenv("DB_PATH", str(db_path))

    response = asyncio.run(
        _request(
            "/api/signals/contrarian",
            params={"report_date": "2024-05-01", "manager_id": 1},
        )
    )
    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["manager_name"] == "Alpha Partners"
    assert payload[0]["consensus_direction"] == "BUY"


def test_get_conviction_scores_defaults_to_latest_filing(tmp_path, monkeypatch):
    db_path = tmp_path / "signals.db"
    _seed_db(db_path)
    monkeypatch.setenv("DB_PATH", str(db_path))

    response = asyncio.run(
        _request(
            "/api/signals/conviction/1",
            params={"min_conviction_pct": 40},
        )
    )
    assert response.status_code == 200
    payload = response.json()
    assert [item["cusip"] for item in payload] == ["AAA111111"]
    assert payload[0]["conviction_pct"] == 66.67


def test_signals_endpoints_return_empty_arrays_for_missing_data(tmp_path, monkeypatch):
    db_path = tmp_path / "signals.db"
    _seed_db(db_path)
    monkeypatch.setenv("DB_PATH", str(db_path))

    crowded = asyncio.run(_request("/api/signals/crowded", params={"report_date": "2023-01-01"}))
    contrarian = asyncio.run(
        _request("/api/signals/contrarian", params={"report_date": "2023-01-01", "manager_id": 99})
    )
    conviction = asyncio.run(_request("/api/signals/conviction/99"))

    assert crowded.status_code == 200 and crowded.json() == []
    assert contrarian.status_code == 200 and contrarian.json() == []
    assert conviction.status_code == 200 and conviction.json() == []


def test_get_contrarian_signals_supports_sqlite_managers_id_shape(tmp_path, monkeypatch):
    db_path = tmp_path / "signals_legacy.db"
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("CREATE TABLE managers (id INTEGER PRIMARY KEY, name TEXT)")
        conn.execute(
            "CREATE TABLE contrarian_signals (signal_id INTEGER PRIMARY KEY, manager_id INTEGER, cusip TEXT, name_of_issuer TEXT, direction TEXT, consensus_direction TEXT, manager_delta_shares INTEGER, manager_delta_value REAL, consensus_count INTEGER, report_date TEXT, detected_at TEXT)"
        )
        conn.execute("INSERT INTO managers(id, name) VALUES (1, 'Legacy Manager')")
        conn.execute(
            "INSERT INTO contrarian_signals(signal_id, manager_id, cusip, name_of_issuer, direction, consensus_direction, manager_delta_shares, manager_delta_value, consensus_count, report_date, detected_at) VALUES (1, 1, 'AAA111111', 'Example Corp', 'SELL', 'BUY', -10, -250.0, 3, '2024-05-01', '2024-05-01T09:00:00')"
        )
        conn.commit()
    finally:
        conn.close()

    monkeypatch.setenv("DB_PATH", str(db_path))
    response = asyncio.run(
        _request("/api/signals/contrarian", params={"report_date": "2024-05-01"})
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload[0]["manager_name"] == "Legacy Manager"
