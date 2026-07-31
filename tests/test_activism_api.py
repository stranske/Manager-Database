import asyncio
import sqlite3
import sys
from pathlib import Path
from typing import Any, cast

import httpx

sys.path.append(str(Path(__file__).resolve().parents[1]))

from api.activism import query_activism_campaigns
from api.chat import app
from etl.activism_campaign_flow import materialize_activism_campaigns
from tests.route_helpers import route_paths


async def _request(path: str, params: dict | None = None):
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
        conn.execute("""CREATE TABLE activism_filings (
                filing_id INTEGER PRIMARY KEY,
                manager_id INTEGER NOT NULL,
                filing_type TEXT NOT NULL,
                subject_company TEXT NOT NULL,
                subject_cusip TEXT,
                ownership_pct REAL,
                shares INTEGER,
                group_members TEXT,
                purpose_snippet TEXT,
                filed_date TEXT NOT NULL,
                url TEXT,
                raw_key TEXT,
                created_at TEXT
            )""")
        conn.execute("""CREATE TABLE activism_events (
                event_id INTEGER PRIMARY KEY,
                manager_id INTEGER NOT NULL,
                filing_id INTEGER NOT NULL,
                event_type TEXT NOT NULL,
                subject_company TEXT NOT NULL,
                subject_cusip TEXT,
                ownership_pct REAL,
                previous_pct REAL,
                delta_pct REAL,
                threshold_crossed REAL,
                detected_at TEXT
            )""")
        conn.executemany(
            "INSERT INTO managers(manager_id, name) VALUES (?, ?)",
            [(1, "Elliott Management"), (2, "SIR Capital")],
        )
        conn.executemany(
            """INSERT INTO activism_filings(
                filing_id, manager_id, filing_type, subject_company, subject_cusip,
                ownership_pct, shares, group_members, purpose_snippet, filed_date, url, raw_key,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                (
                    10,
                    1,
                    "SC 13D",
                    "Apple Inc.",
                    "037833100",
                    5.1,
                    1000000,
                    "[]",
                    "board refresh",
                    "2024-05-01",
                    "https://sec.example/10",
                    "raw/10",
                    "2024-05-01T08:00:00",
                ),
                (
                    11,
                    1,
                    "SC 13D/A",
                    "Apple Inc.",
                    "037833100",
                    10.4,
                    1200000,
                    "[]",
                    "board refresh",
                    "2024-05-03",
                    "https://sec.example/11",
                    "raw/11",
                    "2024-05-03T08:00:00",
                ),
                (
                    12,
                    2,
                    "SC 13G",
                    "Tesla, Inc.",
                    "88160R101",
                    4.4,
                    500000,
                    "[]",
                    "passive stake",
                    "2024-05-02",
                    "https://sec.example/12",
                    "raw/12",
                    "2024-05-02T08:00:00",
                ),
            ],
        )
        conn.executemany(
            """INSERT INTO activism_events(
                event_id, manager_id, filing_id, event_type, subject_company, subject_cusip,
                ownership_pct, previous_pct, delta_pct, threshold_crossed, detected_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                (
                    100,
                    1,
                    10,
                    "initial_stake",
                    "Apple Inc.",
                    "037833100",
                    5.1,
                    None,
                    None,
                    None,
                    "2024-05-01T09:00:00",
                ),
                (
                    101,
                    1,
                    11,
                    "threshold_crossing",
                    "Apple Inc.",
                    "037833100",
                    10.4,
                    5.1,
                    5.3,
                    10.0,
                    "2024-05-03T09:00:00",
                ),
                (
                    102,
                    1,
                    11,
                    "stake_increase",
                    "Apple Inc.",
                    "037833100",
                    10.4,
                    5.1,
                    5.3,
                    None,
                    "2024-05-03T09:01:00",
                ),
                (
                    103,
                    2,
                    12,
                    "initial_stake",
                    "Tesla, Inc.",
                    "88160R101",
                    4.4,
                    None,
                    None,
                    None,
                    "2024-05-02T11:00:00",
                ),
            ],
        )
        materialize_activism_campaigns(conn)
        conn.commit()
    finally:
        conn.close()


def test_activism_router_is_registered(tmp_path, monkeypatch):
    db_path = tmp_path / "activism.db"
    _seed_db(db_path)
    monkeypatch.setenv("DB_PATH", str(db_path))

    expected_paths = {
        "/api/activism/filings",
        "/api/activism/events",
        "/api/activism/timeline/{manager_id}",
        "/api/activism/active-campaigns",
        "/api/activism/campaigns",
        "/api/activism/campaigns/{campaign_id}",
        "/api/activism/campaigns/{campaign_id}/timeline",
    }
    assert expected_paths.issubset(route_paths(app.routes))

    openapi = app.openapi()
    for path in expected_paths:
        assert "Activism" in next(iter(openapi["paths"][path].values()))["tags"]


def test_list_activism_filings_filters(tmp_path, monkeypatch):
    db_path = tmp_path / "activism.db"
    _seed_db(db_path)
    monkeypatch.setenv("DB_PATH", str(db_path))

    response = asyncio.run(
        _request(
            "/api/activism/filings",
            params={
                "manager_id": 1,
                "cusip": "037833100",
                "filing_type": "SC 13D/A",
                "since": "2024-05-02",
            },
        )
    )
    assert response.status_code == 200
    payload = response.json()
    assert [item["filing_id"] for item in payload] == [11]
    assert payload[0]["manager_name"] == "Elliott Management"
    assert payload[0]["ownership_pct"] == 10.4


def test_list_activism_events_filters(tmp_path, monkeypatch):
    db_path = tmp_path / "activism.db"
    _seed_db(db_path)
    monkeypatch.setenv("DB_PATH", str(db_path))

    response = asyncio.run(
        _request(
            "/api/activism/events",
            params={
                "manager_id": 1,
                "event_type": "threshold_crossing",
                "cusip": "037833100",
                "since": "2024-05-03",
            },
        )
    )
    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["event_id"] == 101
    assert payload[0]["threshold_crossed"] == 10.0


def test_timeline_returns_chronological_entries(tmp_path, monkeypatch):
    db_path = tmp_path / "activism.db"
    _seed_db(db_path)
    monkeypatch.setenv("DB_PATH", str(db_path))

    response = asyncio.run(_request("/api/activism/timeline/1"))
    assert response.status_code == 200
    payload = response.json()
    assert [item["date"] for item in payload] == [
        "2024-05-01",
        "2024-05-01",
        "2024-05-03",
        "2024-05-03",
        "2024-05-03",
    ]
    assert [item["type"] for item in payload] == ["event", "filing", "event", "event", "filing"]
    assert any(item["description"].startswith("stake_increase") for item in payload)


def test_active_campaigns_applies_threshold(tmp_path, monkeypatch):
    db_path = tmp_path / "activism.db"
    _seed_db(db_path)
    monkeypatch.setenv("DB_PATH", str(db_path))

    response = asyncio.run(
        _request(
            "/api/activism/active-campaigns",
            params={"min_ownership_pct": 5.0},
        )
    )
    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["subject_company"] == "Apple Inc."
    assert payload[0]["event_count"] == 3
    assert payload[0]["latest_event_type"] == "stake_increase"


def test_campaign_endpoints_return_grouped_timeline(tmp_path, monkeypatch):
    db_path = tmp_path / "activism.db"
    _seed_db(db_path)
    monkeypatch.setenv("DB_PATH", str(db_path))

    response = asyncio.run(
        _request("/api/activism/campaigns", params={"manager_id": 1, "status": "active"})
    )
    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    campaign = payload[0]
    assert campaign["target_identifier"] == "037833100"
    assert campaign["filing_count"] == 2
    assert campaign["first_filed"] == "2024-05-01"
    assert campaign["last_filed"] == "2024-05-03"

    detail = asyncio.run(_request(f"/api/activism/campaigns/{campaign['campaign_id']}"))
    assert detail.status_code == 200
    assert detail.json()["target_company"] == "Apple Inc."

    timeline = asyncio.run(_request(f"/api/activism/campaigns/{campaign['campaign_id']}/timeline"))
    assert timeline.status_code == 200
    entries = timeline.json()
    assert [entry["form_type"] for entry in entries] == ["SC 13D", "SC 13D/A", "SC 13D/A"]
    # Ordering is deterministic by filing date, so the two amendment events cannot swap.
    assert [entry["event_type"] for entry in entries] == [
        "initial_stake",
        "threshold_crossing",
        "stake_increase",
    ]
    assert [entry["event_date"] for entry in entries] == [
        "2024-05-01",
        "2024-05-03",
        "2024-05-03",
    ]


def test_campaign_list_applies_target_date_and_limit_filters(tmp_path, monkeypatch):
    db_path = tmp_path / "activism.db"
    _seed_db(db_path)
    monkeypatch.setenv("DB_PATH", str(db_path))

    by_target = asyncio.run(
        _request("/api/activism/campaigns", params={"target_identifier": "88160r101"})
    )
    assert by_target.status_code == 200
    assert [row["target_company"] for row in by_target.json()] == ["Tesla, Inc."]

    filed_from = asyncio.run(
        _request("/api/activism/campaigns", params={"filed_from": "2024-05-03"})
    )
    assert [row["target_identifier"] for row in filed_from.json()] == ["037833100"]

    filed_to = asyncio.run(_request("/api/activism/campaigns", params={"filed_to": "2024-05-01"}))
    assert [row["target_identifier"] for row in filed_to.json()] == ["037833100"]

    limited = asyncio.run(_request("/api/activism/campaigns", params={"limit": 1}))
    assert len(limited.json()) == 1


def test_campaign_query_drops_non_finite_ownership(tmp_path):
    """SQLite REAL keeps Infinity; every consumer of the query layer must see None."""
    db_path = tmp_path / "activism.db"
    _seed_db(db_path)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "UPDATE activism_campaigns SET peak_ownership_pct = 1e999, "
            "latest_ownership_pct = 1e999 WHERE target_identifier = '037833100'"
        )
        conn.commit()

        campaigns = query_activism_campaigns(conn, target_identifier="037833100")
    finally:
        conn.close()

    assert len(campaigns) == 1
    assert campaigns[0].peak_ownership_pct is None
    assert campaigns[0].latest_ownership_pct is None
