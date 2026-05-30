from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import pytest

from alerts.db import ensure_alert_tables, serialize_json
from etl import digest_flow
from etl.daily_diff_flow import daily_diff_flow
from tools.run_contract import write_artifact_bundle


def _seed_diff_db(db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "CREATE TABLE managers (manager_id INTEGER PRIMARY KEY, name TEXT, cik TEXT UNIQUE)"
        )
        conn.execute(
            "CREATE TABLE filings ("
            "filing_id INTEGER PRIMARY KEY, manager_id INTEGER, "
            "type TEXT, filed_date TEXT, source TEXT, raw_key TEXT)"
        )
        conn.execute(
            "CREATE TABLE holdings ("
            "holding_id INTEGER PRIMARY KEY AUTOINCREMENT, "
            "filing_id INTEGER, cusip TEXT, name_of_issuer TEXT, "
            "shares INTEGER, value_usd REAL)"
        )
        conn.execute("INSERT INTO managers(manager_id, name, cik) VALUES (1, 'TestFund', '0')")
        conn.executemany(
            "INSERT INTO filings(filing_id, manager_id, type, filed_date, source) "
            "VALUES (?, ?, ?, ?, ?)",
            [
                (101, 1, "13F-HR", "2024-01-01", "edgar"),
                (102, 1, "13F-HR", "2024-04-01", "edgar"),
            ],
        )
        conn.executemany(
            "INSERT INTO holdings(filing_id, cusip, name_of_issuer, shares, value_usd) "
            "VALUES (?, ?, ?, ?, ?)",
            [
                (101, "AAA", "CorpA", 100, 1000),
                (102, "AAA", "CorpA", 120, 1200),
            ],
        )
        conn.commit()
    finally:
        conn.close()


def _seed_digest_db(db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("CREATE TABLE managers (manager_id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
        conn.execute("""CREATE TABLE filings (
                filing_id INTEGER PRIMARY KEY,
                manager_id INTEGER NOT NULL,
                type TEXT NOT NULL,
                filed_date TEXT,
                source TEXT NOT NULL,
                url TEXT,
                created_at TEXT
            )""")
        conn.execute("""CREATE TABLE news_items (
                news_id INTEGER PRIMARY KEY,
                manager_id INTEGER,
                published_at TEXT NOT NULL,
                source TEXT NOT NULL,
                headline TEXT NOT NULL,
                url TEXT,
                body_snippet TEXT,
                topics TEXT DEFAULT '[]',
                confidence REAL
            )""")
        conn.execute("INSERT INTO managers(manager_id, name) VALUES (1, 'Alpha Capital')")
        conn.execute(
            """INSERT INTO filings(filing_id, manager_id, type, filed_date, source, url, created_at)
               VALUES (10, 1, '13F-HR', '2026-05-08', 'edgar', 'https://filing.test/10',
                       '2026-05-08T20:00:00+00:00')"""
        )
        conn.execute(
            """INSERT INTO news_items(news_id, manager_id, published_at, source, headline, url)
               VALUES (20, 1, '2026-05-08T21:00:00+00:00', 'rss',
                       'Alpha Capital opens new office', 'https://news.test/20')"""
        )
        ensure_alert_tables(conn)
        conn.execute(
            """INSERT INTO alert_rules(rule_id, name, event_type, condition_json, channels,
                                       enabled, manager_id, created_by)
               VALUES (30, 'Large filing alert', 'new_filing', '{}', '["streamlit"]', 1, 1, 'test')"""
        )
        conn.execute(
            """INSERT INTO alert_history(alert_id, rule_id, fired_at, event_type, payload_json,
                                         delivered_channels, acknowledged)
               VALUES (40, 30, '2026-05-08T22:00:00+00:00', 'new_filing', ?, '[]', 0)""",
            (serialize_json({"summary": "New 13F received", "type": "13F-HR"}),),
        )
        conn.commit()
    finally:
        conn.close()


def test_bundle_roundtrip(tmp_path: Path) -> None:
    refs = write_artifact_bundle(
        "run-1",
        "daily_diff",
        {"deltas.csv": "cusip\nAAA\n", "notes.txt": b"local only\n"},
        inputs={"date": "2024-05-01"},
        root=tmp_path / "artifacts",
    )

    manifest_path = tmp_path / "artifacts" / "daily_diff" / "run-1" / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    assert manifest["run_id"] == "run-1"
    assert manifest["tool"] == "daily_diff"
    assert manifest["inputs"] == {"date": "2024-05-01"}
    assert manifest["files"] == refs
    for item in manifest["files"]:
        payload = Path(item["path"]).read_bytes()
        assert item["sha256"] == hashlib.sha256(payload).hexdigest()
        assert item["bytes"] == len(payload)


def test_daily_diff_writes_manifest(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    db_path = tmp_path / "dev.db"
    _seed_diff_db(db_path)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.delenv("DB_URL", raising=False)

    result = daily_diff_flow.fn(date="2024-05-01")

    manifest_path = tmp_path / "artifacts" / "daily_diff" / result.run_id / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    csv_entry = next(item for item in manifest["files"] if item["name"] == "deltas.csv")
    csv_path = Path(csv_entry["path"])

    assert result.artifacts == manifest["files"]
    assert csv_path.exists()
    assert csv_entry["sha256"] == hashlib.sha256(csv_path.read_bytes()).hexdigest()
    assert "cusip,name_of_issuer,delta_type,shares_prev,shares_curr,value_prev,value_curr" in (
        csv_path.read_text(encoding="utf-8").splitlines()[0]
    )


@pytest.mark.asyncio
async def test_digest_flow_bundle_default_and_legacy_output_override(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "digest.db"
    _seed_digest_db(db_path)
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("ALERT_EMAIL_FROM", raising=False)
    monkeypatch.delenv("ALERT_EMAIL_TO", raising=False)
    monkeypatch.delenv("DIGEST_OUTPUT_PATH", raising=False)

    result = await digest_flow.digest_flow.fn(
        db_path=str(db_path),
        lookback_hours=24,
        dry_run=True,
        now=datetime(2026, 5, 9, 0, 0, tzinfo=UTC),
    )

    digest_entry = next(item for item in result["artifacts"] if item["name"] == "digest.txt")
    digest_path = Path(digest_entry["path"])
    assert digest_path.exists()
    assert digest_entry["sha256"] == hashlib.sha256(digest_path.read_bytes()).hexdigest()

    legacy_path = tmp_path / "legacy-digest.txt"
    monkeypatch.setenv("DIGEST_OUTPUT_PATH", str(legacy_path))
    override_result = await digest_flow.digest_flow.fn(
        db_path=str(db_path),
        lookback_hours=24,
        dry_run=True,
        now=datetime(2026, 5, 9, 0, 0, tzinfo=UTC),
    )

    assert legacy_path.exists()
    assert "Alpha Capital" in legacy_path.read_text(encoding="utf-8")
    assert override_result["artifacts"] == []
