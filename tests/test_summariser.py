import datetime as dt
import sqlite3
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

import pytest

from etl.daily_diff_flow import _ensure_daily_diffs_table
from etl.summariser_flow import summarise, summariser_flow


def setup_db(path: Path) -> str:
    conn = sqlite3.connect(path)
    conn.execute("""CREATE TABLE managers (
            manager_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            cik TEXT UNIQUE
        )""")
    conn.execute(
        "INSERT INTO managers(manager_id, name, cik) VALUES (?, ?, ?)",
        (1, "Example Manager", "1"),
    )
    _ensure_daily_diffs_table(conn)
    conn.execute(
        "INSERT INTO daily_diffs "
        "(manager_id, report_date, cusip, name_of_issuer, delta_type) "
        "VALUES (?, ?, ?, ?, ?)",
        (1, "2024-01-02", "AAA", "Alpha Corp", "ADD"),
    )
    conn.execute(
        "INSERT INTO daily_diffs "
        "(manager_id, report_date, cusip, name_of_issuer, delta_type) "
        "VALUES (?, ?, ?, ?, ?)",
        (1, "2024-01-02", "BBB", "Beta Corp", "EXIT"),
    )
    conn.commit()
    conn.close()
    return str(path)


def setup_empty_db(path: Path) -> str:
    conn = sqlite3.connect(path)
    conn.execute("""CREATE TABLE managers (
            manager_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            cik TEXT UNIQUE
        )""")
    _ensure_daily_diffs_table(conn)
    conn.commit()
    conn.close()
    return str(path)


@pytest.mark.asyncio
async def test_summarise(tmp_path, monkeypatch):
    db_file = tmp_path / "dev.db"
    setup_db(db_file)
    monkeypatch.setenv("DB_PATH", str(db_file))
    result = await summarise.fn("2024-01-02")
    assert result == "2 changes on 2024-01-02"


@pytest.mark.asyncio
async def test_summarise_against_canonical_daily_diffs(tmp_path, monkeypatch):
    db_file = tmp_path / "dev.db"
    setup_db(db_file)
    monkeypatch.setenv("DB_PATH", str(db_file))

    result = await summarise.fn("2024-01-02")

    assert result == "2 changes on 2024-01-02"


@pytest.mark.asyncio
async def test_summarise_uses_canonical_postgres_query(monkeypatch):
    captured = {}

    class StrictPostgresConn:
        def close(self):
            captured["closed"] = True

    def fake_read_sql_query(sql, conn, params):
        assert isinstance(conn, StrictPostgresConn)
        captured["sql"] = sql
        captured["params"] = params
        import pandas as pd

        return pd.DataFrame(
            [{"manager_name": "Example Manager", "cusip": "AAA", "delta_type": "ADD"}]
        )

    monkeypatch.setattr("etl.summariser_flow.connect_db", lambda: StrictPostgresConn())
    monkeypatch.setattr("etl.summariser_flow.pd.read_sql_query", fake_read_sql_query)

    result = await summarise.fn("2024-01-02")

    assert result == "1 changes on 2024-01-02"
    legacy_table = "daily_" + "diff "
    legacy_table_with_newline = " daily_" + "diff\n"
    legacy_column = "chan" + "ge"
    assert "daily_diffs" in captured["sql"]
    assert legacy_table not in captured["sql"]
    assert legacy_table_with_newline not in captured["sql"]
    assert legacy_column not in captured["sql"]
    assert "report_date = %s" in captured["sql"]
    assert captured["params"] == ("2024-01-02",)
    assert captured["closed"] is True


@pytest.mark.asyncio
async def test_summarise_with_no_rows(tmp_path, monkeypatch):
    db_file = tmp_path / "dev.db"
    setup_empty_db(db_file)
    monkeypatch.setenv("DB_PATH", str(db_file))
    # Validate zero-row summaries return a stable message.
    result = await summarise.fn("2024-01-05")
    assert result == "0 changes on 2024-01-05"


@pytest.mark.asyncio
async def test_summarise_posts_to_slack(monkeypatch, tmp_path):
    db_file = tmp_path / "dev.db"
    setup_db(db_file)
    monkeypatch.setenv("DB_PATH", str(db_file))
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://example.test/slack")
    calls = {"posted": False, "logged": False}

    def fake_tracked_call(source, endpoint):
        # Capture the webhook and ensure the context manager yields a logger.
        assert source == "slack"
        assert endpoint == "https://example.test/slack"
        calls["logged"] = True

        def log(_resp):
            calls["posted"] = True

        class _Context:
            async def __aenter__(self):
                return log

            async def __aexit__(self, exc_type, exc, tb):
                return False

        return _Context()

    def fake_post(url, json):
        assert url == "https://example.test/slack"
        assert "text" in json
        return type("Resp", (), {"status_code": 200, "content": b"ok"})()

    monkeypatch.setattr("etl.summariser_flow.tracked_call", fake_tracked_call)
    monkeypatch.setattr("etl.summariser_flow.requests.post", fake_post)

    result = await summarise.fn("2024-01-02")

    assert result == "2 changes on 2024-01-02"
    assert calls == {"posted": True, "logged": True}


@pytest.mark.asyncio
async def test_summariser_flow_defaults_to_yesterday(monkeypatch):
    captured = {"date": None}

    async def fake_summarise(date: str) -> str:
        captured["date"] = date
        return "ok"

    class DateShim:
        @staticmethod
        def today() -> dt.date:
            return dt.date(2024, 1, 3)

    class DateTimeShim:
        date = DateShim
        timedelta = dt.timedelta

    # Freeze "today" so the default date path is deterministic.
    monkeypatch.setattr("etl.summariser_flow.dt", DateTimeShim)
    monkeypatch.setattr("etl.summariser_flow.summarise", fake_summarise)

    result = await summariser_flow.fn()

    assert result == "ok"
    assert captured["date"] == "2024-01-02"
