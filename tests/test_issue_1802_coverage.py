"""Regression coverage for the ranked #1802 coverage targets."""

import asyncio
import datetime as dt
import sqlite3

import pandas as pd
import pytest
import streamlit as st

from etl import digest_flow, ingest_flow
from ui import daily_report


def _digest_with_content() -> digest_flow.DigestDocument:
    return digest_flow.DigestDocument(
        generated_at=dt.datetime(2026, 1, 2, tzinfo=dt.UTC),
        lookback_hours=12,
        filings=[digest_flow.DigestFiling("A", "13F", "2026-01-01", "SEC")],
        news=[digest_flow.DigestNewsItem("B", "News", "Wire", "2026-01-02")],
        alerts=[digest_flow.DigestAlert("Rule", "event", "2026-01-02", "Summary", "C")],
    )


def test_digest_rendering_email_and_csv_helpers_cover_empty_and_populated_paths():
    populated = _digest_with_content()
    assert "B: News" in digest_flow.render_digest_plain_text(populated)
    assert "[C]" in digest_flow.render_digest_plain_text(populated)
    assert "<h3>News (1)</h3>" in digest_flow.render_digest_html(populated)
    assert "No filings" in digest_flow.render_digest_plain_text(
        digest_flow.DigestDocument(dt.datetime(2026, 1, 2, tzinfo=dt.UTC), 1)
    )
    assert "No filings" in digest_flow.render_digest_html(
        digest_flow.DigestDocument(dt.datetime(2026, 1, 2, tzinfo=dt.UTC), 1)
    )
    message = digest_flow.build_digest_email_message(
        populated, sender="sender@example.test", recipients=["one@example.test", "two@example.test"]
    )
    assert message["To"] == "one@example.test, two@example.test"
    assert digest_flow._split_csv(" one@example.test, , two@example.test ") == [
        "one@example.test",
        "two@example.test",
    ]
    assert digest_flow._split_csv(None) == []


class _Response:
    def __init__(self, status_code: int):
        self.status_code = status_code


class _SendGridClient:
    def __init__(self, status_code: int):
        self.status_code = status_code
        self.kwargs = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False

    async def post(self, *_args, **kwargs):
        self.kwargs = kwargs
        return _Response(self.status_code)


def test_digest_delivery_covers_sendgrid_statuses_smtp_and_flow_artifact(monkeypatch, tmp_path):
    digest = _digest_with_content()
    monkeypatch.setenv("DIGEST_EMAIL_FROM", "sender@example.test")
    monkeypatch.setenv("DIGEST_EMAIL_TO", "reader@example.test")
    monkeypatch.setenv("DIGEST_EMAIL_PROVIDER", "sendgrid")
    monkeypatch.setenv("SENDGRID_API_KEY", "test-key")
    client = _SendGridClient(202)
    monkeypatch.setattr(digest_flow.httpx, "AsyncClient", lambda **_kwargs: client)
    assert asyncio.run(digest_flow.deliver_digest_email(digest)).success is True
    assert client.kwargs["json"]["personalizations"][0]["to"] == [{"email": "reader@example.test"}]
    monkeypatch.setattr(digest_flow.httpx, "AsyncClient", lambda **_kwargs: _SendGridClient(500))
    assert "HTTP 500" in asyncio.run(digest_flow.deliver_digest_email(digest)).error_message

    monkeypatch.setenv("DIGEST_EMAIL_PROVIDER", "smtp")
    monkeypatch.setenv("SMTP_HOST", "smtp.example.test")
    sent = []
    monkeypatch.setattr(
        digest_flow, "send_email_message_via_smtp", lambda message, **kwargs: sent.append(kwargs)
    )
    assert asyncio.run(digest_flow.deliver_digest_email(digest)).success is True
    assert sent[0]["host"] == "smtp.example.test"

    conn = sqlite3.connect(":memory:")
    monkeypatch.setattr(digest_flow, "connect_db", lambda _path: conn)
    monkeypatch.setattr(
        digest_flow, "build_digest", type("Task", (), {"fn": lambda *_args: digest})()
    )
    output = tmp_path / "digest.txt"
    result = asyncio.run(
        digest_flow.digest_flow.fn(output_path=str(output), dry_run=True, now=digest.generated_at)
    )
    assert output.read_text() == digest_flow.render_digest_plain_text(digest)
    assert result["rendered"] == output.read_text()
    assert result["delivery"]["skipped"] is True


def test_daily_report_helpers_and_missing_source_paths(monkeypatch):
    assert daily_report.parse_topics(None) == []
    assert daily_report.parse_topics("  ") == []
    assert "color:#666" in daily_report.topic_badges(None)
    assert "#0ea5e9" in daily_report.topic_badges("one,two")
    assert daily_report.headline_markdown("x", None) == "x"
    assert "→ $0.00" in daily_report.format_value_delta(1, 1)
    assert "0.0%" in daily_report.format_percent_change(1, 1)
    assert daily_report._is_missing_report_source_error(Exception("undefinedtable"))

    conn = sqlite3.connect(":memory:")
    monkeypatch.setattr(daily_report, "connect_db", lambda: conn)
    monkeypatch.setattr(
        daily_report.pd,
        "read_sql_query",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    st.cache_data.clear()
    assert daily_report.load_news("2026-01-01").empty
    assert daily_report.load_activism_events("2026-01-01").empty


def test_daily_report_dates_skip_absent_tables_and_choose_nearest(monkeypatch):
    conn = sqlite3.connect(":memory:")
    responses = iter(
        [
            pd.DataFrame({"report_date": ["2026-01-03"]}),
            RuntimeError("no such table: filings"),
            pd.DataFrame({"report_date": ["2026-01-04"]}),
            pd.DataFrame({"report_date": [None]}),
            pd.DataFrame({"report_date": ["2026-01-01", "2026-01-05"]}),
            RuntimeError("no such table: filings"),
            pd.DataFrame({"report_date": ["2026-01-03"]}),
            pd.DataFrame({"report_date": []}),
        ]
    )
    monkeypatch.setattr(daily_report, "connect_db", lambda: conn)

    def read_sql(*_args, **_kwargs):
        result = next(responses)
        if isinstance(result, Exception):
            raise result
        return result

    monkeypatch.setattr(daily_report.pd, "read_sql_query", read_sql)
    st.cache_data.clear()
    assert daily_report.latest_report_date() == dt.date(2026, 1, 4)
    # Ties prefer the newer available report.
    assert daily_report.nearest_report_date("2026-01-04") == dt.date(2026, 1, 5)


def test_daily_report_main_covers_populated_secondary_tabs(monkeypatch):
    class Tab:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    class Column:
        def metric(self, *_args, **_kwargs):
            return None

    markdown = []
    dataframes = []
    monkeypatch.setattr(daily_report, "require_login", lambda: True)
    monkeypatch.setattr(daily_report, "latest_report_date", lambda: dt.date(2026, 1, 2))
    monkeypatch.setattr(
        daily_report,
        "load_diffs",
        lambda _date: pd.DataFrame(
            {
                "manager_name": [],
                "delta_type": [],
                "cusip": [],
                "name_of_issuer": [],
                "shares_prev": [],
                "shares_curr": [],
                "value_prev": [],
                "value_curr": [],
            }
        ),
    )
    monkeypatch.setattr(daily_report, "load_contrarian_signals", lambda _date: pd.DataFrame())
    monkeypatch.setattr(
        daily_report,
        "load_activism_events",
        lambda _date: pd.DataFrame(
            [
                {
                    "event_type": "initial_stake",
                    "manager_name": None,
                    "subject_company": None,
                    "ownership_pct": 5,
                    "previous_pct": 0,
                    "filed_date": "2026-01-02",
                }
            ]
        ),
    )
    monkeypatch.setattr(
        daily_report,
        "load_crowded_trades",
        lambda _date: pd.DataFrame(
            [
                {
                    "cusip": "ABC",
                    "name_of_issuer": "Issuer",
                    "manager_count": 2,
                    "total_value_usd": 4,
                    "avg_conviction_pct": 3,
                    "manager_names": ["A", "B"],
                }
            ]
        ),
    )
    monkeypatch.setattr(
        daily_report,
        "load_news",
        lambda _date: pd.DataFrame(
            [
                {
                    "headline": "Headline",
                    "url": "https://example.test",
                    "published_at": "2026-01-02T10:00:00",
                    "source": "Wire",
                    "topics": "macro",
                    "manager_name": "A",
                }
            ]
        ),
    )
    monkeypatch.setattr(
        daily_report.st, "date_input", lambda *_args, **_kwargs: dt.date(2026, 1, 1)
    )
    monkeypatch.setattr(daily_report.st, "button", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(daily_report.st, "tabs", lambda _labels: [Tab(), Tab(), Tab()])
    monkeypatch.setattr(daily_report.st, "columns", lambda _n: [Column() for _ in range(5)])
    monkeypatch.setattr(
        daily_report.st,
        "selectbox",
        lambda label, *_args, **_kwargs: "Total value" if "Sort" in label else "All topics",
    )
    monkeypatch.setattr(
        daily_report.st, "markdown", lambda value, **_kwargs: markdown.append(value)
    )
    monkeypatch.setattr(
        daily_report.st,
        "dataframe",
        lambda value, *_args, **_kwargs: dataframes.append(value),
    )
    monkeypatch.setattr(daily_report.st, "download_button", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(daily_report.st, "caption", lambda *_args, **_kwargs: None)
    daily_report.main()
    assert any("Linked Headlines" in value for value in markdown)
    assert any("Headline" in value for value in markdown)
    assert any("initial_stake" in value and "Unknown" in value for value in markdown)
    assert any(
        isinstance(value, pd.DataFrame)
        and value["CUSIP"].eq("ABC").any()
        and value["Issuer"].eq("Issuer").any()
        for value in dataframes
    )


def test_ingest_helpers_cover_authority_hashes_and_canonical_holdings(monkeypatch):
    conn = sqlite3.connect(":memory:")
    conn.executescript("""
        CREATE TABLE filings (filing_id INTEGER PRIMARY KEY, manager_id INTEGER, filed_date TEXT, type TEXT);
        CREATE TABLE holdings (filing_id INTEGER, cusip TEXT, name_of_issuer TEXT, shares INTEGER,
            value_usd INTEGER, content_hash TEXT, knowledge_time TEXT, version INTEGER, superseded_at TEXT);
        INSERT INTO filings VALUES (1, 7, '2026-01-01', '13F-HR');
        INSERT INTO filings VALUES (2, 7, '2026-01-01', '13F-HR/A');
        """)
    rows = [{"cusip": "ABC", "nameOfIssuer": "Issuer", "sshPrnamt": 3, "value": 4}]
    assert ingest_flow._filing_type([], {}, "uk") == "unknown"
    assert ingest_flow._filing_type([{}], {}, "uk") == "unknown"
    assert ingest_flow._filing_type([{"form": "CS01"}], {}, "uk") == "CS01"
    assert ingest_flow._filing_type([], {"form": "13D"}, "us") == "13D"
    assert (
        ingest_flow._insert_holdings_rows(
            conn,
            filing_id=1,
            manager_id=7,
            identifier="id",
            external_id="x",
            filed_date=None,
            parsed_rows=rows,
            jurisdiction="us",
            knowledge_time="now",
            version=2,
            content_hash="hash",
        )
        == 1
    )
    assert ingest_flow._current_content_hash(conn, filing_id=1) == "hash"
    assert (
        ingest_flow._authority_filing_id_for_period(
            conn,
            manager_id=7,
            event_time="2026-01-01",
            period_expr="filed_date",
            filing_columns={"filing_id", "filed_date", "type"},
        )
        == 2
    )
    assert (
        ingest_flow._supersede_prior_period_holdings(
            conn, manager_id=7, filing_id=2, knowledge_time="later"
        )
        is True
    )
    assert conn.execute("SELECT superseded_at FROM holdings").fetchone()[0] == "later"
    monkeypatch.setenv("MAX_RESULTS_IN_MEMORY", "invalid")
    assert ingest_flow._max_results_in_memory() == 100000
    monkeypatch.setenv("MAX_RESULTS_IN_MEMORY", "0")
    assert ingest_flow._max_results_in_memory() == 100000


def test_ingest_flow_collects_rows_handles_per_identifier_failures_and_rejects_bad_fetcher(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(ingest_flow, "RAW_DIR", tmp_path)

    async def fetch(identifier, _since):
        if identifier == "empty":
            raise UserWarning("none")
        if identifier == "bad":
            raise RuntimeError("broken")
        return [{"identifier": identifier}]

    result = asyncio.run(
        ingest_flow.ingest_flow.fn(
            jurisdiction="us", identifiers=["ok", "empty", "bad"], since="2026-01-01", fetcher=fetch
        )
    )
    assert result == [{"identifier": "ok"}]
    assert (tmp_path / "parsed.json").read_text() == '[{"identifier": "ok"}]'
    with pytest.raises(TypeError, match="fetcher must be callable"):
        asyncio.run(ingest_flow.ingest_flow.fn(jurisdiction="us", identifiers=[], fetcher=object()))
