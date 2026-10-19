"""Focused, discriminating coverage for coverage-autopilot issue #1835."""

from __future__ import annotations

import asyncio
import logging
import sqlite3
from datetime import UTC, datetime
from types import SimpleNamespace

import httpx
import pytest

from api import alerts as alerts_api
from api import data as data_api
from api import handler, parser
from api.search import SearchResult
from etl import logging_setup, news_flow
from ui import alerts as alerts_ui
from ui import search as search_ui


def test_news_helpers_normalize_aliases_timestamps_and_sources(monkeypatch):
    """News watermark inputs accept legacy aliases but reject malformed dates."""
    monkeypatch.setenv("NEWS_SOURCES", " rss, , gdelt ")

    assert news_flow._normalize_aliases(None) == []
    assert news_flow._normalize_aliases(7) == ["7"]
    assert news_flow._normalize_aliases("  ") == []
    assert news_flow._normalize_aliases('[" Alpha ", ""]') == ["Alpha"]
    assert news_flow._normalize_aliases('{"Alpha", "Beta"}') == ["Alpha", "Beta"]
    assert news_flow._resolve_sources(None) == ["rss", "gdelt"]
    assert news_flow._resolve_sources(["manual"]) == ["manual"]
    assert news_flow._parse_iso_timestamp(None) is None
    assert news_flow._parse_iso_timestamp(" ") is None
    assert news_flow._parse_iso_timestamp("bad") is None
    assert news_flow._parse_iso_timestamp("2026-01-01T00:00:00Z") == datetime(
        2026, 1, 1, tzinfo=UTC
    )
    assert news_flow._latest_published_at([{"published_at": "bad"}]) is None


def test_news_watermark_fallback_and_update_paths():
    conn = sqlite3.connect(":memory:")
    try:
        conn.execute("CREATE TABLE news_items (published_at TEXT, source TEXT)")
        conn.execute(
            "CREATE TABLE watermarks (source TEXT PRIMARY KEY, latest_published_at TEXT, updated_at TEXT)"
        )
        assert news_flow._fallback_source_since_from_news(conn, "rss") is None
        conn.executemany(
            "INSERT INTO news_items (published_at, source) VALUES (?, ?)",
            [
                ("2026-01-01T00:00:00Z", "rss"),
                ("2026-01-03T00:00:00Z", "rss"),
                ("2026-01-02T00:00:00Z", "gdelt"),
            ],
        )
        assert news_flow._fallback_source_since_from_news(conn, "rss") == "2026-01-03T00:00:00Z"
        assert news_flow.resolve_source_since.fn("gdelt", None, conn) == "2026-01-02T00:00:00Z"
        assert news_flow.update_source_watermark.fn("rss", [], conn) is None
        assert (
            news_flow.update_source_watermark.fn(
                "rss", [{"published_at": "2026-01-02T00:00:00Z"}], conn
            )
            == "2026-01-02T00:00:00+00:00"
        )
        assert (
            news_flow.update_source_watermark.fn(
                "rss", [{"published_at": "2026-01-01T00:00:00Z"}], conn
            )
            == "2026-01-02T00:00:00+00:00"
        )
    finally:
        conn.close()


def test_news_fallback_handles_missing_news_table():
    conn = sqlite3.connect(":memory:")
    try:
        assert news_flow._fallback_source_since_from_news(conn, "rss") is None
    finally:
        conn.close()


def test_logging_cloudwatch_idempotence_and_emit_failure(monkeypatch):
    class AlreadyExists(Exception):
        pass

    class Client:
        exceptions = SimpleNamespace(ResourceAlreadyExistsException=AlreadyExists)

        def create_log_group(self, **_kwargs):
            raise AlreadyExists()

        def create_log_stream(self, **_kwargs):
            raise AlreadyExists()

        def put_log_events(self, **_kwargs):
            raise RuntimeError("offline")

    monkeypatch.setattr(logging_setup.boto3, "client", lambda *_args, **_kwargs: Client())
    handler_instance = logging_setup.CloudWatchHandler("group", "stream")
    handler_instance.handleError = lambda record: setattr(record, "handled", True)  # type: ignore[method-assign]
    record = logging.LogRecord("coverage", logging.INFO, __file__, 1, "message", (), None)
    handler_instance.emit(record)
    assert record.handled is True

    logging_setup.reset_logging()
    logging_setup.configure_logging("round-1835")
    handler_count = len(logging.getLogger().handlers)
    logging_setup.configure_logging("ignored")
    assert len(logging.getLogger().handlers) == handler_count
    logging_setup.reset_logging()


def test_parser_handler_and_data_cover_success_and_failure_paths(monkeypatch):
    assert parser.parse_response(None).ok is False
    assert parser.parse_response(b'{"data": [1]}').data == [1]
    assert parser.parse_response('{"missing": 1}').ok is False
    assert handler.handleRequest('{"data": {"ok": true}}').body == b'{"data":{"ok":true}}'

    monkeypatch.setattr(
        handler, "parseResponse", lambda _raw: (_ for _ in ()).throw(RuntimeError())
    )
    assert handler.handleRequest("anything").status_code == 400

    async def request_failure(_url: str) -> str:
        raise httpx.HTTPStatusError(
            "bad", request=httpx.Request("GET", "x"), response=httpx.Response(500)
        )

    monkeypatch.setenv("DATA_API_URL", "https://upstream.invalid/data")
    monkeypatch.setattr(data_api, "_fetch_upstream_payload", request_failure)
    assert asyncio.run(data_api.data_endpoint()).status_code == 502


def test_alert_filter_helpers_cover_boolean_and_actor_boundaries():
    conn = sqlite3.connect(":memory:")
    try:
        where, params = alerts_api._history_where(
            conn,
            since=datetime(2026, 1, 1, tzinfo=UTC),
            until=datetime(2026, 1, 2, tzinfo=UTC),
            acknowledged=False,
            event_type="news_spike",
            prefix="ah.",
        )
    finally:
        conn.close()
    assert (
        where
        == "ah.fired_at >= ? AND ah.fired_at < ? AND ah.acknowledged = ? AND ah.event_type = ?"
    )
    assert params[-2:] == [0, "news_spike"]
    assert alerts_api._normalize_actor(" ui ") == "ui"
    with pytest.raises(Exception, match="must not be empty"):
        alerts_api._normalize_actor(" ")


class _SearchStreamlit:
    def __init__(self, query: str, results: list[SearchResult]):
        self.query = query
        self.results = results
        self.infos: list[str] = []
        self.metrics: list[tuple[str, int]] = []

    def header(self, _text):
        pass

    def text_input(self, _text):
        return self.query

    def number_input(self, *_args, **_kwargs):
        return 3

    def info(self, text):
        self.infos.append(text)

    def subheader(self, _text):
        pass

    def columns(self, count):
        return [self] * count

    def metric(self, label, value):
        self.metrics.append((label, value))

    def caption(self, _text):
        pass

    def expander(self, *_args, **_kwargs):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def markdown(self, *_args, **_kwargs):
        pass

    def write(self, *_args, **_kwargs):
        pass

    def link_button(self, *_args, **_kwargs):
        pass

    def divider(self):
        pass


def test_search_main_handles_empty_and_renders_grouped_results(monkeypatch):
    class Conn:
        def close(self):
            self.closed = True

    conn = Conn()
    empty_ui = _SearchStreamlit("alpha", [])
    monkeypatch.setattr(search_ui, "st", empty_ui)
    monkeypatch.setattr(search_ui, "require_login", lambda: True)
    monkeypatch.setattr(search_ui, "connect_db", lambda: conn)
    monkeypatch.setattr(search_ui, "universal_search", lambda *_args: [])
    search_ui.main()
    assert empty_ui.infos == ["No results found."]

    result_ui = _SearchStreamlit("alpha", [])
    result = SearchResult(
        entity_type="news",
        entity_id=1,
        headline="Headline",
        snippet="Snippet",
        url="https://example.test",
        relevance=0.8,
        manager_name=None,
        timestamp=None,
    )
    monkeypatch.setattr(search_ui, "st", result_ui)
    monkeypatch.setattr(search_ui, "universal_search", lambda *_args: [result])
    search_ui.main()
    assert result_ui.metrics == [("Total", 1), ("News", 1)]


def test_alert_ui_main_and_inbox_error_paths(monkeypatch):
    class Streamlit:
        def __init__(self):
            self.calls: list[str] = []
            self.infos: list[str] = []
            self.errors: list[str] = []

        def title(self, text):
            self.calls.append(text)

        def caption(self, text):
            self.calls.append(text)

        def divider(self):
            self.calls.append("divider")

        def stop(self):
            raise RuntimeError("stopped")

        def subheader(self, _text):
            pass

        def columns(self, count):
            return [self] * count

        def selectbox(self, label, *_args, **_kwargs):
            return "all"

        def date_input(self, *_args, **_kwargs):
            return "not-a-range"

        def button(self, label, **_kwargs):
            return label == "Acknowledge All"

        def error(self, text):
            self.errors.append(text)

        def info(self, text):
            self.infos.append(text)

    fake = Streamlit()
    original_inbox = alerts_ui._render_alert_inbox
    monkeypatch.setattr(alerts_ui, "st", fake)
    monkeypatch.setattr(alerts_ui, "require_login", lambda: True)
    monkeypatch.setattr(alerts_ui, "_api_base_url", lambda: "http://api.test")
    monkeypatch.setattr(alerts_ui, "_render_rule_builder", lambda: fake.calls.append("rules"))
    monkeypatch.setattr(alerts_ui, "_render_alert_inbox", lambda: fake.calls.append("inbox"))
    monkeypatch.setattr(alerts_ui, "_render_alert_stats", lambda: fake.calls.append("stats"))
    alerts_ui.main()
    assert fake.calls == [
        "Alerts",
        "API: http://api.test",
        "rules",
        "divider",
        "inbox",
        "divider",
        "stats",
    ]

    monkeypatch.setattr(alerts_ui, "_render_alert_inbox", original_inbox)
    monkeypatch.setattr(alerts_ui, "_load_alerts", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(alerts_ui, "_api_request", lambda *_args, **_kwargs: (False, "nope"))
    alerts_ui._render_alert_inbox()
    assert fake.errors == ["Unable to acknowledge alerts: nope"]
    assert fake.infos == ["No alerts for the selected filters."]
