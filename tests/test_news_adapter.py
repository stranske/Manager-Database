from contextlib import asynccontextmanager
from types import SimpleNamespace

import httpx
import pytest

from adapters import news


@pytest.mark.asyncio
async def test_fetch_rss_parses_entries_filters_by_since_and_logs_calls(monkeypatch):
    entries = [
        {
            "title": "New Enforcement Action",
            "link": "https://example.test/a",
            "summary": "Summary A",
            "published": "Wed, 03 Jan 2026 10:30:00 GMT",
        },
        {
            "title": "Old Item",
            "link": "https://example.test/b",
            "summary": "Summary B",
            "published": "Wed, 01 Jan 2025 10:30:00 GMT",
        },
    ]
    called_endpoints = []
    log_calls = []
    parsed_payloads = []

    @asynccontextmanager
    async def dummy_tracked_call(source, endpoint, **_kwargs):
        called_endpoints.append((source, endpoint))

        def _log(resp):
            log_calls.append(resp.status_code)

        yield _log

    class DummyClient:
        def __init__(self, *args, **kwargs):
            _ = args, kwargs

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def get(self, url):
            return httpx.Response(200, request=httpx.Request("GET", url), text="<xml/>")

    monkeypatch.setattr(news.httpx, "AsyncClient", DummyClient)
    monkeypatch.setattr(news, "tracked_call", dummy_tracked_call)

    def parse_feed(payload):
        parsed_payloads.append(payload)
        return SimpleNamespace(entries=entries, bozo=False)

    monkeypatch.setattr(news, "feedparser", SimpleNamespace(parse=parse_feed))
    monkeypatch.setenv(
        "NEWS_RSS_FEEDS",
        "https://feed-one.test/rss, https://feed-two.test/rss",
    )

    items = await news._fetch_rss("2026-01-01T00:00:00")

    assert len(items) == 2
    assert {item["url"] for item in items} == {"https://example.test/a"}
    assert all(item["headline"] == "New Enforcement Action" for item in items)
    assert all(item["source"] == "rss" for item in items)
    assert all(item["body_snippet"] == "Summary A" for item in items)
    assert called_endpoints == [
        ("rss", "https://feed-one.test/rss"),
        ("rss", "https://feed-two.test/rss"),
    ]
    assert log_calls == [200, 200]
    assert parsed_payloads == [b"<xml/>", b"<xml/>"]


def test_configured_rss_feeds_uses_defaults_when_env_missing(monkeypatch):
    monkeypatch.delenv("NEWS_RSS_FEEDS", raising=False)
    assert news._configured_rss_feeds() == list(news.DEFAULT_RSS_FEEDS)
