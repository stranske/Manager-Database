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


def test_configured_rss_feeds_defaults_match_sec_sources(monkeypatch):
    monkeypatch.delenv("NEWS_RSS_FEEDS", raising=False)
    assert news._configured_rss_feeds() == [
        "https://www.sec.gov/news/pressreleases.rss",
        "https://www.sec.gov/rss/litigation/litreleases.xml",
    ]


def test_configured_rss_feeds_falls_back_to_defaults_when_env_has_no_urls(monkeypatch):
    monkeypatch.setenv("NEWS_RSS_FEEDS", " , , ")
    assert news._configured_rss_feeds() == list(news.DEFAULT_RSS_FEEDS)


@pytest.mark.asyncio
async def test_list_new_items_rss_delegates_to_fetch_rss(monkeypatch):
    expected = [
        {
            "headline": "A",
            "url": "https://example.test",
            "published_at": "2026-01-01T00:00:00+00:00",
        }
    ]

    async def fake_fetch_rss(since):
        assert since == "2026-01-01T00:00:00"
        return expected

    monkeypatch.setattr(news, "_fetch_rss", fake_fetch_rss)

    result = await news.list_new_items("rss", "2026-01-01T00:00:00")

    assert result == expected


@pytest.mark.asyncio
async def test_list_new_items_gdelt_delegates_to_fetch_gdelt(monkeypatch):
    expected = [
        {
            "headline": "Alpha",
            "url": "https://example.test/alpha",
            "published_at": "2026-01-03T12:30:00+00:00",
        }
    ]

    async def fake_fetch_gdelt(since):
        assert since == "2026-01-01T00:00:00"
        return expected

    monkeypatch.setattr(news, "_fetch_gdelt", fake_fetch_gdelt)

    result = await news.list_new_items("gdelt", "2026-01-01T00:00:00")

    assert result == expected


def test_configured_gdelt_managers_reads_env(monkeypatch):
    monkeypatch.setenv("NEWS_GDELT_MANAGERS", "Alpha Capital, Beta Partners , ")
    assert news._configured_gdelt_managers() == ["Alpha Capital", "Beta Partners"]


@pytest.mark.asyncio
async def test_fetch_gdelt_parses_articles_filters_by_since_and_logs_calls(monkeypatch):
    called_endpoints = []
    log_calls = []

    @asynccontextmanager
    async def dummy_tracked_call(source, endpoint, **_kwargs):
        called_endpoints.append((source, endpoint))

        def _log(resp):
            log_calls.append(resp.status_code)

        yield _log

    responses = {
        "Alpha": {
            "articles": [
                {
                    "title": "Alpha beats estimates",
                    "url": "https://example.test/alpha",
                    "seendate": "20260103123000",
                    "socialimage": "https://img.test/alpha.jpg",
                    "domain": "example.test",
                },
                {
                    "title": "Old alpha item",
                    "url": "https://example.test/old-alpha",
                    "seendate": "20251231115959",
                    "socialimage": "",
                    "domain": "example.test",
                },
            ]
        },
        "Beta": {
            "articles": [
                {
                    "title": "Bad date item",
                    "url": "https://example.test/bad-date",
                    "seendate": "not-a-date",
                    "socialimage": "",
                    "domain": "example.test",
                },
                {
                    "title": "Beta launches new strategy",
                    "url": "https://example.test/beta",
                    "seendate": "20260104153000",
                    "socialimage": "https://img.test/beta.jpg",
                    "domain": "news.test",
                },
            ]
        },
    }

    class DummyClient:
        def __init__(self, *args, **kwargs):
            _ = args, kwargs

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def get(self, _url, params=None):
            assert params is not None
            manager = params["query"]
            payload = responses[manager]
            return httpx.Response(
                200,
                request=httpx.Request("GET", news.GDELT_DOC_API, params=params),
                json=payload,
            )

    monkeypatch.setattr(news.httpx, "AsyncClient", DummyClient)
    monkeypatch.setattr(news, "tracked_call", dummy_tracked_call)
    monkeypatch.setenv("NEWS_GDELT_MANAGERS", "Alpha,Beta")

    items = await news._fetch_gdelt("2026-01-01T00:00:00")

    assert len(items) == 2
    assert {item["headline"] for item in items} == {
        "Alpha beats estimates",
        "Beta launches new strategy",
    }
    assert all(item["source"] == "gdelt" for item in items)
    assert all(item["body_snippet"] == "" for item in items)
    assert {item["domain"] for item in items} == {"example.test", "news.test"}
    assert {item["socialimage"] for item in items} == {
        "https://img.test/alpha.jpg",
        "https://img.test/beta.jpg",
    }
    assert called_endpoints == [
        (
            "gdelt",
            "https://api.gdeltproject.org/api/v2/doc/doc?query=Alpha&mode=artlist&maxrecords=50&format=json",
        ),
        (
            "gdelt",
            "https://api.gdeltproject.org/api/v2/doc/doc?query=Beta&mode=artlist&maxrecords=50&format=json",
        ),
    ]
    assert log_calls == [200, 200]
    assert items[0]["published_at"] > items[1]["published_at"]


@pytest.mark.asyncio
async def test_fetch_gdelt_rate_limits_requests(monkeypatch):
    sleep_calls = []

    async def fake_sleep(delay):
        sleep_calls.append(delay)

    monotonic_values = iter([10.0, 10.2, 11.5])

    def fake_monotonic():
        return next(monotonic_values)

    @asynccontextmanager
    async def dummy_tracked_call(_source, _endpoint, **_kwargs):
        def _log(_resp):
            return None

        yield _log

    class DummyClient:
        def __init__(self, *args, **kwargs):
            _ = args, kwargs

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def get(self, _url, params=None):
            _ = params
            return httpx.Response(
                200,
                request=httpx.Request("GET", news.GDELT_DOC_API),
                json={"articles": []},
            )

    monkeypatch.setattr(news, "tracked_call", dummy_tracked_call)
    monkeypatch.setattr(news.httpx, "AsyncClient", DummyClient)
    monkeypatch.setattr(news, "monotonic", fake_monotonic)
    monkeypatch.setattr(news.asyncio, "sleep", fake_sleep)
    monkeypatch.setenv("NEWS_GDELT_MANAGERS", "A,B")

    await news._fetch_gdelt("2026-01-01T00:00:00")

    assert len(sleep_calls) == 1
    assert sleep_calls[0] == pytest.approx(0.8)


def test_tag_adds_topics_and_confidence():
    item = {
        "headline": "SEC enforcement action after merger bid",
        "body_snippet": "The company announced a new fund strategy with acquisition plans.",
    }

    tagged = news.tag(item)

    assert tagged is item
    assert set(tagged["topics"]) == {"regulatory", "merger", "fund_launch"}
    assert tagged["confidence"] == pytest.approx(7 / 16)


@pytest.mark.asyncio
async def test_download_strips_html_truncates_and_logs_calls(monkeypatch):
    called_endpoints = []
    log_calls = []
    html_doc = "<html><body><h1>Headline</h1><p>" + ("A" * 2100) + "</p></body></html>"

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
            return httpx.Response(200, request=httpx.Request("GET", url), text=html_doc)

    monkeypatch.setattr(news, "tracked_call", dummy_tracked_call)
    monkeypatch.setattr(news.httpx, "AsyncClient", DummyClient)

    content = await news.download({"url": "https://example.test/story", "source": "rss"})

    assert len(content) == 2000
    assert content.startswith("Headline ")
    assert called_endpoints == [("rss", "https://example.test/story")]
    assert log_calls == [200]


@pytest.mark.asyncio
async def test_download_returns_empty_string_on_http_error(monkeypatch):
    called_endpoints = []
    log_calls = []

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
            return httpx.Response(500, request=httpx.Request("GET", url), text="oops")

    monkeypatch.setattr(news, "tracked_call", dummy_tracked_call)
    monkeypatch.setattr(news.httpx, "AsyncClient", DummyClient)

    content = await news.download({"url": "https://example.test/fail", "source": "gdelt"})

    assert content == ""
    assert called_endpoints == [("gdelt", "https://example.test/fail")]
    assert log_calls == [500]
