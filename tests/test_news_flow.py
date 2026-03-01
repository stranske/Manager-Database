import pytest

import etl.news_flow as news_flow


@pytest.mark.asyncio
async def test_news_flow_uses_default_sources(monkeypatch):
    monkeypatch.setenv("NEWS_SOURCES", "rss,gdelt")

    result = await news_flow.news_flow.fn()

    assert result == {"sources": ["rss", "gdelt"], "since": None}


@pytest.mark.asyncio
async def test_news_flow_respects_explicit_sources():
    result = await news_flow.news_flow.fn(sources=["custom"], since="2024-01-01T00:00:00Z")

    assert result == {
        "sources": ["custom"],
        "since": "2024-01-01T00:00:00Z",
    }
