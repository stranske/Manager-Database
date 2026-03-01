import sqlite3

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


@pytest.mark.asyncio
async def test_fetch_news_lists_and_tags_items(monkeypatch):
    calls: dict[str, list] = {"list": [], "tag": []}

    async def fake_list_new_items(source, since):
        calls["list"].append((source, since))
        return [
            {"headline": "first", "source": source},
            {"headline": "second", "source": source},
        ]

    def fake_tag(item):
        calls["tag"].append(item["headline"])
        tagged = dict(item)
        tagged["topics"] = ["markets"]
        tagged["confidence"] = 0.8
        return tagged

    monkeypatch.setattr(news_flow.news, "list_new_items", fake_list_new_items)
    monkeypatch.setattr(news_flow.news, "tag", fake_tag)

    result = await news_flow.fetch_news.fn("rss", "2024-01-01T00:00:00Z")

    assert calls["list"] == [("rss", "2024-01-01T00:00:00Z")]
    assert calls["tag"] == ["first", "second"]
    assert result == [
        {
            "headline": "first",
            "source": "rss",
            "topics": ["markets"],
            "confidence": 0.8,
        },
        {
            "headline": "second",
            "source": "rss",
            "topics": ["markets"],
            "confidence": 0.8,
        },
    ]


@pytest.mark.asyncio
async def test_fetch_news_falls_back_to_original_item_when_tag_returns_none(monkeypatch):
    calls: dict[str, list] = {"list": []}

    async def fake_list_new_items(source, since):
        calls["list"].append((source, since))
        return [{"headline": "item", "source": source}]

    def fake_tag(item):
        return None

    monkeypatch.setattr(news_flow.news, "list_new_items", fake_list_new_items)
    monkeypatch.setattr(news_flow.news, "tag", fake_tag)

    result = await news_flow.fetch_news.fn("gdelt", None)

    assert calls["list"] == [("gdelt", None)]
    assert result == [{"headline": "item", "source": "gdelt"}]


def test_match_entities_links_items_by_name_and_alias():
    conn = sqlite3.connect(":memory:")
    try:
        conn.execute("""CREATE TABLE managers (
                manager_id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                aliases TEXT
            )""")
        conn.executemany(
            "INSERT INTO managers(manager_id, name, aliases) VALUES (?, ?, ?)",
            [
                (1, "Alpha Capital", '["AlphaCap", "Alpha"]'),
                (2, "Beta Partners", '["Beta"]'),
            ],
        )
        items = [
            {
                "headline": "Alpha Capital launches a new strategy",
                "body_snippet": "",
                "source": "rss",
            },
            {
                "headline": "Market update",
                "body_snippet": "Analysts cite Beta as a major buyer",
                "source": "rss",
            },
            {"headline": "General market recap", "body_snippet": "", "source": "rss"},
        ]

        result = news_flow.match_entities.fn(items, conn)

        assert result[0]["manager_id"] == 1
        assert result[1]["manager_id"] == 2
        assert result[2]["manager_id"] is None
    finally:
        conn.close()


def test_match_entities_supports_postgres_array_literal_aliases():
    conn = sqlite3.connect(":memory:")
    try:
        conn.execute("""CREATE TABLE managers (
                manager_id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                aliases TEXT
            )""")
        conn.execute(
            "INSERT INTO managers(manager_id, name, aliases) VALUES (?, ?, ?)",
            (10, "Gamma Advisors", '{"Gamma","GAM"}'),
        )
        items = [{"headline": "GAM updates portfolio", "body_snippet": "", "source": "gdelt"}]

        result = news_flow.match_entities.fn(items, conn)

        assert result[0]["manager_id"] == 10
    finally:
        conn.close()
