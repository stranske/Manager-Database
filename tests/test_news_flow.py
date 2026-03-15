import sqlite3

import pytest

import etl.news_flow as news_flow


def _create_managers_table(conn):
    conn.execute("""CREATE TABLE IF NOT EXISTS managers (
            manager_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            aliases TEXT
        )""")


@pytest.mark.asyncio
async def test_news_flow_uses_default_sources(monkeypatch, tmp_path):
    monkeypatch.setenv("NEWS_SOURCES", "rss,gdelt")
    monkeypatch.setenv("DB_PATH", str(tmp_path / "news.db"))
    conn = sqlite3.connect(tmp_path / "news.db")
    _create_managers_table(conn)
    _create_news_items_table(conn)
    conn.commit()
    conn.close()
    list_calls = []

    async def fake_list_new_items(source, since):
        list_calls.append((source, since))
        return [
            {
                "published_at": f"2026-01-01T00:00:0{len(list_calls)}+00:00",
                "source": source,
                "headline": f"{source} headline",
                "url": f"https://example.com/{source}",
                "body_snippet": f"{source} snippet",
            }
        ]

    def fake_tag(item):
        tagged = dict(item)
        tagged["topics"] = ["markets"]
        tagged["confidence"] = 0.9
        return tagged

    monkeypatch.setattr(news_flow.news, "list_new_items", fake_list_new_items)
    monkeypatch.setattr(news_flow.news, "tag", fake_tag)

    result = await news_flow.news_flow.fn()
    verify_conn = sqlite3.connect(tmp_path / "news.db")
    try:
        rows = verify_conn.execute(
            """SELECT source, published_at, headline, url, body_snippet, topics, confidence
               FROM news_items ORDER BY source"""
        ).fetchall()
    finally:
        verify_conn.close()

    assert result["sources"] == ["rss", "gdelt"]
    assert result["since"] is None
    assert list_calls == [("rss", None), ("gdelt", None)]
    assert result["fetched"] == 2
    assert result["inserted"] == 2
    assert rows == [
        (
            "gdelt",
            "2026-01-01T00:00:02+00:00",
            "gdelt headline",
            "https://example.com/gdelt",
            "gdelt snippet",
            '["markets"]',
            0.9,
        ),
        (
            "rss",
            "2026-01-01T00:00:01+00:00",
            "rss headline",
            "https://example.com/rss",
            "rss snippet",
            '["markets"]',
            0.9,
        ),
    ]


@pytest.mark.asyncio
async def test_news_flow_defaults_to_rss_and_gdelt_when_env_missing(monkeypatch, tmp_path):
    monkeypatch.delenv("NEWS_SOURCES", raising=False)
    monkeypatch.setenv("DB_PATH", str(tmp_path / "news.db"))
    conn = sqlite3.connect(tmp_path / "news.db")
    _create_managers_table(conn)
    _create_news_items_table(conn)
    conn.commit()
    conn.close()
    calls = []

    async def fake_list_new_items(source, since):
        calls.append((source, since))
        return []

    monkeypatch.setattr(news_flow.news, "list_new_items", fake_list_new_items)

    result = await news_flow.news_flow.fn()

    assert result["sources"] == ["rss", "gdelt"]
    assert result["fetched"] == 0
    assert result["inserted"] == 0
    assert calls == [("rss", None), ("gdelt", None)]


@pytest.mark.asyncio
async def test_news_flow_defaults_to_rss_and_gdelt_when_env_empty(monkeypatch, tmp_path):
    monkeypatch.setenv("NEWS_SOURCES", " , ")
    monkeypatch.setenv("DB_PATH", str(tmp_path / "news.db"))
    conn = sqlite3.connect(tmp_path / "news.db")
    _create_managers_table(conn)
    _create_news_items_table(conn)
    conn.commit()
    conn.close()
    calls = []

    async def fake_list_new_items(source, since):
        calls.append((source, since))
        return []

    monkeypatch.setattr(news_flow.news, "list_new_items", fake_list_new_items)

    result = await news_flow.news_flow.fn()

    assert result["sources"] == ["rss", "gdelt"]
    assert result["fetched"] == 0
    assert result["inserted"] == 0
    assert calls == [("rss", None), ("gdelt", None)]


@pytest.mark.asyncio
async def test_news_flow_respects_explicit_sources(monkeypatch, tmp_path):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "news.db"))
    conn = sqlite3.connect(tmp_path / "news.db")
    _create_managers_table(conn)
    conn.commit()
    conn.close()
    calls = []

    async def fake_list_new_items(source, since):
        calls.append((source, since))
        return []

    monkeypatch.setattr(news_flow.news, "list_new_items", fake_list_new_items)

    result = await news_flow.news_flow.fn(sources=["custom"], since="2024-01-01T00:00:00Z")

    assert calls == [("custom", "2024-01-01T00:00:00Z")]
    assert result["sources"] == ["custom"]
    assert result["since"] == "2024-01-01T00:00:00Z"
    assert result["source_since"] == {"custom": "2024-01-01T00:00:00Z"}


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


def _create_news_items_table(conn):
    conn.execute("""CREATE TABLE news_items (
                news_id INTEGER PRIMARY KEY AUTOINCREMENT,
                manager_id INTEGER,
                published_at TEXT NOT NULL,
                source TEXT NOT NULL,
                headline TEXT NOT NULL,
                url TEXT,
                body_snippet TEXT,
                topics TEXT,
                confidence REAL
            )""")


def _create_watermarks_table(conn):
    conn.execute("""CREATE TABLE watermarks (
                source TEXT PRIMARY KEY,
                latest_published_at TEXT NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )""")


def test_persist_news_inserts_items():
    conn = sqlite3.connect(":memory:")
    try:
        _create_news_items_table(conn)
        items = [
            {
                "manager_id": 1,
                "published_at": "2026-01-01T00:00:00+00:00",
                "source": "rss",
                "headline": "Alpha update",
                "url": "https://example.com/a",
                "body_snippet": "Alpha Capital update",
                "topics": ["activist"],
                "confidence": 0.8,
            },
            {
                "manager_id": None,
                "published_at": "2026-01-01T01:00:00+00:00",
                "source": "gdelt",
                "headline": "Market recap",
                "url": "https://example.com/b",
                "body_snippet": "",
                "topics": ["regulatory"],
                "confidence": 0.4,
            },
        ]

        inserted = news_flow.persist_news.fn(items, conn)

        rows = conn.execute(
            """SELECT manager_id, published_at, source, headline, url, topics, confidence
               FROM news_items ORDER BY news_id"""
        ).fetchall()
        assert inserted == 2
        assert len(rows) == 2
        assert rows[0][0] == 1
        assert rows[0][4] == "https://example.com/a"
        assert rows[0][5] == '["activist"]'
        assert rows[1][0] is None
    finally:
        conn.close()


def test_resolve_source_since_uses_watermark_then_news_fallback():
    conn = sqlite3.connect(":memory:")
    try:
        _create_news_items_table(conn)
        _create_watermarks_table(conn)
        conn.execute(
            """INSERT INTO news_items (published_at, source, headline, url, body_snippet, topics, confidence)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                "2026-01-01T01:00:00+00:00",
                "rss",
                "From news items",
                "https://example.com/fallback",
                "",
                "[]",
                0.2,
            ),
        )
        conn.execute(
            "INSERT INTO watermarks (source, latest_published_at) VALUES (?, ?)",
            ("rss", "2026-01-01T02:00:00+00:00"),
        )
        conn.commit()

        from_watermark = news_flow.resolve_source_since.fn("rss", None, conn)
        from_explicit = news_flow.resolve_source_since.fn("rss", "2026-01-01T03:00:00+00:00", conn)
        from_fallback = news_flow.resolve_source_since.fn("gdelt", None, conn)

        assert from_watermark == "2026-01-01T02:00:00+00:00"
        assert from_explicit == "2026-01-01T03:00:00+00:00"
        assert from_fallback is None
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_news_flow_advances_watermark_after_each_run(monkeypatch, tmp_path):
    db_path = tmp_path / "news.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    conn = sqlite3.connect(db_path)
    _create_managers_table(conn)
    conn.execute(
        "INSERT INTO managers (manager_id, name, aliases) VALUES (?, ?, ?)",
        (1, "Alpha Capital", '["Alpha"]'),
    )
    _create_news_items_table(conn)
    conn.commit()
    conn.close()
    calls = []

    async def fake_list_new_items(source, since):
        calls.append((source, since))
        if since is None:
            return [
                {
                    "published_at": "2026-01-01T00:00:00+00:00",
                    "source": source,
                    "headline": "Alpha Capital launches fund",
                    "url": "https://example.com/1",
                    "body_snippet": "Launch details",
                }
            ]
        return [
            {
                "published_at": "2026-01-01T01:00:00+00:00",
                "source": source,
                "headline": "Alpha Capital expands team",
                "url": "https://example.com/2",
                "body_snippet": "Expansion details",
            }
        ]

    monkeypatch.setattr(news_flow.news, "list_new_items", fake_list_new_items)
    monkeypatch.setattr(news_flow.news, "tag", lambda item: item)

    first = await news_flow.news_flow.fn(sources=["rss"])
    second = await news_flow.news_flow.fn(sources=["rss"])

    assert first["source_since"] == {"rss": None}
    assert calls == [("rss", None), ("rss", "2026-01-01T00:00:00+00:00")]
    assert second["source_since"] == {"rss": "2026-01-01T00:00:00+00:00"}

    verify_conn = sqlite3.connect(db_path)
    try:
        count = verify_conn.execute("SELECT COUNT(*) FROM news_items").fetchone()[0]
        watermark = verify_conn.execute(
            "SELECT latest_published_at FROM watermarks WHERE source = ?",
            ("rss",),
        ).fetchone()[0]
        assert count == 2
        assert watermark == "2026-01-01T01:00:00+00:00"
    finally:
        verify_conn.close()


def test_persist_news_ignores_duplicate_url_and_published_at():
    conn = sqlite3.connect(":memory:")
    try:
        _create_news_items_table(conn)
        item = {
            "manager_id": 1,
            "published_at": "2026-01-01T00:00:00+00:00",
            "source": "rss",
            "headline": "Alpha update",
            "url": "https://example.com/a",
            "body_snippet": "Alpha Capital update",
            "topics": ["activist"],
            "confidence": 0.8,
        }

        first = news_flow.persist_news.fn([item], conn)
        second = news_flow.persist_news.fn([item], conn)

        count = conn.execute("SELECT COUNT(*) FROM news_items").fetchone()[0]
        assert first == 1
        assert second == 0
        assert count == 1
    finally:
        conn.close()


def test_inserted_news_items_filters_existing_rows():
    conn = sqlite3.connect(":memory:")
    try:
        _create_news_items_table(conn)
        existing = {
            "manager_id": 1,
            "published_at": "2026-01-01T00:00:00+00:00",
            "source": "rss",
            "headline": "Alpha update",
            "url": "https://example.com/a",
            "body_snippet": "Alpha Capital update",
            "topics": ["activist"],
            "confidence": 0.8,
        }
        new = {**existing, "published_at": "2026-01-01T01:00:00+00:00", "url": "https://example.com/b"}
        news_flow.persist_news.fn([existing], conn)

        inserted = news_flow.inserted_news_items([existing, new], conn)
    finally:
        conn.close()

    assert inserted == [new]


@pytest.mark.asyncio
async def test_emit_news_spike_alerts_groups_items_by_manager(monkeypatch):
    conn = sqlite3.connect(":memory:")
    calls = []

    async def fake_fire_alerts_for_event(db_conn, event):
        _ = db_conn
        calls.append(event)
        return [1]

    monkeypatch.setattr(news_flow, "fire_alerts_for_event", fake_fire_alerts_for_event)

    count = await news_flow.emit_news_spike_alerts(
        [
            {
                "manager_id": 1,
                "headline": "Alpha update",
                "source": "rss",
            },
            {
                "manager_id": 1,
                "headline": "Alpha follow-up",
                "source": "gdelt",
            },
            {
                "manager_id": 2,
                "headline": "Beta update",
                "source": "rss",
            },
        ],
        conn,
    )

    conn.close()

    assert count == 2
    assert [event.manager_id for event in calls] == [1, 2]
    assert calls[0].payload["news_count"] == 2
    assert calls[0].payload["sources"] == ["gdelt", "rss"]


def test_news_deployment_has_hourly_schedule():
    assert news_flow.news_deployment.name == "news-hourly"
    schedule = news_flow.news_deployment.schedules[0].schedule
    assert schedule.cron == "0 * * * *"
