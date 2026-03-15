import asyncio
import sqlite3
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import Mock

sys.path.append(str(Path(__file__).resolve().parents[1]))

import httpx
from fastapi.encoders import jsonable_encoder

import api.chat as chat_api_module
from api.search import SearchResult, universal_search
from ui.search import (
    _count_results_by_entity_type,
    _entity_badge_html,
    _format_result_meta_html,
    _group_results_by_entity_type,
    search_news,
)


class _FakeCursor:
    def __init__(self, rows):
        self._rows = rows

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)


class _FakePostgresConn:
    def __init__(self):
        self.queries: list[str] = []

    def execute(self, sql, params=()):
        sql_text = " ".join(str(sql).split())
        self.queries.append(sql_text)
        lowered = sql_text.lower()
        if "select to_regclass" in lowered:
            table_name = params[0]
            return _FakeCursor([(table_name,)])
        if "information_schema.columns" in lowered:
            table_name = params[0]
            columns_map = {
                "managers": [("manager_id",), ("name",), ("aliases",)],
                "filings": [
                    ("filing_id",),
                    ("manager_id",),
                    ("type",),
                    ("raw_key",),
                    ("period_end",),
                    ("url",),
                ],
                "holdings": [("holding_id",), ("filing_id",), ("name_of_issuer",), ("cusip",)],
                "news_items": [("news_id",), ("manager_id",), ("headline",), ("body_snippet",)],
                "documents": [("doc_id",), ("manager_id",), ("filename",), ("text",)],
            }
            return _FakeCursor(columns_map.get(table_name, []))
        if "from managers m" in lowered:
            return _FakeCursor([(1, "Elliott Management", "Elliott", 0.9)])
        if "from filings f" in lowered:
            return _FakeCursor(
                [(2, "Elliott Management", "13F-HR", "raw-1", "2025-03-31", None, 0.8)]
            )
        if "from holdings h" in lowered:
            return _FakeCursor(
                [(3, "Elliott Management", "Elliott Corp", "123456789", "2025-04-01", 0.6, 0.0)]
            )
        if "from news_items n" in lowered:
            return _FakeCursor(
                [
                    (
                        4,
                        "Elliott Management",
                        "Elliott launches campaign",
                        "Body",
                        None,
                        "2025-04-05",
                        0.95,
                    )
                ]
            )
        if "from documents d" in lowered:
            return _FakeCursor(
                [(5, "Elliott Management", "memo.txt", "Elliott strategy memo", "2025-04-03", 0.5)]
            )
        return _FakeCursor([])


def setup_db(tmp_path: Path) -> str:
    db_path = tmp_path / "dev.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE managers (manager_id TEXT, name TEXT)")
    conn.execute(
        "CREATE TABLE news_items (headline TEXT, url TEXT, published_at TEXT, source TEXT, topics TEXT, body_snippet TEXT, manager_id TEXT)"
    )
    conn.executemany(
        "INSERT INTO managers VALUES (?,?)",
        [("m1", "Manager One"), ("m2", "Manager Two")],
    )
    data = [
        (
            "Alpha Beta",
            "https://example.com/alpha",
            "2024-01-01T09:00:00",
            "src",
            "macro",
            "Markets opened flat",
            "m1",
        ),
        (
            "Gamma Delta",
            "https://example.com/gamma",
            "2024-01-02T10:00:00",
            "src",
            "earnings",
            "Strong guidance posted",
            "m2",
        ),
    ]
    conn.executemany("INSERT INTO news_items VALUES (?,?,?,?,?,?,?)", data)
    conn.commit()
    conn.close()
    return str(db_path)


def test_search_fts(tmp_path: Path, monkeypatch):
    db_path = setup_db(tmp_path)
    monkeypatch.setenv("DB_PATH", db_path)
    df = search_news("Gamma")
    assert list(df["headline"]) == ["Gamma Delta"]
    assert df.iloc[0]["manager_name"] == "Manager Two"
    assert list(df.columns) == [
        "headline",
        "url",
        "published_at",
        "source",
        "topics",
        "manager_name",
    ]


def test_search_uses_body_snippet_sqlite(tmp_path: Path, monkeypatch):
    db_path = setup_db(tmp_path)
    monkeypatch.setenv("DB_PATH", db_path)
    df = search_news("guidance")
    assert list(df["headline"]) == ["Gamma Delta"]


def test_universal_search_returns_ranked_multi_entity_results():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE managers (id INTEGER PRIMARY KEY, name TEXT, role TEXT)")
    conn.execute("CREATE TABLE news (headline TEXT, source TEXT, published TEXT)")
    conn.execute("CREATE TABLE documents (id INTEGER PRIMARY KEY, content TEXT, embedding TEXT)")
    conn.execute(
        "CREATE TABLE filings (filing_id INTEGER PRIMARY KEY, manager_id INTEGER, type TEXT, raw_key TEXT, period_end TEXT, url TEXT)"
    )
    conn.execute(
        "CREATE TABLE holdings (holding_id INTEGER PRIMARY KEY, filing_id INTEGER, name_of_issuer TEXT, cusip TEXT)"
    )
    conn.execute(
        "INSERT INTO managers(id, name, role) VALUES (1, 'Elliott Management', 'Activist')"
    )
    conn.execute(
        "INSERT INTO news(headline, source, published) VALUES ('Elliott targets XYZ board', 'WSJ', '2025-01-02')"
    )
    conn.execute(
        "INSERT INTO documents(id, content, embedding) VALUES (1, 'Internal Elliott investment memo', '[]')"
    )
    conn.execute(
        "INSERT INTO filings(filing_id, manager_id, type, raw_key, period_end, url) VALUES (10, 1, '13F-HR', 'raw-10', '2025-01-01', NULL)"
    )
    conn.execute(
        "INSERT INTO holdings(holding_id, filing_id, name_of_issuer, cusip) VALUES (20, 10, 'Elliott Corp', '123456789')"
    )

    results = universal_search("Elliott", conn, limit=20)

    entity_types = {item.entity_type for item in results}
    assert {"manager", "filing", "news", "document", "holding"}.issubset(entity_types)
    assert results == sorted(results, key=lambda item: item.relevance, reverse=True)


def test_universal_search_filters_results_by_entity_type():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE managers (id INTEGER PRIMARY KEY, name TEXT, role TEXT)")
    conn.execute("CREATE TABLE news (headline TEXT, source TEXT, published TEXT)")
    conn.execute("CREATE TABLE documents (id INTEGER PRIMARY KEY, content TEXT, embedding TEXT)")
    conn.execute(
        "CREATE TABLE filings (filing_id INTEGER PRIMARY KEY, manager_id INTEGER, type TEXT, raw_key TEXT, period_end TEXT, url TEXT)"
    )
    conn.execute(
        "CREATE TABLE holdings (holding_id INTEGER PRIMARY KEY, filing_id INTEGER, name_of_issuer TEXT, cusip TEXT)"
    )
    conn.execute(
        "INSERT INTO managers(id, name, role) VALUES (1, 'Elliott Management', 'Activist')"
    )
    conn.execute(
        "INSERT INTO news(headline, source, published) VALUES ('Elliott targets XYZ board', 'WSJ', '2025-01-02')"
    )
    conn.execute(
        "INSERT INTO documents(id, content, embedding) VALUES (1, 'Internal Elliott investment memo', '[]')"
    )

    results = universal_search("Elliott", conn, limit=20, entity_type="news")

    assert results
    assert {item.entity_type for item in results} == {"news"}


def test_universal_search_postgres_fts_queries_and_results():
    conn = _FakePostgresConn()
    results = universal_search("Elliott", conn, limit=10)

    entity_types = {item.entity_type for item in results}
    assert {"manager", "filing", "holding", "news", "document"}.issubset(entity_types)
    assert any("to_tsvector" in query.lower() for query in conn.queries)


def test_universal_search_sqlite_uses_embedding_search_for_documents(tmp_path: Path, monkeypatch):
    db_path = tmp_path / "search.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE documents (id INTEGER PRIMARY KEY, content TEXT, embedding TEXT, created_at TEXT)"
    )
    conn.execute(
        "INSERT INTO documents(id, content, embedding, created_at) VALUES (1, 'Elliott strategy memo', '[]', '2025-01-01')"
    )
    conn.commit()

    calls: list[tuple[str, str | None, int]] = []

    def _fake_search_documents(query: str, db_path: str | None = None, k: int = 3):
        calls.append((query, db_path, k))
        return [{"content": "Elliott strategy memo", "distance": 0.05}]

    monkeypatch.setitem(
        sys.modules,
        "embeddings",
        SimpleNamespace(search_documents=_fake_search_documents),
    )

    results = universal_search("activist campaign", conn, limit=5)

    assert calls and calls[0][0] == "activist campaign"
    assert calls[0][1] == str(db_path)
    assert any(item.entity_type == "document" and item.entity_id == 1 for item in results)


def test_universal_search_sqlite_matches_filings_by_manager_name():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE managers (id INTEGER PRIMARY KEY, name TEXT)")
    conn.execute(
        "CREATE TABLE filings (filing_id INTEGER PRIMARY KEY, manager_id INTEGER, type TEXT, raw_key TEXT, period_end TEXT, url TEXT)"
    )
    conn.execute(
        "CREATE TABLE holdings (holding_id INTEGER PRIMARY KEY, filing_id INTEGER, name_of_issuer TEXT, cusip TEXT)"
    )
    conn.execute("INSERT INTO managers(id, name) VALUES (1, 'Elliott Management')")
    conn.execute(
        "INSERT INTO filings(filing_id, manager_id, type, raw_key, period_end, url) VALUES (10, 1, '13F-HR', 'raw-10', '2025-01-01', NULL)"
    )
    conn.execute(
        "INSERT INTO holdings(holding_id, filing_id, name_of_issuer, cusip) VALUES (20, 10, 'Sample Issuer', '123456789')"
    )

    results = universal_search("Elliott", conn, limit=20)

    assert any(item.entity_type == "filing" and item.entity_id == 10 for item in results)
    filing_result = next(item for item in results if item.entity_type == "filing")
    assert filing_result.manager_name == "Elliott Management"


def test_universal_search_sqlite_includes_activism_filings():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE managers (id INTEGER PRIMARY KEY, name TEXT)")
    conn.execute(
        "CREATE TABLE activism_filings ("
        "filing_id INTEGER PRIMARY KEY, manager_id INTEGER, filing_type TEXT, "
        "subject_company TEXT, subject_cusip TEXT, ownership_pct REAL, filed_date TEXT, url TEXT)"
    )
    conn.execute("INSERT INTO managers(id, name) VALUES (1, 'Elliott Management')")
    conn.execute(
        "INSERT INTO activism_filings(filing_id, manager_id, filing_type, subject_company, "
        "subject_cusip, ownership_pct, filed_date, url) VALUES "
        "(10, 1, 'SC 13D', 'Apple Inc.', '037833100', 5.1, '2025-01-01', 'https://sec.example/10')"
    )

    results = universal_search("Apple", conn, limit=20)

    activism_result = next(item for item in results if item.entity_id == 10)
    assert activism_result.entity_type == "filing"
    assert activism_result.headline == "13D Filing: Elliott Management -> Apple Inc. (5.1%)"
    assert activism_result.url == "https://sec.example/10"


def test_universal_search_sqlite_resolves_manager_name_for_news_items():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE managers (id INTEGER PRIMARY KEY, name TEXT)")
    conn.execute(
        "CREATE TABLE news_items (news_id INTEGER PRIMARY KEY, manager_id INTEGER, headline TEXT, body_snippet TEXT, url TEXT, published_at TEXT)"
    )
    conn.execute("INSERT INTO managers(id, name) VALUES (1, 'Elliott Management')")
    conn.execute(
        "INSERT INTO news_items(news_id, manager_id, headline, body_snippet, url, published_at) "
        "VALUES (11, 1, 'Elliott update', 'Body', NULL, '2025-01-01')"
    )

    results = universal_search("Elliott", conn, limit=20)

    news_result = next(item for item in results if item.entity_type == "news")
    assert news_result.manager_name == "Elliott Management"


def _seed_api_search_db(db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE managers (id INTEGER PRIMARY KEY, name TEXT, role TEXT)")
    conn.execute("CREATE TABLE news (headline TEXT, source TEXT, published TEXT)")
    conn.execute("CREATE TABLE documents (id INTEGER PRIMARY KEY, content TEXT, embedding TEXT)")
    conn.execute(
        "CREATE TABLE filings (filing_id INTEGER PRIMARY KEY, manager_id INTEGER, type TEXT, raw_key TEXT, period_end TEXT, url TEXT)"
    )
    conn.execute(
        "CREATE TABLE holdings (holding_id INTEGER PRIMARY KEY, filing_id INTEGER, name_of_issuer TEXT, cusip TEXT)"
    )
    conn.execute(
        "INSERT INTO managers(id, name, role) VALUES (1, 'Elliott Management', 'Activist')"
    )
    conn.execute(
        "INSERT INTO news(headline, source, published) VALUES ('Elliott launches campaign', 'WSJ', '2025-01-02')"
    )
    conn.execute(
        "INSERT INTO documents(id, content, embedding) VALUES (1, 'Internal Elliott strategy memo', '[]')"
    )
    conn.execute(
        "INSERT INTO filings(filing_id, manager_id, type, raw_key, period_end, url) VALUES (10, 1, '13F-HR', 'raw-10', '2025-01-01', NULL)"
    )
    conn.execute(
        "INSERT INTO holdings(holding_id, filing_id, name_of_issuer, cusip) VALUES (20, 10, 'Elliott Corp', '123456789')"
    )
    conn.commit()
    conn.close()


def test_api_search_endpoint_returns_results(tmp_path: Path, monkeypatch):
    db_path = tmp_path / "search_api.db"
    _seed_api_search_db(db_path)

    monkeypatch.setitem(
        sys.modules,
        "embeddings",
        SimpleNamespace(search_documents=lambda *_args, **_kwargs: []),
    )
    monkeypatch.setattr(chat_api_module, "connect_db", lambda: sqlite3.connect(db_path))
    results = asyncio.run(chat_api_module.search_api(q="Elliott", limit=20, entity_type=None))

    assert results
    entity_types = {item.entity_type for item in results}
    assert {"manager", "filing", "news", "document"}.issubset(entity_types)
    assert {"entity_type", "entity_id", "headline", "snippet", "relevance"}.issubset(
        results[0].model_dump().keys()
    )


def test_api_search_route_is_registered():
    paths = {route.path for route in chat_api_module.app.routes}
    assert "/api/search" in paths


def test_api_search_endpoint_filters_entity_type(tmp_path: Path, monkeypatch):
    db_path = tmp_path / "search_api_filter.db"
    _seed_api_search_db(db_path)

    monkeypatch.setitem(
        sys.modules,
        "embeddings",
        SimpleNamespace(search_documents=lambda *_args, **_kwargs: []),
    )
    monkeypatch.setattr(chat_api_module, "connect_db", lambda: sqlite3.connect(db_path))
    results = asyncio.run(
        chat_api_module.search_api(
            q="Elliott",
            entity_type="news",
            limit=20,
        )
    )

    assert results
    assert {item.entity_type for item in results} == {"news"}


def test_api_search_results_are_json_serializable(tmp_path: Path, monkeypatch):
    db_path = tmp_path / "search_api_http.db"
    _seed_api_search_db(db_path)

    monkeypatch.setitem(
        sys.modules,
        "embeddings",
        SimpleNamespace(search_documents=lambda *_args, **_kwargs: []),
    )
    monkeypatch.setattr(chat_api_module, "connect_db", lambda: sqlite3.connect(db_path))
    results = asyncio.run(chat_api_module.search_api(q="Elliott", limit=20, entity_type=None))
    payload = jsonable_encoder(results)

    assert isinstance(payload, list)
    assert payload
    assert isinstance(payload[0], dict)
    assert {"entity_type", "entity_id", "headline", "snippet", "relevance"}.issubset(
        payload[0].keys()
    )


def test_api_search_filtered_results_are_json_serializable(tmp_path: Path, monkeypatch):
    db_path = tmp_path / "search_api_http_filter.db"
    _seed_api_search_db(db_path)

    monkeypatch.setitem(
        sys.modules,
        "embeddings",
        SimpleNamespace(search_documents=lambda *_args, **_kwargs: []),
    )
    monkeypatch.setattr(chat_api_module, "connect_db", lambda: sqlite3.connect(db_path))
    results = asyncio.run(chat_api_module.search_api(q="Elliott", entity_type="news", limit=20))
    payload = jsonable_encoder(results)

    assert payload
    assert {item["entity_type"] for item in payload} == {"news"}


async def _http_get(
    path: str, *, params: dict[str, str | int | float | bool | None] | None = None
) -> httpx.Response:
    await chat_api_module.app.router.startup()
    try:
        transport = httpx.ASGITransport(app=cast(Any, chat_api_module.app))
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", timeout=5.0
        ) as client:
            return await client.get(path, params=params)
    finally:
        await chat_api_module.app.router.shutdown()


def test_api_search_http_endpoint_returns_searchresult_payload(tmp_path: Path, monkeypatch):
    db_path = tmp_path / "search_api_http_client.db"
    _seed_api_search_db(db_path)

    monkeypatch.setitem(
        sys.modules,
        "embeddings",
        SimpleNamespace(search_documents=lambda *_args, **_kwargs: []),
    )
    monkeypatch.setattr(chat_api_module, "connect_db", lambda: sqlite3.connect(db_path))

    response = asyncio.run(_http_get("/api/search", params={"q": "Elliott", "limit": 20}))

    assert response.status_code == 200
    payload = response.json()
    assert isinstance(payload, list)
    assert payload
    assert isinstance(payload[0], dict)
    assert {
        "entity_type",
        "entity_id",
        "manager_name",
        "headline",
        "snippet",
        "relevance",
        "url",
        "timestamp",
    }.issubset(payload[0].keys())


def test_api_search_http_endpoint_filters_news_entity_type(tmp_path: Path, monkeypatch):
    db_path = tmp_path / "search_api_http_client_filter.db"
    _seed_api_search_db(db_path)

    monkeypatch.setitem(
        sys.modules,
        "embeddings",
        SimpleNamespace(search_documents=lambda *_args, **_kwargs: []),
    )
    monkeypatch.setattr(chat_api_module, "connect_db", lambda: sqlite3.connect(db_path))

    response = asyncio.run(
        _http_get(
            "/api/search",
            params={"q": "Elliott", "entity_type": "news", "limit": 20},
        )
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload
    assert {item["entity_type"] for item in payload} == {"news"}


def test_count_results_by_entity_type_orders_known_types():
    results = [
        SearchResult(
            entity_type="news",
            entity_id=1,
            manager_name=None,
            headline="n1",
            snippet="",
            relevance=0.8,
            url=None,
            timestamp=None,
        ),
        SearchResult(
            entity_type="manager",
            entity_id=2,
            manager_name="Elliott",
            headline="Elliott",
            snippet="",
            relevance=0.7,
            url=None,
            timestamp=None,
        ),
        SearchResult(
            entity_type="news",
            entity_id=3,
            manager_name=None,
            headline="n2",
            snippet="",
            relevance=0.6,
            url=None,
            timestamp=None,
        ),
    ]

    counts = _count_results_by_entity_type(results)

    assert list(counts.keys()) == ["manager", "news"]
    assert counts["manager"] == 1
    assert counts["news"] == 2


def test_group_results_by_entity_type_orders_by_top_relevance():
    results = [
        SearchResult(
            entity_type="holding",
            entity_id=1,
            manager_name=None,
            headline="h",
            snippet="",
            relevance=0.65,
            url=None,
            timestamp=None,
        ),
        SearchResult(
            entity_type="news",
            entity_id=2,
            manager_name=None,
            headline="n",
            snippet="",
            relevance=0.9,
            url=None,
            timestamp=None,
        ),
        SearchResult(
            entity_type="filing",
            entity_id=3,
            manager_name=None,
            headline="f",
            snippet="",
            relevance=0.8,
            url=None,
            timestamp=None,
        ),
    ]

    grouped = _group_results_by_entity_type(results)

    assert [entity_type for entity_type, _ in grouped] == ["news", "filing", "holding"]


def test_entity_badge_html_contains_uppercase_entity_type():
    badge = _entity_badge_html("news")

    assert "NEWS" in badge
    assert "border-radius:999px" in badge


def test_format_result_meta_html_includes_badge_relevance_and_context():
    result = SearchResult(
        entity_type="news",
        entity_id=1,
        manager_name="Elliott Management",
        headline="Elliott launches campaign",
        snippet="Body",
        relevance=0.92,
        url=None,
        timestamp="2025-04-05",
    )

    meta_html = _format_result_meta_html(result)

    assert "NEWS" in meta_html
    assert "Relevance 0.92" in meta_html
    assert "Manager: Elliott Management" in meta_html
    assert "2025-04-05" in meta_html


def test_search_ui_main_renders_unified_grouped_results(monkeypatch):
    import ui.search as search_ui

    class _FakeColumn:
        def __init__(self):
            self.metrics: list[tuple[str, int]] = []

        def metric(self, label: str, value: int) -> None:
            self.metrics.append((label, value))

    class _FakeExpander:
        def __init__(self, collector: list[str], label: str):
            self._collector = collector
            self._label = label

        def __enter__(self):
            self._collector.append(self._label)
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class _FakeStreamlit:
        def __init__(self):
            self.subheaders: list[str] = []
            self.expanders: list[str] = []
            self.columns_calls: list[int] = []
            self.columns_created: list[list[_FakeColumn]] = []

        def header(self, _text: str) -> None:
            return None

        def text_input(self, _label: str) -> str:
            return "Elliott"

        def number_input(self, _label: str, **_kwargs) -> int:
            return 20

        def subheader(self, text: str) -> None:
            self.subheaders.append(text)

        def columns(self, count: int) -> list[_FakeColumn]:
            self.columns_calls.append(count)
            cols = [_FakeColumn() for _ in range(count)]
            self.columns_created.append(cols)
            return cols

        def caption(self, _text: str) -> None:
            return None

        def expander(self, label: str, **_kwargs):
            return _FakeExpander(self.expanders, label)

        def markdown(self, _text: str, **_kwargs) -> None:
            return None

        def write(self, _text: str) -> None:
            return None

        def link_button(self, _label: str, _url: str) -> None:
            return None

        def divider(self) -> None:
            return None

        def info(self, _text: str) -> None:
            return None

        def stop(self) -> None:
            raise AssertionError("stop() should not be called when logged in")

    fake_st = _FakeStreamlit()
    fake_conn = Mock()

    monkeypatch.setattr(search_ui, "st", fake_st)
    monkeypatch.setattr(search_ui, "require_login", lambda: True)
    monkeypatch.setattr(search_ui, "connect_db", lambda: fake_conn)
    monkeypatch.setattr(
        search_ui,
        "universal_search",
        lambda _q, _conn, _limit: [
            SearchResult(
                entity_type="news",
                entity_id=1,
                manager_name="Elliott Management",
                headline="Elliott launches campaign",
                snippet="Body",
                relevance=0.95,
                url="https://example.com/news/1",
                timestamp="2025-01-01",
            ),
            SearchResult(
                entity_type="filing",
                entity_id=2,
                manager_name="Elliott Management",
                headline="13F-HR filing",
                snippet="Raw key: raw-1",
                relevance=0.8,
                url=None,
                timestamp="2025-01-01",
            ),
        ],
    )

    search_ui.main()

    fake_conn.close.assert_called_once()
    assert "Summary" in fake_st.subheaders
    assert "Results" in fake_st.subheaders
    assert "News (1)" in fake_st.expanders
    assert "Filings (1)" in fake_st.expanders
