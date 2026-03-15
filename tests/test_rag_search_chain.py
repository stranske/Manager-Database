from __future__ import annotations

import sqlite3

from chains.rag_search import RAGSearchChain


class FakeResponse:
    def __init__(self, content: str):
        self.content = content
        self.response_metadata: dict[str, str] = {}


class FakeLLM:
    def __init__(self, response_text: str):
        self._response_text = response_text
        self.prompts: list[str] = []

    def invoke(self, prompt: str, config=None):
        self.prompts.append(prompt)
        return FakeResponse(self._response_text)


def _build_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE managers (manager_id INTEGER PRIMARY KEY, name TEXT, cik TEXT)")
    conn.execute(
        "CREATE TABLE filings (filing_id INTEGER PRIMARY KEY, manager_id INTEGER, type TEXT, filed_date TEXT, url TEXT)"
    )
    conn.execute(
        "CREATE TABLE holdings (holding_id INTEGER PRIMARY KEY, filing_id INTEGER, cusip TEXT, name_of_issuer TEXT, shares INTEGER, value_usd REAL)"
    )
    conn.execute(
        "CREATE TABLE news_items (news_id INTEGER PRIMARY KEY, manager_id INTEGER, published_at TEXT, source TEXT, headline TEXT, url TEXT, body_snippet TEXT)"
    )
    conn.execute(
        "CREATE TABLE activism_filings (filing_id INTEGER PRIMARY KEY, manager_id INTEGER, filing_type TEXT, subject_company TEXT, filed_date TEXT, url TEXT)"
    )
    conn.execute(
        "CREATE TABLE crowded_trades (crowd_id INTEGER PRIMARY KEY, cusip TEXT, name_of_issuer TEXT, manager_count INTEGER, manager_ids TEXT, total_value_usd REAL, report_date TEXT)"
    )
    conn.execute("INSERT INTO managers(manager_id, name, cik) VALUES (1, 'Elliott', '0001791786')")
    conn.execute(
        "INSERT INTO filings(filing_id, manager_id, type, filed_date, url) VALUES (11, 1, '13F-HR', '2026-03-01', 'https://example.com/filings/11')"
    )
    conn.execute(
        "INSERT INTO holdings(holding_id, filing_id, cusip, name_of_issuer, shares, value_usd) VALUES (1, 11, '037833100', 'Apple Inc.', 1000, 150000.0)"
    )
    conn.execute(
        "INSERT INTO news_items(news_id, manager_id, published_at, source, headline, url, body_snippet) VALUES (1, 1, '2026-03-02T08:00:00', 'Reuters', 'Elliott increases Apple stake', 'https://example.com/news/1', 'snippet')"
    )
    conn.execute(
        "INSERT INTO activism_filings(filing_id, manager_id, filing_type, subject_company, filed_date, url) VALUES (21, 1, 'SC 13D', 'Apple Inc.', '2026-03-03', 'https://example.com/activism/21')"
    )
    conn.execute(
        "INSERT INTO crowded_trades(crowd_id, cusip, name_of_issuer, manager_count, manager_ids, total_value_usd, report_date) VALUES (1, '037833100', 'Apple Inc.', 4, '[1,2,3,4]', 2500000.0, '2026-03-05')"
    )
    return conn


def test_entity_extraction_matches_manager_ids():
    conn = _build_db()
    chain = RAGSearchChain(db_conn=conn)

    entities = chain._entity_extraction("What does Elliott hold?")

    assert entities["manager_ids"] == [1]
    assert entities["manager_names"] == ["Elliott"]
    conn.close()


def test_structured_search_returns_holdings_and_sources():
    conn = _build_db()
    chain = RAGSearchChain(db_conn=conn)

    context, sources = chain._structured_search(
        {"manager_ids": [1], "cusips": [], "keywords": [], "date_range": None}
    )

    assert "Latest holdings" in context
    assert any(source.get("filing_id") == 11 for source in sources)
    assert any(
        source.get("news_reference") == "Elliott increases Apple stake" for source in sources
    )
    conn.close()


def test_run_combines_vector_and_structured_context(monkeypatch):
    conn = _build_db()
    llm = FakeLLM("Elliott still owns Apple and recent filings confirm the position.")
    chain = RAGSearchChain(llm=llm, db_conn=conn)
    monkeypatch.setattr(
        "chains.rag_search.search_documents",
        lambda query, k=5, manager_id=None: [
            {
                "doc_id": 7,
                "content": "Internal memo: Elliott remains constructive on Apple.",
                "filename": "memo-apple.md",
                "kind": "memo",
                "manager_name": "Elliott",
            }
        ],
    )

    result = chain.run("What do we know about Elliott's Apple position?")

    assert result["answer"].startswith("Elliott still owns Apple")
    assert result["confidence"] == "high"
    assert any(source.get("document_id") == 7 for source in result["sources"])
    assert any(source.get("filing_id") == 11 for source in result["sources"])
    assert llm.prompts and "Structured data" in llm.prompts[0]
    conn.close()


def test_run_returns_low_confidence_without_context(monkeypatch):
    conn = _build_db()
    chain = RAGSearchChain(db_conn=conn)
    monkeypatch.setattr("chains.rag_search.search_documents", lambda *args, **kwargs: [])

    result = chain.run("What do we know about an unrelated manager?")

    assert result["confidence"] == "low"
    assert "do not have enough context" in result["answer"]
    conn.close()
