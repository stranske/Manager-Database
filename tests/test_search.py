import sqlite3
import sys
from pathlib import Path
from types import SimpleNamespace

sys.path.append(str(Path(__file__).resolve().parents[1]))

from api.search import universal_search
from ui.search import search_news


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
    conn.execute("CREATE TABLE news (headline TEXT, source TEXT, published TEXT)")
    data = [
        ("Alpha Beta", "src", "2024-01-01"),
        ("Gamma Delta", "src", "2024-01-02"),
    ]
    conn.executemany("INSERT INTO news VALUES (?,?,?)", data)
    conn.commit()
    conn.close()
    return str(db_path)


def test_search_fts(tmp_path: Path, monkeypatch):
    db_path = setup_db(tmp_path)
    monkeypatch.setenv("DB_PATH", db_path)
    df = search_news("Gamma")
    assert list(df["headline"]) == ["Gamma Delta"]


def test_universal_search_returns_ranked_multi_entity_results():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE managers (id INTEGER PRIMARY KEY, name TEXT, role TEXT)")
    conn.execute("CREATE TABLE news (headline TEXT, source TEXT, published TEXT)")
    conn.execute("CREATE TABLE documents (id INTEGER PRIMARY KEY, content TEXT, embedding TEXT)")
    conn.execute(
        "CREATE TABLE holdings (cik TEXT, accession TEXT, filed TEXT, nameOfIssuer TEXT, cusip TEXT, value INTEGER, sshPrnamt INTEGER)"
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
        "INSERT INTO holdings(cik, accession, filed, nameOfIssuer, cusip, value, sshPrnamt) VALUES ('0001', 'ACC-1', '2025-01-01', 'Elliott Corp', '123456789', 10, 5)"
    )

    results = universal_search("Elliott", conn, limit=20)

    entity_types = {item.entity_type for item in results}
    assert {"manager", "news", "document", "holding"}.issubset(entity_types)
    assert results == sorted(results, key=lambda item: item.relevance, reverse=True)


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
