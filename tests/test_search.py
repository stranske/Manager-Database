import sqlite3
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from api.search import universal_search
from ui.search import search_news


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
