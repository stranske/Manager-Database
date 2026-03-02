import sqlite3
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from ui.search import search_news


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
