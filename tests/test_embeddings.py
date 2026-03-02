import sqlite3
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from embeddings import embed_text, search_documents, store_document


def test_store_document_with_metadata_populates_columns(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("USE_SIMPLE_EMBED", "1")

    doc_id = store_document(
        "quarterly review notes",
        str(db_path),
        manager_id=1,
        kind="memo",
        filename="Q1_review.md",
    )

    conn = sqlite3.connect(db_path)
    columns = [row[1] for row in conn.execute("PRAGMA table_info(documents)").fetchall()]
    row = conn.execute(
        "SELECT manager_id, kind, filename, sha256, text, embedding FROM documents WHERE doc_id = ?",
        (doc_id,),
    ).fetchone()
    conn.close()

    assert columns == [
        "doc_id",
        "manager_id",
        "kind",
        "filename",
        "sha256",
        "text",
        "embedding",
        "created_at",
    ]
    assert row is not None
    assert row[0] == 1
    assert row[1] == "memo"
    assert row[2] == "Q1_review.md"
    assert row[3]
    assert row[4] == "quarterly review notes"
    assert row[5]


def test_store_and_search(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("USE_SIMPLE_EMBED", "1")
    store_document("hello world", str(db_path))
    store_document("goodbye", str(db_path))
    results = search_documents("hello", str(db_path))
    assert results[0]["content"] == "hello world"
    assert results[0]["kind"] == "note"
    assert "filename" in results[0]
    assert "manager_name" in results[0]


def test_store_document_deduplicates_by_sha256(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("USE_SIMPLE_EMBED", "1")
    first = store_document("same text", str(db_path))
    second = store_document("same text", str(db_path))
    assert second == first

    conn = sqlite3.connect(db_path)
    count = conn.execute("SELECT COUNT(*) FROM documents").fetchone()[0]
    conn.close()
    assert count == 1


def test_search_documents_manager_filter_and_shape(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("USE_SIMPLE_EMBED", "1")
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE managers (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL)")
    conn.execute("INSERT INTO managers(name) VALUES ('Grace Hopper')")
    conn.execute("INSERT INTO managers(name) VALUES ('Ada Lovelace')")
    conn.commit()
    conn.close()

    store_document(
        "portfolio alpha", str(db_path), manager_id=1, kind="filing_text", filename="a.xml"
    )
    store_document(
        "portfolio beta", str(db_path), manager_id=2, kind="filing_text", filename="b.xml"
    )

    results = search_documents("portfolio", str(db_path), manager_id=1)
    assert len(results) == 1
    assert results[0]["content"] == "portfolio alpha"
    assert results[0]["kind"] == "filing_text"
    assert results[0]["filename"] == "a.xml"
    assert results[0]["manager_name"] == "Grace Hopper"
    assert isinstance(results[0]["doc_id"], int)
    assert isinstance(results[0]["distance"], float)


def test_store_document_and_search_legacy_schema(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("USE_SIMPLE_EMBED", "1")
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE documents (id INTEGER PRIMARY KEY AUTOINCREMENT, content TEXT, sha256 TEXT, embedding TEXT)"
    )
    conn.commit()
    conn.close()

    store_document("legacy hello", str(db_path))
    results = search_documents("legacy", str(db_path))
    assert results[0]["content"] == "legacy hello"
    assert results[0]["kind"] == "note"
    assert results[0]["manager_name"] is None


def test_search_documents_empty_returns_list(tmp_path):
    db_path = tmp_path / "dev.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE documents (id INTEGER PRIMARY KEY AUTOINCREMENT, content TEXT, embedding TEXT)"
    )
    conn.commit()
    conn.close()
    results = search_documents("query", str(db_path))
    assert results == []


def test_embed_text_simple_mode(monkeypatch):
    monkeypatch.setenv("USE_SIMPLE_EMBED", "1")
    vec = embed_text("aaab")
    assert sum(vec) == 1.0
    assert vec[0] > vec[1]


def test_embed_text_uses_model_when_available(monkeypatch):
    class FakeVector:
        def tolist(self):
            return [1.0, 2.0]

    class FakeModel:
        def encode(self, _text):
            return FakeVector()

    monkeypatch.delenv("USE_SIMPLE_EMBED", raising=False)
    monkeypatch.setattr("embeddings.MODEL", FakeModel())
    assert embed_text("hello") == [1.0, 2.0]


def test_store_and_search_pgvector(monkeypatch):
    class Connection:
        def __init__(self):
            self.info = object()
            self.executed = []
            self.committed = False
            self.closed = False

        def execute(self, sql, params=None):
            self.executed.append((sql, params))
            if sql.startswith("SELECT doc_id FROM documents WHERE sha256"):
                return type("Result", (), {"fetchone": lambda self: None})()
            if sql.startswith("SELECT d.doc_id"):
                return type(
                    "Result",
                    (),
                    {"fetchall": lambda self: [(1, "hello", "note", "q1.md", "Grace Hopper", 0.1)]},
                )()
            if sql.startswith("INSERT INTO documents"):
                return type("Result", (), {"fetchone": lambda self: (1,)})()
            return type("Result", (), {"fetchall": lambda self: []})()

        def commit(self):
            self.committed = True

        def close(self):
            self.closed = True

    conn = Connection()

    def fake_register_vector(_conn):
        assert _conn is conn

    monkeypatch.setattr("embeddings.connect_db", lambda _path=None: conn)
    monkeypatch.setattr("embeddings.register_vector", fake_register_vector)
    monkeypatch.setattr("embeddings.Vector", lambda vec: vec)
    monkeypatch.setattr("embeddings.embed_text", lambda _text: [0.25, 0.75])

    store_document("hello", "ignored.db")
    results = search_documents("hello", "ignored.db", k=1, manager_id=1)

    assert results == [
        {
            "doc_id": 1,
            "content": "hello",
            "kind": "note",
            "filename": "q1.md",
            "manager_name": "Grace Hopper",
            "distance": 0.1,
        }
    ]
    assert conn.committed is True
    assert conn.closed is True
