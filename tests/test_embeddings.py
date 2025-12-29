import os
import sqlite3
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from embeddings import embed_text, search_documents, store_document


def test_store_and_search(tmp_path):
    db_path = tmp_path / "dev.db"
    conn = sqlite3.connect(db_path)
    conn.close()
    os.environ["USE_SIMPLE_EMBED"] = "1"
    store_document("hello world", str(db_path))
    store_document("goodbye", str(db_path))
    results = search_documents("hello", str(db_path))
    assert results[0]["content"] == "hello world"


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
            if sql.startswith("SELECT"):
                return type("Result", (), {"fetchall": lambda self: [("hello", 0.1)]})()
            return type("Result", (), {"fetchall": lambda self: []})()

        def commit(self):
            self.committed = True

        def close(self):
            self.closed = True

    conn = Connection()

    def fake_register_vector(_conn):
        # Ensure register_vector is invoked for Postgres connections.
        assert _conn is conn

    monkeypatch.setattr("embeddings.connect_db", lambda _path=None: conn)
    monkeypatch.setattr("embeddings.register_vector", fake_register_vector)
    monkeypatch.setattr("embeddings.Vector", lambda vec: vec)
    monkeypatch.setattr("embeddings.embed_text", lambda _text: [0.25, 0.75])

    store_document("hello", "ignored.db")
    results = search_documents("hello", "ignored.db", k=1)

    assert results == [{"content": "hello", "distance": 0.1}]
    assert conn.committed is True
    assert conn.closed is True
