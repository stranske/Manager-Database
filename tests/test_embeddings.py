import sqlite3
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from embeddings import (
    _is_postgres_connection,
    _postgres_columns,
    _sqlite_columns,
    embed_text,
    search_documents,
    store_document,
)
from scripts.check_dialect_portability import scan


def _assert_postgres_safe(sql: str) -> None:
    forbidden = ("AUTOINCREMENT", "INSERT OR IGNORE", "PRAGMA")
    upper_sql = sql.upper()
    assert not any(token in upper_sql for token in forbidden), sql
    assert "?" not in sql, sql


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


def test_store_document_creates_sha256_unique_index_sqlite(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("USE_SIMPLE_EMBED", "1")
    store_document("index me", str(db_path))

    conn = sqlite3.connect(db_path)
    indexes = conn.execute("PRAGMA index_list(documents)").fetchall()
    conn.close()

    index_by_name = {row[1]: row for row in indexes}
    assert "idx_documents_sha256_unique" in index_by_name
    # SQLite PRAGMA index_list returns uniqueness at position 2.
    assert index_by_name["idx_documents_sha256_unique"][2] == 1


def test_store_document_sqlite_create_table_uses_autoincrement(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("USE_SIMPLE_EMBED", "1")

    traced_sql: list[str] = []
    original_connect = sqlite3.connect

    def tracing_connect(path, *args, **kwargs):
        conn = original_connect(path, *args, **kwargs)
        conn.set_trace_callback(traced_sql.append)
        return conn

    monkeypatch.setattr("embeddings.sqlite3.connect", tracing_connect)
    store_document("capture ddl", str(db_path))

    create_statements = [sql for sql in traced_sql if "CREATE TABLE IF NOT EXISTS documents" in sql]
    assert create_statements
    assert any("AUTOINCREMENT" in sql.upper() for sql in create_statements)


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


def test_search_documents_manager_filter_with_manager_id_pk(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("USE_SIMPLE_EMBED", "1")
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE managers (manager_id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
    conn.execute("INSERT INTO managers(manager_id, name) VALUES (10, 'Grace Hopper')")
    conn.execute("INSERT INTO managers(manager_id, name) VALUES (11, 'Ada Lovelace')")
    conn.execute("""CREATE TABLE documents (
            doc_id INTEGER PRIMARY KEY AUTOINCREMENT,
            manager_id INTEGER,
            kind TEXT NOT NULL DEFAULT 'note',
            filename TEXT,
            sha256 TEXT,
            text TEXT,
            embedding TEXT
        )""")
    conn.commit()
    conn.close()

    store_document(
        "portfolio canonical", str(db_path), manager_id=10, kind="filing_text", filename="c.xml"
    )
    store_document(
        "portfolio other", str(db_path), manager_id=11, kind="filing_text", filename="d.xml"
    )

    results = search_documents("portfolio", str(db_path), manager_id=10)
    assert len(results) == 1
    assert results[0]["content"] == "portfolio canonical"
    assert results[0]["manager_name"] == "Grace Hopper"


def test_search_documents_manager_filter_without_manager_name_column(tmp_path, monkeypatch):
    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("USE_SIMPLE_EMBED", "1")
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE managers (id INTEGER PRIMARY KEY, cik TEXT, registry_ids TEXT)")
    conn.execute("INSERT INTO managers(id, cik, registry_ids) VALUES (21, '0000000021', '{}')")
    conn.commit()
    conn.close()

    store_document(
        "portfolio anonymized",
        str(db_path),
        manager_id=21,
        kind="filing_text",
        filename="anon.xml",
    )

    results = search_documents("portfolio", str(db_path), manager_id=21)
    assert len(results) == 1
    assert results[0]["content"] == "portfolio anonymized"
    assert results[0]["manager_name"] is None


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


def test_connection_dialect_detection_matches_adapter_branching(tmp_path):
    sqlite_conn = sqlite3.connect(tmp_path / "dev.db")

    class WrappedPostgresConnection:
        pass

    try:
        assert _is_postgres_connection(sqlite_conn) is False
        assert _is_postgres_connection(WrappedPostgresConnection()) is True
    finally:
        sqlite_conn.close()


def test_sqlite_schema_inspection_handles_legacy_and_missing_tables(tmp_path):
    conn = sqlite3.connect(tmp_path / "dev.db")

    try:
        assert _sqlite_columns(conn, "documents") == set()
        conn.execute("CREATE TABLE documents (id INTEGER PRIMARY KEY AUTOINCREMENT, content TEXT)")

        assert _sqlite_columns(conn, "documents") == {"id", "content"}
    finally:
        conn.close()


def test_postgres_schema_inspection_uses_information_schema(monkeypatch):
    class Connection:
        def __init__(self):
            self.info = object()
            self.executed = []

        def execute(self, sql, params=None):
            _assert_postgres_safe(sql)
            self.executed.append((sql, params))
            if "information_schema.columns" in sql:
                return type("Result", (), {"fetchall": lambda self: [("doc_id",), ("text",)]})()
            return type("Result", (), {"fetchall": lambda self: []})()

    conn = Connection()

    assert _postgres_columns(conn, "documents") == {"doc_id", "text"}
    assert any("information_schema.columns" in sql for sql, _params in conn.executed)


def test_store_and_search_pgvector(monkeypatch):
    class Connection:
        def __init__(self):
            self.info = object()
            self.executed = []
            self.committed = False
            self.closed = False

        def execute(self, sql, params=None):
            _assert_postgres_safe(sql)
            self.executed.append((sql, params))
            if "information_schema.columns" in sql:
                return type(
                    "Result",
                    (),
                    {
                        "fetchall": lambda self: [
                            ("doc_id",),
                            ("manager_id",),
                            ("kind",),
                            ("filename",),
                            ("sha256",),
                            ("text",),
                            ("embedding",),
                        ]
                    },
                )()
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


def test_store_document_postgres_uses_dialect_specific_schema_and_insert(monkeypatch):
    class Connection:
        def __init__(self):
            self.info = object()
            self.executed = []

        def execute(self, sql, params=None):
            _assert_postgres_safe(sql)
            self.executed.append((sql, params))
            if "information_schema.columns" in sql:
                return type(
                    "Result",
                    (),
                    {
                        "fetchall": lambda self: [
                            ("doc_id",),
                            ("manager_id",),
                            ("kind",),
                            ("filename",),
                            ("sha256",),
                            ("text",),
                            ("embedding",),
                        ]
                    },
                )()
            if sql.startswith("INSERT INTO documents"):
                return type("Result", (), {"fetchone": lambda self: (9,)})()
            return type("Result", (), {"fetchall": lambda self: []})()

        def commit(self):
            pass

        def close(self):
            pass

    conn = Connection()
    monkeypatch.setattr("embeddings.connect_db", lambda _path=None: conn)
    monkeypatch.setattr("embeddings.register_vector", None)
    monkeypatch.setattr("embeddings.embed_text", lambda _text: [0.4, 0.6])

    assert store_document("dialect branch", "ignored.db") == 9

    executed_sql = "\n".join(sql for sql, _params in conn.executed)
    assert "doc_id bigserial PRIMARY KEY" in executed_sql
    assert "ON CONFLICT (sha256) WHERE sha256 IS NOT NULL DO NOTHING" in executed_sql
    assert "AUTOINCREMENT" not in executed_sql
    assert "PRAGMA table_info" not in executed_sql
    assert "INSERT OR IGNORE" not in executed_sql


def test_store_document_pgvector_conflict_returns_existing(monkeypatch):
    class Connection:
        def __init__(self):
            self.info = object()
            self.executed = []
            self.committed = False
            self.closed = False

        def execute(self, sql, params=None):
            _assert_postgres_safe(sql)
            self.executed.append((sql, params))
            if "information_schema.columns" in sql:
                return type(
                    "Result",
                    (),
                    {
                        "fetchall": lambda self: [
                            ("doc_id",),
                            ("manager_id",),
                            ("kind",),
                            ("filename",),
                            ("sha256",),
                            ("text",),
                            ("embedding",),
                        ]
                    },
                )()
            if sql.startswith("INSERT INTO documents"):
                return type("Result", (), {"fetchone": lambda self: None})()
            if sql.startswith("SELECT doc_id FROM documents WHERE sha256"):
                return type("Result", (), {"fetchone": lambda self: (42,)})()
            return type("Result", (), {"fetchall": lambda self: []})()

        def commit(self):
            self.committed = True

        def close(self):
            self.closed = True

    conn = Connection()
    monkeypatch.setattr("embeddings.connect_db", lambda _path=None: conn)
    monkeypatch.setattr("embeddings.register_vector", None)
    monkeypatch.setattr("embeddings.embed_text", lambda _text: [0.2, 0.8])

    doc_id = store_document("same text", "ignored.db")

    assert doc_id == 42
    assert any(
        "ON CONFLICT (sha256) WHERE sha256 IS NOT NULL DO NOTHING" in sql
        for sql, _params in conn.executed
    )
    assert conn.committed is True
    assert conn.closed is True


def test_search_documents_postgres_without_registered_vector_uses_percent_placeholders(
    monkeypatch,
):
    class Connection:
        def __init__(self):
            self.info = object()
            self.executed = []
            self.closed = False

        def execute(self, sql, params=None):
            _assert_postgres_safe(sql)
            self.executed.append((sql, params))
            if sql.startswith("SELECT d.doc_id"):
                return type(
                    "Result",
                    (),
                    {"fetchall": lambda self: [(7, "alpha", "memo", None, None, 0.25)]},
                )()
            return type("Result", (), {"fetchall": lambda self: []})()

        def close(self):
            self.closed = True

    conn = Connection()
    monkeypatch.setattr("embeddings.connect_db", lambda _path=None: conn)
    monkeypatch.setattr("embeddings.register_vector", None)
    monkeypatch.setattr("embeddings.embed_text", lambda _text: [0.1, 0.9])

    results = search_documents("alpha", "ignored.db", k=1, manager_id=99)

    assert results == [
        {
            "doc_id": 7,
            "content": "alpha",
            "kind": "memo",
            "filename": None,
            "manager_name": None,
            "distance": 0.25,
        }
    ]
    assert conn.executed[0][1] == ([0.1, 0.9], 99, 1)
    assert conn.closed is True


def test_embeddings_pass_dialect_gate_without_allowlist() -> None:
    repo_root = Path(__file__).resolve().parents[1]

    findings = scan([repo_root / "embeddings.py"], repo_root=repo_root, allowlist={})

    assert findings == []
