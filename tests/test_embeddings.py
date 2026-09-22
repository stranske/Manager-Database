import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from embeddings import (
    PGVECTOR_DIMENSIONS,
    _is_postgres_connection,
    _pgvector_embedding,
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


def test_search_documents_uses_borrowed_uncommitted_connection(monkeypatch):
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE documents (doc_id INTEGER PRIMARY KEY, text TEXT, embedding TEXT)")
    conn.execute(
        "INSERT INTO documents (doc_id, text, embedding) VALUES (1, 'new note', '[0.1, 0.2]')"
    )
    monkeypatch.setattr(
        "embeddings.connect_db",
        lambda _path=None: pytest.fail("search opened a second connection"),
    )
    monkeypatch.setattr("embeddings.embed_text", lambda _query: [0.1, 0.2])

    hits = search_documents("new", connection=conn)

    assert [hit["doc_id"] for hit in hits] == [1]
    assert conn.execute("SELECT count(*) FROM documents").fetchone() == (1,)
    conn.close()


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


@pytest.mark.parametrize("initial_manager", [None, 1])
def test_shared_document_preserves_each_manager(tmp_path, monkeypatch, initial_manager):
    from api.search import universal_search

    monkeypatch.setenv("USE_SIMPLE_EMBED", "1")
    db_path = str(tmp_path / "shared.db")
    with sqlite3.connect(db_path) as conn:
        conn.execute("CREATE TABLE managers (manager_id INTEGER PRIMARY KEY, name TEXT)")
        conn.executemany(
            "INSERT INTO managers VALUES (?, ?)", [(1, "First"), (2, "Second"), (3, "Other")]
        )
    first = store_document("shared memo", db_path, manager_id=initial_manager)
    assert store_document("shared memo", db_path, manager_id=1) == first
    assert store_document("shared memo", db_path, manager_id=2) == first
    assert store_document("shared memo", db_path, manager_id=2) == first
    for manager_id, name in [(1, "First"), (2, "Second")]:
        hits = search_documents("shared memo", db_path, manager_id=manager_id)
        assert len(hits) == 1
        assert hits[0]["doc_id"] == first
        assert hits[0]["content"] == "shared memo"
        assert hits[0]["manager_name"] == name
    assert search_documents("shared memo", db_path, manager_id=3) == []
    assert len(search_documents("shared memo", db_path)) == 1
    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM document_managers").fetchone()[0] == 2
        # The compatibility owner remains the original explicitly supplied value.
        assert conn.execute("SELECT manager_id FROM documents").fetchone()[0] == initial_manager
        hits = universal_search("shared memo", conn, entity_type="document")
        assert len(hits) == 1
        assert hits[0].entity_id == first
        assert hits[0].manager_name == "First, Second"


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


def test_pgvector_embedding_pads_simple_mode_to_schema_width(monkeypatch):
    monkeypatch.setenv("USE_SIMPLE_EMBED", "1")

    vec = _pgvector_embedding("aaab")

    assert len(vec) == PGVECTOR_DIMENSIONS
    assert sum(vec) == 1.0
    assert vec[0] > vec[1]
    assert all(value == 0.0 for value in vec[26:])


def test_pgvector_embedding_truncates_oversized_model_vectors(monkeypatch):
    monkeypatch.setattr("embeddings.embed_text", lambda _text: [1.0] * (PGVECTOR_DIMENSIONS + 1))

    vec = _pgvector_embedding("hello")

    assert len(vec) == PGVECTOR_DIMENSIONS


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
    assert any("d.embedding <=> %s::vector AS dist" in sql for sql, _params in conn.executed)


@pytest.mark.parametrize("borrowed", [True, False])
def test_store_document_postgres_uses_dialect_specific_schema_and_insert(monkeypatch, borrowed):
    ddl_prefixes = ("CREATE EXTENSION", "CREATE TABLE", "CREATE UNIQUE INDEX")

    class Connection:
        def __init__(self):
            self.info = object()
            self.executed = []
            self.committed = False
            self.closed = False

        def execute(self, sql, params=None):
            _assert_postgres_safe(sql)
            assert not sql.strip().upper().startswith(ddl_prefixes)
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
            self.committed = True

        def close(self):
            self.closed = True

    conn = Connection()
    monkeypatch.setattr("embeddings.connect_db", lambda _path=None: conn)
    monkeypatch.setattr("embeddings.register_vector", None)
    monkeypatch.setattr("embeddings.embed_text", lambda _text: [0.4, 0.6])

    assert (
        store_document("dialect branch", "ignored.db", connection=conn if borrowed else None) == 9
    )
    assert conn.committed is (not borrowed)
    assert conn.closed is (not borrowed)

    executed_sql = "\n".join(sql for sql, _params in conn.executed)
    assert "information_schema.columns" in executed_sql
    assert "doc_id bigserial PRIMARY KEY" not in executed_sql
    assert "CREATE EXTENSION" not in executed_sql
    assert "CREATE UNIQUE INDEX" not in executed_sql
    assert "ON CONFLICT (sha256) WHERE sha256 IS NOT NULL DO NOTHING" in executed_sql
    assert "AUTOINCREMENT" not in executed_sql
    assert "PRAGMA table_info" not in executed_sql
    assert "INSERT OR IGNORE" not in executed_sql


@pytest.mark.parametrize("borrowed", [True, False])
def test_store_document_postgres_requires_migrated_documents_schema(monkeypatch, borrowed):
    class Connection:
        def __init__(self):
            self.info = object()
            self.closed = False

        def execute(self, sql, params=None):
            _assert_postgres_safe(sql)
            if "information_schema.columns" in sql:
                return type("Result", (), {"fetchall": lambda self: [("doc_id",), ("text",)]})()
            return type("Result", (), {"fetchall": lambda self: []})()

        def commit(self):
            raise AssertionError("commit should not run when schema validation fails")

        def close(self):
            self.closed = True

    conn = Connection()
    monkeypatch.setattr("embeddings.connect_db", lambda _path=None: conn)
    monkeypatch.setattr("embeddings.register_vector", None)

    with pytest.raises(RuntimeError, match="Postgres documents schema is not migrated"):
        store_document("dialect branch", "ignored.db", connection=conn if borrowed else None)
    assert conn.closed is (not borrowed)


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
    qvec, manager_id, limit = conn.executed[0][1]
    assert "d.embedding <=> %s::vector AS dist" in conn.executed[0][0]
    assert qvec[:2] == [0.1, 0.9]
    assert len(qvec) == PGVECTOR_DIMENSIONS
    assert manager_id == 99
    assert limit == 1
    assert conn.closed is True


def test_search_documents_postgres_manager_join_uses_document_owner(monkeypatch):
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
                    {
                        "fetchall": lambda self: [
                            (7, "alpha", "memo", "note.pdf", "Doc Owner", 0.25)
                        ]
                    },
                )()
            return type("Result", (), {"fetchall": lambda self: []})()

        def close(self):
            self.closed = True

    conn = Connection()
    monkeypatch.setattr("embeddings.connect_db", lambda _path=None: conn)
    monkeypatch.setattr("embeddings.register_vector", None)
    monkeypatch.setattr("embeddings.embed_text", lambda _text: [0.1, 0.9])

    results = search_documents("alpha", "ignored.db", k=1, manager_id=42)

    assert results == [
        {
            "doc_id": 7,
            "content": "alpha",
            "kind": "memo",
            "filename": "note.pdf",
            "manager_name": "Doc Owner",
            "distance": 0.25,
        }
    ]
    sql, params = conn.executed[0]
    assert "LEFT JOIN managers m ON d.manager_id = m.manager_id" in sql
    assert "ON 42 = m.manager_id" not in sql
    assert "ON 42=m.manager_id" not in sql.replace(" ", "")
    qvec, filter_manager_id, limit = params
    assert filter_manager_id == 42
    assert limit == 1
    assert conn.closed is True


def test_embeddings_pass_dialect_gate_without_allowlist() -> None:
    repo_root = Path(__file__).resolve().parents[1]

    findings = scan([repo_root / "embeddings.py"], repo_root=repo_root, allowlist={})

    assert findings == []


def test_store_document_borrowed_connection_leaves_transaction_to_caller(tmp_path, monkeypatch):
    monkeypatch.delenv("DB_URL", raising=False)
    monkeypatch.setenv("USE_SIMPLE_EMBED", "1")
    db_path = tmp_path / "borrowed.db"
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("CREATE TABLE managers (id INTEGER PRIMARY KEY, name TEXT)")
        conn.execute("INSERT INTO managers VALUES (7, 'Filing Manager')")
        conn.commit()
        conn.execute("BEGIN")
        store_document(
            "ingested filing text",
            str(db_path),
            manager_id=7,
            kind="filing_text",
            filename="accession.xml",
            connection=conn,
        )
        assert conn.in_transaction  # storage neither committed nor closed our connection
        conn.commit()
        results = search_documents("filing", str(db_path), manager_id=7)
        assert len(results) == 1
        assert results[0]["content"] == "ingested filing text"
        assert results[0]["filename"] == "accession.xml"
        assert results[0]["kind"] == "filing_text"
        assert results[0]["manager_name"] == "Filing Manager"
        assert search_documents("filing", str(db_path), manager_id=8) == []
        store_document("rolled back filing", manager_id=7, connection=conn)
        conn.rollback()
        assert conn.execute("SELECT COUNT(*) FROM documents").fetchone()[0] == 1
    finally:
        conn.close()


@pytest.mark.parametrize("k", [1, 2, 3])
def test_search_documents_equal_distance_filings_use_stable_id_order(tmp_path, monkeypatch, k):
    monkeypatch.delenv("DB_URL", raising=False)
    monkeypatch.setenv("USE_SIMPLE_EMBED", "1")
    db_path = str(tmp_path / "ties.db")
    ids = [
        store_document(f"filing {n}", db_path, manager_id=7, kind="filing_text") for n in range(3)
    ]
    results = search_documents("filing", db_path, k=k, manager_id=7)
    assert [row["doc_id"] for row in results] == ids[:k]
    assert all(row["distance"] == 0 for row in results)


def test_legacy_association_backfill_runs_once(tmp_path, monkeypatch):
    monkeypatch.setenv("USE_SIMPLE_EMBED", "1")
    db_path = str(tmp_path / "backfill.db")
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "CREATE TABLE documents (doc_id INTEGER PRIMARY KEY, manager_id INTEGER, text TEXT, embedding TEXT)"
        )
        conn.execute("INSERT INTO documents VALUES (1, 7, 'legacy', NULL)")
    statements = []
    original_connect = sqlite3.connect

    def connect(path):
        conn = original_connect(path)
        conn.set_trace_callback(statements.append)
        return conn

    monkeypatch.setattr("embeddings.connect_db", connect)
    store_document("first", db_path, manager_id=8)
    store_document("second", db_path)
    store_document("third", db_path, manager_id=9)
    backfills = [sql for sql in statements if "SELECT doc_id, manager_id FROM documents" in sql]
    assert len(backfills) == 1
    with original_connect(db_path) as conn:
        assert conn.execute("SELECT * FROM document_managers ORDER BY doc_id").fetchall() == [
            (1, 7),
            (2, 8),
            (4, 9),
        ]


def test_association_only_legacy_schema_retains_manager_name(tmp_path, monkeypatch):
    monkeypatch.setenv("USE_SIMPLE_EMBED", "1")
    db_path = str(tmp_path / "legacy_associations.db")
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "CREATE TABLE documents (id INTEGER PRIMARY KEY, content TEXT, embedding TEXT)"
        )
        conn.execute("CREATE TABLE managers (id INTEGER PRIMARY KEY, name TEXT)")
        conn.execute("INSERT INTO managers VALUES (1, 'Legacy Manager')")
    doc_id = store_document("legacy memo", db_path, manager_id=1)
    hits = search_documents("memo", db_path, manager_id=1)
    assert hits[0]["doc_id"] == doc_id
    assert hits[0]["manager_name"] == "Legacy Manager"
    assert search_documents("memo", db_path)[0]["manager_name"] is None
