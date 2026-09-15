"""Exercise the additive association migration on SQLite and optional live Postgres."""

import importlib.util
import os
from pathlib import Path
from uuid import uuid4

import pytest
import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("backend", ["sqlite", "postgres"])
def test_document_association_migration_and_search(backend, tmp_path, monkeypatch):
    import embeddings
    from api.search import universal_search

    monkeypatch.setenv("USE_SIMPLE_EMBED", "1")
    monkeypatch.delenv("DB_URL", raising=False)
    db_path = str(tmp_path / "migration.db")
    schema = "document_test_" + uuid4().hex
    if backend == "postgres":
        url = os.getenv("DOCUMENT_TEST_POSTGRES_URL")
        if not url:
            pytest.skip("Set DOCUMENT_TEST_POSTGRES_URL to a disposable pgvector database")
        import psycopg

        admin = psycopg.connect(url, autocommit=True)
        admin.execute("CREATE EXTENSION IF NOT EXISTS vector")
        admin.execute(f"CREATE SCHEMA {schema}")
        engine = sa.create_engine(
            url.replace("postgresql://", "postgresql+psycopg://"),
            connect_args={"options": f"-csearch_path={schema},public"},
        )

        def connect(_path=None):
            return psycopg.connect(url, options=f"-csearch_path={schema},public", autocommit=True)

        monkeypatch.setattr(embeddings, "connect_db", connect)
        pk = "BIGSERIAL PRIMARY KEY"
        vector_type = "vector(384)"
        timestamp_type = "timestamptz"
        aliases = ", aliases text[]"
    else:
        import sqlite3

        def connect(_path=None):
            return sqlite3.connect(db_path)

        engine = sa.create_engine(f"sqlite:///{db_path}")
        pk = "INTEGER PRIMARY KEY"
        vector_type = "TEXT"
        timestamp_type = "TEXT"
        aliases = ""
    try:
        with engine.begin() as conn:
            conn.exec_driver_sql(
                f"CREATE TABLE managers (manager_id INTEGER PRIMARY KEY, name TEXT{aliases})"
            )
            conn.exec_driver_sql(
                "INSERT INTO managers(manager_id, name) VALUES (1, 'First'), (2, 'Second'), (3, 'Other')"
            )
            conn.exec_driver_sql(
                f"CREATE TABLE documents (doc_id {pk}, manager_id INTEGER, kind TEXT, "
                f"filename TEXT, sha256 TEXT, text TEXT, embedding {vector_type}, created_at {timestamp_type})"
            )
            conn.exec_driver_sql(
                "CREATE UNIQUE INDEX idx_documents_sha256_unique ON documents(sha256) WHERE sha256 IS NOT NULL"
            )
            conn.exec_driver_sql(
                "INSERT INTO documents(manager_id, kind, text) VALUES (1, 'note', 'legacy owned'), (NULL, 'note', 'legacy unowned')"
            )
            spec = importlib.util.spec_from_file_location(
                "document_migration", ROOT / "alembic/versions/022_document_managers.py"
            )
            migration = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(migration)
            with Operations.context(MigrationContext.configure(conn)):
                migration.upgrade()
            inspector = sa.inspect(conn)
            assert (
                inspector.get_pk_constraint("document_managers")["name"] == "pk_document_managers"
            )
            assert {fk["name"] for fk in inspector.get_foreign_keys("document_managers")} == {
                "fk_document_managers_doc_id_documents",
                "fk_document_managers_manager_id_managers",
            }
            assert conn.exec_driver_sql(
                "SELECT doc_id, manager_id FROM document_managers"
            ).fetchall() == [(1, 1)]

        first = embeddings.store_document("shared memo", db_path, manager_id=1)
        assert embeddings.store_document("shared memo", db_path, manager_id=2) == first
        assert embeddings.store_document("shared memo", db_path, manager_id=2) == first
        unowned = embeddings.store_document("initially unowned memo", db_path)
        assert embeddings.store_document("initially unowned memo", db_path, manager_id=2) == unowned
        for manager_id, expected in [(1, {first}), (2, {first, unowned}), (3, set())]:
            hits = embeddings.search_documents("memo", db_path, k=20, manager_id=manager_id)
            # Legacy rows have no embedding; only newly stored memo content is relevant.
            hits = [hit for hit in hits if "memo" in hit["content"]]
            assert {hit["doc_id"] for hit in hits} == expected
            assert len(hits) == len(expected)
        with connect() as conn:
            results = universal_search("shared", conn, entity_type="document")
            matching = [item for item in results if item.entity_id == first]
            assert len(matching) == 1
            assert matching[0].manager_name == "First, Second"
        with engine.begin() as conn:
            assert conn.exec_driver_sql("SELECT COUNT(*) FROM document_managers").scalar() == 4
            with Operations.context(MigrationContext.configure(conn)):
                # A SQLite runtime/bootstrap may already have created the additive table.
                migration.upgrade()
                assert conn.exec_driver_sql("SELECT COUNT(*) FROM document_managers").scalar() == 4
                migration.downgrade()
            assert not sa.inspect(conn).has_table("document_managers")
    finally:
        engine.dispose()
        if backend == "postgres":
            admin.execute(f"DROP SCHEMA {schema} CASCADE")
            admin.close()
