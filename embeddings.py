"""Simple text embedding utilities for Stage 4."""

from __future__ import annotations

import hashlib
import heapq
import json
import math
import os
from collections import Counter
from typing import Any

from adapters.base import connect_db

try:  # heavy optional dependency
    from sentence_transformers import SentenceTransformer

    MODEL = SentenceTransformer("all-MiniLM-L6-v2")
except Exception:  # pragma: no cover - optional
    MODEL = None

try:  # optional PGVector integration
    from pgvector.psycopg import Vector, register_vector
except Exception:  # pragma: no cover - optional
    register_vector = None
    Vector = list  # type: ignore[misc]


def _simple_embed(text: str) -> list[float]:
    letters = Counter(c.lower() for c in text if c.isalpha())
    vec = [letters.get(chr(i + 97), 0) for i in range(26)]
    norm = sum(vec) or 1
    return [v / norm for v in vec]


def embed_text(text: str) -> list[float]:
    """Return an embedding for ``text``.

    Uses ``sentence-transformers`` if available and ``USE_SIMPLE_EMBED`` is not
    set; otherwise falls back to a letter-frequency vector for fast tests.
    """
    if os.getenv("USE_SIMPLE_EMBED") == "1" or MODEL is None:
        return _simple_embed(text)
    vec = MODEL.encode(text)
    return vec.tolist()


def store_document(
    text: str,
    db_path: str | None = None,
    manager_id: int | None = None,
    kind: str = "note",
    filename: str | None = None,
) -> int:
    """Store text and its embedding in the documents table.

    Args:
        text: Document text content
        db_path: Database path (optional, uses default)
        manager_id: FK to managers table (optional)
        kind: Document type ('memo', 'note', 'pdf', 'filing_text')
        filename: Original filename (optional)

    Returns:
        doc_id of the inserted/existing document
    """
    conn = connect_db(db_path)
    is_pg = conn.__class__.__name__ == "Connection" and hasattr(conn, "info")
    sha256 = hashlib.sha256(text.encode("utf-8")).hexdigest()
    if is_pg:
        if register_vector:
            register_vector(conn)
        conn.execute("CREATE EXTENSION IF NOT EXISTS vector")
        conn.execute("""CREATE TABLE IF NOT EXISTS documents (
                doc_id bigserial PRIMARY KEY,
                manager_id bigint REFERENCES managers(manager_id),
                kind text NOT NULL DEFAULT 'note',
                filename text,
                sha256 text,
                text text,
                embedding vector(384),
                created_at timestamptz DEFAULT now()
            )""")
        try:
            conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_documents_sha256_unique "
                "ON documents (sha256) WHERE sha256 IS NOT NULL"
            )
        except Exception:
            pass
        existing = conn.execute(
            "SELECT doc_id FROM documents WHERE sha256 = %s",
            (sha256,),
        ).fetchone()
        if existing:
            conn.commit()
            conn.close()
            return int(existing[0])
        emb = Vector(embed_text(text)) if register_vector else embed_text(text)
        result = conn.execute(
            (
                "INSERT INTO documents(manager_id, kind, filename, sha256, text, embedding) "
                "VALUES (%s,%s,%s,%s,%s,%s) RETURNING doc_id"
            ),
            (manager_id, kind, filename, sha256, text, emb),
        )
        row = result.fetchone()
        if row is None:
            raise RuntimeError("Failed to insert document")
        doc_id = int(row[0])
    else:
        conn.execute("""CREATE TABLE IF NOT EXISTS documents (
                doc_id INTEGER PRIMARY KEY AUTOINCREMENT,
                manager_id INTEGER,
                kind TEXT NOT NULL DEFAULT 'note',
                filename TEXT,
                sha256 TEXT,
                text TEXT,
                embedding TEXT,
                created_at TEXT DEFAULT (datetime('now'))
            )""")
        columns = {row[1] for row in conn.execute("PRAGMA table_info(documents)").fetchall()}
        id_col = "doc_id" if "doc_id" in columns else "id"
        text_col = "text" if "text" in columns else "content"
        if "sha256" in columns:
            conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_documents_sha256_unique "
                "ON documents (sha256) WHERE sha256 IS NOT NULL"
            )
        if "sha256" in columns:
            existing = conn.execute(
                f"SELECT {id_col} FROM documents WHERE sha256 = ?",
                (sha256,),
            ).fetchone()
            if existing:
                conn.commit()
                conn.close()
                return int(existing[0])
        emb = json.dumps(embed_text(text))
        insert_cols: list[str] = []
        insert_values: list[Any] = []
        if "manager_id" in columns:
            insert_cols.append("manager_id")
            insert_values.append(manager_id)
        if "kind" in columns:
            insert_cols.append("kind")
            insert_values.append(kind)
        if "filename" in columns:
            insert_cols.append("filename")
            insert_values.append(filename)
        if "sha256" in columns:
            insert_cols.append("sha256")
            insert_values.append(sha256)
        insert_cols.append(text_col)
        insert_values.append(text)
        insert_cols.append("embedding")
        insert_values.append(emb)
        placeholders = ",".join("?" for _ in insert_cols)
        cur = conn.execute(
            f"INSERT INTO documents({', '.join(insert_cols)}) VALUES ({placeholders})",
            insert_values,
        )
        doc_id = int(cur.lastrowid)
    conn.commit()
    conn.close()
    return doc_id


def search_documents(
    query: str,
    db_path: str | None = None,
    k: int = 3,
    manager_id: int | None = None,
) -> list[dict[str, Any]]:
    """Return top ``k`` docs similar to ``query``, optionally filtered by manager."""
    if k <= 0:
        return []
    conn = connect_db(db_path)
    is_pg = conn.__class__.__name__ == "Connection" and hasattr(conn, "info")
    if is_pg and register_vector:
        register_vector(conn)
        qvec = Vector(embed_text(query))
        where_clause = ""
        params: list[Any] = [qvec]
        if manager_id is not None:
            where_clause = "WHERE d.manager_id = %s"
            params.append(manager_id)
        params.append(k)
        rows = conn.execute(
            (
                "SELECT d.doc_id, d.text, d.kind, d.filename, m.name, d.embedding <=> %s AS dist "
                "FROM documents d LEFT JOIN managers m ON d.manager_id = m.manager_id "
                f"{where_clause} "
                "ORDER BY dist LIMIT %s"
            ),
            tuple(params),
        ).fetchall()
        conn.close()
        return [
            {
                "doc_id": doc_id,
                "content": content,
                "kind": kind,
                "filename": filename,
                "manager_name": manager_name,
                "distance": dist,
            }
            for doc_id, content, kind, filename, manager_name, dist in rows
        ]
    # Process documents one at a time to avoid loading entire dataset into memory
    # Use a heap to keep only top k results, bounding memory to O(k) instead of O(n)
    columns = {row[1] for row in conn.execute("PRAGMA table_info(documents)").fetchall()}
    if not columns:
        conn.close()
        return []
    id_col = "doc_id" if "doc_id" in columns else "id"
    text_col = "text" if "text" in columns else "content"
    has_manager_id = "manager_id" in columns
    manager_table_exists = bool(
        conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='managers'"
        ).fetchone()
    )
    manager_pk_col = None
    if has_manager_id and manager_table_exists:
        manager_columns = {row[1] for row in conn.execute("PRAGMA table_info(managers)").fetchall()}
        if "manager_id" in manager_columns:
            manager_pk_col = "manager_id"
        elif "id" in manager_columns:
            manager_pk_col = "id"
    if manager_id is not None and not has_manager_id:
        conn.close()
        return []
    where_clause = ""
    sqlite_params: list[Any] = []
    if manager_id is not None:
        where_clause = "WHERE d.manager_id = ?"
        sqlite_params.append(manager_id)
    kind_expr = "COALESCE(d.kind, 'note')" if "kind" in columns else "'note'"
    filename_expr = "d.filename" if "filename" in columns else "NULL"
    manager_name_expr = "m.name" if manager_pk_col else "NULL"
    join_clause = (
        f"LEFT JOIN managers m ON d.manager_id = m.{manager_pk_col}" if manager_pk_col else ""
    )
    cur = conn.execute(
        (
            f"SELECT d.{id_col}, d.{text_col}, "
            f"{kind_expr}, {filename_expr}, "
            f"{manager_name_expr}, d.embedding "
            f"FROM documents d {join_clause} {where_clause}"
        ),
        tuple(sqlite_params),
    )
    qvec = embed_text(query)
    # Use a max heap (negate distances for heapq which is a min heap)
    heap: list[tuple[float, dict[str, Any]]] = []
    for doc_id, content, kind, filename, manager_name, emb_json in cur:
        if not emb_json:
            continue
        emb = json.loads(emb_json)
        dist = math.sqrt(sum((a - b) ** 2 for a, b in zip(qvec, emb, strict=False)))
        result = {
            "doc_id": doc_id,
            "content": content,
            "kind": kind,
            "filename": filename,
            "manager_name": manager_name,
            "distance": dist,
        }
        # Keep only k smallest distances using a max heap
        if len(heap) < k:
            heapq.heappush(heap, (-dist, result))
        elif dist < -heap[0][0]:  # If this distance is smaller than the largest in heap
            heapq.heapreplace(heap, (-dist, result))
    conn.close()
    # Extract results and sort by distance (ascending)
    results = [item[1] for item in heap]
    results.sort(key=lambda r: r["distance"])
    return results
