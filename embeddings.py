"""Simple text embedding utilities for Stage 4."""

from __future__ import annotations

import heapq
import hashlib
import json
import math
import os
import sqlite3
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


def _ensure_sqlite_documents_table(conn: sqlite3.Connection) -> None:
    """Create/backfill the SQLite documents table used by uploads and search."""
    conn.execute("""CREATE TABLE IF NOT EXISTS documents (
            doc_id INTEGER PRIMARY KEY AUTOINCREMENT,
            content TEXT NOT NULL,
            embedding TEXT,
            filename TEXT,
            kind TEXT,
            manager_id INTEGER,
            content_sha TEXT UNIQUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")
    cursor = conn.execute("PRAGMA table_info(documents)")
    columns = {row[1] for row in cursor.fetchall()}
    if "doc_id" not in columns:
        conn.execute("ALTER TABLE documents ADD COLUMN doc_id INTEGER")
        conn.execute("UPDATE documents SET doc_id = id WHERE doc_id IS NULL")
    if "filename" not in columns:
        conn.execute("ALTER TABLE documents ADD COLUMN filename TEXT")
    if "kind" not in columns:
        conn.execute("ALTER TABLE documents ADD COLUMN kind TEXT")
    if "manager_id" not in columns:
        conn.execute("ALTER TABLE documents ADD COLUMN manager_id INTEGER")
    if "content_sha" not in columns:
        conn.execute("ALTER TABLE documents ADD COLUMN content_sha TEXT")
    if "created_at" not in columns:
        conn.execute("ALTER TABLE documents ADD COLUMN created_at TIMESTAMP")
        conn.execute("UPDATE documents SET created_at = CURRENT_TIMESTAMP WHERE created_at IS NULL")
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_documents_content_sha ON documents(content_sha)"
    )


def _ensure_pg_documents_table(conn) -> None:
    """Create/backfill the Postgres documents table used by uploads and search."""
    if register_vector:
        register_vector(conn)
    conn.execute("CREATE EXTENSION IF NOT EXISTS vector")
    conn.execute("""CREATE TABLE IF NOT EXISTS documents (
            doc_id bigserial PRIMARY KEY,
            content text NOT NULL,
            embedding vector(384),
            filename text,
            kind text,
            manager_id bigint,
            content_sha text UNIQUE,
            created_at timestamptz DEFAULT now()
        )""")
    cursor = conn.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name = %s",
        ("documents",),
    )
    columns = {row[0] for row in cursor.fetchall()}
    if "doc_id" not in columns:
        conn.execute("ALTER TABLE documents ADD COLUMN doc_id bigserial")
    if "filename" not in columns:
        conn.execute("ALTER TABLE documents ADD COLUMN filename text")
    if "kind" not in columns:
        conn.execute("ALTER TABLE documents ADD COLUMN kind text")
    if "manager_id" not in columns:
        conn.execute("ALTER TABLE documents ADD COLUMN manager_id bigint")
    if "content_sha" not in columns:
        conn.execute("ALTER TABLE documents ADD COLUMN content_sha text")
    if "created_at" not in columns:
        conn.execute("ALTER TABLE documents ADD COLUMN created_at timestamptz DEFAULT now()")
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_documents_content_sha ON documents(content_sha)"
    )


def store_document(
    text: str,
    db_path: str | None = None,
    *,
    manager_id: int | None = None,
    kind: str = "note",
    filename: str | None = None,
) -> int:
    """Store ``text`` and its embedding in the ``documents`` table and return ``doc_id``."""
    conn = connect_db(db_path)
    is_pg = conn.__class__.__name__ == "Connection" and hasattr(conn, "info")
    content_sha = hashlib.sha256(text.encode("utf-8")).hexdigest()
    if is_pg:
        _ensure_pg_documents_table(conn)
        existing = conn.execute(
            "SELECT doc_id FROM documents WHERE content_sha = %s LIMIT 1",
            (content_sha,),
        ).fetchone()
        if existing and existing[0] is not None:
            conn.close()
            return int(existing[0])
        emb = Vector(embed_text(text)) if register_vector else embed_text(text)
        row = conn.execute(
            "INSERT INTO documents(content, embedding, filename, kind, manager_id, content_sha) "
            "VALUES (%s,%s,%s,%s,%s,%s) RETURNING doc_id",
            (text, emb, filename, kind, manager_id, content_sha),
        ).fetchone()
        doc_id = int(row[0]) if row and row[0] is not None else 0
    else:
        _ensure_sqlite_documents_table(conn)
        existing = conn.execute(
            "SELECT doc_id FROM documents WHERE content_sha = ? LIMIT 1",
            (content_sha,),
        ).fetchone()
        if existing and existing[0] is not None:
            conn.close()
            return int(existing[0])
        emb = json.dumps(embed_text(text))
        cursor = conn.execute(
            "INSERT INTO documents(content, embedding, filename, kind, manager_id, content_sha) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (text, emb, filename, kind, manager_id, content_sha),
        )
        lastrowid = cursor.lastrowid
        doc_id = int(lastrowid) if lastrowid is not None else 0
    conn.commit()
    conn.close()
    return doc_id


def search_documents(query: str, db_path: str | None = None, k: int = 3) -> list[dict[str, Any]]:
    """Return top ``k`` docs similar to ``query``."""
    conn = connect_db(db_path)
    is_pg = conn.__class__.__name__ == "Connection" and hasattr(conn, "info")
    if is_pg and register_vector:
        register_vector(conn)
        qvec = Vector(embed_text(query))
        rows = conn.execute(
            "SELECT content, embedding <=> %s AS dist FROM documents ORDER BY dist LIMIT %s",
            (qvec, k),
        ).fetchall()
        conn.close()
        return [{"content": content, "distance": dist} for content, dist in rows]
    # Process documents one at a time to avoid loading entire dataset into memory
    # Use a heap to keep only top k results, bounding memory to O(k) instead of O(n)
    cur = conn.execute("SELECT content, embedding FROM documents WHERE embedding IS NOT NULL")
    qvec = embed_text(query)
    # Use a max heap (negate distances for heapq which is a min heap)
    heap: list[tuple[float, dict[str, Any]]] = []
    for content, emb_json in cur:
        emb = json.loads(emb_json)
        dist = math.sqrt(sum((a - b) ** 2 for a, b in zip(qvec, emb, strict=False)))
        result = {"content": content, "distance": dist}
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
