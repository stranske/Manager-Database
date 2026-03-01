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
                id SERIAL PRIMARY KEY,
                content TEXT,
                sha256 TEXT UNIQUE,
                embedding vector(384)
            )""")
        existing = conn.execute(
            "SELECT id FROM documents WHERE sha256 = %s",
            (sha256,),
        ).fetchone()
        if existing:
            conn.commit()
            conn.close()
            return int(existing[0])
        emb = Vector(embed_text(text)) if register_vector else embed_text(text)
        result = conn.execute(
            "INSERT INTO documents(content, sha256, embedding) VALUES (%s,%s,%s) RETURNING id",
            (text, sha256, emb),
        )
        row = result.fetchone()
        if row is None:
            raise RuntimeError("Failed to insert document")
        doc_id = int(row[0])
    else:
        conn.execute("""CREATE TABLE IF NOT EXISTS documents (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                content TEXT,
                sha256 TEXT UNIQUE,
                embedding TEXT
            )""")
        existing = conn.execute(
            "SELECT id FROM documents WHERE sha256 = ?",
            (sha256,),
        ).fetchone()
        if existing:
            conn.commit()
            conn.close()
            return int(existing[0])
        emb = json.dumps(embed_text(text))
        cur = conn.execute(
            "INSERT INTO documents(content, sha256, embedding) VALUES (?, ?, ?)",
            (text, sha256, emb),
        )
        doc_id = int(cur.lastrowid)
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
    cur = conn.execute("SELECT content, embedding FROM documents")
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
