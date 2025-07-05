"""Simple text embedding utilities for Stage 4."""

from __future__ import annotations

import json
import math
from collections import Counter
from typing import Any, Dict, List

from adapters.base import connect_db


def embed_text(text: str) -> List[float]:
    """Return a simple letter-frequency vector for ``text``."""
    letters = Counter(c.lower() for c in text if c.isalpha())
    vec = [letters.get(chr(i + 97), 0) for i in range(26)]
    norm = sum(vec) or 1
    return [v / norm for v in vec]


def store_document(text: str, db_path: str | None = None) -> None:
    """Store ``text`` and its embedding in the ``documents`` table."""
    conn = connect_db(db_path)
    conn.execute(
        """CREATE TABLE IF NOT EXISTS documents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            content TEXT,
            embedding TEXT
        )"""
    )
    emb = json.dumps(embed_text(text))
    placeholder = (
        "%s"
        if conn.__class__.__name__ == "Connection" and hasattr(conn, "info")
        else "?"
    )
    conn.execute(
        f"INSERT INTO documents(content, embedding) VALUES ({placeholder},{placeholder})",
        (text, emb),
    )
    conn.commit()
    conn.close()


def search_documents(
    query: str, db_path: str | None = None, k: int = 3
) -> List[Dict[str, Any]]:
    """Return top ``k`` docs similar to ``query``."""
    conn = connect_db(db_path)
    rows = conn.execute("SELECT content, embedding FROM documents").fetchall()
    conn.close()
    qvec = embed_text(query)
    results = []
    for content, emb_json in rows:
        emb = json.loads(emb_json)
        dist = math.sqrt(sum((a - b) ** 2 for a, b in zip(qvec, emb)))
        results.append({"content": content, "distance": dist})
    results.sort(key=lambda r: r["distance"])
    return results[:k]
