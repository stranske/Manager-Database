"""Unified search service across manager intelligence entities."""

from __future__ import annotations

import sqlite3
from typing import Any

from pydantic import BaseModel


class SearchResult(BaseModel):
    """Unified search result payload."""

    entity_type: str
    entity_id: int
    manager_name: str | None
    headline: str
    snippet: str
    relevance: float
    url: str | None
    timestamp: str | None


_BASE_RELEVANCE = {
    "news": 0.7,
    "filing": 0.6,
    "document": 0.5,
    "holding": 0.4,
    "manager": 0.3,
}


def _is_sqlite(conn: Any) -> bool:
    return isinstance(conn, sqlite3.Connection)


def _table_exists(conn: Any, table_name: str) -> bool:
    if _is_sqlite(conn):
        row = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?",
            (table_name,),
        ).fetchone()
        return row is not None
    row = conn.execute("SELECT to_regclass(%s)", (table_name,)).fetchone()
    return bool(row and row[0])


def _score_result(entity_type: str, query: str, headline: str, snippet: str) -> float:
    base = _BASE_RELEVANCE.get(entity_type, 0.1)
    normalized_query = query.strip().lower()
    text = f"{headline} {snippet}".lower()
    if not normalized_query:
        return 0.0
    terms = [term for term in normalized_query.split() if term]
    term_hits = sum(text.count(term) for term in terms)
    exact_bonus = 0.15 if normalized_query in text else 0.0
    hit_bonus = min(term_hits * 0.05, 0.2)
    return min(base + exact_bonus + hit_bonus, 1.0)


def universal_search(query: str, conn: Any, limit: int = 20) -> list[SearchResult]:
    """Search across all entity types and return ranked results."""
    if not query.strip() or limit <= 0:
        return []

    like_token = f"%{query.strip()}%"
    is_sqlite = _is_sqlite(conn)
    results: list[SearchResult] = []

    if _table_exists(conn, "managers"):
        placeholder = "?" if is_sqlite else "%s"
        rows = conn.execute(
            f"SELECT id, name, role FROM managers WHERE name LIKE {placeholder} OR role LIKE {placeholder} LIMIT {placeholder}",
            (like_token, like_token, limit),
        ).fetchall()
        for entity_id, name, role in rows:
            snippet = role or ""
            results.append(
                SearchResult(
                    entity_type="manager",
                    entity_id=int(entity_id),
                    manager_name=name,
                    headline=name,
                    snippet=snippet,
                    relevance=_score_result("manager", query, name, snippet),
                    url=None,
                    timestamp=None,
                )
            )

    if _table_exists(conn, "news"):
        placeholder = "?" if is_sqlite else "%s"
        rows = conn.execute(
            f"SELECT rowid, headline, source, published FROM news WHERE headline LIKE {placeholder} LIMIT {placeholder}",
            (like_token, limit),
        ).fetchall()
        for entity_id, headline, source, published in rows:
            snippet = source or ""
            results.append(
                SearchResult(
                    entity_type="news",
                    entity_id=int(entity_id),
                    manager_name=None,
                    headline=headline or "News item",
                    snippet=snippet,
                    relevance=_score_result("news", query, headline or "", snippet),
                    url=None,
                    timestamp=str(published) if published is not None else None,
                )
            )

    if _table_exists(conn, "documents"):
        placeholder = "?" if is_sqlite else "%s"
        rows = conn.execute(
            f"SELECT id, content FROM documents WHERE content LIKE {placeholder} LIMIT {placeholder}",
            (like_token, limit),
        ).fetchall()
        for entity_id, content in rows:
            text = (content or "").strip()
            headline = text[:80] if text else "Document"
            snippet = text[:180]
            results.append(
                SearchResult(
                    entity_type="document",
                    entity_id=int(entity_id),
                    manager_name=None,
                    headline=headline,
                    snippet=snippet,
                    relevance=_score_result("document", query, headline, snippet),
                    url=None,
                    timestamp=None,
                )
            )

    if _table_exists(conn, "holdings"):
        placeholder = "?" if is_sqlite else "%s"
        rows = conn.execute(
            f"SELECT rowid, cik, accession, filed, nameOfIssuer, cusip FROM holdings WHERE nameOfIssuer LIKE {placeholder} OR cusip LIKE {placeholder} LIMIT {placeholder}",
            (like_token, like_token, limit),
        ).fetchall()
        for entity_id, cik, accession, filed, issuer, cusip in rows:
            headline = f"{issuer or 'Holding'} ({cusip or 'n/a'})"
            snippet = f"Accession {accession or 'n/a'} filed {filed or 'n/a'}"
            results.append(
                SearchResult(
                    entity_type="holding",
                    entity_id=int(entity_id),
                    manager_name=str(cik) if cik is not None else None,
                    headline=headline,
                    snippet=snippet,
                    relevance=_score_result("holding", query, headline, snippet),
                    url=None,
                    timestamp=str(filed) if filed is not None else None,
                )
            )

        filing_rows = conn.execute(
            f"SELECT MIN(rowid), cik, accession, filed, COUNT(*) FROM holdings WHERE accession LIKE {placeholder} OR filed LIKE {placeholder} GROUP BY cik, accession, filed LIMIT {placeholder}",
            (like_token, like_token, limit),
        ).fetchall()
        for entity_id, cik, accession, filed, row_count in filing_rows:
            headline = f"Filing {accession or 'unknown'}"
            snippet = f"{row_count} holdings; filed {filed or 'n/a'}"
            results.append(
                SearchResult(
                    entity_type="filing",
                    entity_id=int(entity_id),
                    manager_name=str(cik) if cik is not None else None,
                    headline=headline,
                    snippet=snippet,
                    relevance=_score_result("filing", query, headline, snippet),
                    url=None,
                    timestamp=str(filed) if filed is not None else None,
                )
            )

    results.sort(key=lambda item: item.relevance, reverse=True)
    return results[:limit]
