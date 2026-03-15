"""Unified search service across manager intelligence entities."""

from __future__ import annotations

import sqlite3
from typing import Any, Literal

from pydantic import BaseModel, Field

SearchEntityType = Literal["filing", "holding", "news", "document", "manager"]


class SearchResult(BaseModel):
    """Unified search result payload."""

    entity_type: SearchEntityType
    entity_id: int
    manager_name: str | None
    headline: str
    snippet: str
    relevance: float = Field(ge=0.0, le=1.0)
    url: str | None
    timestamp: str | None


_BASE_RELEVANCE = {
    "news": 0.7,
    "filing": 0.6,
    "document": 0.5,
    "holding": 0.4,
    "manager": 0.3,
}

_VALID_ENTITY_TYPES: set[str] = {"filing", "holding", "news", "document", "manager"}


def _is_sqlite(conn: Any) -> bool:
    return isinstance(conn, sqlite3.Connection)


def _get_columns(conn: Any, table_name: str) -> set[str]:
    if not _table_exists(conn, table_name):
        return set()
    if _is_sqlite(conn):
        rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        return {str(row[1]) for row in rows}
    rows = conn.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = %s",
        (table_name,),
    ).fetchall()
    return {str(row[0]) for row in rows}


def _table_exists(conn: Any, table_name: str) -> bool:
    if _is_sqlite(conn):
        row = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?",
            (table_name,),
        ).fetchone()
        return row is not None
    row = conn.execute("SELECT to_regclass(%s)", (table_name,)).fetchone()
    return bool(row and row[0])


def _normalize_rank(rank: float | int | None) -> float:
    if rank is None:
        return 0.0
    rank_value = float(rank)
    if rank_value <= 0.0:
        return 0.0
    return min(rank_value / (rank_value + 1.0), 1.0)


def _vector_relevance(distance: float | int | None) -> float:
    if distance is None:
        return 0.0
    dist = max(float(distance), 0.0)
    if dist > 1.0:
        dist = 1.0
    return 1.0 - dist


def _score_result(
    entity_type: str,
    query: str,
    headline: str,
    snippet: str,
    *,
    fts_rank: float | int | None = None,
    vector_distance: float | int | None = None,
) -> float:
    base = _BASE_RELEVANCE.get(entity_type, 0.1)
    normalized_query = query.strip().lower()
    text = f"{headline} {snippet}".lower()
    if not normalized_query:
        return 0.0
    terms = [term for term in normalized_query.split() if term]
    term_hits = sum(text.count(term) for term in terms)
    exact_bonus = 0.15 if normalized_query in text else 0.0
    hit_bonus = min(term_hits * 0.05, 0.2)
    fts_bonus = _normalize_rank(fts_rank) * 0.2
    vector_bonus = _vector_relevance(vector_distance) * 0.15
    return min(base + exact_bonus + hit_bonus + fts_bonus + vector_bonus, 1.0)


def _format_activism_filing_headline(
    manager_name: str | None,
    subject_company: str | None,
    ownership_pct: float | int | None,
) -> str:
    manager = (manager_name or "Unknown manager").strip() or "Unknown manager"
    company = (subject_company or "Unknown subject").strip() or "Unknown subject"
    if ownership_pct is None:
        pct_text = "n/a"
    else:
        pct_text = f"{float(ownership_pct):.1f}%"
    return f"13D Filing: {manager} -> {company} ({pct_text})"


def _sqlite_manager_name_map(conn: Any) -> dict[int, str]:
    manager_columns = _get_columns(conn, "managers")
    if not manager_columns or "name" not in manager_columns:
        return {}
    manager_id_col = "manager_id" if "manager_id" in manager_columns else "id"
    rows = conn.execute(f"SELECT {manager_id_col}, name FROM managers").fetchall()
    return {
        int(manager_id): str(name) for manager_id, name in rows if manager_id is not None and name
    }


def _search_postgres(query: str, conn: Any, limit: int) -> list[SearchResult]:
    results: list[SearchResult] = []

    if _table_exists(conn, "managers"):
        rows = conn.execute(
            """
            SELECT
                m.manager_id,
                m.name,
                COALESCE(array_to_string(m.aliases, ' '), '') AS alias_text,
                ts_rank(
                    to_tsvector('english', COALESCE(m.name, '') || ' ' || COALESCE(array_to_string(m.aliases, ' '), '')),
                    plainto_tsquery('english', %s)
                ) AS fts_rank
            FROM managers m
            WHERE to_tsvector('english', COALESCE(m.name, '') || ' ' || COALESCE(array_to_string(m.aliases, ' '), ''))
                @@ plainto_tsquery('english', %s)
            ORDER BY fts_rank DESC
            LIMIT %s
            """,
            (query, query, limit),
        ).fetchall()
        for manager_id, name, alias_text, fts_rank in rows:
            snippet = alias_text or ""
            results.append(
                SearchResult(
                    entity_type="manager",
                    entity_id=int(manager_id),
                    manager_name=name,
                    headline=name or "Manager",
                    snippet=snippet,
                    relevance=_score_result(
                        "manager",
                        query,
                        name or "",
                        snippet,
                        fts_rank=fts_rank,
                    ),
                    url=None,
                    timestamp=None,
                )
            )

    if _table_exists(conn, "filings"):
        rows = conn.execute(
            """
            SELECT
                f.filing_id,
                m.name,
                f.type,
                f.raw_key,
                f.period_end,
                f.url,
                ts_rank(
                    to_tsvector('english', COALESCE(m.name, '') || ' ' || COALESCE(f.type, '') || ' ' || COALESCE(f.raw_key, '') || ' ' || COALESCE(f.period_end::text, '')),
                    plainto_tsquery('english', %s)
                ) AS fts_rank
            FROM filings f
            LEFT JOIN managers m ON m.manager_id = f.manager_id
            WHERE to_tsvector('english', COALESCE(m.name, '') || ' ' || COALESCE(f.type, '') || ' ' || COALESCE(f.raw_key, '') || ' ' || COALESCE(f.period_end::text, ''))
                @@ plainto_tsquery('english', %s)
            ORDER BY fts_rank DESC, f.created_at DESC
            LIMIT %s
            """,
            (query, query, limit),
        ).fetchall()
        for filing_id, manager_name, filing_type, raw_key, period_end, url, fts_rank in rows:
            headline = f"{filing_type or 'Filing'} filing"
            snippet = f"Raw key: {raw_key or 'n/a'} | Period end: {period_end or 'n/a'}"
            results.append(
                SearchResult(
                    entity_type="filing",
                    entity_id=int(filing_id),
                    manager_name=manager_name,
                    headline=headline,
                    snippet=snippet,
                    relevance=_score_result(
                        "filing",
                        query,
                        headline,
                        snippet,
                        fts_rank=fts_rank,
                    ),
                    url=url,
                    timestamp=str(period_end) if period_end is not None else None,
                )
            )

    if _table_exists(conn, "activism_filings"):
        rows = conn.execute(
            """
            SELECT
                af.filing_id,
                m.name,
                af.filing_type,
                af.subject_company,
                af.subject_cusip,
                af.ownership_pct,
                af.filed_date,
                af.url,
                ts_rank(
                    to_tsvector(
                        'english',
                        COALESCE(m.name, '') || ' ' || COALESCE(af.filing_type, '') || ' '
                        || COALESCE(af.subject_company, '') || ' ' || COALESCE(af.subject_cusip, '')
                    ),
                    plainto_tsquery('english', %s)
                ) AS fts_rank
            FROM activism_filings af
            LEFT JOIN managers m ON m.manager_id = af.manager_id
            WHERE to_tsvector(
                    'english',
                    COALESCE(m.name, '') || ' ' || COALESCE(af.filing_type, '') || ' '
                    || COALESCE(af.subject_company, '') || ' ' || COALESCE(af.subject_cusip, '')
                ) @@ plainto_tsquery('english', %s)
               OR upper(COALESCE(af.subject_cusip, '')) = upper(%s)
            ORDER BY fts_rank DESC, af.filed_date DESC
            LIMIT %s
            """,
            (query, query, query, limit),
        ).fetchall()
        for (
            filing_id,
            manager_name,
            filing_type,
            subject_company,
            cusip,
            ownership_pct,
            filed_date,
            url,
            fts_rank,
        ) in rows:
            headline = _format_activism_filing_headline(
                manager_name,
                subject_company,
                ownership_pct,
            )
            snippet = (
                f"{filing_type or 'Activism filing'} | CUSIP: {cusip or 'n/a'} | "
                f"Filed: {filed_date or 'n/a'}"
            )
            results.append(
                SearchResult(
                    entity_type="filing",
                    entity_id=int(filing_id),
                    manager_name=manager_name,
                    headline=headline,
                    snippet=snippet,
                    relevance=_score_result(
                        "filing",
                        query,
                        headline,
                        snippet,
                        fts_rank=fts_rank,
                    ),
                    url=url,
                    timestamp=str(filed_date) if filed_date is not None else None,
                )
            )

    if _table_exists(conn, "holdings"):
        rows = conn.execute(
            """
            SELECT
                h.holding_id,
                m.name,
                h.name_of_issuer,
                h.cusip,
                f.filed_date,
                ts_rank(
                    to_tsvector('english', COALESCE(h.name_of_issuer, '')),
                    plainto_tsquery('english', %s)
                ) AS issuer_rank,
                CASE WHEN upper(COALESCE(h.cusip, '')) = upper(%s) THEN 1.0 ELSE 0.0 END AS cusip_rank
            FROM holdings h
            JOIN filings f ON f.filing_id = h.filing_id
            LEFT JOIN managers m ON m.manager_id = f.manager_id
            WHERE to_tsvector('english', COALESCE(h.name_of_issuer, '')) @@ plainto_tsquery('english', %s)
               OR upper(COALESCE(h.cusip, '')) = upper(%s)
            ORDER BY GREATEST(
                ts_rank(to_tsvector('english', COALESCE(h.name_of_issuer, '')), plainto_tsquery('english', %s)),
                CASE WHEN upper(COALESCE(h.cusip, '')) = upper(%s) THEN 1.0 ELSE 0.0 END
            ) DESC
            LIMIT %s
            """,
            (query, query, query, query, query, query, limit),
        ).fetchall()
        for holding_id, manager_name, issuer, cusip, filed_date, issuer_rank, cusip_rank in rows:
            headline = f"{issuer or 'Holding'} ({cusip or 'n/a'})"
            snippet = f"CUSIP: {cusip or 'n/a'}"
            results.append(
                SearchResult(
                    entity_type="holding",
                    entity_id=int(holding_id),
                    manager_name=manager_name,
                    headline=headline,
                    snippet=snippet,
                    relevance=_score_result(
                        "holding",
                        query,
                        headline,
                        snippet,
                        fts_rank=max(float(issuer_rank or 0.0), float(cusip_rank or 0.0)),
                    ),
                    url=None,
                    timestamp=str(filed_date) if filed_date is not None else None,
                )
            )

    if _table_exists(conn, "news_items"):
        rows = conn.execute(
            """
            SELECT
                n.news_id,
                m.name,
                n.headline,
                n.body_snippet,
                n.url,
                n.published_at,
                ts_rank(
                    to_tsvector('english', COALESCE(n.headline, '') || ' ' || COALESCE(n.body_snippet, '')),
                    plainto_tsquery('english', %s)
                ) AS fts_rank
            FROM news_items n
            LEFT JOIN managers m ON m.manager_id = n.manager_id
            WHERE to_tsvector('english', COALESCE(n.headline, '') || ' ' || COALESCE(n.body_snippet, ''))
                @@ plainto_tsquery('english', %s)
            ORDER BY fts_rank DESC, n.published_at DESC
            LIMIT %s
            """,
            (query, query, limit),
        ).fetchall()
        for news_id, manager_name, headline, body_snippet, url, published_at, fts_rank in rows:
            snippet = body_snippet or ""
            results.append(
                SearchResult(
                    entity_type="news",
                    entity_id=int(news_id),
                    manager_name=manager_name,
                    headline=headline or "News item",
                    snippet=snippet,
                    relevance=_score_result(
                        "news",
                        query,
                        headline or "",
                        snippet,
                        fts_rank=fts_rank,
                    ),
                    url=url,
                    timestamp=str(published_at) if published_at is not None else None,
                )
            )

    if _table_exists(conn, "documents"):
        rows = conn.execute(
            """
            SELECT
                d.doc_id,
                m.name,
                d.filename,
                d.text,
                d.created_at,
                ts_rank(
                    to_tsvector('english', COALESCE(d.filename, '') || ' ' || COALESCE(d.text, '')),
                    plainto_tsquery('english', %s)
                ) AS fts_rank
            FROM documents d
            LEFT JOIN managers m ON m.manager_id = d.manager_id
            WHERE to_tsvector('english', COALESCE(d.filename, '') || ' ' || COALESCE(d.text, ''))
                @@ plainto_tsquery('english', %s)
            ORDER BY fts_rank DESC, d.created_at DESC
            LIMIT %s
            """,
            (query, query, limit),
        ).fetchall()
        for doc_id, manager_name, filename, text, created_at, fts_rank in rows:
            full_text = (text or "").strip()
            headline = filename or (full_text[:80] if full_text else "Document")
            snippet = full_text[:180] if full_text else ""
            results.append(
                SearchResult(
                    entity_type="document",
                    entity_id=int(doc_id),
                    manager_name=manager_name,
                    headline=headline,
                    snippet=snippet,
                    relevance=_score_result(
                        "document",
                        query,
                        headline,
                        snippet,
                        fts_rank=fts_rank,
                    ),
                    url=None,
                    timestamp=str(created_at) if created_at is not None else None,
                )
            )

    return results


def _search_sqlite(query: str, conn: Any, limit: int) -> list[SearchResult]:
    results: list[SearchResult] = []
    like_token = f"%{query.strip()}%"
    manager_name_by_id = _sqlite_manager_name_map(conn)

    manager_columns = _get_columns(conn, "managers")
    if manager_columns:
        manager_id_col = "manager_id" if "manager_id" in manager_columns else "id"
        aliases_col = (
            "aliases"
            if "aliases" in manager_columns
            else ("role" if "role" in manager_columns else "''")
        )
        rows = conn.execute(
            f"SELECT {manager_id_col}, name, COALESCE({aliases_col}, '') FROM managers "
            "WHERE name LIKE ? OR COALESCE(" + aliases_col + ", '') LIKE ? LIMIT ?",
            (like_token, like_token, limit),
        ).fetchall()
        for entity_id, name, aliases_or_role in rows:
            snippet = aliases_or_role or ""
            results.append(
                SearchResult(
                    entity_type="manager",
                    entity_id=int(entity_id),
                    manager_name=name,
                    headline=name or "Manager",
                    snippet=snippet,
                    relevance=_score_result("manager", query, name or "", snippet),
                    url=None,
                    timestamp=None,
                )
            )

    if _table_exists(conn, "news_items"):
        rows = conn.execute(
            "SELECT news_id, manager_id, headline, body_snippet, url, published_at "
            "FROM news_items WHERE headline LIKE ? OR COALESCE(body_snippet, '') LIKE ? "
            "ORDER BY published_at DESC LIMIT ?",
            (like_token, like_token, limit),
        ).fetchall()
        for entity_id, manager_id, headline, body_snippet, url, published_at in rows:
            results.append(
                SearchResult(
                    entity_type="news",
                    entity_id=int(entity_id),
                    manager_name=(
                        manager_name_by_id.get(int(manager_id)) if manager_id is not None else None
                    ),
                    headline=headline or "News item",
                    snippet=body_snippet or "",
                    relevance=_score_result("news", query, headline or "", body_snippet or ""),
                    url=url,
                    timestamp=str(published_at) if published_at is not None else None,
                )
            )
    elif _table_exists(conn, "news"):
        rows = conn.execute(
            "SELECT rowid, headline, source, published FROM news WHERE headline LIKE ? LIMIT ?",
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

    doc_columns = _get_columns(conn, "documents")
    if doc_columns:
        doc_id_col = "doc_id" if "doc_id" in doc_columns else "id"
        doc_text_col = (
            "text" if "text" in doc_columns else ("content" if "content" in doc_columns else None)
        )
        doc_filename_col = "filename" if "filename" in doc_columns else "''"
        doc_manager_col = "manager_id" if "manager_id" in doc_columns else "NULL"
        doc_created_col = "created_at" if "created_at" in doc_columns else "NULL"
        if doc_text_col:
            rows = conn.execute(
                f"SELECT {doc_id_col}, {doc_manager_col}, {doc_filename_col}, {doc_text_col}, {doc_created_col} FROM documents "
                f"WHERE COALESCE({doc_filename_col}, '') LIKE ? OR COALESCE({doc_text_col}, '') LIKE ? LIMIT ?",
                (like_token, like_token, limit),
            ).fetchall()
            for entity_id, manager_id, filename, text, created_at in rows:
                content = (text or "").strip()
                headline = filename or (content[:80] if content else "Document")
                snippet = content[:180]
                results.append(
                    SearchResult(
                        entity_type="document",
                        entity_id=int(entity_id),
                        manager_name=(
                            manager_name_by_id.get(int(manager_id))
                            if manager_id is not None
                            else None
                        ),
                        headline=headline,
                        snippet=snippet,
                        relevance=_score_result("document", query, headline, snippet),
                        url=None,
                        timestamp=str(created_at) if created_at is not None else None,
                    )
                )

            db_path: str | None = None
            try:
                db_rows = conn.execute("PRAGMA database_list").fetchall()
                if db_rows:
                    maybe_path = str(db_rows[0][2] or "")
                    if maybe_path and maybe_path != ":memory:":
                        db_path = maybe_path
            except Exception:
                db_path = None

            if db_path:
                try:
                    from embeddings import search_documents

                    vector_hits = search_documents(query, db_path=db_path, k=limit)
                except Exception:
                    vector_hits = []

                for hit in vector_hits:
                    content = str(hit.get("content") or "").strip()
                    if not content:
                        continue
                    match = conn.execute(
                        f"SELECT {doc_id_col}, {doc_manager_col}, {doc_filename_col}, {doc_created_col} "
                        f"FROM documents WHERE {doc_text_col} = ? LIMIT 1",
                        (content,),
                    ).fetchone()
                    if match is None:
                        continue
                    entity_id, manager_id, filename, created_at = match
                    headline = (filename or "").strip() or content[:80] or "Document"
                    snippet = content[:180]
                    results.append(
                        SearchResult(
                            entity_type="document",
                            entity_id=int(entity_id),
                            manager_name=(
                                manager_name_by_id.get(int(manager_id))
                                if manager_id is not None
                                else None
                            ),
                            headline=headline,
                            snippet=snippet,
                            relevance=_score_result(
                                "document",
                                query,
                                headline,
                                snippet,
                                vector_distance=hit.get("distance"),
                            ),
                            url=None,
                            timestamp=str(created_at) if created_at is not None else None,
                        )
                    )

    holdings_columns = _get_columns(conn, "holdings")
    if holdings_columns:
        if "holding_id" in holdings_columns:
            filing_columns = _get_columns(conn, "filings")
            filing_date_col = (
                "filed_date"
                if "filed_date" in filing_columns
                else ("period_end" if "period_end" in filing_columns else None)
            )
            filing_date_expr = f"f.{filing_date_col}" if filing_date_col else "NULL"
            rows = conn.execute(
                "SELECT h.holding_id, f.manager_id, h.name_of_issuer, h.cusip, "
                + filing_date_expr
                + " "
                "FROM holdings h JOIN filings f ON f.filing_id = h.filing_id "
                "WHERE COALESCE(h.name_of_issuer, '') LIKE ? OR upper(COALESCE(h.cusip, '')) = upper(?) "
                "LIMIT ?",
                (like_token, query.strip(), limit),
            ).fetchall()
            for entity_id, manager_id, issuer, cusip, filed_date in rows:
                headline = f"{issuer or 'Holding'} ({cusip or 'n/a'})"
                snippet = f"CUSIP: {cusip or 'n/a'}"
                results.append(
                    SearchResult(
                        entity_type="holding",
                        entity_id=int(entity_id),
                        manager_name=(
                            manager_name_by_id.get(int(manager_id))
                            if manager_id is not None
                            else None
                        ),
                        headline=headline,
                        snippet=snippet,
                        relevance=_score_result("holding", query, headline, snippet),
                        url=None,
                        timestamp=str(filed_date) if filed_date is not None else None,
                    )
                )
        else:
            rows = conn.execute(
                "SELECT rowid, cik, accession, filed, nameOfIssuer, cusip FROM holdings "
                "WHERE nameOfIssuer LIKE ? OR cusip LIKE ? LIMIT ?",
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
                "SELECT MIN(rowid), cik, accession, filed, COUNT(*) FROM holdings "
                "WHERE accession LIKE ? OR filed LIKE ? GROUP BY cik, accession, filed LIMIT ?",
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

    if _table_exists(conn, "filings") and "holding_id" in holdings_columns:
        filing_columns = _get_columns(conn, "filings")
        filing_manager_expr = "f.manager_id" if "manager_id" in filing_columns else "NULL"
        filing_url_expr = "f.url" if "url" in filing_columns else "NULL"
        filing_type_expr = "f.type" if "type" in filing_columns else "NULL"
        filing_raw_key_expr = "f.raw_key" if "raw_key" in filing_columns else "NULL"
        filing_period_expr = "f.period_end" if "period_end" in filing_columns else "NULL"
        manager_join = ""
        manager_name_expr = "NULL"
        where_clauses = [
            f"COALESCE({filing_type_expr}, '') LIKE ?",
            f"COALESCE({filing_raw_key_expr}, '') LIKE ?",
            f"COALESCE({filing_period_expr}, '') LIKE ?",
        ]
        params: list[Any] = [like_token, like_token, like_token]
        manager_columns = _get_columns(conn, "managers")
        if manager_columns and "name" in manager_columns and filing_manager_expr != "NULL":
            manager_id_col = "manager_id" if "manager_id" in manager_columns else "id"
            manager_join = f" LEFT JOIN managers m ON m.{manager_id_col} = {filing_manager_expr}"
            manager_name_expr = "m.name"
            where_clauses.append("COALESCE(m.name, '') LIKE ?")
            params.append(like_token)

        rows = conn.execute(
            f"SELECT f.filing_id, {filing_manager_expr}, {manager_name_expr}, "
            f"{filing_type_expr}, {filing_raw_key_expr}, {filing_period_expr}, {filing_url_expr} "
            f"FROM filings f{manager_join} "
            f"WHERE {' OR '.join(where_clauses)} "
            "LIMIT ?",
            (*params, limit),
        ).fetchall()
        for filing_id, manager_id, manager_name, filing_type, raw_key, period_end, url in rows:
            headline = f"{filing_type or 'Filing'} filing"
            manager_text = f"Manager: {manager_name}" if manager_name else ""
            context = f"Raw key: {raw_key or 'n/a'} | Period end: {period_end or 'n/a'}"
            snippet = " | ".join(filter(None, [manager_text, context]))
            results.append(
                SearchResult(
                    entity_type="filing",
                    entity_id=int(filing_id),
                    manager_name=manager_name
                    or (
                        manager_name_by_id.get(int(manager_id)) if manager_id is not None else None
                    ),
                    headline=headline,
                    snippet=snippet,
                    relevance=_score_result("filing", query, headline, snippet),
                    url=url,
                    timestamp=str(period_end) if period_end is not None else None,
                )
            )

    if _table_exists(conn, "activism_filings"):
        manager_columns = _get_columns(conn, "managers")
        manager_join = ""
        manager_name_expr = "NULL"
        if manager_columns and "name" in manager_columns:
            manager_id_col = "manager_id" if "manager_id" in manager_columns else "id"
            manager_join = f" LEFT JOIN managers m ON m.{manager_id_col} = af.manager_id"
            manager_name_expr = "m.name"
        rows = conn.execute(
            f"SELECT af.filing_id, af.manager_id, {manager_name_expr}, af.filing_type, "
            "af.subject_company, af.subject_cusip, af.ownership_pct, af.filed_date, af.url "
            f"FROM activism_filings af{manager_join} "
            "WHERE COALESCE(af.filing_type, '') LIKE ? "
            "OR COALESCE(af.subject_company, '') LIKE ? "
            "OR upper(COALESCE(af.subject_cusip, '')) = upper(?) "
            + ("OR COALESCE(m.name, '') LIKE ? " if manager_join else "")
            + "LIMIT ?",
            (
                (like_token, like_token, query.strip(), like_token, limit)
                if manager_join
                else (like_token, like_token, query.strip(), limit)
            ),
        ).fetchall()
        for (
            filing_id,
            manager_id,
            manager_name,
            filing_type,
            subject_company,
            cusip,
            ownership_pct,
            filed_date,
            url,
        ) in rows:
            resolved_manager_name = manager_name or (
                manager_name_by_id.get(int(manager_id)) if manager_id is not None else None
            )
            headline = _format_activism_filing_headline(
                resolved_manager_name,
                subject_company,
                ownership_pct,
            )
            snippet = (
                f"{filing_type or 'Activism filing'} | CUSIP: {cusip or 'n/a'} | "
                f"Filed: {filed_date or 'n/a'}"
            )
            results.append(
                SearchResult(
                    entity_type="filing",
                    entity_id=int(filing_id),
                    manager_name=resolved_manager_name,
                    headline=headline,
                    snippet=snippet,
                    relevance=_score_result("filing", query, headline, snippet),
                    url=url,
                    timestamp=str(filed_date) if filed_date is not None else None,
                )
            )

    return results


def universal_search(
    query: str,
    conn: Any,
    limit: int = 20,
    entity_type: SearchEntityType | None = None,
) -> list[SearchResult]:
    """Search across all entity types and return ranked results."""
    if not query.strip() or limit <= 0:
        return []

    results = (
        _search_sqlite(query, conn, limit)
        if _is_sqlite(conn)
        else _search_postgres(query, conn, limit)
    )

    allowed_entity_types = (
        {entity_type}
        if entity_type in _VALID_ENTITY_TYPES
        else (_VALID_ENTITY_TYPES if entity_type is None else set())
    )
    deduped: dict[tuple[str, int], SearchResult] = {}
    for item in results:
        if item.entity_type not in allowed_entity_types:
            continue
        key = (item.entity_type, item.entity_id)
        existing = deduped.get(key)
        if existing is None or item.relevance > existing.relevance:
            deduped[key] = item

    ranked = sorted(deduped.values(), key=lambda item: item.relevance, reverse=True)
    return ranked[:limit]
