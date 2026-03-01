"""Prefect flow orchestrating hourly news harvesting."""

from __future__ import annotations

import json
import logging
import os
import sqlite3
from datetime import UTC, datetime
from typing import Any

from prefect import flow, task

from adapters.base import connect_db
from adapters import news
from etl.logging_setup import configure_logging, log_outcome

configure_logging("news_flow")
logger = logging.getLogger(__name__)


@task
async def fetch_news(source: str, since: str | None = None) -> list[dict[str, Any]]:
    """Fetch and topic-tag new items for a single source."""
    items = await news.list_new_items(source, since)
    tagged_items: list[dict[str, Any]] = []
    for item in items:
        tagged_item = news.tag(item)
        tagged_items.append(tagged_item if tagged_item is not None else item)
    logger.info(
        "Fetched and tagged news items",
        extra={"source": source, "count": len(tagged_items), "since": since},
    )
    return tagged_items


def _normalize_aliases(raw_aliases: Any) -> list[str]:
    if raw_aliases is None:
        return []
    if isinstance(raw_aliases, list | tuple):
        return [str(alias).strip() for alias in raw_aliases if str(alias).strip()]

    if not isinstance(raw_aliases, str):
        value = str(raw_aliases).strip()
        return [value] if value else []

    value = raw_aliases.strip()
    if not value:
        return []

    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        parsed = None
    if isinstance(parsed, list):
        return [str(alias).strip() for alias in parsed if str(alias).strip()]

    if value.startswith("{") and value.endswith("}"):
        inner = value[1:-1].strip()
        if not inner:
            return []
        return [part.strip().strip('"') for part in inner.split(",") if part.strip().strip('"')]
    return [value]


@task
def match_entities(items: list[dict[str, Any]], conn: Any) -> list[dict[str, Any]]:
    """Link news items to managers by name/alias substring matching."""
    rows = conn.execute("SELECT manager_id, name, aliases FROM managers").fetchall()

    manager_terms: list[tuple[int, list[str]]] = []
    for manager_id, name, aliases in rows:
        if manager_id is None:
            continue
        terms: list[str] = []
        if name is not None:
            cleaned_name = str(name).strip().lower()
            if cleaned_name:
                terms.append(cleaned_name)
        terms.extend(alias.lower() for alias in _normalize_aliases(aliases))
        deduped = [term for term in dict.fromkeys(terms) if term]
        if deduped:
            manager_terms.append((int(manager_id), deduped))

    matched = 0
    unmatched = 0

    for item in items:
        text = f"{item.get('headline', '')} {item.get('body_snippet', '')}".lower()
        matched_manager_id: int | None = None
        for manager_id, terms in manager_terms:
            if any(term in text for term in terms):
                matched_manager_id = manager_id
                break

        if matched_manager_id is not None:
            item["manager_id"] = matched_manager_id
            matched += 1
        else:
            item["manager_id"] = None
            unmatched += 1

    logger.info(
        "Entity matching completed",
        extra={
            "items": len(items),
            "managers": len(manager_terms),
            "matched": matched,
            "unmatched": unmatched,
        },
    )
    return items


def _resolve_sources(sources: list[str] | None) -> list[str]:
    if sources is not None:
        return sources
    env_sources = os.getenv("NEWS_SOURCES", "rss,gdelt")
    return [source.strip() for source in env_sources.split(",") if source.strip()]


def _placeholder(conn: Any) -> str:
    return "?" if isinstance(conn, sqlite3.Connection) else "%s"


def _serialize_topics(topics: Any, conn: Any) -> Any:
    if isinstance(conn, sqlite3.Connection):
        if topics is None:
            return "[]"
        if isinstance(topics, str):
            return topics
        return json.dumps(topics)
    return topics if topics is not None else []


def _ensure_news_unique_constraint(conn: Any) -> None:
    conn.execute("""CREATE UNIQUE INDEX IF NOT EXISTS idx_news_items_url_published_at_unique
           ON news_items(url, published_at)""")


def _ensure_watermarks_table(conn: Any) -> None:
    conn.execute("""CREATE TABLE IF NOT EXISTS watermarks (
            source TEXT PRIMARY KEY,
            latest_published_at TEXT NOT NULL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")
    if isinstance(conn, sqlite3.Connection):
        conn.commit()


def _parse_iso_timestamp(value: Any) -> datetime | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed


def _latest_published_at(items: list[dict[str, Any]]) -> str | None:
    latest: datetime | None = None
    for item in items:
        parsed = _parse_iso_timestamp(item.get("published_at"))
        if parsed is None:
            continue
        if latest is None or parsed > latest:
            latest = parsed
    return latest.isoformat() if latest is not None else None


def _fetch_source_watermark(conn: Any, source: str) -> str | None:
    ph = _placeholder(conn)
    row = conn.execute(
        f"SELECT latest_published_at FROM watermarks WHERE source = {ph}",
        (source,),
    ).fetchone()
    if row and row[0]:
        return str(row[0])
    return None


def _fallback_source_since_from_news(conn: Any, source: str) -> str | None:
    ph = _placeholder(conn)
    try:
        row = conn.execute(
            f"SELECT MAX(published_at) FROM news_items WHERE source = {ph}",
            (source,),
        ).fetchone()
    except Exception:
        return None
    if row and row[0]:
        return str(row[0])
    return None


@task
def resolve_source_since(source: str, since: str | None, conn: Any) -> str | None:
    """Resolve the effective since watermark for a source."""
    if since is not None:
        return since
    return _fetch_source_watermark(conn, source) or _fallback_source_since_from_news(conn, source)


@task
def update_source_watermark(source: str, items: list[dict[str, Any]], conn: Any) -> str | None:
    """Advance the source watermark to the latest published timestamp in items."""
    latest = _latest_published_at(items)
    if latest is None:
        return _fetch_source_watermark(conn, source)

    current = _fetch_source_watermark(conn, source)
    current_dt = _parse_iso_timestamp(current)
    latest_dt = _parse_iso_timestamp(latest)
    if latest_dt is None:
        return current
    if current_dt is not None and latest_dt <= current_dt:
        return current

    ph = _placeholder(conn)
    conn.execute(
        f"""INSERT INTO watermarks (source, latest_published_at)
            VALUES ({ph}, {ph})
            ON CONFLICT(source)
            DO UPDATE SET latest_published_at = excluded.latest_published_at,
                          updated_at = CURRENT_TIMESTAMP""",
        (source, latest),
    )
    if isinstance(conn, sqlite3.Connection):
        conn.commit()
    return latest


@task
def persist_news(items: list[dict[str, Any]], conn: Any) -> int:
    """Persist tagged news items, skipping duplicates by (url, published_at)."""
    if not items:
        logger.info("Persist skipped: no items to write")
        return 0

    _ensure_news_unique_constraint(conn)
    ph = _placeholder(conn)
    sql = (
        "INSERT INTO news_items "
        "(manager_id, published_at, source, headline, url, body_snippet, topics, confidence) "
        f"VALUES ({ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}) "
        "ON CONFLICT(url, published_at) DO NOTHING"
    )

    inserted = 0
    for item in items:
        cursor = conn.execute(
            sql,
            (
                item.get("manager_id"),
                item["published_at"],
                item["source"],
                item["headline"],
                item.get("url"),
                item.get("body_snippet"),
                _serialize_topics(item.get("topics", []), conn),
                item.get("confidence"),
            ),
        )
        rowcount = getattr(cursor, "rowcount", 0)
        if isinstance(rowcount, int) and rowcount > 0:
            inserted += 1

    if isinstance(conn, sqlite3.Connection):
        conn.commit()

    logger.info(
        "Persisted news items",
        extra={"attempted": len(items), "inserted": inserted, "duplicates": len(items) - inserted},
    )
    return inserted


@flow
async def news_flow(sources: list[str] | None = None, since: str | None = None):
    """Hourly news harvest flow.

    Fetches news from configured sources, tags topics, matches to managers,
    and persists to news_items table.
    """

    resolved_sources = _resolve_sources(sources)
    logger.info("News flow started", extra={"sources": resolved_sources, "since": since})

    conn = connect_db()
    source_since: dict[str, str | None] = {}
    source_watermarks: dict[str, str | None] = {}
    total_fetched = 0
    total_inserted = 0
    try:
        _ensure_watermarks_table(conn)
        for source in resolved_sources:
            effective_since = resolve_source_since.fn(source, since, conn)
            source_since[source] = effective_since

            fetched_items = await fetch_news.fn(source, effective_since)
            total_fetched += len(fetched_items)

            matched_items = match_entities.fn(fetched_items, conn)
            inserted = persist_news.fn(matched_items, conn)
            total_inserted += inserted

            source_watermarks[source] = update_source_watermark.fn(source, fetched_items, conn)
    finally:
        conn.close()

    has_data = total_fetched > 0
    log_outcome(
        logger,
        "News flow completed",
        has_data=has_data,
        extra={
            "sources": resolved_sources,
            "since": since,
            "source_since": source_since,
            "watermarks": source_watermarks,
            "fetched": total_fetched,
            "inserted": total_inserted,
        },
    )
    return {
        "sources": resolved_sources,
        "since": since,
        "source_since": source_since,
        "watermarks": source_watermarks,
        "fetched": total_fetched,
        "inserted": total_inserted,
    }
