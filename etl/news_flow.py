"""Prefect flow orchestrating hourly news harvesting."""

from __future__ import annotations

import json
import logging
import os
import sqlite3
from typing import Any

from prefect import flow, task

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
    logger.info(
        "News flow started",
        extra={"sources": resolved_sources, "since": since},
    )
    log_outcome(
        logger,
        "News flow completed",
        extra={"sources": resolved_sources, "since": since},
    )
    return {"sources": resolved_sources, "since": since}
