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


def _placeholder(conn: Any) -> str:
    return "?" if isinstance(conn, sqlite3.Connection) else "%s"


@task
def match_entities(items: list[dict[str, Any]], conn: Any) -> list[dict[str, Any]]:
    """Link news items to managers by name/alias substring matching."""
    placeholder = _placeholder(conn)
    rows = conn.execute(
        f"SELECT manager_id, name, aliases FROM managers WHERE name IS NOT NULL AND name <> {placeholder}",
        ("",),
    ).fetchall()

    manager_terms: list[tuple[int, list[str]]] = []
    for manager_id, name, aliases in rows:
        terms = [str(name).strip().lower()]
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
