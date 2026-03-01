"""News ingestion adapter scaffold."""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from time import struct_time
from typing import Any

import httpx

from .base import tracked_call

try:
    import feedparser
except ImportError:  # pragma: no cover - dependency added in follow-up task
    feedparser = None

logger = logging.getLogger(__name__)
DEFAULT_RSS_FEEDS = (
    "https://www.sec.gov/news/pressreleases.rss",
    "https://www.sec.gov/rss/litigation/litreleases.xml",
)


async def list_new_items(source: str, since: str) -> list[dict[str, Any]]:
    """Discover new news items from a source since a watermark timestamp.

    Args:
        source: One of ``rss``, ``gdelt``, ``sec_press``, ``enforcement``.
        since: ISO timestamp watermark.

    Returns:
        List of dicts with keys: headline, url, published_at, source, body_snippet.
    """

    if source in {"rss", "sec_press", "enforcement"}:
        return await _fetch_rss(since)
    if source == "gdelt":
        return await _fetch_gdelt(since)
    raise ValueError(f"Unsupported news source: {source}")


async def download(item: dict[str, Any]) -> str:
    """Fetch the full article text for a news item."""

    _ = item
    return ""


def tag(item: dict[str, Any]) -> dict[str, Any]:
    """Add topic tags and confidence score to a news item."""

    enriched = dict(item)
    enriched.setdefault("topics", [])
    enriched.setdefault("confidence", 0.0)
    return enriched


async def _fetch_rss(since: str) -> list[dict[str, Any]]:
    """Fetch RSS/Atom items newer than the watermark timestamp."""

    parser = feedparser
    if parser is None:
        logger.warning("feedparser is not installed; skipping RSS fetch")
        return []

    since_dt = _parse_iso_timestamp(since)
    feed_urls = _configured_rss_feeds()
    collected: list[dict[str, Any]] = []

    async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
        for feed_url in feed_urls:
            try:
                async with tracked_call("rss", feed_url) as log:
                    response = await client.get(feed_url)
                    log(response)
                response.raise_for_status()
            except (httpx.HTTPError, httpx.TimeoutException) as exc:
                logger.warning("RSS feed request failed for %s: %s", feed_url, exc)
                continue

            parsed_feed = parser.parse(response.text)
            for entry in parsed_feed.entries:
                published_dt = _entry_timestamp(entry)
                if published_dt is None or published_dt <= since_dt:
                    continue

                headline = str(entry.get("title", "")).strip()
                link = str(entry.get("link", "")).strip()
                summary = str(entry.get("summary", "")).strip()
                collected.append(
                    {
                        "headline": headline,
                        "url": link,
                        "published_at": published_dt.isoformat(),
                        "source": "rss",
                        "body_snippet": summary,
                    }
                )

    collected.sort(key=lambda item: item["published_at"], reverse=True)
    return collected


async def _fetch_gdelt(since: str) -> list[dict[str, Any]]:
    """Placeholder GDELT fetcher; implemented in a follow-up task."""

    _ = since
    return []


def _configured_rss_feeds() -> list[str]:
    env_value = os.getenv("NEWS_RSS_FEEDS", "")
    if not env_value.strip():
        return list(DEFAULT_RSS_FEEDS)
    return [url.strip() for url in env_value.split(",") if url.strip()]


def _parse_iso_timestamp(value: str) -> datetime:
    cleaned = value.strip()
    if cleaned.endswith("Z"):
        cleaned = f"{cleaned[:-1]}+00:00"
    parsed = datetime.fromisoformat(cleaned)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _entry_timestamp(entry: Any) -> datetime | None:
    parsed_value = entry.get("published_parsed") or entry.get("updated_parsed")
    if isinstance(parsed_value, struct_time):
        return datetime(*parsed_value[:6], tzinfo=timezone.utc)

    raw_value = entry.get("published") or entry.get("updated")
    if not raw_value:
        return None
    try:
        parsed = parsedate_to_datetime(str(raw_value))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)
