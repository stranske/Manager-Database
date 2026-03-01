"""News ingestion adapter scaffold."""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from time import monotonic
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
GDELT_DOC_API = "https://api.gdeltproject.org/api/v2/doc/doc"


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

            parsed_feed = parser.parse(response.content)
            if getattr(parsed_feed, "bozo", False):
                logger.warning("RSS parse warning for %s", feed_url)
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
    """Fetch GDELT items newer than the watermark timestamp."""

    since_dt = _parse_iso_timestamp(since)
    manager_names = _configured_gdelt_managers()
    if not manager_names:
        return []

    collected: list[dict[str, Any]] = []
    last_request_at: float | None = None

    async with httpx.AsyncClient(timeout=20.0, follow_redirects=True) as client:
        for manager_name in manager_names:
            if last_request_at is not None:
                elapsed = monotonic() - last_request_at
                if elapsed < 1.0:
                    await asyncio.sleep(1.0 - elapsed)

            params = {
                "query": manager_name,
                "mode": "artlist",
                "maxrecords": 50,
                "format": "json",
            }
            endpoint = (
                f"{GDELT_DOC_API}?query={manager_name}&mode=artlist&maxrecords=50&format=json"
            )
            try:
                async with tracked_call("gdelt", endpoint) as log:
                    response = await client.get(GDELT_DOC_API, params=params)
                    log(response)
                response.raise_for_status()
            except (httpx.HTTPError, httpx.TimeoutException) as exc:
                logger.warning("GDELT request failed for %s: %s", manager_name, exc)
                last_request_at = monotonic()
                continue
            last_request_at = monotonic()

            payload = response.json()
            articles = payload.get("articles") or []
            for article in articles:
                published_dt = _gdelt_timestamp(article.get("seendate"))
                if published_dt is None or published_dt <= since_dt:
                    continue
                collected.append(
                    {
                        "headline": str(article.get("title", "")).strip(),
                        "url": str(article.get("url", "")).strip(),
                        "published_at": published_dt.isoformat(),
                        "source": "gdelt",
                        "body_snippet": "",
                        "socialimage": str(article.get("socialimage", "")).strip(),
                        "domain": str(article.get("domain", "")).strip(),
                    }
                )

    collected.sort(key=lambda item: item["published_at"], reverse=True)
    return collected


def _configured_rss_feeds() -> list[str]:
    env_value = os.getenv("NEWS_RSS_FEEDS", "")
    if not env_value.strip():
        return list(DEFAULT_RSS_FEEDS)
    configured = [url.strip() for url in env_value.split(",") if url.strip()]
    if not configured:
        return list(DEFAULT_RSS_FEEDS)
    return configured


def _configured_gdelt_managers() -> list[str]:
    env_value = os.getenv("NEWS_GDELT_MANAGERS", "")
    if not env_value.strip():
        return []
    return [name.strip() for name in env_value.split(",") if name.strip()]


def _parse_iso_timestamp(value: str) -> datetime:
    cleaned = value.strip()
    if cleaned.endswith("Z"):
        cleaned = f"{cleaned[:-1]}+00:00"
    parsed = datetime.fromisoformat(cleaned)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _entry_timestamp(entry: Any) -> datetime | None:
    parsed_value = entry.get("published_parsed") or entry.get("updated_parsed")
    if isinstance(parsed_value, struct_time):
        return datetime(*parsed_value[:6], tzinfo=UTC)

    raw_value = entry.get("published") or entry.get("updated")
    if not raw_value:
        return None
    try:
        parsed = parsedate_to_datetime(str(raw_value))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _gdelt_timestamp(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.strptime(str(value), "%Y%m%d%H%M%S")
    except ValueError:
        return None
    return parsed.replace(tzinfo=UTC)
