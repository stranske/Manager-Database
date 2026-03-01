"""News ingestion adapter scaffold."""

from __future__ import annotations

from typing import Any


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
    """Placeholder RSS fetcher; implemented in a follow-up task."""

    _ = since
    return []


async def _fetch_gdelt(since: str) -> list[dict[str, Any]]:
    """Placeholder GDELT fetcher; implemented in a follow-up task."""

    _ = since
    return []
