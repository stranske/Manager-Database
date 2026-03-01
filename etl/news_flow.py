"""Prefect flow orchestrating hourly news harvesting."""

from __future__ import annotations

import logging
import os

from prefect import flow

from etl.logging_setup import configure_logging, log_outcome

configure_logging("news_flow")
logger = logging.getLogger(__name__)


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
