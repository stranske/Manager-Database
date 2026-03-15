"""Intent routing for the research assistant."""

from __future__ import annotations

INTENTS: dict[str, tuple[str, ...]] = {
    "filing_summary": (
        "summarize",
        "summary",
        "overview",
        "filing",
        "13f",
        "13d",
        "13g",
    ),
    "holdings_analysis": (
        "holdings",
        "positions",
        "portfolio",
        "exposure",
        "compare",
        "crowded trades",
        "conviction",
    ),
    "nl_query": (
        "how many",
        "count",
        "list all",
        "which managers",
        "top ",
        "largest",
        "database query",
        "sql",
    ),
    "rag_search": (
        "research",
        "document",
        "memo",
        "note",
        "what do we know",
        "our notes",
        "uploaded",
    ),
}


def classify_intent(question: str) -> str:
    """Classify a user question into the most appropriate research chain."""
    lowered = question.strip().lower()
    if not lowered:
        return "rag_search"

    for intent_name in ("filing_summary", "holdings_analysis", "nl_query", "rag_search"):
        if any(keyword in lowered for keyword in INTENTS[intent_name]):
            return intent_name
    return "rag_search"
