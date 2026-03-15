"""Public API for LangChain LCEL chains used by manager analysis workflows."""

from importlib import import_module
from typing import Any

__all__ = [
    "FilingSummary",
    "FilingSummaryChain",
    "HoldingsAnalysis",
    "HoldingsAnalysisChain",
    "estimate_token_count",
    "format_delta_summary",
    "format_holdings_table",
    "truncate_context",
    "NLQueryChain",
    "NLQueryResult",
    "RAGSearchChain",
    "RAGSearchResult",
    "classify_intent",
]

_EXPORTS: dict[str, tuple[str, str]] = {
    "FilingSummary": ("filing_summary", "FilingSummary"),
    "FilingSummaryChain": ("filing_summary", "FilingSummaryChain"),
    "HoldingsAnalysis": ("holdings_analysis", "HoldingsAnalysis"),
    "HoldingsAnalysisChain": ("holdings_analysis", "HoldingsAnalysisChain"),
    "estimate_token_count": ("utils", "estimate_token_count"),
    "format_delta_summary": ("utils", "format_delta_summary"),
    "format_holdings_table": ("utils", "format_holdings_table"),
    "truncate_context": ("utils", "truncate_context"),
    "NLQueryChain": ("nl_query", "NLQueryChain"),
    "NLQueryResult": ("nl_query", "NLQueryResult"),
    "RAGSearchChain": ("rag_search", "RAGSearchChain"),
    "RAGSearchResult": ("rag_search", "RAGSearchResult"),
    "classify_intent": ("intent", "classify_intent"),
}


def __getattr__(name: str) -> Any:
    """Lazily resolve public exports from chain modules."""
    if name not in _EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    module_name, attribute_name = _EXPORTS[name]
    module = import_module(f".{module_name}", __name__)
    value = getattr(module, attribute_name)
    globals()[name] = value
    return value
