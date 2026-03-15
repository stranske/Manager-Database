"""Optional LangSmith tracing utilities."""

from __future__ import annotations

import os
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from typing import Any, Literal

_LANGSMITH_ENABLED: bool | None = None
_DEFAULT_PROJECT = "manager-database"


def maybe_enable_langsmith_tracing() -> bool:
    """Enable LangSmith env vars when an API key is present."""
    global _LANGSMITH_ENABLED
    if _LANGSMITH_ENABLED is not None:
        return _LANGSMITH_ENABLED

    api_key = os.environ.get("LANGSMITH_API_KEY")
    if not api_key:
        _LANGSMITH_ENABLED = False
        return False

    os.environ.setdefault("LANGCHAIN_API_KEY", api_key)
    os.environ.setdefault("LANGCHAIN_TRACING_V2", "true")
    os.environ.setdefault("LANGSMITH_PROJECT", _DEFAULT_PROJECT)
    os.environ.setdefault("LANGCHAIN_PROJECT", os.environ["LANGSMITH_PROJECT"])
    _LANGSMITH_ENABLED = True
    return True


@contextmanager
def langsmith_tracing_context(
    *,
    name: str | None = None,
    run_type: Literal[
        "retriever", "llm", "tool", "chain", "embedding", "prompt", "parser"
    ] = "chain",
    inputs: Mapping[str, Any] | None = None,
    project_name: str | None = None,
    tags: Sequence[str] | None = None,
    metadata: Mapping[str, Any] | None = None,
) -> Iterator[Any]:
    """Yield a LangSmith run when tracing is enabled, otherwise yield ``None``."""
    del run_type
    if not maybe_enable_langsmith_tracing():
        yield None
        return

    try:
        import langsmith
    except ImportError:
        yield None
        return

    tracing_context = getattr(langsmith, "tracing_context", None)
    get_current_run_tree = getattr(langsmith, "get_current_run_tree", lambda: None)
    project = project_name or os.environ.get("LANGSMITH_PROJECT") or _DEFAULT_PROJECT
    context_kwargs = {
        "enabled": True,
        "project_name": project,
        "tags": list(tags or []),
        "metadata": dict(metadata or {}),
        "inputs": dict(inputs or {}),
        "name": name,
    }
    if callable(tracing_context):
        with tracing_context(**context_kwargs):
            yield get_current_run_tree()
        return

    yield None


def resolve_trace_url(trace: str | Any | None, *, base_url: str | None = None) -> str | None:
    """Resolve a trace or run object into a clickable LangSmith URL."""
    if trace is None:
        return None
    if isinstance(trace, str):
        stripped = trace.strip()
        if stripped.startswith("http://") or stripped.startswith("https://"):
            return stripped
        if not stripped:
            return None
        root = (
            base_url or os.environ.get("LANGSMITH_BASE_URL") or "https://smith.langchain.com"
        ).rstrip("/")
        return f"{root}/r/{stripped}"

    url = getattr(trace, "url", None)
    if isinstance(url, str) and url:
        return url

    for attr_name in ("get_url", "get_run_url"):
        accessor = getattr(trace, attr_name, None)
        if callable(accessor):
            resolved = accessor()
            if isinstance(resolved, str) and resolved:
                return resolved

    run_id = getattr(trace, "id", None) or getattr(trace, "run_id", None)
    if run_id:
        return resolve_trace_url(str(run_id), base_url=base_url)
    return None
