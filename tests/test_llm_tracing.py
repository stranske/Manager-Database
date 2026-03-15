from __future__ import annotations

import sys
import types

from llm import tracing as llm_tracing


class _FakeRun:
    def __init__(self, url: str | None = None, run_id: str | None = None):
        self.url = url
        self.run_id = run_id


def test_maybe_enable_langsmith_tracing_with_key_present(monkeypatch):
    monkeypatch.setenv("LANGSMITH_API_KEY", "ls-key")
    monkeypatch.setenv("LANGCHAIN_API_KEY", "")
    monkeypatch.setenv("LANGCHAIN_TRACING_V2", "false")
    monkeypatch.setattr(llm_tracing, "_LANGSMITH_ENABLED", None)

    assert llm_tracing.maybe_enable_langsmith_tracing() is True
    assert llm_tracing.maybe_enable_langsmith_tracing() is True
    assert llm_tracing.os.environ["LANGCHAIN_API_KEY"] == "ls-key"
    assert llm_tracing.os.environ["LANGCHAIN_TRACING_V2"] == "true"


def test_maybe_enable_langsmith_tracing_without_key(monkeypatch):
    monkeypatch.delenv("LANGSMITH_API_KEY", raising=False)
    monkeypatch.setattr(llm_tracing, "_LANGSMITH_ENABLED", None)

    assert llm_tracing.maybe_enable_langsmith_tracing() is False


def test_langsmith_tracing_context_is_noop_when_disabled(monkeypatch):
    monkeypatch.delenv("LANGSMITH_API_KEY", raising=False)
    monkeypatch.setattr(llm_tracing, "_LANGSMITH_ENABLED", None)

    with llm_tracing.langsmith_tracing_context(name="filing-summary") as run:
        assert run is None


def test_langsmith_tracing_context_uses_langsmith_when_enabled(monkeypatch):
    events = []

    class _TracingContext:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        def __enter__(self):
            events.append(self.kwargs)
            return None

        def __exit__(self, exc_type, exc, tb):
            return False

    fake_run = _FakeRun(url="https://smith.langchain.com/r/abc123")
    fake_module = types.SimpleNamespace(
        tracing_context=lambda **kwargs: _TracingContext(**kwargs),
        get_current_run_tree=lambda: fake_run,
    )
    monkeypatch.setitem(sys.modules, "langsmith", fake_module)
    monkeypatch.setenv("LANGSMITH_API_KEY", "ls-key")
    monkeypatch.setattr(llm_tracing, "_LANGSMITH_ENABLED", None)

    with llm_tracing.langsmith_tracing_context(
        name="filing-summary",
        run_type="chain",
        inputs={"filing_id": 42},
        tags=["filing-summary"],
    ) as run:
        assert run is fake_run

    assert events[0]["name"] == "filing-summary"
    assert events[0]["inputs"] == {"filing_id": 42}
    assert events[0]["run_type"] == "chain"


def test_langsmith_tracing_context_swallows_runtime_errors(monkeypatch):
    class _BrokenTracingContext:
        def __enter__(self):
            raise RuntimeError("broken tracing")

        def __exit__(self, exc_type, exc, tb):
            return False

    fake_module = types.SimpleNamespace(
        tracing_context=lambda **kwargs: _BrokenTracingContext(),
        get_current_run_tree=lambda: _FakeRun(url="https://smith.langchain.com/r/abc123"),
    )
    monkeypatch.setitem(sys.modules, "langsmith", fake_module)
    monkeypatch.setenv("LANGSMITH_API_KEY", "ls-key")
    monkeypatch.setattr(llm_tracing, "_LANGSMITH_ENABLED", None)

    with llm_tracing.langsmith_tracing_context(name="filing-summary") as run:
        assert run is None


def test_resolve_trace_url_supports_string_run_object_and_none(monkeypatch):
    monkeypatch.setenv("LANGSMITH_BASE_URL", "https://smith.example.test")

    assert llm_tracing.resolve_trace_url("abc123") == "https://smith.example.test/r/abc123"
    assert (
        llm_tracing.resolve_trace_url(_FakeRun(url="https://trace.test/run"))
        == "https://trace.test/run"
    )
    assert (
        llm_tracing.resolve_trace_url(_FakeRun(run_id="run-123"))
        == "https://smith.example.test/r/run-123"
    )
    assert llm_tracing.resolve_trace_url(None) is None
