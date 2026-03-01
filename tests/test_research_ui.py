import importlib
import sys
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))


class SessionState(dict):
    def __getattr__(self, name: str):
        try:
            return self[name]
        except KeyError as exc:
            raise AttributeError(name) from exc

    def __setattr__(self, name: str, value):
        self[name] = value


class NullContext:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class FakeColumn:
    def __init__(self, st):
        self._st = st

    def caption(self, value: str) -> None:
        self._st.captions.append(value)

    def button(self, label: str) -> bool:
        return self._st.button_presses.get(label, False)


class FakeSidebar:
    def __init__(self, st):
        self._st = st

    def selectbox(self, _label: str, options, **_kwargs):
        return self._st.sidebar_chain_mode or options[0]

    def expander(self, label: str, **_kwargs):
        self._st.expander_labels.append(label)
        return NullContext()


class FakeStreamlit:
    def __init__(self, chat_inputs=None):
        self.session_state = SessionState()
        self.chat_inputs = list(chat_inputs or [])
        self.sidebar = FakeSidebar(self)
        self.sidebar_chain_mode = None
        self.button_presses: dict[str, bool] = {}
        self.page_config_calls = []
        self.title_calls: list[str] = []
        self.markdown_calls: list[str] = []
        self.expander_labels: list[str] = []
        self.captions: list[str] = []
        self.code_calls: list[tuple[str, str | None]] = []
        self.error_calls: list[str] = []

    def set_page_config(self, **kwargs) -> None:
        self.page_config_calls.append(kwargs)

    def title(self, text: str) -> None:
        self.title_calls.append(text)

    def stop(self) -> None:
        raise RuntimeError("stop called")

    def selectbox(self, _label: str, options, **_kwargs):
        return options[0]

    def number_input(self, _label: str, value=0, **_kwargs):
        return value

    def date_input(self, _label: str, value=None, **_kwargs):
        return value if value is not None else []

    def chat_message(self, _role: str):
        return NullContext()

    def spinner(self, _label: str):
        return NullContext()

    def markdown(self, text: str) -> None:
        self.markdown_calls.append(text)

    def expander(self, label: str, **_kwargs):
        self.expander_labels.append(label)
        return NullContext()

    def columns(self, count: int):
        return [FakeColumn(self) for _ in range(count)]

    def chat_input(self, _label: str):
        if self.chat_inputs:
            return self.chat_inputs.pop(0)
        return None

    def caption(self, text: str) -> None:
        self.captions.append(text)

    def code(self, text: str, language: str | None = None) -> None:
        self.code_calls.append((text, language))

    def divider(self) -> None:
        return None

    def rerun(self) -> None:
        return None

    def error(self, text: str) -> None:
        self.error_calls.append(text)


def _load_research_module():
    return importlib.reload(importlib.import_module("ui.research"))


def test_research_page_renders_without_errors(monkeypatch):
    research = _load_research_module()
    fake_st = FakeStreamlit()
    monkeypatch.setattr(research, "st", fake_st)
    monkeypatch.setattr(research, "require_login", lambda: True)
    monkeypatch.setattr(research, "_load_manager_list", lambda: [])

    research.main()

    assert fake_st.page_config_calls
    assert fake_st.title_calls == ["🔬 Research Assistant"]


def test_chat_input_triggers_api_call(monkeypatch):
    research = _load_research_module()
    fake_st = FakeStreamlit(chat_inputs=["Show crowded trades"])
    seen = {}

    def _fake_call(question: str, chain_mode: str, context):
        seen["question"] = question
        seen["chain_mode"] = chain_mode
        seen["context"] = context
        return {
            "answer": "Most crowded positions are A, B, C.",
            "chain_used": "holdings_analysis",
            "sources": [],
            "sql": None,
            "trace_url": None,
            "latency_ms": 12,
        }

    monkeypatch.setattr(research, "st", fake_st)
    monkeypatch.setattr(research, "require_login", lambda: True)
    monkeypatch.setattr(research, "_load_manager_list", lambda: [])
    monkeypatch.setattr(research, "_call_chat_api", _fake_call)

    research.main()

    assert seen["question"] == "Show crowded trades"
    assert seen["chain_mode"] == "Auto (recommended)"
    assert seen["context"] is None
    assert len(fake_st.session_state.messages) == 2
    assert fake_st.session_state.messages[0]["role"] == "user"
    assert fake_st.session_state.messages[1]["role"] == "assistant"


def test_sources_panel_displays_document_id_and_url(monkeypatch):
    research = _load_research_module()
    fake_st = FakeStreamlit()

    def _fake_call(_question: str, _chain_mode: str, _context):
        return {
            "answer": "Found relevant filing evidence.",
            "chain_used": "rag_search",
            "sources": [
                {
                    "type": "filing",
                    "document_id": "doc-13f-001",
                    "url": "https://example.com/filings/13f-001",
                    "description": "13F filing reference",
                }
            ],
            "sql": None,
            "trace_url": None,
            "latency_ms": 9,
        }

    monkeypatch.setattr(research, "st", fake_st)
    monkeypatch.setattr(research, "_call_chat_api", _fake_call)
    research._init_session_state()
    research._run_chat_turn("Summarize this filing", "Auto (recommended)", None)

    rendered_sources = [line for line in fake_st.markdown_calls if line.startswith("- **filing**")]
    assert rendered_sources
    assert "doc `doc-13f-001`" in rendered_sources[0]
    assert "[link](https://example.com/filings/13f-001)" in rendered_sources[0]
    assert "📄 Sources" in fake_st.expander_labels


def test_sources_panel_displays_filing_urls_and_news_references(monkeypatch):
    research = _load_research_module()
    fake_st = FakeStreamlit()

    source_line = research._source_markdown(
        {
            "type": "news",
            "document_id": "doc-news-42",
            "filing_url": "https://example.com/filings/42",
            "filing_urls": [
                "https://example.com/filings/42/a",
                "https://example.com/filings/42/b",
            ],
            "news_reference": "Reuters",
            "news_references": ["Bloomberg", "WSJ"],
            "description": "Cross-source coverage",
        }
    )

    monkeypatch.setattr(research, "st", fake_st)
    research._render_sources(
        [
            {
                "type": "news",
                "document_id": "doc-news-42",
                "filing_url": "https://example.com/filings/42",
                "filing_urls": [
                    "https://example.com/filings/42/a",
                    "https://example.com/filings/42/b",
                ],
                "news_reference": "Reuters",
                "news_references": ["Bloomberg", "WSJ"],
                "description": "Cross-source coverage",
            }
        ]
    )

    assert "doc `doc-news-42`" in source_line
    assert "[filing](https://example.com/filings/42)" in source_line
    assert "[filing](https://example.com/filings/42/a)" in source_line
    assert "[filing](https://example.com/filings/42/b)" in source_line
    assert "news: Reuters" in source_line
    assert "news: Bloomberg" in source_line
    assert "news: WSJ" in source_line
    assert any("news: Reuters" in line for line in fake_st.markdown_calls)


def test_sql_panel_displays_for_nl_query(monkeypatch):
    research = _load_research_module()
    fake_st = FakeStreamlit(chat_inputs=["Show top managers by position count"])

    def _fake_call(_question: str, _chain_mode: str, _context):
        return {
            "answer": "Top managers listed.",
            "chain_used": "nl_query",
            "sources": [],
            "sql": "SELECT manager_name, COUNT(*) FROM holdings GROUP BY manager_name LIMIT 5;",
            "trace_url": None,
            "latency_ms": 18,
        }

    monkeypatch.setattr(research, "st", fake_st)
    monkeypatch.setattr(research, "require_login", lambda: True)
    monkeypatch.setattr(research, "_load_manager_list", lambda: [])
    monkeypatch.setattr(research, "_call_chat_api", _fake_call)

    research.main()

    assert (
        "SELECT manager_name, COUNT(*) FROM holdings GROUP BY manager_name LIMIT 5;",
        "sql",
    ) in (fake_st.code_calls)
    assert "🔍 Generated SQL" in fake_st.expander_labels
    assert fake_st.session_state.messages[-1]["sql"].startswith("SELECT manager_name")


def test_history_renders_saved_assistant_metadata(monkeypatch):
    research = _load_research_module()
    fake_st = FakeStreamlit()
    monkeypatch.setattr(research, "st", fake_st)

    fake_st.session_state.messages = [
        {"role": "user", "content": "Show a query"},
        {
            "role": "assistant",
            "content": "Here is the result.",
            "sources": [{"type": "news", "description": "Reuters note"}],
            "chain_used": "nl_query",
            "latency_ms": 22,
            "trace_url": "https://trace.local/1",
            "sql": "SELECT 1;",
        },
    ]

    research._render_history()

    assert "Chain: nl_query" in fake_st.captions
    assert "Latency: 22ms" in fake_st.captions
    assert "[Trace](https://trace.local/1)" in fake_st.captions
    assert ("SELECT 1;", "sql") in fake_st.code_calls
    assert "🔍 Generated SQL" in fake_st.expander_labels


def test_call_chat_api_sends_expected_payload_and_session_header(monkeypatch):
    research = _load_research_module()
    fake_st = FakeStreamlit()
    fake_st.session_state.chat_session_id = "session-123"
    monkeypatch.setattr(research, "st", fake_st)

    captured = {}

    class FakeResponse:
        status_code = 200

        @staticmethod
        def json():
            return {
                "answer": "ok",
                "chain_used": "nl_query",
                "sources": [],
                "sql": None,
                "trace_url": None,
                "latency_ms": 5,
            }

    def _fake_post(url, *, json, headers, timeout):
        captured["url"] = url
        captured["json"] = json
        captured["headers"] = headers
        captured["timeout"] = timeout
        return FakeResponse()

    monkeypatch.setattr(research.requests, "post", _fake_post)

    result = research._call_chat_api(
        "Show latest filings",
        "Database Query",
        {"manager_name": "Example Capital"},
    )

    assert captured["url"] == research.CHAT_API_URL
    assert captured["json"] == {
        "question": "Show latest filings",
        "chain": "nl_query",
        "context": {"manager_name": "Example Capital"},
    }
    assert captured["headers"] == {"x-session-id": "session-123"}
    assert captured["timeout"] == research.REQUEST_TIMEOUT_SECONDS
    assert result["answer"] == "ok"


def test_call_chat_api_raises_runtime_error_for_http_failure(monkeypatch):
    research = _load_research_module()
    fake_st = FakeStreamlit()
    fake_st.session_state.chat_session_id = "session-err"
    monkeypatch.setattr(research, "st", fake_st)

    class FailingResponse:
        status_code = 503
        text = "service unavailable"

        @staticmethod
        def json():
            return {"detail": "No LLM provider configured"}

    monkeypatch.setattr(research.requests, "post", lambda *args, **kwargs: FailingResponse())

    with pytest.raises(RuntimeError) as exc_info:
        research._call_chat_api("hello", "Auto (recommended)", None)
    exc = exc_info.value
    assert "503" in str(exc)
    assert "No LLM provider configured" in str(exc)
