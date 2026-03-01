import importlib
import sys
from pathlib import Path

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
