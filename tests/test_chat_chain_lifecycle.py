"""Chain loading and execution preserve errors, payloads and connection cleanup."""

import asyncio
from types import SimpleNamespace

import pytest

from api import chat


class Connection:
    def __init__(self, *, fail_close=False):
        self.closes = 0
        self.fail_close = fail_close

    def close(self):
        self.closes += 1
        if self.fail_close:
            raise RuntimeError("cleanup unavailable")


def prepare_execution(monkeypatch, chain, context=None):
    context_connection = Connection()
    monkeypatch.setattr(chat, "connect_db", lambda: context_connection)
    monkeypatch.setattr(chat, "_normalize_chain_context", lambda value, conn: context or {})
    monkeypatch.setattr(chat, "_build_chain", lambda *_: chain)
    return context_connection


def test_build_chain_unexpected_import_error_fails_closed(monkeypatch):
    original = RuntimeError("broken module initialization")

    def broken_import(_):
        raise original

    monkeypatch.setattr(chat.importlib, "import_module", broken_import)
    monkeypatch.setattr(chat, "_chat_chain_fallback_enabled", lambda: False)
    error = None
    try:
        chat._build_chain("rag_search", None)
    except chat.ChainUnavailableError as exc:
        error = exc
    assert isinstance(error, chat.ChainUnavailableError)
    assert str(error) == "Chat chain 'rag_search' failed during import"
    assert error.__cause__ is original


def test_build_chain_constructor_failure_closes_connection(monkeypatch):
    connection = Connection()
    original = RuntimeError("constructor failed")

    def broken_chain(**kwargs):
        assert kwargs == {"llm": "client", "db_conn": connection}
        raise original

    monkeypatch.setattr(
        chat.importlib, "import_module", lambda _: SimpleNamespace(RAGSearchChain=broken_chain)
    )
    monkeypatch.setattr(chat, "connect_db", lambda: connection)
    with pytest.raises(RuntimeError, match="constructor failed") as caught:
        chat._build_chain("rag_search", SimpleNamespace(client="client"))
    assert caught.value is original
    assert connection.closes == 1


@pytest.mark.parametrize("context", [{}, {"manager_ids": [17]}], ids=["empty", "populated"])
def test_invoke_only_chain_preserves_context_payload_and_cleanup(monkeypatch, context):
    calls = []
    chain_connection = Connection()

    def invoke(payload):
        calls.append(payload)
        return {"answer": "answer", "sources": {"document_id": 17}, "sql": "SELECT 1"}

    context_connection = prepare_execution(
        monkeypatch, SimpleNamespace(invoke=invoke, db=chain_connection), context
    )
    result = asyncio.run(chat._run_chain("rag_search", "question", context, None))
    assert calls == [{"question": "question", "context": context}]
    assert result == ("answer", [{"document_id": 17}], "SELECT 1", None)
    assert context_connection.closes == chain_connection.closes == 1


def test_chain_without_executor_fails_and_closes_connection(monkeypatch):
    chain_connection = Connection()
    context_connection = prepare_execution(monkeypatch, SimpleNamespace(db=chain_connection))
    error = None
    try:
        asyncio.run(chat._run_chain("rag_search", "question", None, None))
    except RuntimeError as exc:
        error = exc
    assert isinstance(error, RuntimeError)
    assert str(error) == "Chain 'rag_search' does not expose run/invoke"
    assert context_connection.closes == chain_connection.closes == 1


def test_cleanup_error_does_not_replace_successful_chain_payload(monkeypatch):
    chain_connection = Connection(fail_close=True)
    context_connection = prepare_execution(
        monkeypatch, SimpleNamespace(run=lambda **_: "answer", db=chain_connection)
    )
    result = asyncio.run(chat._run_chain("rag_search", "question", None, None))
    assert result == ("answer", [], None, None)
    assert context_connection.closes == chain_connection.closes == 1


def test_missing_filing_id_returns_400_and_closes_connection(monkeypatch):
    calls = []
    chain_connection = Connection()

    def run(filing_id):
        calls.append(filing_id)
        return {"manager_name": "unexpected"}

    context_connection = prepare_execution(
        monkeypatch, SimpleNamespace(run=run, db=chain_connection)
    )
    error = None
    try:
        asyncio.run(chat._run_chain("filing_summary", "question", None, None))
    except chat.HTTPException as exc:
        error = exc
    assert isinstance(error, chat.HTTPException)
    assert error.status_code == 400
    assert error.detail == "filing_summary requires context.filing_id"
    assert isinstance(error.__cause__, TypeError)
    assert calls == []
    assert context_connection.closes == chain_connection.closes == 1


def test_coroutine_chain_answer_is_awaited_before_normalization(monkeypatch):
    chain_connection = Connection()

    async def run(**kwargs):
        assert kwargs == {"question": "question", "context": None}
        return {"answer": "async answer", "trace_url": "https://trace.invalid/17"}

    context_connection = prepare_execution(
        monkeypatch, SimpleNamespace(run=run, db=chain_connection)
    )
    result = asyncio.run(chat._run_chain("rag_search", "question", None, None))
    assert result == ("async answer", [], None, "https://trace.invalid/17")
    assert context_connection.closes == chain_connection.closes == 1
