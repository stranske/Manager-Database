from __future__ import annotations

import asyncio
import json
import re
import sqlite3
import threading
from contextlib import closing
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import Mock

import httpx
import pytest
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import AIMessage, BaseMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from langchain_core.runnables import RunnableLambda

import api.chat as chat_api_module
from chains.nl_query import NLQueryChain
from llm import langsmith_fleet as fleet
from llm.cost_tracking import (
    USAGE_BYTES_UNKNOWN,
    ProviderUsage,
    UsageCapture,
    _as_cost,
    capture_provider_usage,
    estimate_cost_usd,
    extract_usage_from_llm_result,
    legacy_placeholder_usage_bytes,
    log_llm_usage,
    observed_cost_usd,
    observed_usage_bytes,
    persist_observed_provider_usage,
    record_provider_invocation,
)
from tools.langchain_client import ClientInfo
from tools.registry import TOOL_REGISTRY

# Deterministic provider observations replayed in issue #1939 acceptance tests.
FOCUSED_CHAT_USAGE: dict[str, Any] = {
    "provider": "openai",
    "model": "gpt-4o-mini",
    "tokens_in": 512,
    "tokens_out": 128,
    "latency_ms": 842,
    "trace_url": "https://smith.langchain.com/r/chat-replay-1939",
}

FOCUSED_ANALYSIS_USAGE: dict[str, Any] = {
    "provider": "anthropic",
    "model": "claude-sonnet-4-20250514",
    "tokens_in": 2048,
    "tokens_out": 896,
    "latency_ms": 1520,
}


# Recorded replay expectations for issue #1939 (input fixture → persisted api_usage readback).
def _legacy_bytes_readback_fails_for_observed_fixture(
    observed_bytes: int, *, legacy_bytes: int
) -> None:
    """Pre-1939 compatibility control: token-bearing invocations persisted ``bytes=0``."""
    assert legacy_bytes == legacy_placeholder_usage_bytes()
    with pytest.raises(AssertionError):
        assert observed_bytes == legacy_bytes


ISSUE_1939_REPLAY_EXPECTATIONS: dict[str, dict[str, Any]] = {
    "chat": {
        "invocation": "chat",
        "input": FOCUSED_CHAT_USAGE,
        "api_usage_bytes": observed_usage_bytes(
            FOCUSED_CHAT_USAGE["tokens_in"], FOCUSED_CHAT_USAGE["tokens_out"]
        ),
        "legacy_bytes_control": legacy_placeholder_usage_bytes(),
        "cost_usd": observed_cost_usd(
            FOCUSED_CHAT_USAGE["model"],
            FOCUSED_CHAT_USAGE["tokens_in"],
            FOCUSED_CHAT_USAGE["tokens_out"],
        ),
    },
    "analysis": {
        "invocation": "analysis",
        "input": FOCUSED_ANALYSIS_USAGE,
        "api_usage_bytes": observed_usage_bytes(
            FOCUSED_ANALYSIS_USAGE["tokens_in"], FOCUSED_ANALYSIS_USAGE["tokens_out"]
        ),
        "legacy_bytes_control": legacy_placeholder_usage_bytes(),
        "cost_usd": observed_cost_usd(
            FOCUSED_ANALYSIS_USAGE["model"],
            FOCUSED_ANALYSIS_USAGE["tokens_in"],
            FOCUSED_ANALYSIS_USAGE["tokens_out"],
        ),
    },
    "missing_usage": {
        "invocation": "chat",
        "input": {
            "provider": "openai",
            "model": "gpt-4o-mini",
            "tokens_in": None,
            "tokens_out": None,
            "latency_ms": 120,
        },
        "api_usage_bytes": USAGE_BYTES_UNKNOWN,
        "legacy_bytes_control": legacy_placeholder_usage_bytes(),
        "cost_usd": None,
    },
}

# Live POST /api/chat replay: one ``api_usage`` row per provider invocation (issue #1939).
ISSUE_1939_LIVE_REPLAY_EXPECTED_BYTES: list[int] = [640, 350, 1200, 15, USAGE_BYTES_UNKNOWN]

ISSUE_1939_ACCEPTANCE_REPLAY_RECORD: dict[str, Any] = {
    "focused_inputs": {"chat": FOCUSED_CHAT_USAGE, "analysis": FOCUSED_ANALYSIS_USAGE},
    "compatibility_control": {
        "legacy_bytes_per_row": legacy_placeholder_usage_bytes(),
        "legacy_readback_sequence": [0, 0, 0, 0, 0],
    },
    "observed_readback_bytes": ISSUE_1939_LIVE_REPLAY_EXPECTED_BYTES,
    "provider_invocations_per_request": [1, 2, 1, 1],
}


def test_estimate_cost_usd_uses_model_pricing():
    assert estimate_cost_usd("gpt-4o-mini", 1000, 500) == 0.00045
    assert estimate_cost_usd("unknown-model", 1000, 500) == 0.0


def test_legacy_bytes_placeholder_compatibility_control_for_focused_chat_input():
    """Pre-1939 ``log_llm_usage`` always wrote ``bytes=0`` regardless of token counts."""
    assert legacy_placeholder_usage_bytes() == 0
    assert observed_usage_bytes(
        FOCUSED_CHAT_USAGE["tokens_in"], FOCUSED_CHAT_USAGE["tokens_out"]
    ) == (FOCUSED_CHAT_USAGE["tokens_in"] + FOCUSED_CHAT_USAGE["tokens_out"])
    assert legacy_placeholder_usage_bytes() != observed_usage_bytes(
        FOCUSED_CHAT_USAGE["tokens_in"], FOCUSED_CHAT_USAGE["tokens_out"]
    )


def test_legacy_bytes_placeholder_compatibility_control_for_focused_analysis_input():
    assert legacy_placeholder_usage_bytes() == 0
    assert observed_usage_bytes(
        FOCUSED_ANALYSIS_USAGE["tokens_in"], FOCUSED_ANALYSIS_USAGE["tokens_out"]
    ) == (FOCUSED_ANALYSIS_USAGE["tokens_in"] + FOCUSED_ANALYSIS_USAGE["tokens_out"])


@pytest.mark.parametrize("replay_key", ["chat", "analysis"])
def test_pin_cost_tracking_focused_fixture_supported_entry_point_readback(replay_key, monkeypatch):
    """Pin cost_tracking for focused #1939 inputs: legacy ``bytes=0`` fails; observed readback passes."""
    replay = ISSUE_1939_REPLAY_EXPECTATIONS[replay_key]
    inp = replay["input"]

    conn_legacy = sqlite3.connect(":memory:")
    monkeypatch.setattr(
        "llm.cost_tracking.observed_usage_bytes",
        lambda _tokens_in, _tokens_out: legacy_placeholder_usage_bytes(),
    )
    persist_observed_provider_usage(conn_legacy, invocation=replay["invocation"], **inp)
    legacy_readback = conn_legacy.execute("SELECT bytes, cost_usd FROM api_usage").fetchone()
    assert legacy_readback[0] == replay["legacy_bytes_control"]
    with pytest.raises(AssertionError):
        assert legacy_readback == (replay["api_usage_bytes"], replay["cost_usd"])
    conn_legacy.close()

    conn = sqlite3.connect(":memory:")
    monkeypatch.setattr("llm.cost_tracking.observed_usage_bytes", observed_usage_bytes)
    persist_observed_provider_usage(conn, invocation=replay["invocation"], **inp)
    readback = conn.execute("SELECT bytes, cost_usd FROM api_usage").fetchone()
    assert readback == (replay["api_usage_bytes"], replay["cost_usd"])
    assert conn.execute("SELECT COUNT(*) FROM api_usage").fetchone()[0] == 1
    assert readback[0] != replay["legacy_bytes_control"]
    _legacy_bytes_readback_fails_for_observed_fixture(
        readback[0], legacy_bytes=replay["legacy_bytes_control"]
    )
    conn.close()


def test_focused_missing_usage_persists_unknown_via_supported_entry_point():
    """Supported entry point records explicit unknown bytes/cost when usage is absent."""
    replay = ISSUE_1939_REPLAY_EXPECTATIONS["missing_usage"]
    conn = sqlite3.connect(":memory:")
    inp = replay["input"]
    persist_observed_provider_usage(
        conn,
        invocation=replay["invocation"],
        provider=inp["provider"],
        model=inp["model"],
        tokens_in=inp["tokens_in"],
        tokens_out=inp["tokens_out"],
        latency_ms=inp["latency_ms"],
    )
    readback = conn.execute("SELECT bytes, cost_usd FROM api_usage").fetchone()
    assert readback == (replay["api_usage_bytes"], replay["cost_usd"])
    assert readback[0] != replay["legacy_bytes_control"]
    conn.close()


@pytest.mark.parametrize("replay_key", ["chat", "analysis"])
def test_focused_fixture_legacy_readback_zero_observed_readback_matches_tokens(replay_key):
    """Same replay fixture: pre-1939 readback stored ``bytes=0``; post-change stores token sum."""
    conn = sqlite3.connect(":memory:")
    replay = ISSUE_1939_REPLAY_EXPECTATIONS[replay_key]
    legacy_bytes = replay["legacy_bytes_control"]
    observed_bytes = replay["api_usage_bytes"]

    persist_observed_provider_usage(conn, invocation=replay["invocation"], **replay["input"])
    readback = conn.execute("SELECT bytes, cost_usd FROM api_usage").fetchone()

    assert legacy_bytes == 0
    assert readback == (observed_bytes, replay["cost_usd"])
    assert readback[0] != legacy_bytes
    # Deliberate break on pre-1939 behavior: ``assert readback[0] == legacy_bytes`` fails (e.g. 640 != 0).
    conn.close()


@pytest.mark.parametrize("replay_key", ["chat", "analysis"])
def test_pre_1939_legacy_bytes_control_fails_same_fixture_readback(replay_key):
    """Compatibility control: asserting legacy ``bytes=0`` would fail on the recorded replay."""
    conn = sqlite3.connect(":memory:")
    replay = ISSUE_1939_REPLAY_EXPECTATIONS[replay_key]
    persist_observed_provider_usage(conn, invocation=replay["invocation"], **replay["input"])
    persisted_bytes = conn.execute("SELECT bytes FROM api_usage").fetchone()[0]
    assert persisted_bytes == replay["api_usage_bytes"]
    assert persisted_bytes != replay["legacy_bytes_control"]
    _legacy_bytes_readback_fails_for_observed_fixture(
        persisted_bytes, legacy_bytes=replay["legacy_bytes_control"]
    )
    conn.close()


def test_issue_1939_acceptance_replay_record_pins_focused_fixture_readback():
    """Pin live acceptance replay against focused #1939 inputs and the legacy bytes control."""
    record = ISSUE_1939_ACCEPTANCE_REPLAY_RECORD
    chat = ISSUE_1939_REPLAY_EXPECTATIONS["chat"]
    missing = ISSUE_1939_REPLAY_EXPECTATIONS["missing_usage"]
    assert record["observed_readback_bytes"][0] == chat["api_usage_bytes"]
    assert record["observed_readback_bytes"][-1] == missing["api_usage_bytes"]
    assert record["compatibility_control"]["legacy_bytes_per_row"] == chat["legacy_bytes_control"]
    with pytest.raises(AssertionError):
        assert (
            record["compatibility_control"]["legacy_readback_sequence"]
            == record["observed_readback_bytes"]
        )


def test_log_llm_usage_owned_sqlite_connection_commits_and_closes(tmp_path, monkeypatch):
    db_path = tmp_path / "usage.db"
    conn = sqlite3.connect(db_path)
    monkeypatch.setattr("llm.cost_tracking.connect_db", lambda: conn)

    persist_observed_provider_usage(None, invocation="chat", **FOCUSED_CHAT_USAGE)

    with pytest.raises(sqlite3.ProgrammingError, match="closed database"):
        conn.execute("SELECT 1")

    with closing(sqlite3.connect(db_path)) as reader:
        assert reader.execute("SELECT COUNT(*) FROM api_usage").fetchone()[0] == 1


def test_log_llm_usage_borrowed_connection_stays_open(tmp_path):
    db_path = tmp_path / "borrowed.db"
    writer = sqlite3.connect(db_path)
    try:
        persist_observed_provider_usage(writer, invocation="analysis", **FOCUSED_ANALYSIS_USAGE)
        assert writer.execute("SELECT COUNT(*) FROM api_usage").fetchone()[0] == 1
        with closing(sqlite3.connect(db_path)) as reader:
            assert reader.execute("SELECT COUNT(*) FROM api_usage").fetchone()[0] == 0
        writer.rollback()
        assert writer.execute("SELECT COUNT(*) FROM api_usage").fetchone()[0] == 0
        writer.execute("SELECT 1").fetchone()
    finally:
        writer.close()


def test_observed_cost_rejects_non_finite_reported_values():
    assert observed_cost_usd("gpt-4o-mini", 10, 5, float("inf")) is None
    assert observed_cost_usd("gpt-4o-mini", 10, 5, float("nan")) is None
    assert observed_cost_usd("gpt-4o-mini", 10, 5, -0.01) is None
    assert _as_cost(float("inf")) is None
    assert _as_cost(-1.0) is None


def test_estimate_cost_usd_uses_cached_input_rate():
    assert estimate_cost_usd("gpt-4o-mini", 1000, 0, tokens_cached_in=600) == 0.000105


def test_cached_input_cost_matches_across_stored_row_and_fleet_observation(tmp_path, monkeypatch):
    db_path = tmp_path / "cached_usage.db"
    fleet_path = tmp_path / "fleet.ndjson"
    with closing(sqlite3.connect(db_path)) as seed:
        seed.execute("CREATE TABLE managers (manager_id INTEGER PRIMARY KEY, name TEXT, cik TEXT)")
        seed.execute("INSERT INTO managers VALUES (1, 'Elliott', '0001791786')")
        seed.commit()
    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.delenv("DB_URL", raising=False)
    monkeypatch.setenv(fleet.ENV_FLEET_PATH, str(fleet_path))
    monkeypatch.delenv(fleet.ENV_LANGSMITH_KEY, raising=False)
    chat_api_module.CHAT_RATE_LIMITER.clear()
    chat_api_module.CHAT_IP_RATE_LIMITER.clear()

    cached_provider = _ScriptedProvider(
        script=[
            _ai(
                _NL_JSON,
                usage_metadata={
                    "input_tokens": 1000,
                    "output_tokens": 0,
                    "total_tokens": 1000,
                    "input_token_details": {"cache_read": 600},
                },
            )
        ]
    )
    monkeypatch.setattr(
        chat_api_module,
        "_build_chat_client_info",
        lambda: ClientInfo(cached_provider, "openai", "gpt-4o-mini"),
    )

    async def _post_cached() -> int:
        await cast(Any, chat_api_module.app.router).startup()
        try:
            transport = httpx.ASGITransport(app=cast(Any, chat_api_module.app))
            async with httpx.AsyncClient(
                transport=transport, base_url="http://test", timeout=10.0
            ) as client:
                response = await client.post(
                    "/api/chat", json={"question": "List managers", "chain": "nl_query"}
                )
                return response.status_code
        finally:
            await cast(Any, chat_api_module.app.router).shutdown()

    assert asyncio.run(_post_cached()) == 200

    with closing(sqlite3.connect(db_path)) as reader:
        stored_cost = reader.execute("SELECT cost_usd FROM api_usage").fetchone()[0]
    fleet_record = json.loads(fleet_path.read_text(encoding="utf-8").splitlines()[0])

    assert stored_cost == 0.000105
    assert fleet_record["domain"]["cost_usd"] == 0.000105
    assert stored_cost == fleet_record["domain"]["cost_usd"]

    # Direct record_provider_invocation matches observation.cost_usd and fresh-connection readback.
    direct_db = tmp_path / "cached_direct.db"
    monkeypatch.setenv("DB_PATH", str(direct_db))
    capture = UsageCapture(request_id="cached1939", provider="openai", model="gpt-4o-mini")
    with capture_provider_usage(capture):
        observation = record_provider_invocation(
            None,
            invocation="chat",
            usage=ProviderUsage(
                tokens_in=1000,
                tokens_out=0,
                cost_usd=None,
                tokens_cached_in=600,
            ),
            latency_ms=115,
            provider="openai",
            model="gpt-4o-mini",
        )
    with closing(sqlite3.connect(direct_db)) as reader:
        direct_stored_cost = reader.execute("SELECT cost_usd FROM api_usage").fetchone()[0]
    assert observation.cost_usd == 0.000105
    assert capture.total_cost_usd() == 0.000105
    assert direct_stored_cost == 0.000105

    # Explicit unknown cost (invalid provider-reported cost) stays NULL instead of re-estimating.
    unknown_db = tmp_path / "unknown_cost.db"
    monkeypatch.setenv("DB_PATH", str(unknown_db))
    unknown_obs = record_provider_invocation(
        None,
        invocation="chat",
        usage=ProviderUsage(tokens_in=1000, tokens_out=0, cost_usd=-0.01),
        latency_ms=80,
        provider="openai",
        model="gpt-4o-mini",
    )
    with closing(sqlite3.connect(unknown_db)) as reader:
        unknown_stored_cost = reader.execute("SELECT cost_usd FROM api_usage").fetchone()[0]
    assert unknown_obs.cost_usd is None
    assert unknown_stored_cost is None


def test_two_sqlite_writers_upgrade_legacy_monthly_view_and_preserve_borrowed_tx(
    tmp_path, monkeypatch
):
    db_path = tmp_path / "concurrent_legacy.db"
    with closing(sqlite3.connect(db_path)) as seed:
        seed.execute("""CREATE TABLE api_usage (
                id INTEGER PRIMARY KEY,
                ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                source TEXT,
                endpoint TEXT,
                status INT,
                bytes INT,
                latency_ms INT,
                cost_usd REAL
            )""")
        seed.execute(
            "CREATE VIEW monthly_usage AS "
            "SELECT substr(ts, 1, 7) || '-01' AS month, source, "
            "COUNT(*) AS calls, SUM(bytes) AS mb, SUM(cost_usd) AS cost "
            "FROM api_usage GROUP BY 1,2"
        )
        seed.execute(
            "INSERT INTO api_usage(source, endpoint, status, bytes, latency_ms, cost_usd) "
            "VALUES ('edgar', '/submissions', 200, 4096, 50, 0.0)"
        )
        seed.commit()

    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.delenv("DB_URL", raising=False)

    barrier = threading.Barrier(2)
    errors: list[BaseException] = []

    def _write(idx: int) -> None:
        try:
            barrier.wait(timeout=5.0)
            persist_observed_provider_usage(
                None,
                invocation="chat",
                provider="openai",
                model="gpt-4o-mini",
                tokens_in=1000,
                tokens_out=0,
                latency_ms=100 + idx,
                request_id=f"writer-{idx}",
            )
        except BaseException as exc:  # pragma: no cover - asserted empty below
            errors.append(exc)

    threads = [threading.Thread(target=_write, args=(i,)) for i in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10.0)
        assert not t.is_alive()

    assert errors == []

    with closing(sqlite3.connect(db_path)) as reader:
        llm_count = reader.execute(
            "SELECT COUNT(*) FROM api_usage WHERE source = 'langchain'"
        ).fetchone()[0]
        assert llm_count == 2
        monthly = {
            row[0]: (row[1], row[2], round(float(row[3]), 6))
            for row in reader.execute(
                "SELECT source, calls, mb, cost FROM monthly_usage ORDER BY source"
            ).fetchall()
        }
        assert monthly["edgar"] == (1, 4096, 0.0)
        assert monthly["langchain"] == (2, 0, 0.0003)

    # Current view is not dropped or recreated on subsequent writes.
    traced_sql: list[str] = []
    with closing(sqlite3.connect(db_path)) as conn:
        conn.set_trace_callback(traced_sql.append)
        persist_observed_provider_usage(conn, invocation="chat", **FOCUSED_CHAT_USAGE)
        conn.commit()
    assert not any("DROP VIEW" in stmt.upper() for stmt in traced_sql)
    assert not any("CREATE VIEW" in stmt.upper() for stmt in traced_sql)

    # Borrowed active transaction against a legacy view is neither committed nor rolled back.
    borrowed_path = tmp_path / "borrowed_legacy.db"
    with closing(sqlite3.connect(borrowed_path)) as seed:
        seed.execute("CREATE TABLE scratch (id INT)")
        seed.execute("""CREATE TABLE api_usage (
                id INTEGER PRIMARY KEY,
                ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                source TEXT,
                endpoint TEXT,
                status INT,
                bytes INT,
                latency_ms INT,
                cost_usd REAL
            )""")
        seed.execute(
            "CREATE VIEW monthly_usage AS "
            "SELECT substr(ts, 1, 7) || '-01' AS month, source, "
            "COUNT(*) AS calls, SUM(bytes) AS mb, SUM(cost_usd) AS cost "
            "FROM api_usage GROUP BY 1,2"
        )
        seed.commit()

    writer = sqlite3.connect(borrowed_path)
    try:
        writer.execute("INSERT INTO scratch VALUES (1)")
        assert writer.in_transaction
        persist_observed_provider_usage(writer, invocation="chat", **FOCUSED_CHAT_USAGE)
        assert writer.in_transaction
        assert writer.execute("SELECT COUNT(*) FROM scratch").fetchone()[0] == 1
        assert writer.execute("SELECT COUNT(*) FROM api_usage").fetchone()[0] == 1
        with closing(sqlite3.connect(borrowed_path)) as reader:
            assert reader.execute("SELECT COUNT(*) FROM scratch").fetchone()[0] == 0
            assert reader.execute("SELECT COUNT(*) FROM api_usage").fetchone()[0] == 0
        writer.rollback()
        assert writer.execute("SELECT COUNT(*) FROM scratch").fetchone()[0] == 0
        assert writer.execute("SELECT COUNT(*) FROM api_usage").fetchone()[0] == 0
    finally:
        writer.close()


def test_extract_usage_from_llm_result_merges_message_cost_and_llm_output_tokens():
    message = AIMessage(content="ok")
    message.response_metadata = {"cost": 0.0123}
    result = SimpleNamespace(
        generations=[[SimpleNamespace(message=message)]],
        llm_output={"token_usage": {"prompt_tokens": 12, "completion_tokens": 4}},
    )
    usage = extract_usage_from_llm_result(result)
    assert usage.tokens_in == 12
    assert usage.tokens_out == 4
    assert usage.cost_usd == 0.0123


@pytest.mark.asyncio
async def test_run_chain_retries_sqlite_commit_before_close(monkeypatch):
    calls: list[str] = []

    class CommitConn:
        attempts = 0

        def commit(self) -> None:
            self.attempts += 1
            calls.append("commit")
            if self.attempts == 1:
                raise sqlite3.OperationalError("database is locked")

        def close(self) -> None:
            calls.append("close")

    commit_conn = CommitConn()

    class SuccessChain:
        db = commit_conn

        def run(self, *, question, context):
            return {"answer": "ok", "sql": None}

    monkeypatch.setattr(chat_api_module, "connect_db", lambda: SimpleNamespace(close=lambda: None))
    monkeypatch.setattr(chat_api_module, "is_sqlite", lambda conn: isinstance(conn, CommitConn))
    monkeypatch.setattr(chat_api_module, "_build_chain", lambda *_a, **_k: SuccessChain())

    payload = await chat_api_module._run_chain(
        "nl_query", "question", {}, SimpleNamespace(client=None)
    )
    assert payload is not None
    assert commit_conn.attempts == 2
    assert calls.count("commit") == 2
    assert calls[-1] == "close"


def test_nl_query_chain_records_direct_provider_model_labels():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE managers (manager_id INTEGER PRIMARY KEY, name TEXT, cik TEXT)")
    try:

        class AttributionLLM:
            model_name = "gpt-4o-mini"
            _llm_type = "openai"

            def invoke(self, prompt: str, config=None):
                return AIMessage(
                    content=json.dumps(
                        {
                            "sql": "SELECT manager_id, name FROM managers LIMIT 1",
                            "explanation": "",
                            "columns": [],
                        }
                    ),
                    usage_metadata={"input_tokens": 9, "output_tokens": 3, "total_tokens": 12},
                )

        NLQueryChain(llm=AttributionLLM(), db_conn=conn).run("List managers")
        endpoint = conn.execute("SELECT endpoint FROM api_usage").fetchone()[0]
        assert endpoint.startswith("openai/gpt-4o-mini")
    finally:
        conn.close()


def test_log_llm_usage_owned_non_sqlite_connection_closes_without_sqlite_commit(monkeypatch):
    conn = Mock()
    monkeypatch.setattr("llm.cost_tracking.connect_db", lambda: conn)
    monkeypatch.setattr("llm.cost_tracking.is_sqlite", lambda _conn: False)
    monkeypatch.setattr("llm.cost_tracking.ensure_api_usage_schema", lambda _conn: None)
    monkeypatch.setattr("llm.cost_tracking.get_placeholder", lambda _conn: "%s")

    persist_observed_provider_usage(None, invocation="analysis", **FOCUSED_ANALYSIS_USAGE)

    conn.execute.assert_called_once()
    conn.commit.assert_not_called()
    conn.close.assert_called_once_with()


def test_log_llm_usage_creates_api_usage_and_inserts_row():
    conn = sqlite3.connect(":memory:")
    try:
        log_llm_usage(
            conn,
            provider="openai",
            model="gpt-4o-mini",
            tokens_in=1000,
            tokens_out=500,
            latency_ms=321,
            trace_url="https://smith.langchain.com/r/abc123",
        )

        row = conn.execute(
            "SELECT source, endpoint, status, bytes, latency_ms, cost_usd FROM api_usage"
        ).fetchone()
    finally:
        conn.close()

    assert row == (
        "langchain",
        "openai/gpt-4o-mini https://smith.langchain.com/r/abc123",
        200,
        1500,
        321,
        0.00045,
    )


class _ScriptedProvider(BaseChatModel):
    """Deterministic provider: replays scripted messages and counts real invocations.

    ``with_structured_output`` mirrors a real chat model: the provider is invoked (and reports
    usage) first, then the schema parser may reject the reply, which the chain answers by
    falling back to a second, text invocation.
    """

    model_name: str = "scripted"
    script: list[AIMessage]
    invocations: int = 0

    @property
    def _llm_type(self) -> str:
        return "scripted"

    def _generate(self, messages: list[BaseMessage], stop=None, run_manager=None, **kwargs: Any):
        message = self.script[self.invocations]
        self.invocations += 1
        return ChatResult(generations=[ChatGeneration(message=message)])

    def with_structured_output(self, schema: Any, **kwargs: Any):
        def _parse(message: AIMessage) -> Any:
            return schema.model_validate_json(str(message.content))

        return self | RunnableLambda(_parse)


def _ai(content: str, **usage: Any) -> AIMessage:
    message = AIMessage(content=content)
    if "usage_metadata" in usage:
        message.usage_metadata = usage["usage_metadata"]
    if "response_metadata" in usage:
        message.response_metadata = usage["response_metadata"]
    return message


_ANALYSIS_JSON = json.dumps(
    {
        "thesis": "Concentrated exposure.",
        "top_positions": [],
        "period_changes": [],
        "cross_manager_overlap": None,
        "concentration_metrics": {},
    }
)
_NL_JSON = json.dumps({"sql": "SELECT manager_id, name FROM managers ORDER BY name LIMIT 5"})


def _drive_supported_endpoints(tmp_path, monkeypatch) -> dict[str, Any]:
    """Drive POST /api/chat for chat and holdings analysis with deterministic providers.

    Returns the ``api_usage`` rows read back through a fresh connection (so uncommitted
    writes would be invisible), the fleet records, and the number of provider invocations.
    """
    db_path = tmp_path / "usage.db"
    with closing(sqlite3.connect(db_path)) as seed:
        seed.execute("CREATE TABLE managers (manager_id INTEGER PRIMARY KEY, name TEXT, cik TEXT)")
        seed.execute("INSERT INTO managers VALUES (1, 'Elliott', '0001791786')")
        seed.execute(
            "CREATE TABLE holdings (manager_id INTEGER, report_date TEXT, name_of_issuer TEXT, "
            "cusip TEXT, shares INTEGER, value_usd REAL)"
        )
        seed.execute(
            "CREATE TABLE daily_diffs (manager_id INTEGER, report_date TEXT, delta_type TEXT, "
            "name_of_issuer TEXT, value_prev REAL, value_curr REAL)"
        )
        seed.commit()
    fleet_path = tmp_path / "fleet.ndjson"
    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.delenv("DB_URL", raising=False)
    monkeypatch.setenv(fleet.ENV_FLEET_PATH, str(fleet_path))
    monkeypatch.delenv(fleet.ENV_LANGSMITH_KEY, raising=False)
    chat_api_module.CHAT_RATE_LIMITER.clear()
    chat_api_module.CHAT_IP_RATE_LIMITER.clear()

    chat_provider = _ScriptedProvider(
        script=[
            _ai(
                _NL_JSON,
                usage_metadata={"input_tokens": 512, "output_tokens": 128, "total_tokens": 640},
            )
        ]
    )
    analysis_provider = _ScriptedProvider(
        script=[
            # Structured attempt: reported usage, but the reply cannot be parsed -> fallback.
            _ai(
                "not json",
                usage_metadata={"input_tokens": 300, "output_tokens": 50, "total_tokens": 350},
            ),
            # Text fallback: usage plus a provider-reported cost.
            _ai(
                _ANALYSIS_JSON,
                usage_metadata={"input_tokens": 1000, "output_tokens": 200, "total_tokens": 1200},
                response_metadata={"cost": 0.0123},
            ),
        ]
    )
    unpriced_provider = _ScriptedProvider(
        script=[
            _ai(
                _NL_JSON,
                usage_metadata={"input_tokens": 10, "output_tokens": 5, "total_tokens": 15},
            )
        ]
    )
    no_usage_provider = _ScriptedProvider(script=[_ai(_NL_JSON)])
    turns = [
        ("nl_query", ClientInfo(chat_provider, "openai", "gpt-4o-mini")),
        (
            "holdings_analysis",
            ClientInfo(analysis_provider, "anthropic", "claude-sonnet-4-20250514"),
        ),
        ("nl_query", ClientInfo(unpriced_provider, "acme", "mystery-model")),
        ("nl_query", ClientInfo(no_usage_provider, "openai", "gpt-4o-mini")),
    ]
    pending = iter(turns)
    current: dict[str, ClientInfo] = {}
    monkeypatch.setattr(chat_api_module, "_build_chat_client_info", lambda: current["client"])

    async def _drive() -> list[int]:
        await cast(Any, chat_api_module.app.router).startup()
        try:
            transport = httpx.ASGITransport(app=cast(Any, chat_api_module.app))
            async with httpx.AsyncClient(
                transport=transport, base_url="http://test", timeout=10.0
            ) as client:
                statuses = []
                for chain, info in pending:
                    current["client"] = info
                    response = await client.post(
                        "/api/chat", json={"question": "Which managers hold AAPL?", "chain": chain}
                    )
                    statuses.append(response.status_code)
                return statuses
        finally:
            await cast(Any, chat_api_module.app.router).shutdown()

    statuses = asyncio.run(_drive())
    with closing(sqlite3.connect(db_path)) as reader:
        rows = reader.execute(
            "SELECT source, endpoint, status, bytes, latency_ms, cost_usd FROM api_usage "
            "ORDER BY id"
        ).fetchall()
    records = [
        json.loads(line) for line in fleet_path.read_text(encoding="utf-8").splitlines() if line
    ]
    return {
        "statuses": statuses,
        "rows": rows,
        "records": records,
        "invocations": [
            chat_provider.invocations,
            analysis_provider.invocations,
            unpriced_provider.invocations,
            no_usage_provider.invocations,
        ],
    }


def test_chat_and_analysis_persist_observed_usage_once(tmp_path, monkeypatch):
    """Supported chat and holdings-analysis endpoints persist one row per provider invocation.

    Replays the same deterministic fixture twice: a legacy ``bytes=0`` readback fails, then the
    post-#1939 run stores observed token sums (and ``USAGE_BYTES_UNKNOWN`` when usage is absent).
    """
    replay = ISSUE_1939_ACCEPTANCE_REPLAY_RECORD
    expected_bytes = replay["observed_readback_bytes"]
    legacy_dir = tmp_path / "legacy"
    observed_dir = tmp_path / "observed"
    legacy_dir.mkdir()
    observed_dir.mkdir()

    for chain in ("NLQueryChain", "HoldingsAnalysisChain"):
        assert "api_usage" in TOOL_REGISTRY[chain].db_writes

    monkeypatch.setattr(
        "llm.cost_tracking.observed_usage_bytes",
        lambda _tokens_in, _tokens_out: legacy_placeholder_usage_bytes(),
    )
    legacy = _drive_supported_endpoints(legacy_dir, monkeypatch)
    legacy_bytes = [row[3] for row in legacy["rows"]]
    assert legacy_bytes == replay["compatibility_control"]["legacy_readback_sequence"]
    with pytest.raises(AssertionError):
        assert legacy_bytes == expected_bytes

    monkeypatch.setattr("llm.cost_tracking.observed_usage_bytes", observed_usage_bytes)
    persist_invocations: list[str] = []
    real_persist = persist_observed_provider_usage

    def _count_supported_entry_point(*args: Any, **kwargs: Any) -> None:
        persist_invocations.append(kwargs["invocation"])
        real_persist(*args, **kwargs)

    monkeypatch.setattr(
        "llm.cost_tracking.persist_observed_provider_usage", _count_supported_entry_point
    )
    observed = _drive_supported_endpoints(observed_dir, monkeypatch)

    assert observed["statuses"] == [200, 200, 200, 200]
    # chat 1 + analysis 2 (structured attempt, then text fallback) + unpriced 1 + no-usage 1.
    assert observed["invocations"] == replay["provider_invocations_per_request"]
    rows = observed["rows"]
    assert len(rows) == 5
    assert [row[3] for row in rows] == expected_bytes
    assert len(persist_invocations) == len(rows) == sum(observed["invocations"])
    assert persist_invocations == ["chat", "analysis", "analysis", "chat", "chat"]

    def _request_id(endpoint: str) -> str:
        match = re.search(r"request_id=([0-9a-f]+)", endpoint)
        assert match is not None, endpoint
        return match.group(1)

    request_ids = [_request_id(row[1]) for row in rows]
    # Rows of one request share an id; separate requests never do.
    assert request_ids[1] == request_ids[2]
    assert len({request_ids[0], request_ids[1], request_ids[3], request_ids[4]}) == 4

    def _strip(row: tuple[Any, ...]) -> tuple[Any, ...]:
        return (row[0], row[1].split(" request_id=")[0], row[2], row[3], row[5])

    chat_replay = ISSUE_1939_REPLAY_EXPECTATIONS["chat"]
    missing_replay = ISSUE_1939_REPLAY_EXPECTATIONS["missing_usage"]
    assert [_strip(row) for row in rows] == [
        (
            "langchain",
            "openai/gpt-4o-mini",
            200,
            chat_replay["api_usage_bytes"],
            chat_replay["cost_usd"],
        ),
        ("langchain", "anthropic/claude-sonnet-4-20250514", 200, 350, 0.00165),
        ("langchain", "anthropic/claude-sonnet-4-20250514", 200, 1200, 0.0123),
        # Tokens reported but the model is unpriced and no cost was supplied: cost unknown.
        ("langchain", "acme/mystery-model", 200, 15, None),
        # No usage reported at all: tokens and cost explicitly unknown.
        (
            "langchain",
            "openai/gpt-4o-mini",
            200,
            missing_replay["api_usage_bytes"],
            missing_replay["cost_usd"],
        ),
    ]
    assert all(row[4] >= 0 for row in rows)

    records = observed["records"]
    assert len(records) == 4
    assert [r["domain"]["chain"] for r in records] == [
        "nl_query",
        "holdings_analysis",
        "nl_query",
        "nl_query",
    ]
    # The fleet record joins its rows through the same request id and carries the same
    # provider/model and the same observed tokens and cost.
    for record, request_id in zip(
        records, [request_ids[0], request_ids[1], request_ids[3], request_ids[4]], strict=True
    ):
        assert record["domain"]["request_id_hash"] == fleet._hash_identifier(request_id)
    domains = [r["domain"] for r in records]
    assert (records[0]["provider"], records[0]["model"]) == ("openai", "gpt-4o-mini")
    assert (
        domains[0]["input_tokens"],
        domains[0]["output_tokens"],
        domains[0]["total_tokens"],
    ) == (
        512,
        128,
        640,
    )
    assert domains[0]["cost_usd"] == 0.000154
    assert (records[1]["provider"], records[1]["model"]) == (
        "anthropic",
        "claude-sonnet-4-20250514",
    )
    assert (
        domains[1]["input_tokens"],
        domains[1]["output_tokens"],
        domains[1]["total_tokens"],
    ) == (
        1300,
        250,
        1550,
    )
    assert domains[1]["cost_usd"] == 0.01395
    assert (records[2]["provider"], records[2]["model"]) == ("acme", "mystery-model")
    assert (domains[2]["input_tokens"], domains[2]["output_tokens"]) == (10, 5)
    assert "cost_usd" not in domains[2]
    for key in ("input_tokens", "output_tokens", "total_tokens", "cost_usd"):
        assert key not in domains[3]

    legacy_bytes = legacy_placeholder_usage_bytes()
    for row in rows:
        if row[3] == USAGE_BYTES_UNKNOWN:
            assert row[3] != legacy_bytes
        else:
            assert row[3] != legacy_bytes
            assert row[3] > 0
    _legacy_bytes_readback_fails_for_observed_fixture(rows[0][3], legacy_bytes=legacy_bytes)
