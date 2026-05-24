"""Tests for the centralized LangSmith fleet emitter (issue #1048)."""

from __future__ import annotations

import json

import pytest

from llm import langsmith_fleet as fleet


@pytest.fixture(autouse=True)
def _isolate_langsmith_env(monkeypatch):
    """Keep LangSmith environment deterministic between cases."""

    for key in (
        fleet.ENV_LANGSMITH_KEY,
        fleet.ENV_LANGCHAIN_API_KEY,
        fleet.ENV_LANGCHAIN_PROJECT,
        fleet.ENV_LANGSMITH_PROJECT,
        fleet.ENV_LANGCHAIN_TRACING_V2,
        fleet.ENV_FLEET_PATH,
    ):
        monkeypatch.delenv(key, raising=False)


def _context(**overrides) -> fleet.ChatFleetContext:
    base: dict[str, str | None] = {
        "run_id": "resp-1",
        "request_id": "req-abc",
        "endpoint": "/api/chat",
        "chain": "nl_query",
        "session_id": "header:abc",
        "provider": "openai",
        "model": "gpt-4o-mini",
        "trace_id": None,
        "trace_url": "https://smith.langchain.com/r/run-1",
        "recorded_at": "2026-05-24T05:00:00Z",
    }
    base.update(overrides)
    return fleet.ChatFleetContext(**base)  # type: ignore[arg-type]


def test_no_secret_status_when_key_missing():
    record = fleet.build_chat_fleet_record(
        context=_context(),
        latency_ms=120,
        http_status=200,
    )
    assert record["schema_version"] == "langsmith-fleet/v1"
    assert record["repo"] == "stranske/Manager-Database"
    assert record["surface"] == "chat-api"
    assert record["operation"] == "chat-turn"
    assert record["status"] == "no_secret"
    assert record["domain"]["endpoint"] == "/api/chat"
    assert record["domain"]["chain"] == "nl_query"
    assert record["domain"]["workflow"] == "nl-query"
    assert record["domain"]["http_status"] == 200
    assert record["domain"]["latency_ms"] == 120
    # request_id and session_id are hashed, never echoed raw.
    raw_request_id = "req-abc"
    raw_session_id = "header:abc"
    assert raw_request_id not in json.dumps(record)
    assert raw_session_id not in json.dumps(record)
    assert record["domain"]["request_id_hash"] != raw_request_id
    assert record["domain"]["session_id_hash"] != raw_session_id
    # trace_url and provider/model are surfaced at the top level.
    assert record["trace_url"] == "https://smith.langchain.com/r/run-1"
    assert record["provider"] == "openai"
    assert record["model"] == "gpt-4o-mini"


def test_success_status_when_key_present(monkeypatch):
    monkeypatch.setenv(fleet.ENV_LANGSMITH_KEY, "ls-key")
    record = fleet.build_chat_fleet_record(
        context=_context(),
        latency_ms=42,
        http_status=200,
        token_usage=fleet.TokenUsage(input_tokens=10, output_tokens=20, total_tokens=30),
        cost_usd=0.0012,
        evaluation_score=0.91,
    )
    assert record["status"] == "success"
    assert record["domain"]["input_tokens"] == 10
    assert record["domain"]["output_tokens"] == 20
    assert record["domain"]["total_tokens"] == 30
    assert record["domain"]["cost_usd"] == 0.0012
    assert record["domain"]["evaluation_score"] == 0.91


def test_error_status_when_provider_returns_500(monkeypatch):
    monkeypatch.setenv(fleet.ENV_LANGSMITH_KEY, "ls-key")
    record = fleet.build_chat_fleet_record(
        context=_context(),
        latency_ms=5,
        http_status=500,
        error_category="server_error",
    )
    assert record["status"] == "error"
    assert record["error_category"] == "server_error"
    assert record["domain"]["error_state"] == "server_error"


def test_rate_limited_429_is_fallback_status(monkeypatch):
    monkeypatch.setenv(fleet.ENV_LANGSMITH_KEY, "ls-key")
    record = fleet.build_chat_fleet_record(
        context=_context(),
        latency_ms=1,
        http_status=429,
        error_category="rate_limited",
        rate_limited=True,
    )
    assert record["status"] == "fallback"
    assert record["domain"]["rate_limited"] is True


def test_fallback_status_when_fallback_reason_set(monkeypatch):
    monkeypatch.setenv(fleet.ENV_LANGSMITH_KEY, "ls-key")
    record = fleet.build_chat_fleet_record(
        context=_context(),
        latency_ms=99,
        http_status=200,
        fallback_reason="provider_timeout",
    )
    assert record["status"] == "fallback"
    assert record["domain"]["fallback_state"] == "provider_timeout"


def test_feedback_record_correlates_response_id(monkeypatch):
    monkeypatch.setenv(fleet.ENV_LANGSMITH_KEY, "ls-key")
    record = fleet.build_feedback_fleet_record(
        response_id="resp-7",
        feedback_id=42,
        rating=5,
        chain="nl_query",
        session_id="cookie:xyz",
        forwarded_to_langsmith=True,
    )
    assert record["operation"] == "chat-feedback"
    assert record["status"] == "success"
    assert record["run_id"] == "resp-7"
    assert record["domain"]["response_id"] == "resp-7"
    assert record["domain"]["feedback_id"] == "42"
    assert record["domain"]["rating"] == 5
    assert record["domain"]["forwarded_to_langsmith"] is True
    assert "cookie:xyz" not in json.dumps(record)


def test_feedback_record_no_secret_when_key_missing():
    record = fleet.build_feedback_fleet_record(
        response_id="resp-7",
        feedback_id="f-9",
        rating=3,
    )
    assert record["status"] == "no_secret"
    assert record["domain"]["feedback_id"] == "f-9"


def test_feedback_forwarded_false_stays_no_secret_when_key_missing():
    record = fleet.build_feedback_fleet_record(
        response_id="resp-7",
        feedback_id="f-10",
        rating=3,
        forwarded_to_langsmith=False,
    )
    assert record["status"] == "no_secret"
    assert record["domain"]["forwarded_to_langsmith"] is False


def test_append_fleet_records_writes_ndjson(tmp_path):
    path = tmp_path / "out" / "langsmith-fleet.ndjson"
    rec1 = fleet.build_chat_fleet_record(
        context=_context(run_id="r1", request_id="q1"),
        latency_ms=10,
        http_status=200,
    )
    rec2 = fleet.build_chat_fleet_record(
        context=_context(run_id="r2", request_id="q2"),
        latency_ms=20,
        http_status=200,
    )
    fleet.append_fleet_records(path, [rec1])
    fleet.append_fleet_records(path, [rec2])
    lines = path.read_text().splitlines()
    assert path.with_suffix(path.suffix + ".lock").exists()
    assert len(lines) == 2
    parsed = [json.loads(line) for line in lines]
    assert parsed[0]["run_id"] == "r1"
    assert parsed[1]["run_id"] == "r2"
    assert all(p["schema_version"] == "langsmith-fleet/v1" for p in parsed)


def test_append_fleet_records_respects_retention_limit(tmp_path):
    path = tmp_path / "langsmith-fleet.ndjson"
    for i in range(5):
        fleet.append_fleet_records(
            path,
            [
                fleet.build_chat_fleet_record(
                    context=_context(run_id=f"r{i}", request_id=f"q{i}"),
                    latency_ms=1,
                    http_status=200,
                )
            ],
            retention_limit=3,
        )
    lines = path.read_text().splitlines()
    assert len(lines) == 3
    run_ids = [json.loads(line)["run_id"] for line in lines]
    assert run_ids == ["r2", "r3", "r4"]


def test_default_artifact_path_respects_env(monkeypatch, tmp_path):
    override = tmp_path / "custom" / "fleet.ndjson"
    monkeypatch.setenv(fleet.ENV_FLEET_PATH, str(override))
    assert fleet.default_fleet_artifact_path() == override


def test_default_artifact_path_falls_back_to_project_root(monkeypatch):
    monkeypatch.delenv(fleet.ENV_FLEET_PATH, raising=False)
    resolved = fleet.default_fleet_artifact_path()
    assert resolved.name == fleet.ARTIFACT_NAME
    assert "artifacts/langsmith" in resolved.as_posix()


def test_record_chat_event_persists_and_returns(tmp_path, monkeypatch):
    monkeypatch.delenv(fleet.ENV_LANGSMITH_KEY, raising=False)
    path = tmp_path / "fleet.ndjson"
    record = fleet.record_chat_event(
        context=_context(),
        latency_ms=8,
        http_status=200,
        artifact_path=path,
    )
    assert record["status"] == "no_secret"
    persisted = json.loads(path.read_text().splitlines()[-1])
    assert persisted["run_id"] == "resp-1"
    assert persisted["domain"]["chain"] == "nl_query"


def test_record_feedback_event_persists_and_returns(tmp_path, monkeypatch):
    monkeypatch.setenv(fleet.ENV_LANGSMITH_KEY, "ls-key")
    path = tmp_path / "fleet.ndjson"
    record = fleet.record_feedback_event(
        response_id="resp-99",
        feedback_id=7,
        rating=4,
        chain="filing_summary",
        forwarded_to_langsmith=True,
        artifact_path=path,
    )
    assert record["operation"] == "chat-feedback"
    persisted = json.loads(path.read_text().splitlines()[-1])
    assert persisted["domain"]["feedback_id"] == "7"
    assert persisted["domain"]["workflow"] == "filing-summary"


def test_workflow_tag_mapping_covers_all_chains():
    expected = {"filing_summary", "holdings_analysis", "nl_query", "rag_search"}
    assert expected.issubset(set(fleet.CHAIN_TO_WORKFLOW_TAG.keys()))


def test_no_secret_record_omits_trace_url_when_missing():
    record = fleet.build_chat_fleet_record(
        context=_context(trace_url=None),
        latency_ms=5,
        http_status=200,
    )
    assert "trace_url" not in record


def test_record_provides_repo_and_github_issue_anchors():
    record = fleet.build_chat_fleet_record(
        context=_context(),
        latency_ms=5,
        http_status=200,
    )
    assert record["repo"] == "stranske/Manager-Database"
    assert record["github_issue"] == "stranske/Manager-Database#1048"


def test_feedback_record_forwarded_false_is_fallback(monkeypatch):
    monkeypatch.setenv(fleet.ENV_LANGSMITH_KEY, "ls-key")
    record = fleet.build_feedback_fleet_record(
        response_id="resp-x",
        feedback_id=1,
        rating=2,
        forwarded_to_langsmith=False,
    )
    assert record["status"] == "fallback"
    assert record["domain"]["forwarded_to_langsmith"] is False


def test_append_fleet_records_returns_path_on_empty_input(tmp_path):
    path = tmp_path / "fleet.ndjson"
    result = fleet.append_fleet_records(path, [])
    assert result == path
    assert not path.exists()


def test_append_fleet_records_raises_on_zero_retention(tmp_path):
    path = tmp_path / "fleet.ndjson"
    with pytest.raises(ValueError, match="retention_limit must be >= 1"):
        fleet.append_fleet_records(path, [], retention_limit=0)


def test_error_status_for_500_without_error_category(monkeypatch):
    monkeypatch.setenv(fleet.ENV_LANGSMITH_KEY, "ls-key")
    record = fleet.build_chat_fleet_record(
        context=_context(),
        latency_ms=1,
        http_status=500,
    )
    assert record["status"] == "error"


def test_record_includes_github_pr_when_set():
    record = fleet.build_chat_fleet_record(
        context=_context(github_pr="stranske/Manager-Database#42"),
        latency_ms=5,
        http_status=200,
    )
    assert record["github_pr"] == "stranske/Manager-Database#42"


def test_record_includes_trace_id_when_set():
    record = fleet.build_chat_fleet_record(
        context=_context(trace_id="trace-xyz"),
        latency_ms=5,
        http_status=200,
    )
    assert record["trace_id"] == "trace-xyz"


def test_record_includes_artifact_ref_when_passed():
    record = fleet.build_chat_fleet_record(
        context=_context(),
        latency_ms=5,
        http_status=200,
        artifact_ref="gs://bucket/fleet.ndjson",
    )
    assert record["artifact_ref"] == "gs://bucket/fleet.ndjson"
