"""Focused contract coverage for chat request helpers."""

from datetime import date

import pytest
from fastapi import HTTPException

from api import chat


def test_circuit_breaker_opens_resets_and_success_clears_state():
    now = [10.0]
    breaker = chat.CircuitBreaker(
        failure_threshold=2, reset_timeout_s=5, monotonic_fn=lambda: now[0]
    )

    assert not breaker.is_open()
    breaker.record_failure()
    assert not breaker.is_open()
    breaker.record_failure()
    assert breaker.is_open()

    breaker.record_success()
    assert not breaker.is_open()
    breaker.record_failure()
    breaker.record_failure()
    now[0] = 15.0
    assert not breaker.is_open()


@pytest.mark.parametrize(
    ("raw", "expected"),
    [(None, 7.5), ("0", 7.5), ("-1", 7.5), ("nan", 7.5), ("3.25", 3.25)],
)
def test_health_timeout_uses_only_finite_positive_environment_values(monkeypatch, raw, expected):
    if raw is None:
        monkeypatch.delenv("CHAT_TEST_TIMEOUT", raising=False)
    else:
        monkeypatch.setenv("CHAT_TEST_TIMEOUT", raw)

    assert chat._health_timeout_seconds("CHAT_TEST_TIMEOUT", 7.5) == expected


def test_signed_session_cookie_requires_matching_secret(monkeypatch):
    monkeypatch.setenv("CHAT_SESSION_COOKIE_SECRET", "secret")
    signed = chat._sign_chat_session_id("session-17", "secret")

    assert chat._verified_chat_session_cookie(signed) == "session-17"
    assert chat._verified_chat_session_cookie("session-17.invalid") is None
    assert chat._verified_chat_session_cookie("missing-separator") is None
    monkeypatch.delenv("CHAT_SESSION_COOKIE_SECRET")
    assert chat._verified_chat_session_cookie(signed) is None


def test_normalizers_drop_invalid_values_and_parse_dates():
    assert chat._normalize_manager_ids(["1", None, "bad", 2]) == [1, 2]
    assert chat._normalize_cusips([" abc ", "", None, 17]) == ["ABC", "17"]
    assert chat._parse_date_range({"start": "2024-01-01", "end": "2024-03-31"}) == (
        date(2024, 1, 1),
        date(2024, 3, 31),
    )
    assert chat._parse_date_range(["2024-01-01", "not-a-date"]) is None
    assert chat._parse_date_range("2024-01-01") is None


def test_normalize_chain_context_resolves_manager_name_and_valid_filters(monkeypatch):
    monkeypatch.setattr(chat, "_manager_ids_for_name", lambda _conn, _name: [4])
    context = chat._normalize_chain_context(
        {
            "manager_name": " Northstar ",
            "cusips": [" 123456789 "],
            "date_range": ["2024-01-01", "2024-03-31"],
        },
        object(),
    )

    assert context == {
        "manager_name": " Northstar ",
        "manager_ids": [4],
        "cusips": ["123456789"],
        "date_range": {"start": "2024-01-01", "end": "2024-03-31"},
    }
    assert chat._validated_holdings_filters(context) == (
        [4],
        ["123456789"],
        (date(2024, 1, 1), date(2024, 3, 31)),
    )


@pytest.mark.parametrize(
    ("context", "detail"),
    [
        ({"manager_ids": ["bad"]}, "Manager filter"),
        ({"cusips": [""]}, "CUSIP filter"),
        ({"date_range": ["2024-04-01", "2024-01-01"]}, "Date range"),
    ],
)
def test_validated_holdings_filters_reject_invalid_explicit_filters(context, detail):
    with pytest.raises(HTTPException, match=detail):
        chat._validated_holdings_filters(context)


def test_classify_intent_uses_deterministic_fallback_when_import_is_unavailable(monkeypatch):
    def unavailable(_name):
        raise ModuleNotFoundError("chains.intent")

    monkeypatch.setattr(chat.importlib, "import_module", unavailable)

    assert chat._classify_intent("write a SQL query") == "nl_query"
    assert chat._classify_intent("summarize the 13F filing") == "filing_summary"
    assert chat._classify_intent("analyze the holding") == "holdings_analysis"
    assert chat._classify_intent("what changed?") == "rag_search"


def test_display_formatters_include_only_meaningful_optional_fields():
    filing = chat._format_filing_summary_payload(
        {
            "manager_name": "Northstar",
            "filing_date": "2024-03-31",
            "total_positions": 2,
            "total_aum_estimate": 12,
            "key_positions": [{"issuer": "Alpha"}, "unusable"],
            "notable_changes": ["Added Alpha"],
            "risk_flags": ["Concentrated"],
        }
    )
    holdings = chat._format_holdings_analysis_payload(
        {
            "thesis": "Concentrated portfolio.",
            "top_positions": [{"cusip": "123456789"}],
            "period_changes": [{"summary": "Increased"}],
            "concentration_metrics": {"top_10": 0.7},
        }
    )

    assert "Top positions: Alpha" in filing["answer"]
    assert "Notable changes: Added Alpha" in filing["answer"]
    assert "Risk flags: Concentrated" in filing["answer"]
    assert "Top positions: 123456789" in holdings["answer"]
    assert "Period changes: Increased" in holdings["answer"]
    assert "Concentration metrics" in holdings["answer"]


@pytest.mark.parametrize(
    ("trace_url", "expected"),
    [
        (None, None),
        ("", None),
        ("https://trace.invalid/r/run-17/", "run-17"),
        ("trace-id", None),
    ],
)
def test_response_id_is_extracted_only_from_path_like_trace_urls(trace_url, expected):
    assert chat._response_id_from_trace_url(trace_url) == expected


def test_dependency_error_and_timeout_bounds_are_stable(monkeypatch):
    monkeypatch.setenv("HEALTH_SUMMARY_TIMEOUT_S", "9")
    assert chat._health_summary_timeout_seconds() == 0.2
    monkeypatch.setenv("HEALTH_SUMMARY_TIMEOUT_S", "0.01")
    assert chat._health_summary_timeout_seconds() == 0.05
    assert (
        chat._format_dependency_error(RuntimeError("  failed dependency  ")) == "failed dependency"
    )
    assert chat._format_dependency_error(ValueError()) == "ValueError"
