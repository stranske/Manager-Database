from __future__ import annotations

from chains.utils import (
    estimate_token_count,
    format_delta_summary,
    format_holdings_table,
    truncate_context,
)


def test_format_holdings_table_output_format() -> None:
    rows = [
        {
            "name_of_issuer": "ALPHA TECH INC",
            "cusip": "111111111",
            "shares": 1000,
            "value_usd": 1_250_000.5,
        },
        {
            "name_of_issuer": "BETA HEALTH PLC",
            "cusip": "222222222",
            "shares": 500,
            "value_usd": 800_000.0,
        },
    ]

    rendered = format_holdings_table(rows)

    assert "rank | issuer | cusip | shares | value_usd" in rendered
    assert "1 | ALPHA TECH INC | 111111111 | 1,000 | 1,250,000.50" in rendered
    assert "2 | BETA HEALTH PLC | 222222222 | 500 | 800,000.00" in rendered


def test_truncate_context_respects_token_budget() -> None:
    text = "A" * 240
    assert estimate_token_count(text) == 60

    truncated = truncate_context(text, max_tokens=20)
    assert estimate_token_count(truncated) <= 20
    assert truncated.endswith("[TRUNCATED]")


def test_format_delta_summary_groups_delta_types() -> None:
    diffs = [
        {
            "delta_type": "ADD",
            "name_of_issuer": "ALPHA TECH INC",
            "value_prev": 0,
            "value_curr": 100,
        },
        {
            "delta_type": "EXIT",
            "name_of_issuer": "BETA HEALTH PLC",
            "value_prev": 200,
            "value_curr": 0,
        },
        {
            "delta_type": "INCREASE",
            "name_of_issuer": "GAMMA RETAIL LTD",
            "value_prev": 50,
            "value_curr": 75,
        },
        {
            "delta_type": "DECREASE",
            "name_of_issuer": "OMEGA ENERGY SA",
            "value_prev": 90,
            "value_curr": 60,
        },
    ]

    summary = format_delta_summary(diffs)

    assert "ADD: ALPHA TECH INC ($0 -> $100)" in summary
    assert "EXIT: BETA HEALTH PLC ($200 -> $0)" in summary
    assert "INCREASE: GAMMA RETAIL LTD ($50 -> $75)" in summary
    assert "DECREASE: OMEGA ENERGY SA ($90 -> $60)" in summary
