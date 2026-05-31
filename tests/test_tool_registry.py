"""Acceptance tests for machine-readable tool data-zone declarations."""

import sqlite3

from diff_holdings import diff_holdings
from tools.registry import TOOL_REGISTRY


def _seed_canonical_db(conn: sqlite3.Connection) -> None:
    conn.execute(
        "CREATE TABLE managers (manager_id INTEGER PRIMARY KEY, name TEXT, cik TEXT UNIQUE)"
    )
    conn.execute(
        "CREATE TABLE filings ("
        "filing_id INTEGER PRIMARY KEY, manager_id INTEGER, "
        "type TEXT, filed_date TEXT, source TEXT)"
    )
    conn.execute(
        "CREATE TABLE holdings ("
        "holding_id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "filing_id INTEGER, cusip TEXT, name_of_issuer TEXT, "
        "shares INTEGER, value_usd REAL)"
    )
    conn.execute("INSERT INTO managers(manager_id, name, cik) VALUES (1, 'TestFund', '0000000000')")
    conn.executemany(
        "INSERT INTO filings(filing_id, manager_id, type, filed_date, source) VALUES (?,?,?,?,?)",
        [
            (101, 1, "13F-HR", "2024-04-01", "edgar"),
            (102, 1, "13F-HR", "2024-01-01", "edgar"),
        ],
    )
    conn.executemany(
        "INSERT INTO holdings(filing_id, cusip, name_of_issuer, shares, value_usd) VALUES (?,?,?,?,?)",
        [
            (101, "AAA", "CorpA", 120, 1200),
            (102, "AAA", "CorpA", 100, 1000),
        ],
    )
    conn.commit()


def test_every_tool_declares_zone():
    assert TOOL_REGISTRY
    for descriptor in TOOL_REGISTRY.values():
        assert descriptor.data_zone
        assert descriptor.llm_boundary in {"none", "external_authorized"}


def test_deterministic_tools_have_no_llm():
    assert TOOL_REGISTRY["diff_holdings"].llm_boundary == "none"
    assert TOOL_REGISTRY["daily_diff_flow"].llm_boundary == "none"
    assert TOOL_REGISTRY["RAGSearchChain"].llm_boundary == "external_authorized"
    assert TOOL_REGISTRY["FilingSummaryChain"].llm_boundary == "external_authorized"
    assert TOOL_REGISTRY["NLQueryChain"].llm_boundary == "external_authorized"


def test_diff_holdings_runresult_reports_registry_boundary():
    conn = sqlite3.connect(":memory:")
    _seed_canonical_db(conn)

    result = diff_holdings(1, conn)
    conn.close()

    assert result.llm_boundary == "none"
    assert result.data_zone == "public_filings"
