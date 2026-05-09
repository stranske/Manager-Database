from __future__ import annotations

import sqlite3
from typing import Any

from langchain_core.runnables import RunnableLambda

from chains.filing_summary import FilingSummaryChain
from chains.holdings_analysis import HoldingsAnalysisChain
from tests._pg_fakes import StrictPostgresConn
from tools.langchain_client import ClientInfo


def _client_info() -> ClientInfo:
    llm: Any = RunnableLambda(lambda _payload: "{}")
    return ClientInfo(client=llm, provider="test-provider", model="test-model")


def test_filing_summary_usage_log_uses_postgres_schema_and_placeholders() -> None:
    conn = StrictPostgresConn()
    chain = FilingSummaryChain(client_info=_client_info(), db_conn=conn)

    chain._log_usage(filing_id=123, output_text="{}", latency_ms=7, status=1)

    executed_sql = " ".join(conn.statements)
    assert "BIGSERIAL PRIMARY KEY" in executed_sql
    assert "VALUES (%s, %s, %s, %s, %s, %s)" in executed_sql
    assert conn.params[-1] == ("filing_summary_chain", "filing_id:123", 1, 2, 7, 0.0)
    assert conn.committed is True


def test_filing_summary_usage_log_uses_sqlite_schema_and_placeholders() -> None:
    conn = sqlite3.connect(":memory:")
    chain = FilingSummaryChain(client_info=_client_info(), db_conn=conn)

    chain._log_usage(filing_id=123, output_text="{}", latency_ms=7, status=1)

    table_sql = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='api_usage'"
    ).fetchone()
    assert table_sql is not None
    assert "INTEGER PRIMARY KEY" in table_sql[0]
    assert "BIGSERIAL" not in table_sql[0]
    row = conn.execute(
        "SELECT source, endpoint, status, bytes, latency_ms, cost_usd FROM api_usage"
    ).fetchone()
    assert row == ("filing_summary_chain", "filing_id:123", 1, 2, 7, 0.0)


def test_holdings_analysis_usage_log_uses_postgres_schema_and_placeholders() -> None:
    conn = StrictPostgresConn()
    chain = HoldingsAnalysisChain(client_info=_client_info(), db_conn=conn)

    chain._log_usage(question="Which managers hold AAPL?", output_text="{}", latency_ms=9, status=1)

    executed_sql = " ".join(conn.statements)
    assert "BIGSERIAL PRIMARY KEY" in executed_sql
    assert "VALUES (%s, %s, %s, %s, %s, %s)" in executed_sql
    assert conn.params[-1] == ("holdings_analysis_chain", "Which managers hold AAPL?", 1, 2, 9, 0.0)
    assert conn.committed is True


def test_holdings_analysis_usage_log_uses_sqlite_schema_and_placeholders() -> None:
    conn = sqlite3.connect(":memory:")
    chain = HoldingsAnalysisChain(client_info=_client_info(), db_conn=conn)

    chain._log_usage(question="Which managers hold AAPL?", output_text="{}", latency_ms=9, status=1)

    table_sql = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='api_usage'"
    ).fetchone()
    assert table_sql is not None
    assert "INTEGER PRIMARY KEY" in table_sql[0]
    assert "BIGSERIAL" not in table_sql[0]
    row = conn.execute(
        "SELECT source, endpoint, status, bytes, latency_ms, cost_usd FROM api_usage"
    ).fetchone()
    assert row == ("holdings_analysis_chain", "Which managers hold AAPL?", 1, 2, 9, 0.0)
