from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

from langchain_core.runnables import RunnableLambda

sys.path.append(str(Path(__file__).resolve().parents[1]))

from chains.filing_summary import FilingSummaryChain
from tools.langchain_client import ClientInfo


class _MockCursor:
    def __init__(
        self,
        *,
        fetchone_map: dict[tuple[str, tuple[Any, ...]], dict[str, Any] | None],
        fetchall_map: dict[tuple[str, tuple[Any, ...]], list[dict[str, Any]]],
    ) -> None:
        self._fetchone_map = fetchone_map
        self._fetchall_map = fetchall_map
        self.last_query: str | None = None
        self.last_params: tuple[Any, ...] | None = None
        self.calls: list[tuple[str, tuple[Any, ...]]] = []
        self.description: list[tuple[str, ...]] | None = None

    def execute(self, query: str, params: tuple[Any, ...]) -> None:
        self.last_query = query
        self.last_params = params
        self.calls.append((query, params))

    def fetchone(self) -> dict[str, Any] | None:
        key = (self.last_query or "", self.last_params or ())
        return self._fetchone_map.get(key)

    def fetchall(self) -> list[dict[str, Any]]:
        key = (self.last_query or "", self.last_params or ())
        return self._fetchall_map.get(key, [])


class _MockDB:
    def __init__(self, cursor: _MockCursor) -> None:
        self._cursor = cursor

    def cursor(self) -> _MockCursor:
        return self._cursor


def _make_chain(db: _MockDB) -> FilingSummaryChain:
    llm = RunnableLambda(lambda _payload: "{}")
    client_info = ClientInfo(client=llm, provider="test-provider", model="test-model")
    return FilingSummaryChain(client_info=client_info, db_conn=db)


def test_load_filing_data_with_mock_database_data() -> None:
    filing_id = 1001
    manager_id = 7

    filing_query = "SELECT * FROM filings WHERE filing_id = %s"
    holdings_query = "SELECT * FROM holdings WHERE filing_id = %s ORDER BY value_usd DESC LIMIT 20"
    manager_query = "SELECT name FROM managers WHERE manager_id = %s"
    diffs_query = (
        "SELECT * FROM daily_diffs WHERE manager_id = %s AND report_date = %s "
        "ORDER BY value_curr DESC"
    )

    filing_row = {
        "filing_id": filing_id,
        "manager_id": manager_id,
        "period_end": "2025-12-31",
        "filed_date": "2026-02-14",
        "total_positions": 42,
        "total_value_usd": 123_456_789.0,
    }
    holdings_rows = [
        {
            "name_of_issuer": "ALPHA TECH INC",
            "cusip": "111111111",
            "shares": 1_000_000,
            "value_usd": 80_000_000.0,
        },
        {
            "name_of_issuer": "BRAVO HEALTH CO",
            "cusip": "222222222",
            "shares": 500_000,
            "value_usd": 43_456_789.0,
        },
    ]
    diff_rows = [
        {
            "delta_type": "ADD",
            "name_of_issuer": "ALPHA TECH INC",
            "value_prev": 0,
            "value_curr": 80_000_000,
        },
        {
            "delta_type": "EXIT",
            "name_of_issuer": "OMEGA RETAIL LTD",
            "value_prev": 12_500_000,
            "value_curr": 0,
        },
    ]

    cursor = _MockCursor(
        fetchone_map={
            (filing_query, (filing_id,)): filing_row,
            (manager_query, (manager_id,)): {"name": "Alpha Capital"},
        },
        fetchall_map={
            (holdings_query, (filing_id,)): holdings_rows,
            (diffs_query, (manager_id, "2025-12-31")): diff_rows,
        },
    )
    chain = _make_chain(_MockDB(cursor))

    result = chain._load_filing_data(filing_id)

    assert result["filing_id"] == filing_id
    assert result["manager_name"] == "Alpha Capital"
    assert result["filing_date"] == "2026-02-14"
    assert result["period_end"] == "2025-12-31"
    assert result["total_positions"] == 42
    assert result["total_value_usd"] == 123_456_789.0
    assert result["top_holdings"] == holdings_rows

    assert "rank | issuer | cusip | shares | value_usd" in result["top_holdings_table"]
    assert "ALPHA TECH INC | 111111111 | 1,000,000 | 80,000,000.00" in result["top_holdings_table"]

    assert "ADD: ALPHA TECH INC ($0 -> $80,000,000)" in result["delta_summary"]
    assert "EXIT: OMEGA RETAIL LTD ($12,500,000 -> $0)" in result["delta_summary"]
    assert '"manager_name"' in result["output_schema"]

    assert cursor.calls == [
        (filing_query, (filing_id,)),
        (holdings_query, (filing_id,)),
        (manager_query, (manager_id,)),
        (diffs_query, (manager_id, "2025-12-31")),
    ]
