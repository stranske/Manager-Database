from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import pytest
from langchain_core.runnables import RunnableLambda

sys.path.append(str(Path(__file__).resolve().parents[1]))

import chains.filing_summary as filing_summary_module
from chains.filing_summary import FilingSummary, FilingSummaryChain
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

    def execute(self, *_args: Any, **_kwargs: Any) -> None:
        return None

    def commit(self) -> None:
        return None


class _StructuredLLM:
    def __init__(self, *, structured_result: dict[str, Any], fallback_text: str = "{}") -> None:
        self._structured_result = structured_result
        self._fallback_text = fallback_text

    def __call__(self, _prompt: Any) -> str:
        return self._fallback_text

    def with_structured_output(self, _schema: type[FilingSummary]) -> RunnableLambda:
        return RunnableLambda(lambda _payload: self._structured_result)


class _FailingStructuredLLM:
    def __init__(self, fallback_text: str) -> None:
        self._fallback_text = fallback_text

    def __call__(self, _prompt: Any) -> str:
        return self._fallback_text

    def with_structured_output(self, _schema: type[FilingSummary]) -> RunnableLambda:
        return RunnableLambda(lambda _payload: (_ for _ in ()).throw(RuntimeError("boom")))


def _build_queries() -> dict[str, str]:
    return {
        "filing": "SELECT * FROM filings WHERE filing_id = %s",
        "holdings": "SELECT * FROM holdings WHERE filing_id = %s ORDER BY value_usd DESC LIMIT 20",
        "manager": "SELECT name FROM managers WHERE manager_id = %s",
        "diffs": "SELECT * FROM daily_diffs WHERE manager_id = %s AND report_date = %s ORDER BY value_curr DESC",
    }


def _build_db_for_filing(
    *,
    filing_id: int = 1001,
    manager_id: int = 7,
    holdings_count: int = 2,
    include_diffs: bool = True,
    include_filing: bool = True,
) -> tuple[_MockDB, dict[str, str], list[dict[str, Any]]]:
    queries = _build_queries()

    filing_row = {
        "filing_id": filing_id,
        "manager_id": manager_id,
        "period_end": "2025-12-31",
        "filed_date": "2026-02-14",
        "total_positions": holdings_count,
        "total_value_usd": float(sum(10_000_000 + i for i in range(holdings_count))),
    }
    holdings_rows = [
        {
            "name_of_issuer": f"ISSUER-{idx:02d}",
            "cusip": f"{idx:09d}",
            "shares": 100_000 + idx,
            "value_usd": float(10_000_000 + idx),
        }
        for idx in range(holdings_count)
    ]
    diff_rows: list[dict[str, Any]] = []
    if holdings_rows:
        diff_rows = [
            {
                "delta_type": "ADD",
                "name_of_issuer": holdings_rows[0]["name_of_issuer"],
                "value_prev": 0,
                "value_curr": holdings_rows[0]["value_usd"],
            }
        ]

    fetchone_map: dict[tuple[str, tuple[Any, ...]], dict[str, Any] | None] = {
        (queries["manager"], (manager_id,)): {"name": "Alpha Capital"},
    }
    if include_filing:
        fetchone_map[(queries["filing"], (filing_id,))] = filing_row

    fetchall_map: dict[tuple[str, tuple[Any, ...]], list[dict[str, Any]]] = {
        (queries["holdings"], (filing_id,)): holdings_rows,
        (queries["diffs"], (manager_id, "2025-12-31")): diff_rows if include_diffs else [],
    }

    cursor = _MockCursor(fetchone_map=fetchone_map, fetchall_map=fetchall_map)
    return _MockDB(cursor), queries, holdings_rows


def _make_chain(db: _MockDB, llm: Any | None = None) -> FilingSummaryChain:
    llm = llm or RunnableLambda(lambda _payload: "{}")
    client_info = ClientInfo(client=llm, provider="test-provider", model="test-model")
    return FilingSummaryChain(client_info=client_info, db_conn=db)


def test_load_filing_data_with_mock_database_data() -> None:
    db, queries, holdings_rows = _build_db_for_filing()
    chain = _make_chain(db)

    result = chain._load_filing_data(1001)

    assert result["filing_id"] == 1001
    assert result["manager_name"] == "Alpha Capital"
    assert result["filing_date"] == "2026-02-14"
    assert result["period_end"] == "2025-12-31"
    assert result["total_positions"] == 2
    assert result["top_holdings"] == holdings_rows
    assert "rank | issuer | cusip | shares | value_usd" in result["top_holdings_table"]
    assert "ADD: ISSUER-00" in result["delta_summary"]
    assert '"manager_name"' in result["output_schema"]

    assert db.cursor().calls == [
        (queries["filing"], (1001,)),
        (queries["holdings"], (1001,)),
        (queries["manager"], (7,)),
        (queries["diffs"], (7, "2025-12-31")),
    ]


def test_run_uses_structured_output_when_available() -> None:
    db, _, _ = _build_db_for_filing()
    llm = _StructuredLLM(
        structured_result={
            "manager_name": "Alpha Capital",
            "filing_date": "2026-02-14",
            "total_positions": 2,
            "total_aum_estimate": "$20.00M",
            "key_positions": [{"cusip": "000000000", "value_usd": 10_000_000}],
            "notable_changes": ["Added ISSUER-00"],
            "sector_concentration": [{"sector": "Technology", "weight": 0.5}],
            "risk_flags": [],
        }
    )
    chain = _make_chain(db, llm=llm)

    result = chain.run(1001)

    assert isinstance(result, FilingSummary)
    assert result.manager_name == "Alpha Capital"
    assert result.total_positions == 2
    assert result.total_aum_estimate == "$20.00M"


def test_run_falls_back_to_json_parser_when_structured_output_fails() -> None:
    db, _, _ = _build_db_for_filing(holdings_count=20)
    llm = _FailingStructuredLLM(
        fallback_text=json.dumps(
            {
                "manager_name": "Alpha Capital",
                "filing_date": "2026-02-14",
                "total_positions": 20,
                "total_aum_estimate": "$200.00M",
                "key_positions": [{"cusip": "000000000", "value_usd": 10_000_000}],
                "notable_changes": ["Added ISSUER-00"],
                "sector_concentration": [{"sector": "Technology", "weight": 0.5}],
                "risk_flags": ["Concentrated top-10"],
            }
        )
    )
    chain = _make_chain(db, llm=llm)

    result = chain.run(1001)

    assert result.total_positions == 20
    assert result.notable_changes == ["Added ISSUER-00"]
    assert result.risk_flags == ["Concentrated top-10"]


def test_run_with_realish_data_uses_fallback_summary_when_llm_unstructured() -> None:
    db, _, holdings_rows = _build_db_for_filing(holdings_count=20, include_diffs=False)
    llm: RunnableLambda[Any, str] = RunnableLambda(lambda _payload: "This is not JSON")
    chain = _make_chain(db, llm=llm)

    result = chain.run(1001)

    assert result.total_positions == 20
    assert result.manager_name == "Alpha Capital"
    assert len(result.key_positions) == 10
    assert result.key_positions[0]["cusip"] == holdings_rows[0]["cusip"]
    assert result.notable_changes == [
        "Unable to parse structured response; generated fallback summary."
    ]


def test_error_handling_for_missing_filing_and_empty_holdings() -> None:
    missing_db, _, _ = _build_db_for_filing(include_filing=False)
    missing_chain = _make_chain(missing_db)

    with pytest.raises(ValueError, match="Filing 1001 not found"):
        missing_chain.run(1001)

    empty_db, _, _ = _build_db_for_filing(holdings_count=0, include_diffs=False)
    empty_chain = _make_chain(empty_db, llm=RunnableLambda(lambda _payload: "not-json"))
    empty_result = empty_chain.run(1001)

    assert empty_result.total_positions == 0
    assert empty_result.key_positions == []


def test_langsmith_tracing_context_is_entered(monkeypatch: pytest.MonkeyPatch) -> None:
    entered = {"value": False}

    class _TracingContext:
        def __enter__(self) -> dict[str, Any]:
            entered["value"] = True
            return {"name": "filing-summary"}

        def __exit__(self, _exc_type: Any, _exc: Any, _tb: Any) -> None:
            return None

    monkeypatch.setattr(
        filing_summary_module,
        "langsmith_tracing_context",
        lambda **_kwargs: _TracingContext(),
    )

    db, _, _ = _build_db_for_filing()
    chain = _make_chain(db)

    chain.run(1001)

    assert entered["value"] is True
