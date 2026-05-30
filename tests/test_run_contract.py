"""Acceptance tests for the uniform tool ``RunResult`` envelope (issue #1087)."""

import sqlite3
import sys
from dataclasses import fields as dataclass_fields
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from diff_holdings import diff_holdings
from etl.daily_diff_flow import daily_diff_flow
from llm.langsmith_fleet import ChatFleetContext
from tools.run_contract import RunCost, RunResult, new_run_id


def _seed_canonical_db(conn: sqlite3.Connection) -> None:
    """Seed the canonical managers/filings/holdings schema with two filings."""
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
        [(101, 1, "13F-HR", "2024-04-01", "edgar"), (102, 1, "13F-HR", "2024-01-01", "edgar")],
    )
    conn.executemany(
        "INSERT INTO holdings(filing_id, cusip, name_of_issuer, shares, value_usd) VALUES (?,?,?,?,?)",
        [
            (101, "AAA", "CorpA", 120, 1200),  # INCREASE
            (101, "CCC", "CorpC", 40, 400),  # ADD
            (101, "EEE", "CorpE", 8, 80),  # DECREASE
            (102, "AAA", "CorpA", 100, 1000),
            (102, "BBB", "CorpB", 30, 300),  # EXIT
            (102, "EEE", "CorpE", 10, 100),
        ],
    )
    conn.commit()


def test_diff_holdings_returns_runresult():
    """``diff_holdings`` returns a populated, replayable ``RunResult``."""
    conn = sqlite3.connect(":memory:")
    _seed_canonical_db(conn)

    result = diff_holdings(1, conn)
    conn.close()

    assert isinstance(result, RunResult)
    assert result.tool == "diff_holdings"
    assert result.run_id  # non-empty correlation id
    assert result.inputs == {"manager_id": 1}
    # ``.deltas`` is the back-compat accessor for the historical list return.
    assert result.deltas == [
        {
            "cusip": "AAA",
            "name_of_issuer": "CorpA",
            "delta_type": "INCREASE",
            "shares_prev": 100,
            "shares_curr": 120,
            "value_prev": 1000,
            "value_curr": 1200,
        },
        {
            "cusip": "BBB",
            "name_of_issuer": "CorpB",
            "delta_type": "EXIT",
            "shares_prev": 30,
            "shares_curr": None,
            "value_prev": 300,
            "value_curr": None,
        },
        {
            "cusip": "CCC",
            "name_of_issuer": "CorpC",
            "delta_type": "ADD",
            "shares_prev": None,
            "shares_curr": 40,
            "value_prev": None,
            "value_curr": 400,
        },
        {
            "cusip": "EEE",
            "name_of_issuer": "CorpE",
            "delta_type": "DECREASE",
            "shares_prev": 10,
            "shares_curr": 8,
            "value_prev": 100,
            "value_curr": 80,
        },
    ]
    assert result.deltas is result.outputs


def test_runresult_json_roundtrip():
    """A populated ``RunResult`` survives a JSON serialize/reconstruct cycle (replay)."""
    original = RunResult(
        run_id=new_run_id(),
        tool="diff_holdings",
        requested_by="analyst@example.test",
        reason="quarterly review",
        inputs={"manager_id": 7},
        outputs=[{"cusip": "AAA", "delta_type": "ADD"}],
        artifacts=["s3://bucket/run.json"],
        warnings=["manager 9 skipped (< 2 filings)"],
        cost=RunCost(usd=0.42, tokens=128),
        latency_ms=12,
        provenance={"manager_id": 7},
        status="success",
    )

    rebuilt = RunResult.model_validate_json(original.model_dump_json())

    assert rebuilt == original
    assert rebuilt.deltas == original.outputs


@pytest.mark.asyncio
async def test_cost_not_constant_zero(tmp_path):
    """``tracked_call`` records a real cost when a per-call rate is configured.

    Fails against the previous implementation, which hard-coded ``cost_usd=0.0``.
    """
    from adapters.base import tracked_call

    db_path = tmp_path / "usage.db"

    class DummyResp:
        status_code = 200
        content = b"abcdefghij"  # 10 bytes

    # Configured per-call rate: $0.001 per response byte.
    async with tracked_call(
        "paid-source",
        "http://x",
        db_path=str(db_path),
        cost_usd=lambda resp: len(resp.content) * 0.001,
    ) as log:
        log(DummyResp())

    conn = sqlite3.connect(db_path)
    cost = conn.execute("SELECT cost_usd FROM api_usage").fetchone()[0]
    conn.close()

    assert cost > 0.0


def test_daily_diff_flow_returns_runresult(tmp_path, monkeypatch):
    """``daily_diff_flow`` returns a ``RunResult`` whose total_changes matches the rows written."""
    db_path = tmp_path / "dev.db"
    conn = sqlite3.connect(db_path)
    _seed_canonical_db(conn)
    conn.close()

    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.delenv("DB_URL", raising=False)

    result = daily_diff_flow.fn(date="2024-05-01")

    assert isinstance(result, RunResult)
    assert result.tool == "daily_diff_flow"

    conn = sqlite3.connect(db_path)
    written = conn.execute("SELECT COUNT(*) FROM daily_diffs").fetchone()[0]
    conn.close()

    assert result.outputs["total_changes"] == written
    assert result.outputs["managers_processed"] == 1


def test_run_id_field_name_matches_chat_context():
    """Tool runs reuse the ``run_id`` field name standardized by the chat surface."""
    chat_fields = {f.name for f in dataclass_fields(ChatFleetContext)}
    assert "run_id" in chat_fields
    assert "run_id" in RunResult.model_fields
