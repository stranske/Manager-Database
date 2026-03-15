from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import etl.evaluation_flow as evaluation_flow
from llm.evaluation import ManagerDBEvaluator


def _setup_db(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE managers (manager_id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
    conn.execute(
        "CREATE TABLE holdings (holding_id INTEGER PRIMARY KEY AUTOINCREMENT, filing_id INTEGER, cusip TEXT, name_of_issuer TEXT, value_usd REAL)"
    )
    conn.execute(
        "INSERT INTO managers(manager_id, name) VALUES (1, 'Elliott Investment Management')"
    )
    conn.executemany(
        "INSERT INTO holdings(filing_id, cusip, name_of_issuer, value_usd) VALUES (?, ?, ?, ?)",
        [
            (1, "037833100", "Apple Inc", 4000000),
            (1, "594918104", "Microsoft Corp", 3000000),
            (1, "02079K305", "Alphabet Inc", 3000000),
            (2, "037833100", "Apple Inc", 2500000),
        ],
    )
    conn.commit()
    return conn


def test_evaluate_filing_summary_accuracy_scores_perfect(tmp_path):
    conn = _setup_db(tmp_path / "eval.db")
    evaluator = ManagerDBEvaluator(conn)

    result = evaluator.evaluate_filing_summary_accuracy(
        {
            "outputs": {
                "total_positions": 3,
                "total_aum_estimate": "$10.00M",
                "key_positions": [
                    {"cusip": "037833100", "name_of_issuer": "Apple Inc", "value_usd": 4000000},
                    {
                        "cusip": "594918104",
                        "name_of_issuer": "Microsoft Corp",
                        "value_usd": 3000000,
                    },
                ],
            }
        },
        {"filing_id": 1},
    )

    assert result.score == 1.0
    conn.close()


def test_evaluate_sql_correctness_distinguishes_matching_and_wrong_sql(tmp_path):
    conn = _setup_db(tmp_path / "eval.db")
    evaluator = ManagerDBEvaluator(conn)

    good = evaluator.evaluate_sql_correctness(
        {"outputs": {"sql": "SELECT COUNT(*) AS manager_count FROM managers LIMIT 100"}},
        {
            "expected_sql_pattern": r"SELECT.+COUNT.+FROM\s+managers",
            "expected_result_type": "single_number",
        },
    )
    bad = evaluator.evaluate_sql_correctness(
        {"outputs": {"sql": "SELECT * FROM missing_table LIMIT 100"}},
        {"expected_sql_pattern": r"SELECT.+FROM\s+managers"},
    )

    assert good.score == 1.0
    assert bad.score == 0.0
    conn.close()


def test_evaluate_sql_safety_catches_dangerous_queries(tmp_path):
    conn = _setup_db(tmp_path / "eval.db")
    evaluator = ManagerDBEvaluator(conn)

    safe = evaluator.evaluate_sql_safety(
        {"outputs": {"sql": "SELECT manager_id FROM managers LIMIT 5"}}, {}
    )
    unsafe = evaluator.evaluate_sql_safety({"outputs": {"sql": "DELETE FROM managers"}}, {})

    assert safe.score == 1.0
    assert unsafe.score == 0.0
    conn.close()


def test_evaluate_rag_faithfulness_and_hallucination(tmp_path):
    conn = _setup_db(tmp_path / "eval.db")
    evaluator = ManagerDBEvaluator(conn)
    example = {
        "context": "Apple Inc (037833100) was increased on 2026-01-01.",
        "retrieval_sources": [{"document_id": "doc-1", "description": "Source doc"}],
        "allowed_values": ["Apple Inc", "037833100", "2026-01-01"],
    }

    faithful = evaluator.evaluate_rag_faithfulness(
        {"outputs": {"answer": "Apple Inc (037833100) was increased on 2026-01-01."}},
        example,
    )
    hallucinated = evaluator.evaluate_hallucination(
        {"outputs": {"answer": "Tesla Inc (88160R101) doubled on 2026-02-02."}},
        example,
    )

    assert faithful.score == 1.0
    assert hallucinated.score <= 0.34
    conn.close()


def test_evaluate_rag_source_attribution_rejects_phantom_sources(tmp_path):
    conn = _setup_db(tmp_path / "eval.db")
    evaluator = ManagerDBEvaluator(conn)

    good = evaluator.evaluate_rag_source_attribution(
        {"outputs": {"sources": [{"document_id": "doc-1", "description": "Source doc"}]}},
        {"retrieval_sources": [{"document_id": "doc-1", "description": "Source doc"}]},
    )
    bad = evaluator.evaluate_rag_source_attribution(
        {"outputs": {"sources": [{"document_id": "doc-x", "description": "Phantom"}]}},
        {"retrieval_sources": [{"document_id": "doc-1", "description": "Source doc"}]},
    )

    assert good.score == 1.0
    assert bad.score < 1.0
    conn.close()


def test_load_dataset_reads_json_array(tmp_path):
    path = tmp_path / "dataset.json"
    path.write_text(json.dumps([{"question": "q"}]), encoding="utf-8")

    loaded = ManagerDBEvaluator.load_dataset(path)

    assert loaded == [{"question": "q"}]


def test_evaluation_flow_reports_failures_and_alerts(monkeypatch):
    datasets = {
        "filing_summary": [{"filing_id": 1, "run": {"outputs": {"total_positions": 0}}}],
        "nl_query": [],
        "rag_search": [],
    }

    class FakeEvaluator:
        def evaluate_filing_summary_accuracy(self, _run, _example):
            return type("Result", (), {"score": 0.5})()

        def close(self) -> None:
            return None

    monkeypatch.setattr(evaluation_flow, "connect_db", lambda: sqlite3.connect(":memory:"))
    monkeypatch.setattr(
        evaluation_flow,
        "evaluate_and_record_alerts",
        lambda _conn, _event: [1],
    )

    summary = evaluation_flow.run_evaluation_suite.fn(
        evaluator=FakeEvaluator(), datasets=datasets, db_conn=sqlite3.connect(":memory:")
    )
    summary["alerts_fired"] = evaluation_flow.fire_quality_alerts.fn(
        summary, db_conn=sqlite3.connect(":memory:")
    )

    assert summary["failures"] == {"filing_summary_accuracy": 0.5}
    assert summary["alerts_fired"] == 1
