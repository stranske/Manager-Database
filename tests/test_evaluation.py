from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

import etl.evaluation_flow as evaluation_flow
from etl.evaluation_flow import QUALITY_THRESHOLDS
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
    assert good.key == "source_attribution"
    assert bad.score < 1.0
    assert bad.key == "source_attribution"
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
        "fire_alerts_for_event_sync",
        lambda _conn, _event: [1],
    )

    with closing(sqlite3.connect(":memory:")) as conn:
        summary = evaluation_flow.run_evaluation_suite.fn(
            evaluator=FakeEvaluator(), datasets=datasets, db_conn=conn
        )
        summary["alerts_fired"] = evaluation_flow.fire_quality_alerts.fn(summary, db_conn=conn)

    assert summary["failures"] == {"filing_summary_accuracy": 0.5}
    assert summary["alerts_fired"] == 1


def test_load_evaluation_datasets_maps_each_file_to_its_key(tmp_path):
    fixtures = {
        "filing_summary_eval.json": [{"filing_id": 17}],
        "nl_query_eval.json": [{"question": "How many managers?"}],
        "rag_search_eval.json": [{"context": "Apple holdings"}],
    }
    for filename, entries in fixtures.items():
        (tmp_path / filename).write_text(json.dumps(entries), encoding="utf-8")

    assert evaluation_flow.load_evaluation_datasets.fn(tmp_path) == {
        "filing_summary": [{"filing_id": 17}],
        "nl_query": [{"question": "How many managers?"}],
        "rag_search": [{"context": "Apple holdings"}],
    }


def test_deterministic_llms_handle_fallback_prompts():
    assert evaluation_flow._DeterministicFilingSummaryLLM()("unused") == "{}"
    result = json.loads(evaluation_flow._DeterministicNLQueryLLM().invoke("other question"))
    assert result["columns"] == ["name_of_issuer", "value_usd"]
    assert "WHERE m.name LIKE '%Elliott%'" in result["sql"]


@pytest.mark.parametrize("owned", [False, True], ids=["borrowed", "owned"])
def test_log_evaluation_summary_persists_metrics_and_status(tmp_path, monkeypatch, owned):
    db_path = tmp_path / "usage.db"
    conn = sqlite3.connect(db_path)
    monkeypatch.setattr(evaluation_flow, "connect_db", lambda: conn)
    kwargs = {} if owned else {"db_conn": conn}
    evaluation_flow.log_evaluation_summary.fn(
        {"metrics": {"sql_safety": 1.0}, "failures": {}}, **kwargs
    )
    if owned:
        with pytest.raises(sqlite3.ProgrammingError, match="closed database"):
            conn.execute("SELECT 1")
    else:
        assert conn.execute("SELECT 1").fetchone() == (1,)
        conn.close()

    # Reopen the file so an uncommitted insert cannot satisfy the assertion.
    with closing(sqlite3.connect(db_path)) as reader:
        assert reader.execute(
            "SELECT source, endpoint, status, bytes, latency_ms, cost_usd FROM api_usage"
        ).fetchall() == [("evaluation", 'evaluation:{"sql_safety": 1.0}', 200, 19, 0, 0.0)]
        evaluation_flow.log_evaluation_summary.fn(
            {"metrics": {}, "failures": {"sql_safety": 0.0}}, db_conn=reader
        )
        assert reader.execute("SELECT status FROM api_usage ORDER BY rowid").fetchall() == [
            (200,),
            (503,),
        ]


def test_log_evaluation_summary_closes_owned_non_sqlite_connection(monkeypatch):
    conn = Mock()
    monkeypatch.setattr(evaluation_flow, "connect_db", lambda: conn)
    schema = Mock()
    monkeypatch.setattr(evaluation_flow, "ensure_api_usage_schema", schema)
    monkeypatch.setattr(evaluation_flow, "get_placeholder", lambda _conn: "%s")

    evaluation_flow.log_evaluation_summary.fn({"metrics": {}})

    schema.assert_called_once_with(conn)
    conn.execute.assert_called_once_with(
        "INSERT INTO api_usage(source, endpoint, status, bytes, latency_ms, cost_usd) "
        "VALUES (%s, %s, %s, %s, %s, %s)",
        ("evaluation", "evaluation:{}", 200, 2, 0, 0.0),
    )
    conn.commit.assert_not_called()
    conn.close.assert_called_once_with()


@pytest.mark.parametrize("owned", [False, True], ids=["borrowed", "owned"])
@pytest.mark.parametrize("fails", [False, True], ids=["success", "failure"])
def test_run_evaluation_suite_cleans_up_only_owned_resources(monkeypatch, owned, fails):
    conn = Mock()
    evaluator = Mock()
    evaluator.evaluate_filing_summary_accuracy.return_value = SimpleNamespace(score=0.75)
    error = RuntimeError("evaluation failed")
    if fails:
        evaluator.evaluate_filing_summary_accuracy.side_effect = error
    create_evaluator = Mock(return_value=evaluator)
    monkeypatch.setattr(evaluation_flow, "connect_db", lambda: conn)
    monkeypatch.setattr(evaluation_flow, "ManagerDBEvaluator", create_evaluator)
    datasets = {"filing_summary": [{"filing_id": 17, "run": {"outputs": {}}}]}
    load = Mock(return_value=datasets)
    monkeypatch.setattr(evaluation_flow, "load_evaluation_datasets", SimpleNamespace(fn=load))
    kwargs = {} if owned else {"db_conn": conn, "evaluator": evaluator, "datasets": datasets}

    if fails:
        with pytest.raises(RuntimeError) as caught:
            evaluation_flow.run_evaluation_suite.fn(**kwargs)
        assert caught.value is error
    else:
        actual = evaluation_flow.run_evaluation_suite.fn(**kwargs)
        stamp = evaluation_flow.datetime.fromisoformat(actual["alert_observed_at"])
        assert stamp.utcoffset().total_seconds() == 0
        assert actual == {
            "alert_observed_at": actual["alert_observed_at"],
            "datasets": {"filing_summary_accuracy": [0.75]},
            "metrics": {"filing_summary_accuracy": 0.75},
            "failures": {"filing_summary_accuracy": 0.75},
        }
    evaluator.evaluate_filing_summary_accuracy.assert_called_once_with(
        {"outputs": {}}, datasets["filing_summary"][0]
    )
    assert conn.close.call_count == int(owned)
    assert evaluator.close.call_count == int(owned)
    if owned:
        load.assert_called_once_with()
        create_evaluator.assert_called_once_with(conn)
    else:
        load.assert_not_called()
        create_evaluator.assert_not_called()


def test_run_live_evaluation_rejects_borrowed_non_sqlite_connection():
    conn = Mock()
    with pytest.raises(TypeError, match="requires a SQLite"):
        evaluation_flow.run_live_evaluation_suite.fn(db_conn=conn)
    conn.close.assert_not_called()


@pytest.mark.parametrize("fails", [False, True], ids=["success", "failure"])
def test_run_live_evaluation_closes_owned_connection(monkeypatch, fails):
    connections = []
    connect = sqlite3.connect

    def track_connection(database):
        conn = connect(database)
        connections.append(conn)
        return conn

    monkeypatch.setattr(evaluation_flow.sqlite3, "connect", track_connection)
    if fails:
        monkeypatch.setattr(
            evaluation_flow,
            "build_live_evaluation_datasets",
            Mock(side_effect=RuntimeError("live failed")),
        )
        with pytest.raises(RuntimeError, match="live failed"):
            evaluation_flow.run_live_evaluation_suite.fn()
    else:
        assert evaluation_flow.run_live_evaluation_suite.fn()["failures"] == {}

    assert len(connections) == 1
    with pytest.raises(sqlite3.ProgrammingError, match="closed database"):
        connections[0].execute("SELECT 1")


def test_fire_quality_alerts_does_not_connect_without_failures(monkeypatch):
    connect = Mock()
    record = Mock()
    monkeypatch.setattr(evaluation_flow, "connect_db", connect)
    monkeypatch.setattr(evaluation_flow, "fire_alerts_for_event_sync", record)
    assert evaluation_flow.fire_quality_alerts.fn({"failures": {}}) == 0
    connect.assert_not_called()
    record.assert_not_called()


@pytest.mark.parametrize("owned", [False, True], ids=["borrowed", "owned"])
@pytest.mark.parametrize("fails", [False, True], ids=["success", "failure"])
def test_fire_quality_alerts_records_payload_and_closes_owned_connection(monkeypatch, owned, fails):
    conn = Mock()
    record = Mock(return_value=[11, 12])
    error = RuntimeError("alert delivery failed")
    if fails:
        record.side_effect = error
    monkeypatch.setattr(evaluation_flow, "connect_db", lambda: conn)
    monkeypatch.setattr(evaluation_flow, "fire_alerts_for_event_sync", record)
    kwargs = {} if owned else {"db_conn": conn}
    summary = {"failures": {"sql_safety": 0.0}}
    closes_before = conn.close.call_count

    if fails:
        with pytest.raises(RuntimeError) as caught:
            evaluation_flow.fire_quality_alerts.fn(summary, **kwargs)
        assert caught.value is error
    else:
        assert evaluation_flow.fire_quality_alerts.fn(summary, **kwargs) == 2

    assert conn.close.call_count - closes_before == int(owned)
    record.assert_called_once()
    recorded_conn, event = record.call_args.args
    assert recorded_conn is conn
    assert event.event_type == "etl_failure"
    assert event.payload == {
        "pipeline": "research-assistant-evaluation",
        "failures": {"sql_safety": 0.0},
        "thresholds": QUALITY_THRESHOLDS,
    }


def test_evaluation_flows_orchestrate_their_own_summaries(monkeypatch):
    calls = []
    datasets = {"fixture": [{"filing_id": 17}]}
    batch_summary = {"metrics": {"sql_safety": 0.0}, "failures": {"sql_safety": 0.0}}
    live_summary = {"metrics": {"sql_safety": 1.0}, "failures": {}}

    def load():
        calls.append(("load", None))
        return datasets

    def run_suite(*, datasets):
        calls.append(("run", datasets))
        return batch_summary

    def run_live():
        calls.append(("live-run", None))
        return live_summary

    monkeypatch.setattr(evaluation_flow, "load_evaluation_datasets", SimpleNamespace(fn=load))
    monkeypatch.setattr(evaluation_flow, "run_evaluation_suite", SimpleNamespace(fn=run_suite))
    monkeypatch.setattr(evaluation_flow, "run_live_evaluation_suite", SimpleNamespace(fn=run_live))
    monkeypatch.setattr(
        evaluation_flow,
        "log_evaluation_summary",
        SimpleNamespace(fn=lambda summary: calls.append(("log", summary.copy()))),
    )
    monkeypatch.setattr(
        evaluation_flow,
        "fire_quality_alerts",
        SimpleNamespace(fn=lambda summary: calls.append(("alerts", summary.copy())) or 2),
    )

    assert evaluation_flow.evaluation_flow.fn() is batch_summary
    assert batch_summary["alerts_fired"] == 2
    assert evaluation_flow.live_evaluation_flow.fn() is live_summary
    assert live_summary["alerts_fired"] == 2
    assert calls == [
        ("load", None),
        ("run", datasets),
        ("log", {"metrics": {"sql_safety": 0.0}, "failures": {"sql_safety": 0.0}}),
        ("alerts", {"metrics": {"sql_safety": 0.0}, "failures": {"sql_safety": 0.0}}),
        ("live-run", None),
        ("log", {"metrics": {"sql_safety": 1.0}, "failures": {}}),
        ("alerts", {"metrics": {"sql_safety": 1.0}, "failures": {}}),
    ]


def test_live_evaluation_builds_outputs_from_actual_chains(tmp_path):
    conn = sqlite3.connect(tmp_path / "live-eval.db")

    datasets = evaluation_flow.build_live_evaluation_datasets(conn)

    filing_output = datasets["filing_summary"][0]["run"]["outputs"]
    nl_output = datasets["nl_query"][0]["run"]["outputs"]
    rag_output = datasets["rag_search"][0]["run"]["outputs"]
    assert filing_output["total_positions"] == 3
    assert any(position["cusip"] == "037833100" for position in filing_output["key_positions"])
    assert nl_output["results"] == [{"manager_count": 1}]
    assert "Structured data" not in rag_output["answer"]
    assert any(source.get("document_id") == "doc-live-1" for source in rag_output["sources"])
    assert any(source.get("filing_id") == 1 for source in rag_output["sources"])
    conn.close()


def test_live_evaluation_suite_scores_live_chain_outputs(tmp_path):
    conn = sqlite3.connect(tmp_path / "live-eval.db")

    summary = evaluation_flow.run_live_evaluation_suite.fn(db_conn=conn)

    assert summary["metrics"]["filing_summary_accuracy"] == 1.0
    assert summary["metrics"]["sql_correctness"] == 1.0
    assert summary["metrics"]["sql_safety"] == 1.0
    assert summary["metrics"]["rag_faithfulness"] == 1.0
    assert summary["metrics"]["source_attribution"] == 1.0
    assert summary["metrics"]["hallucination"] == 1.0
    assert summary["failures"] == {}
    conn.close()


@pytest.mark.golden
def test_live_eval_meets_thresholds(tmp_path):
    """Golden gate: every live metric must meet its QUALITY_THRESHOLDS bound.

    Runs the deterministic, credential-free live evaluation suite over the
    in-memory seeded corpus and asserts each metric clears its threshold with
    no recorded failures. This is the assertion the CI ``golden`` job depends
    on: temporarily raising any bound in ``QUALITY_THRESHOLDS`` above what the
    deterministic fixtures score makes this test fail.
    """
    conn = sqlite3.connect(tmp_path / "golden-eval.db")

    summary = evaluation_flow.run_live_evaluation_suite.fn(db_conn=conn)

    metrics = summary["metrics"]
    # Every threshold key must be scored and clear its bound.
    for key, bound in QUALITY_THRESHOLDS.items():
        assert key in metrics, f"metric {key!r} missing from live summary"
        assert metrics[key] >= bound, f"{key}={metrics[key]} below threshold {bound}"
    assert summary["failures"] == {}
    conn.close()


def test_seed_live_evaluation_database_is_deterministic(tmp_path):
    conn = sqlite3.connect(tmp_path / "live-eval.db")

    evaluation_flow.seed_live_evaluation_database(conn)
    first_snapshot = {
        "managers": conn.execute("SELECT manager_id, name, cik FROM managers").fetchall(),
        "filings": conn.execute(
            "SELECT filing_id, manager_id, type, period_end, filed_date FROM filings"
        ).fetchall(),
        "holdings": conn.execute(
            "SELECT filing_id, cusip, name_of_issuer, shares, value_usd FROM holdings ORDER BY holding_id"
        ).fetchall(),
        "daily_diffs": conn.execute(
            "SELECT manager_id, filing_id, cusip, delta_type, shares_curr FROM daily_diffs ORDER BY diff_id"
        ).fetchall(),
        "news_items": conn.execute(
            "SELECT news_id, manager_id, source, headline FROM news_items"
        ).fetchall(),
    }

    evaluation_flow.seed_live_evaluation_database(conn)
    second_snapshot = {
        "managers": conn.execute("SELECT manager_id, name, cik FROM managers").fetchall(),
        "filings": conn.execute(
            "SELECT filing_id, manager_id, type, period_end, filed_date FROM filings"
        ).fetchall(),
        "holdings": conn.execute(
            "SELECT filing_id, cusip, name_of_issuer, shares, value_usd FROM holdings ORDER BY holding_id"
        ).fetchall(),
        "daily_diffs": conn.execute(
            "SELECT manager_id, filing_id, cusip, delta_type, shares_curr FROM daily_diffs ORDER BY diff_id"
        ).fetchall(),
        "news_items": conn.execute(
            "SELECT news_id, manager_id, source, headline FROM news_items"
        ).fetchall(),
    }

    assert first_snapshot == second_snapshot
    assert len(second_snapshot["holdings"]) == 3
    assert len(second_snapshot["daily_diffs"]) == 1
    conn.close()
