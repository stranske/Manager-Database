"""Weekly evaluation flow for research-assistant quality monitoring."""

from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path
from typing import Any

from prefect import flow, task
from prefect.schedules import Cron

from adapters.base import connect_db
from alerts.integration import evaluate_and_record_alerts
from alerts.models import AlertEvent
from llm.evaluation import ManagerDBEvaluator

QUALITY_THRESHOLDS: dict[str, float] = {
    "filing_summary_accuracy": 0.8,
    "sql_correctness": 0.85,
    "rag_faithfulness": 0.7,
    "hallucination": 0.95,
}
EVALUATION_FLOW_CRON = os.getenv("EVALUATION_FLOW_CRON", "0 6 * * 0")
EVALUATION_FLOW_TIMEZONE = os.getenv("EVALUATION_FLOW_TIMEZONE", "UTC")
_DATASET_DIR = Path(__file__).resolve().parents[1] / "tests" / "eval_datasets"


def _placeholder(conn: Any) -> str:
    return "?" if isinstance(conn, sqlite3.Connection) else "%s"


def _ensure_api_usage_table(conn: Any) -> None:
    if isinstance(conn, sqlite3.Connection):
        conn.execute("""CREATE TABLE IF NOT EXISTS api_usage (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                source TEXT,
                endpoint TEXT,
                status INT,
                bytes INT,
                latency_ms INT,
                cost_usd REAL
            )""")
        return

    conn.execute("""CREATE TABLE IF NOT EXISTS api_usage (
            id bigserial PRIMARY KEY,
            ts timestamptz DEFAULT now(),
            source text,
            endpoint text,
            status int,
            bytes int,
            latency_ms int,
            cost_usd numeric(10,4)
        )""")


@task
def load_evaluation_datasets(dataset_dir: Path = _DATASET_DIR) -> dict[str, list[dict[str, Any]]]:
    return {
        "filing_summary": ManagerDBEvaluator.load_dataset(dataset_dir / "filing_summary_eval.json"),
        "nl_query": ManagerDBEvaluator.load_dataset(dataset_dir / "nl_query_eval.json"),
        "rag_search": ManagerDBEvaluator.load_dataset(dataset_dir / "rag_search_eval.json"),
    }


@task
def run_evaluation_suite(
    db_conn: Any | None = None,
    *,
    evaluator: ManagerDBEvaluator | None = None,
    datasets: dict[str, list[dict[str, Any]]] | None = None,
) -> dict[str, Any]:
    conn = db_conn or connect_db()
    owns_connection = db_conn is None
    datasets = datasets or load_evaluation_datasets.fn()
    owned_evaluator = evaluator is None
    evaluator = evaluator or ManagerDBEvaluator(conn)
    summary: dict[str, Any] = {"datasets": {}}

    try:
        for entry in datasets.get("filing_summary", []):
            result = evaluator.evaluate_filing_summary_accuracy(entry.get("run", {}), entry)
            summary.setdefault("datasets", {}).setdefault("filing_summary_accuracy", []).append(
                float(result.score or 0.0)
            )
        for entry in datasets.get("nl_query", []):
            result = evaluator.evaluate_sql_correctness(entry.get("run", {}), entry)
            summary.setdefault("datasets", {}).setdefault("sql_correctness", []).append(
                float(result.score or 0.0)
            )
        for entry in datasets.get("rag_search", []):
            faithfulness = evaluator.evaluate_rag_faithfulness(entry.get("run", {}), entry)
            hallucination = evaluator.evaluate_hallucination(entry.get("run", {}), entry)
            summary.setdefault("datasets", {}).setdefault("rag_faithfulness", []).append(
                float(faithfulness.score or 0.0)
            )
            summary.setdefault("datasets", {}).setdefault("hallucination", []).append(
                float(hallucination.score or 0.0)
            )

        metrics = {
            key: round(sum(values) / len(values), 3) if values else 0.0
            for key, values in summary["datasets"].items()
        }
        summary["metrics"] = metrics
        summary["failures"] = {
            key: value for key, value in metrics.items() if value < QUALITY_THRESHOLDS.get(key, 0.0)
        }
        return summary
    finally:
        if owned_evaluator:
            evaluator.close()
        if owns_connection:
            conn.close()


@task
def log_evaluation_summary(summary: dict[str, Any], db_conn: Any | None = None) -> None:
    conn = db_conn or connect_db()
    owns_connection = db_conn is None
    _ensure_api_usage_table(conn)
    metrics = json.dumps(summary.get("metrics", {}), sort_keys=True)
    ph = _placeholder(conn)
    conn.execute(
        f"INSERT INTO api_usage(source, endpoint, status, bytes, latency_ms, cost_usd) VALUES ({ph}, {ph}, {ph}, {ph}, {ph}, {ph})",
        (
            "evaluation",
            f"evaluation:{metrics}",
            200 if not summary.get("failures") else 503,
            len(metrics),
            0,
            0.0,
        ),
    )
    if isinstance(conn, sqlite3.Connection):
        conn.commit()
    if owns_connection:
        conn.close()


@task
def fire_quality_alerts(summary: dict[str, Any], db_conn: Any | None = None) -> int:
    failures = dict(summary.get("failures", {}))
    if not failures:
        return 0

    conn = db_conn or connect_db()
    owns_connection = db_conn is None
    try:
        alert_ids = evaluate_and_record_alerts(
            conn,
            AlertEvent(
                event_type="etl_failure",
                payload={
                    "pipeline": "research-assistant-evaluation",
                    "failures": failures,
                    "thresholds": QUALITY_THRESHOLDS,
                },
            ),
        )
        return len(alert_ids)
    finally:
        if owns_connection:
            conn.close()


@flow(name="research-assistant-evaluation")
def evaluation_flow() -> dict[str, Any]:
    datasets = load_evaluation_datasets.fn()
    summary = run_evaluation_suite.fn(datasets=datasets)
    log_evaluation_summary.fn(summary)
    summary["alerts_fired"] = fire_quality_alerts.fn(summary)
    return summary


evaluation_flow_deployment = evaluation_flow.to_deployment(
    name="research-assistant-evaluation",
    schedule=Cron(EVALUATION_FLOW_CRON, timezone=EVALUATION_FLOW_TIMEZONE),
)
