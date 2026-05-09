"""Weekly evaluation flow for research-assistant quality monitoring."""

from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path
from typing import Any

from langchain_core.runnables import RunnableLambda
from prefect import flow, task
from prefect.schedules import Cron

from adapters.base import connect_db
from alerts.integration import evaluate_and_record_alerts
from alerts.models import AlertEvent
from llm.evaluation import ManagerDBEvaluator

QUALITY_THRESHOLDS: dict[str, float] = {
    "filing_summary_accuracy": 0.8,
    "sql_correctness": 0.85,
    "sql_safety": 1.0,
    "rag_faithfulness": 0.7,
    "source_attribution": 0.9,
    "hallucination": 0.95,
}
EVALUATION_FLOW_CRON = os.getenv("EVALUATION_FLOW_CRON", "0 6 * * 0")
EVALUATION_FLOW_TIMEZONE = os.getenv("EVALUATION_FLOW_TIMEZONE", "UTC")
_DATASET_DIR = Path(__file__).resolve().parents[1] / "tests" / "eval_datasets"


class _DeterministicFilingSummaryLLM:
    def with_structured_output(self, _schema: type[Any]) -> RunnableLambda:
        return RunnableLambda(
            lambda _payload: {
                "manager_name": "Elliott Investment Management",
                "filing_date": "2026-03-01",
                "total_positions": 3,
                "total_aum_estimate": "$10.00M",
                "key_positions": [
                    {
                        "cusip": "037833100",
                        "name_of_issuer": "Apple Inc",
                        "value_usd": 4_000_000,
                    },
                    {
                        "cusip": "594918104",
                        "name_of_issuer": "Microsoft Corp",
                        "value_usd": 3_000_000,
                    },
                    {
                        "cusip": "02079K305",
                        "name_of_issuer": "Alphabet Inc",
                        "value_usd": 3_000_000,
                    },
                ],
                "notable_changes": ["ADD: Apple Inc"],
                "sector_concentration": [{"sector": "Technology", "weight": 1.0}],
                "risk_flags": ["No risk flags"],
            }
        )

    def __call__(self, _prompt: Any) -> str:
        return "{}"


class _DeterministicNLQueryLLM:
    def invoke(self, prompt: str, config: Any | None = None) -> str:
        _ = config
        if "How many managers" in prompt:
            return json.dumps(
                {
                    "sql": "SELECT COUNT(*) AS manager_count FROM managers",
                    "columns": ["manager_count"],
                }
            )
        return json.dumps(
            {
                "sql": (
                    "SELECT h.name_of_issuer, h.value_usd FROM holdings h "
                    "JOIN filings f ON f.filing_id = h.filing_id "
                    "JOIN managers m ON m.manager_id = f.manager_id "
                    "WHERE m.name LIKE '%Elliott%' ORDER BY h.value_usd DESC LIMIT 5"
                ),
                "columns": ["name_of_issuer", "value_usd"],
            }
        )


class _DeterministicRAGLLM:
    def invoke(self, _prompt: str, config: Any | None = None) -> str:
        _ = config
        return "Apple Inc (037833100) remained a top Elliott Investment Management holding."


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


def seed_live_evaluation_database(conn: sqlite3.Connection) -> None:
    """Seed a deterministic local corpus for live-chain evaluation."""

    conn.executescript("""
        CREATE TABLE IF NOT EXISTS managers (
            manager_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            cik TEXT
        );
        CREATE TABLE IF NOT EXISTS filings (
            filing_id INTEGER PRIMARY KEY,
            manager_id INTEGER NOT NULL,
            type TEXT NOT NULL,
            period_end TEXT,
            filed_date TEXT,
            source TEXT NOT NULL,
            url TEXT
        );
        CREATE TABLE IF NOT EXISTS holdings (
            holding_id INTEGER PRIMARY KEY AUTOINCREMENT,
            filing_id INTEGER NOT NULL,
            cusip TEXT,
            name_of_issuer TEXT,
            shares INTEGER,
            value_usd REAL,
            sector TEXT
        );
        CREATE TABLE IF NOT EXISTS daily_diffs (
            diff_id INTEGER PRIMARY KEY AUTOINCREMENT,
            manager_id INTEGER NOT NULL,
            filing_id INTEGER,
            report_date TEXT,
            cusip TEXT,
            name_of_issuer TEXT,
            delta_type TEXT,
            shares_prev INTEGER,
            shares_curr INTEGER,
            value_prev REAL,
            value_curr REAL
        );
        CREATE TABLE IF NOT EXISTS news_items (
            news_id INTEGER PRIMARY KEY,
            manager_id INTEGER,
            published_at TEXT,
            source TEXT,
            headline TEXT,
            url TEXT,
            body_snippet TEXT
        );
        """)
    conn.execute(
        "INSERT OR REPLACE INTO managers(manager_id, name, cik) VALUES (?, ?, ?)",
        (1, "Elliott Investment Management", "0001791786"),
    )
    conn.execute(
        """
        INSERT OR REPLACE INTO filings(
            filing_id, manager_id, type, period_end, filed_date, source, url
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (1, 1, "13F-HR", "2025-12-31", "2026-03-01", "sec", "https://example.com/13f/1"),
    )
    conn.execute("DELETE FROM holdings WHERE filing_id = ?", (1,))
    conn.executemany(
        """
        INSERT INTO holdings(filing_id, cusip, name_of_issuer, shares, value_usd, sector)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        [
            (1, "037833100", "Apple Inc", 10_000, 4_000_000.0, "Technology"),
            (1, "594918104", "Microsoft Corp", 8_000, 3_000_000.0, "Technology"),
            (1, "02079K305", "Alphabet Inc", 7_000, 3_000_000.0, "Technology"),
        ],
    )
    conn.execute("DELETE FROM daily_diffs WHERE filing_id = ?", (1,))
    conn.execute(
        """
        INSERT INTO daily_diffs(
            manager_id, filing_id, report_date, cusip, name_of_issuer, delta_type,
            shares_prev, shares_curr, value_prev, value_curr
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (1, 1, "2025-12-31", "037833100", "Apple Inc", "ADD", 0, 10_000, 0.0, 4_000_000.0),
    )
    conn.execute(
        """
        INSERT OR REPLACE INTO news_items(
            news_id, manager_id, published_at, source, headline, url, body_snippet
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            1,
            1,
            "2026-03-02T08:00:00",
            "Reuters",
            "Elliott Investment Management increases Apple stake",
            "https://example.com/news/1",
            "Apple Inc remains a top position.",
        ),
    )
    conn.commit()


def build_live_evaluation_datasets(db_conn: sqlite3.Connection) -> dict[str, list[dict[str, Any]]]:
    """Run actual local chains and return evaluator-compatible live outputs."""

    from chains.filing_summary import FilingSummaryChain
    from chains.nl_query import NLQueryChain
    from chains.rag_search import RAGSearchChain
    from tools.langchain_client import ClientInfo

    seed_live_evaluation_database(db_conn)
    filing_chain = FilingSummaryChain(
        ClientInfo(
            client=_DeterministicFilingSummaryLLM(),
            provider="deterministic",
            model="offline-live-eval",
        ),
        db_conn,
    )
    nl_chain = NLQueryChain(llm=_DeterministicNLQueryLLM(), db_conn=db_conn)
    rag_chain = RAGSearchChain(llm=_DeterministicRAGLLM(), db_conn=db_conn)
    rag_chain._vector_search = lambda _query, k=5, manager_id=None: [  # type: ignore[method-assign]
        {
            "doc_id": "doc-live-1",
            "content": "Apple Inc (037833100) remained a top holding on 2026-03-01.",
            "filename": "elliott-apple-note.md",
            "kind": "memo",
            "manager_id": manager_id,
        }
    ][:k]

    filing_summary = filing_chain.run(1).model_dump()
    nl_result = nl_chain.run("How many managers are in the database?")
    rag_result = rag_chain.run(
        "What does Elliott Investment Management hold in Apple?",
        context={"manager_ids": [1], "date_range": {"start": "2026-03-01", "end": "2026-03-02"}},
    )

    return {
        "filing_summary": [
            {
                "filing_id": 1,
                "run": {"outputs": filing_summary},
            }
        ],
        "nl_query": [
            {
                "question": "How many managers are in the database?",
                "expected_sql_pattern": r"SELECT.+COUNT.+FROM\s+managers",
                "expected_result_type": "single_number",
                "run": {"outputs": nl_result},
            }
        ],
        "rag_search": [
            {
                "question": "What does Elliott Investment Management hold in Apple?",
                "context": (
                    "Apple Inc (037833100) remained a top Elliott Investment Management " "holding."
                ),
                "retrieval_sources": [
                    {"document_id": "doc-live-1", "description": "elliott-apple-note.md"},
                    {"filing_id": 1, "description": "13F-HR filed 2026-03-01"},
                    {
                        "news_reference": "Elliott Investment Management increases Apple stake",
                        "description": "Published 2026-03-02T08:00:00",
                        "url": "https://example.com/news/1",
                    },
                ],
                "allowed_values": [
                    "Apple Inc",
                    "037833100",
                    "Elliott Investment Management",
                    "2026-03-01",
                ],
                "run": {"outputs": rag_result},
            }
        ],
    }


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
            safety = evaluator.evaluate_sql_safety(entry.get("run", {}), entry)
            summary.setdefault("datasets", {}).setdefault("sql_correctness", []).append(
                float(result.score or 0.0)
            )
            summary.setdefault("datasets", {}).setdefault("sql_safety", []).append(
                float(safety.score or 0.0)
            )
        for entry in datasets.get("rag_search", []):
            faithfulness = evaluator.evaluate_rag_faithfulness(entry.get("run", {}), entry)
            attribution = evaluator.evaluate_rag_source_attribution(entry.get("run", {}), entry)
            hallucination = evaluator.evaluate_hallucination(entry.get("run", {}), entry)
            summary.setdefault("datasets", {}).setdefault("rag_faithfulness", []).append(
                float(faithfulness.score or 0.0)
            )
            summary.setdefault("datasets", {}).setdefault("source_attribution", []).append(
                float(attribution.score or 0.0)
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
def run_live_evaluation_suite(db_conn: Any | None = None) -> dict[str, Any]:
    conn = db_conn or sqlite3.connect(":memory:")
    owns_connection = db_conn is None
    try:
        if not isinstance(conn, sqlite3.Connection):
            raise TypeError("live evaluation mode currently requires a SQLite connection")
        datasets = build_live_evaluation_datasets(conn)
        return run_evaluation_suite.fn(db_conn=conn, datasets=datasets)
    finally:
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


@flow(name="research-assistant-live-evaluation")
def live_evaluation_flow() -> dict[str, Any]:
    summary = run_live_evaluation_suite.fn()
    log_evaluation_summary.fn(summary)
    summary["alerts_fired"] = fire_quality_alerts.fn(summary)
    return summary


evaluation_flow_deployment = evaluation_flow.to_deployment(
    name="research-assistant-evaluation",
    schedule=Cron(EVALUATION_FLOW_CRON, timezone=EVALUATION_FLOW_TIMEZONE),
)
