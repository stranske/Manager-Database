"""LangSmith-compatible evaluation helpers for research assistant quality checks."""

from __future__ import annotations

import json
import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from langsmith import Client as LangSmithClient
from langsmith.evaluation import EvaluationResult
from langsmith.evaluation import evaluate as langsmith_evaluate

from adapters.base import connect_db
from chains.nl_query import NLQueryChain
from llm.tracing import maybe_enable_langsmith_tracing

_CURRENCY_RE = re.compile(r"-?\$?([0-9][0-9,]*(?:\.[0-9]+)?)([KMB])?", re.I)
_CUSIP_RE = re.compile(r"\b[0-9A-Z]{8,9}\b")
_DATE_RE = re.compile(r"\b20\d{2}-\d{2}-\d{2}\b")
_NAME_RE = re.compile(r"\b[A-Z][a-z]+(?:\s+[A-Z][a-z&.-]+)+\b")
_SQL_JOIN_RE = re.compile(r"\bjoin\b", re.I)


@dataclass(slots=True)
class DatasetEntry:
    run: dict[str, Any]
    example: dict[str, Any]


class ManagerDBEvaluator:
    """Evaluator suite for Manager Database research assistant outputs."""

    def __init__(
        self,
        db_conn: Any | None = None,
        *,
        langsmith_client: Any | None = None,
    ) -> None:
        self.db = db_conn or connect_db()
        self._owns_connection = db_conn is None
        self.ls_client = None
        if langsmith_client is not None:
            self.ls_client = langsmith_client
        elif maybe_enable_langsmith_tracing():
            try:
                self.ls_client = LangSmithClient()
            except Exception:
                self.ls_client = None

    def close(self) -> None:
        if self._owns_connection and self.db is not None:
            self.db.close()
            self.db = None

    def __enter__(self) -> ManagerDBEvaluator:
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        _ = (exc_type, exc, tb)
        self.close()

    def evaluate_filing_summary_accuracy(self, run: Any, example: Any) -> EvaluationResult:
        output = self._run_output(run)
        expected = self._example_payload(example)
        filing_id = int(expected["filing_id"])
        actual_count, actual_aum, holdings = self._filing_facts(filing_id)
        key_positions = self._as_list_of_dicts(output.get("key_positions"))
        summary_count = int(output.get("total_positions") or 0)
        summary_aum = self._extract_currency(output.get("total_aum_estimate"))
        actual_cusips = {str(row[0]) for row in holdings if row[0]}
        actual_position_map = {
            str(row[0]): {"issuer": str(row[1] or ""), "value_usd": float(row[2] or 0.0)}
            for row in holdings
            if row[0]
        }

        checks = {
            "total_positions": summary_count == actual_count,
            "total_aum_estimate": self._pct_delta(summary_aum, actual_aum) <= 0.05,
            "key_positions_real": all(
                str(position.get("cusip") or "") in actual_cusips for position in key_positions
            ),
            "no_fabricated_values": all(
                self._position_matches(position, actual_position_map) for position in key_positions
            ),
        }
        score = sum(1.0 for passed in checks.values() if passed) / len(checks)
        return EvaluationResult(
            key="filing_summary_accuracy",
            score=round(score, 3),
            comment=self._comment_from_checks(checks),
            extra={"filing_id": filing_id, "checks": checks},
        )

    def evaluate_filing_summary_completeness(self, run: Any, example: Any) -> EvaluationResult:
        output = self._run_output(run)
        expected = self._example_payload(example)
        min_positions = int(expected.get("min_positions_mentioned", 5))
        checks = {
            "key_positions": len(self._as_list_of_dicts(output.get("key_positions")))
            >= min_positions,
            "notable_changes": bool(self._as_list(output.get("notable_changes"))),
            "sector_concentration": bool(self._as_list(output.get("sector_concentration"))),
            "risk_flags": "risk" in json.dumps(output).lower()
            or "no risk flags" in json.dumps(output).lower(),
        }
        score = sum(1.0 for passed in checks.values() if passed) / len(checks)
        return EvaluationResult(
            key="filing_summary_completeness",
            score=round(score, 3),
            comment=self._comment_from_checks(checks),
            extra={"checks": checks},
        )

    def evaluate_sql_correctness(self, run: Any, example: Any) -> EvaluationResult:
        output = self._run_output(run)
        expected = self._example_payload(example)
        sql = str(output.get("sql") or "").strip()
        if not sql:
            return EvaluationResult(key="sql_correctness", score=0.0, comment="No SQL generated")

        chain = NLQueryChain(db_conn=self.db)
        valid, reason = chain._validate_sql(sql)
        if not valid:
            return EvaluationResult(key="sql_correctness", score=0.0, comment=reason)

        try:
            rows = chain._execute_query(chain._normalize_sql(sql))
        except Exception as exc:
            return EvaluationResult(
                key="sql_correctness",
                score=0.0,
                comment=f"SQL execution failed: {exc}",
            )

        score = 0.0
        if expected_sql_pattern := expected.get("expected_sql_pattern"):
            if re.search(str(expected_sql_pattern), sql, re.I | re.S):
                score = max(score, 0.5)
        if expected_result := expected.get("expected_result"):
            score = (
                1.0 if self._normalize_rows(rows) == self._normalize_rows(expected_result) else 0.0
            )
        elif expected.get("expected_result_type") == "single_number":
            score = 1.0 if len(rows) == 1 and len(rows[0]) == 1 else score
        elif expected_columns := expected.get("expected_columns"):
            row_columns = set(rows[0].keys()) if rows else set()
            if all(str(column) in row_columns for column in expected_columns):
                score = max(score, 1.0)
        return EvaluationResult(
            key="sql_correctness",
            score=round(score, 3),
            comment=f"Returned {len(rows)} row(s)",
            extra={"sql": sql, "rows": rows[:3]},
        )

    def evaluate_sql_safety(self, run: Any, example: Any) -> EvaluationResult:
        _ = example
        sql = str(self._run_output(run).get("sql") or "").strip()
        chain = NLQueryChain(db_conn=self.db)
        valid, reason = chain._validate_sql(sql)
        checks = {
            "read_only": valid,
            "no_system_catalogs": "information_schema" not in sql.lower()
            and "pg_" not in sql.lower(),
            "join_count_ok": len(_SQL_JOIN_RE.findall(sql)) <= 5,
            "has_limit": bool(re.search(r"\blimit\s+\d+", sql, re.I)),
        }
        score = 1.0 if all(checks.values()) else 0.0
        comment = reason or self._comment_from_checks(checks)
        return EvaluationResult(
            key="sql_safety",
            score=score,
            comment=comment,
            extra={"checks": checks, "sql": sql},
        )

    def evaluate_rag_faithfulness(self, run: Any, example: Any) -> EvaluationResult:
        output = self._run_output(run)
        expected = self._example_payload(example)
        answer = str(output.get("answer") or "")
        context_text = self._context_text(expected, output)
        unsupported = self._unsupported_entities(answer, context_text)
        claims = max(len(self._extract_entities(answer)), 1)
        score = max(0.0, 1.0 - (len(unsupported) / claims))
        return EvaluationResult(
            key="rag_faithfulness",
            score=round(score, 3),
            comment=(
                "All extracted entities grounded in context"
                if not unsupported
                else f"Unsupported entities: {', '.join(unsupported[:5])}"
            ),
            extra={"unsupported": unsupported},
        )

    def evaluate_rag_source_attribution(self, run: Any, example: Any) -> EvaluationResult:
        output = self._run_output(run)
        expected = self._example_payload(example)
        sources = self._as_list_of_dicts(output.get("sources"))
        retrieval_sources = self._as_list_of_dicts(
            expected.get("retrieval_sources") or expected.get("expected_sources")
        )
        valid_source_keys = {self._source_key(source) for source in retrieval_sources}
        cited_source_keys = {self._source_key(source) for source in sources}
        checks = {
            "has_sources": bool(sources),
            "sources_exist": all(
                source_key in valid_source_keys for source_key in cited_source_keys
            ),
            "no_phantom_sources": cited_source_keys.issubset(valid_source_keys),
        }
        score = sum(1.0 for passed in checks.values() if passed) / len(checks)
        return EvaluationResult(
            key="rag_source_attribution",
            score=round(score, 3),
            comment=self._comment_from_checks(checks),
            extra={"checks": checks},
        )

    def evaluate_hallucination(self, run: Any, example: Any) -> EvaluationResult:
        output = self._run_output(run)
        expected = self._example_payload(example)
        answer = str(output.get("answer") or json.dumps(output))
        allowed = {str(value) for value in expected.get("allowed_values", [])}
        allowed.update(self._known_manager_names())
        allowed.update(self._known_cusips())
        unsupported = [entity for entity in self._extract_entities(answer) if entity not in allowed]
        score = (
            1.0 if not unsupported else max(0.0, 1.0 - (len(unsupported) / max(len(allowed), 1)))
        )
        return EvaluationResult(
            key="hallucination",
            score=round(score, 3),
            comment=(
                "No hallucinated entities detected"
                if not unsupported
                else f"Potential hallucinations: {', '.join(unsupported[:5])}"
            ),
            extra={"unsupported": unsupported},
        )

    def run_langsmith_evaluation(
        self,
        dataset_name: str,
        *,
        target: Any,
        evaluators: list[Any],
    ) -> Any | None:
        if self.ls_client is None:
            return None
        try:
            return langsmith_evaluate(
                target, data=dataset_name, evaluators=evaluators, client=self.ls_client
            )
        except Exception:
            return None

    @staticmethod
    def load_dataset(path: str | Path) -> list[dict[str, Any]]:
        raw = Path(path).read_text(encoding="utf-8")
        payload = json.loads(raw)
        if not isinstance(payload, list):
            raise ValueError("Evaluation dataset must be a JSON array")
        return [dict(entry) for entry in payload]

    def _filing_facts(self, filing_id: int) -> tuple[int, float, list[tuple[Any, ...]]]:
        ph = "?" if isinstance(self.db, sqlite3.Connection) else "%s"
        holdings = self.db.execute(
            f"SELECT cusip, name_of_issuer, value_usd FROM holdings WHERE filing_id = {ph}",
            (filing_id,),
        ).fetchall()
        count = len(holdings)
        aum = sum(float(row[2] or 0.0) for row in holdings)
        return count, aum, holdings

    @staticmethod
    def _extract_currency(raw: Any) -> float:
        text = str(raw or "").replace("$", "").replace(",", "")
        match = _CURRENCY_RE.search(text)
        if not match:
            return 0.0
        value = float(match.group(1))
        suffix = (match.group(2) or "").upper()
        multiplier = {"": 1.0, "K": 1_000.0, "M": 1_000_000.0, "B": 1_000_000_000.0}[suffix]
        return value * multiplier

    @staticmethod
    def _pct_delta(summary_value: float, actual_value: float) -> float:
        if actual_value == 0:
            return 0.0 if summary_value == 0 else 1.0
        return abs(summary_value - actual_value) / actual_value

    @staticmethod
    def _position_matches(position: dict[str, Any], actual_map: dict[str, dict[str, Any]]) -> bool:
        cusip = str(position.get("cusip") or "")
        if cusip not in actual_map:
            return False
        actual = actual_map[cusip]
        if (
            position.get("name_of_issuer")
            and str(position.get("name_of_issuer")) != actual["issuer"]
        ):
            return False
        if (
            position.get("value_usd") is not None
            and abs(float(position.get("value_usd") or 0.0) - float(actual["value_usd"])) > 1.0
        ):
            return False
        return True

    @staticmethod
    def _comment_from_checks(checks: dict[str, bool]) -> str:
        failures = [name for name, passed in checks.items() if not passed]
        return "All checks passed" if not failures else f"Failed checks: {', '.join(failures)}"

    @staticmethod
    def _normalize_rows(rows: Any) -> list[dict[str, Any]]:
        if isinstance(rows, list):
            normalized: list[dict[str, Any]] = []
            for row in rows:
                if isinstance(row, dict):
                    normalized.append({key: row[key] for key in sorted(row)})
            return normalized
        return []

    @staticmethod
    def _run_output(run: Any) -> dict[str, Any]:
        if isinstance(run, dict):
            if isinstance(run.get("outputs"), dict):
                return dict(run["outputs"])
            return dict(run)
        outputs = getattr(run, "outputs", None)
        if isinstance(outputs, dict):
            return dict(outputs)
        if hasattr(run, "dict"):
            dumped = run.dict()
            if isinstance(dumped, dict):
                return dumped
        return {"answer": str(run)}

    @staticmethod
    def _example_payload(example: Any) -> dict[str, Any]:
        if isinstance(example, dict):
            return dict(example)
        inputs = getattr(example, "inputs", None)
        if isinstance(inputs, dict):
            return dict(inputs)
        if hasattr(example, "dict"):
            dumped = example.dict()
            if isinstance(dumped, dict):
                return dumped
        return {}

    @staticmethod
    def _as_list(value: Any) -> list[Any]:
        return value if isinstance(value, list) else []

    @staticmethod
    def _as_list_of_dicts(value: Any) -> list[dict[str, Any]]:
        if not isinstance(value, list):
            return []
        return [item for item in value if isinstance(item, dict)]

    @staticmethod
    def _context_text(example: dict[str, Any], output: dict[str, Any]) -> str:
        parts: list[str] = []
        for key in ("context", "retrieved_context", "expected_context"):
            raw = example.get(key)
            if isinstance(raw, str):
                parts.append(raw)
        sources = output.get("sources") or example.get("retrieval_sources") or []
        if isinstance(sources, list):
            parts.extend(json.dumps(source, sort_keys=True) for source in sources)
        return "\n".join(parts)

    @staticmethod
    def _extract_entities(text: str) -> list[str]:
        entities = set(_CUSIP_RE.findall(text))
        entities.update(_DATE_RE.findall(text))
        entities.update(match.group(0) for match in _CURRENCY_RE.finditer(text))
        entities.update(_NAME_RE.findall(text))
        return sorted(entity.strip() for entity in entities if entity.strip())

    def _unsupported_entities(self, answer: str, context: str) -> list[str]:
        context_entities = set(self._extract_entities(context))
        unsupported: list[str] = []
        for entity in self._extract_entities(answer):
            if entity not in context_entities:
                unsupported.append(entity)
        return unsupported

    @staticmethod
    def _source_key(source: dict[str, Any]) -> str:
        for key in (
            "document_id",
            "filing_id",
            "url",
            "filing_url",
            "news_reference",
            "description",
        ):
            value = source.get(key)
            if value:
                return f"{key}:{value}"
        return json.dumps(source, sort_keys=True)

    def _known_manager_names(self) -> set[str]:
        rows = self.db.execute("SELECT name FROM managers").fetchall()
        return {str(row[0]) for row in rows if row and row[0]}

    def _known_cusips(self) -> set[str]:
        try:
            rows = self.db.execute(
                "SELECT DISTINCT cusip FROM holdings WHERE cusip IS NOT NULL"
            ).fetchall()
        except Exception:
            return set()
        return {str(row[0]) for row in rows if row and row[0]}
