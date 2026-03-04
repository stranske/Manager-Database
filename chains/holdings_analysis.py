"""Holdings analysis chain for cross-manager and cross-period questions."""

from __future__ import annotations

import json
import sqlite3
import time
from datetime import date, datetime
from decimal import Decimal
from typing import Any, cast

from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from pydantic import BaseModel, Field, ValidationError

from chains.filing_summary import langsmith_tracing_context
from chains.utils import (
    format_delta_summary,
    format_holdings_table,
    truncate_context,
)
from scripts.langchain.injection_guard import check_prompt_injection
from tools.langchain_client import ClientInfo
from tools.llm_provider import build_langsmith_metadata


class HoldingsAnalysis(BaseModel):
    """Structured output for holdings analysis."""

    thesis: str = Field(description="Overall investment thesis interpretation")
    top_positions: list[dict] = Field(description="Key positions with context")
    period_changes: list[dict] = Field(description="Notable changes over time")
    cross_manager_overlap: list[dict] | None = Field(
        default=None, description="Other managers holding same securities"
    )
    concentration_metrics: dict = Field(description="HHI, top-10 weight, sector breakdown")


HOLDINGS_ANALYSIS_TEMPLATE = ChatPromptTemplate.from_messages(
    [
        (
            "system",
            """You are a financial analyst assistant. Analyse the holdings data provided
and answer the user's question. Use only the data provided — do not fabricate positions
or values. If the data is insufficient to answer, say so explicitly.
Return JSON matching this schema exactly:
{output_schema}""",
        ),
        (
            "human",
            """{question}

Data context:
{data_context}""",
        ),
    ]
)


class HoldingsAnalysisChain:
    def __init__(self, client_info: ClientInfo, db_conn: Any):
        self.llm: Any = client_info.client
        self.db = db_conn
        self._provider_label = client_info.provider_label
        self.chain = cast(Any, HOLDINGS_ANALYSIS_TEMPLATE) | cast(Any, self.llm) | StrOutputParser()
        self._structured_chain = self._build_structured_chain()

    def _build_structured_chain(self):
        with_structured_output = getattr(self.llm, "with_structured_output", None)
        if not callable(with_structured_output):
            return None
        try:
            return cast(Any, HOLDINGS_ANALYSIS_TEMPLATE) | cast(
                Any, with_structured_output(HoldingsAnalysis)
            )
        except Exception:
            return None

    @staticmethod
    def _is_sqlite_connection(conn: Any) -> bool:
        return isinstance(conn, sqlite3.Connection)

    @staticmethod
    def _placeholder(conn: Any) -> str:
        return "?" if HoldingsAnalysisChain._is_sqlite_connection(conn) else "%s"

    @staticmethod
    def _is_missing_table_error(exc: Exception, table_name: str) -> bool:
        message = str(exc).lower()
        return (
            f"no such table: {table_name}" in message
            or (f'relation "{table_name}"' in message and "does not exist" in message)
            or f"undefined table: {table_name}" in message
            or "sqlstate 42p01" in message
        )

    @staticmethod
    def _is_missing_column_error(exc: Exception) -> bool:
        message = str(exc).lower()
        return (
            "no such column:" in message
            or ("column" in message and "does not exist" in message)
            or "undefined column" in message
            or "sqlstate 42703" in message
        )

    @staticmethod
    def _report_date_expr() -> str:
        return "COALESCE(f.period_end, f.filed_date)"

    @staticmethod
    def _cursor_rows_to_dicts(cursor: Any, rows: list[Any]) -> list[dict[str, Any]]:
        if not rows:
            return []
        if isinstance(rows[0], dict):
            return rows
        if hasattr(rows[0], "keys"):
            return [dict(row) for row in rows]
        columns = [entry[0] for entry in (cursor.description or [])]
        return [dict(zip(columns, row, strict=False)) for row in rows]

    @staticmethod
    def _json_default(value: Any) -> Any:
        if isinstance(value, (date, datetime)):
            return value.isoformat()
        if isinstance(value, Decimal):
            return float(value)
        return str(value)

    @staticmethod
    def _extract_json_text(text: str) -> str | None:
        stripped = text.strip()
        if stripped.startswith("{") and stripped.endswith("}"):
            return stripped

        fence_start = stripped.find("```")
        if fence_start >= 0:
            last_fence = stripped.rfind("```")
            if last_fence > fence_start:
                fenced = stripped[fence_start + 3 : last_fence].strip()
                if "\n" in fenced:
                    first_line, remainder = fenced.split("\n", 1)
                    if first_line.strip().lower() in {"json", "application/json"}:
                        fenced = remainder.strip()
                if fenced.startswith("{") and fenced.endswith("}"):
                    return fenced

        start = stripped.find("{")
        end = stripped.rfind("}")
        if start >= 0 and end > start:
            return stripped[start : end + 1]
        return None

    def _execute_fetchall(self, query: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
        cursor = self.db.cursor()
        cursor.execute(query, params)
        rows = cursor.fetchall()
        return self._cursor_rows_to_dicts(cursor, rows)

    def _build_holdings_query(
        self,
        *,
        manager_ids: list[int] | None,
        cusips: list[str] | None,
        date_range: tuple[date, date] | None,
        use_filings_join: bool = True,
    ) -> tuple[str, tuple[Any, ...]]:
        placeholder = self._placeholder(self.db)
        clauses: list[str] = []
        params: list[Any] = []
        report_date_expr = self._report_date_expr()

        if manager_ids:
            manager_placeholders = ", ".join([placeholder] * len(manager_ids))
            table_prefix = "f." if use_filings_join else ""
            clauses.append(f"{table_prefix}manager_id IN ({manager_placeholders})")
            params.extend(manager_ids)

        if cusips:
            cusip_placeholders = ", ".join([placeholder] * len(cusips))
            table_prefix = "h." if use_filings_join else ""
            clauses.append(f"{table_prefix}cusip IN ({cusip_placeholders})")
            params.extend(cusips)

        if date_range is not None:
            if use_filings_join:
                clauses.append(f"{report_date_expr} BETWEEN {placeholder} AND {placeholder}")
            else:
                clauses.append(f"report_date BETWEEN {placeholder} AND {placeholder}")
            params.extend([date_range[0], date_range[1]])

        where_sql = " AND ".join(clauses) if clauses else "1=1"
        if use_filings_join:
            query = (
                "SELECT h.*, f.manager_id, f.period_end, f.filed_date, "
                f"{report_date_expr} AS report_date "
                "FROM holdings h "
                "JOIN filings f ON f.filing_id = h.filing_id "
                f"WHERE {where_sql} "
                f"ORDER BY {report_date_expr} DESC, h.value_usd DESC LIMIT 200"
            )
        else:
            query = (
                "SELECT * FROM holdings "
                f"WHERE {where_sql} "
                "ORDER BY report_date DESC, value_usd DESC LIMIT 200"
            )
        return query, tuple(params)

    def _build_data_context(
        self,
        *,
        manager_ids: list[int] | None = None,
        cusips: list[str] | None = None,
        date_range: tuple[date, date] | None = None,
    ) -> str:
        """Build prompt context from holdings, diffs, conviction, and overlap tables."""
        holdings_query, holdings_params = self._build_holdings_query(
            manager_ids=manager_ids, cusips=cusips, date_range=date_range
        )
        try:
            holdings = self._execute_fetchall(holdings_query, holdings_params)
        except Exception as exc:
            if self._is_sqlite_connection(self.db) and "no such table: filings" in str(exc).lower():
                fallback_query, fallback_params = self._build_holdings_query(
                    manager_ids=manager_ids,
                    cusips=cusips,
                    date_range=date_range,
                    use_filings_join=False,
                )
                holdings = self._execute_fetchall(fallback_query, fallback_params)
            else:
                raise

        sections: list[str] = ["Holdings:", format_holdings_table(holdings, max_rows=50)]

        placeholder = self._placeholder(self.db)
        where_parts: list[str] = []
        where_params: list[Any] = []
        if manager_ids:
            in_sql = ", ".join([placeholder] * len(manager_ids))
            where_parts.append(f"manager_id IN ({in_sql})")
            where_params.extend(manager_ids)
        if date_range is not None:
            where_parts.append(f"report_date BETWEEN {placeholder} AND {placeholder}")
            where_params.extend([date_range[0], date_range[1]])
        where_sql = " AND ".join(where_parts) if where_parts else "1=1"

        diff_attempts: list[tuple[str, tuple[Any, ...]]] = []
        diff_where_parts = list(where_parts)
        diff_where_params = list(where_params)
        if cusips:
            in_sql = ", ".join([placeholder] * len(cusips))
            diff_where_parts.append(f"cusip IN ({in_sql})")
            diff_where_params.extend(cusips)
        diff_where_sql = " AND ".join(diff_where_parts) if diff_where_parts else "1=1"
        diff_attempts.append((diff_where_sql, tuple(diff_where_params)))

        # Some environments expose daily_diffs without a CUSIP column.
        if cusips:
            relaxed_attempt = (where_sql, tuple(where_params))
            if relaxed_attempt not in diff_attempts:
                diff_attempts.append(relaxed_attempt)

        try:
            diffs: list[dict[str, Any]] = []
            for diff_where, diff_params in diff_attempts:
                diff_query = (
                    "SELECT * FROM daily_diffs "
                    f"WHERE {diff_where} "
                    "ORDER BY report_date DESC, value_curr DESC LIMIT 100"
                )
                try:
                    diffs = self._execute_fetchall(diff_query, diff_params)
                    break
                except Exception as exc:
                    if self._is_missing_column_error(exc):
                        continue
                    raise
            sections.extend(["", "Changes:", format_delta_summary(diffs)])
        except Exception:
            sections.extend(["", "Changes:", "No prior-period changes available."])

        try:
            conviction_query = (
                "SELECT * FROM conviction_scores "
                f"WHERE {where_sql} "
                "ORDER BY report_date DESC, conviction_score DESC LIMIT 50"
            )
            conviction = self._execute_fetchall(conviction_query, tuple(where_params))
            if conviction:
                sections.extend(
                    ["", "Conviction Scores:", json.dumps(conviction[:20], default=str)]
                )
        except Exception as exc:
            if not self._is_missing_table_error(exc, "conviction_scores"):
                raise

        overlap_attempts: list[tuple[str, tuple[Any, ...]]] = []

        overlap_where_parts: list[str] = []
        overlap_params: list[Any] = []
        if manager_ids:
            in_sql = ", ".join([placeholder] * len(manager_ids))
            overlap_where_parts.append(f"manager_id IN ({in_sql})")
            overlap_params.extend(manager_ids)
        if cusips:
            in_sql = ", ".join([placeholder] * len(cusips))
            overlap_where_parts.append(f"cusip IN ({in_sql})")
            overlap_params.extend(cusips)
        if date_range is not None:
            overlap_where_parts.append(f"report_date BETWEEN {placeholder} AND {placeholder}")
            overlap_params.extend([date_range[0], date_range[1]])

        overlap_where = " AND ".join(overlap_where_parts) if overlap_where_parts else "1=1"
        overlap_attempts.append((overlap_where, tuple(overlap_params)))

        # Some environments may expose crowded_trades without manager/date columns.
        if manager_ids or date_range is not None:
            relaxed_where_parts: list[str] = []
            relaxed_params: list[Any] = []
            if cusips:
                in_sql = ", ".join([placeholder] * len(cusips))
                relaxed_where_parts.append(f"cusip IN ({in_sql})")
                relaxed_params.extend(cusips)
            relaxed_where = " AND ".join(relaxed_where_parts) if relaxed_where_parts else "1=1"
            relaxed_attempt = (relaxed_where, tuple(relaxed_params))
            if relaxed_attempt not in overlap_attempts:
                overlap_attempts.append(relaxed_attempt)

        try:
            overlap: list[dict[str, Any]] = []
            for overlap_where, overlap_params_tuple in overlap_attempts:
                overlap_query = (
                    "SELECT * FROM crowded_trades "
                    f"WHERE {overlap_where} "
                    "ORDER BY holder_count DESC, total_value_usd DESC LIMIT 50"
                )
                try:
                    overlap = self._execute_fetchall(overlap_query, overlap_params_tuple)
                    break
                except Exception as exc:
                    if self._is_missing_column_error(exc):
                        continue
                    raise
            if overlap:
                sections.extend(
                    ["", "Cross-Manager Overlap:", json.dumps(overlap[:20], default=str)]
                )
        except Exception:
            pass

        return truncate_context("\n".join(sections), max_tokens=4000)

    def _guard_input(self, question: str) -> None:
        result = check_prompt_injection(question)
        if result["blocked"]:
            reason = result.get("reason") or "prompt injection detected"
            raise ValueError(f"Prompt injection blocked: {reason}")

    def _guard_prompt_inputs(self, payload: dict[str, Any]) -> None:
        guard_targets = [payload.get("question"), payload.get("data_context")]
        for raw in guard_targets:
            result = check_prompt_injection(raw)
            if result["blocked"]:
                reason = result.get("reason") or "prompt injection detected"
                raise ValueError(f"Prompt injection blocked: {reason}")

    def _parse_analysis(self, output_text: str, question: str) -> HoldingsAnalysis:
        decoded_payload: dict[str, Any] = {}
        payload = self._extract_json_text(output_text)
        if payload:
            try:
                return HoldingsAnalysis.model_validate_json(payload)
            except ValidationError:
                try:
                    decoded = json.loads(payload)
                    if isinstance(decoded, dict):
                        decoded_payload = decoded
                except Exception:
                    decoded_payload = {}
            except Exception:
                pass

        thesis = decoded_payload.get("thesis")
        if not isinstance(thesis, str) or not thesis.strip():
            thesis = f"Unable to parse structured response for question: {question}"

        top_positions = decoded_payload.get("top_positions")
        if not isinstance(top_positions, list):
            top_positions = []
        else:
            top_positions = [item for item in top_positions if isinstance(item, dict)]

        period_changes = decoded_payload.get("period_changes")
        if not isinstance(period_changes, list):
            period_changes = []
        else:
            period_changes = [item for item in period_changes if isinstance(item, dict)]

        overlap = decoded_payload.get("cross_manager_overlap")
        if overlap is None:
            cross_manager_overlap = None
        elif isinstance(overlap, list):
            cross_manager_overlap = [item for item in overlap if isinstance(item, dict)]
        else:
            cross_manager_overlap = None

        concentration_metrics = decoded_payload.get("concentration_metrics")
        if not isinstance(concentration_metrics, dict):
            concentration_metrics = {}

        return HoldingsAnalysis(
            thesis=thesis,
            top_positions=top_positions,
            period_changes=period_changes,
            cross_manager_overlap=cross_manager_overlap,
            concentration_metrics=concentration_metrics,
        )

    def _log_usage(self, *, question: str, output_text: str, latency_ms: int, status: int) -> None:
        try:
            self.db.execute("""CREATE TABLE IF NOT EXISTS api_usage (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                source TEXT,
                endpoint TEXT,
                status INT,
                bytes INT,
                latency_ms INT,
                cost_usd REAL
            )""")
        except Exception:
            try:
                self.db.execute("""CREATE TABLE IF NOT EXISTS api_usage (
                    id BIGSERIAL PRIMARY KEY,
                    ts TIMESTAMPTZ DEFAULT now(),
                    source TEXT,
                    endpoint TEXT,
                    status INT,
                    bytes INT,
                    latency_ms INT,
                    cost_usd NUMERIC(10,4)
                )""")
            except Exception:
                return

        placeholder = self._placeholder(self.db)
        insert_sql = (
            "INSERT INTO api_usage(source, endpoint, status, bytes, latency_ms, cost_usd) "
            f"VALUES ({placeholder}, {placeholder}, {placeholder}, {placeholder}, {placeholder}, "
            f"{placeholder})"
        )
        try:
            endpoint = question[:64]
            self.db.execute(
                insert_sql,
                (
                    "holdings_analysis_chain",
                    endpoint,
                    int(status),
                    len(output_text.encode("utf-8")),
                    latency_ms,
                    0.0,
                ),
            )
            self.db.commit()
        except Exception:
            pass

    def run(
        self,
        question: str,
        *,
        manager_ids: list[int] | None = None,
        cusips: list[str] | None = None,
        date_range: tuple[date, date] | None = None,
    ) -> HoldingsAnalysis:
        """Answer holdings analysis questions using database-backed context."""
        self._guard_input(question)
        data_context = self._build_data_context(
            manager_ids=manager_ids, cusips=cusips, date_range=date_range
        )
        payload = {
            "question": question,
            "data_context": data_context,
            "output_schema": json.dumps(
                HoldingsAnalysis.model_json_schema(), default=self._json_default
            ),
        }
        self._guard_prompt_inputs(payload)

        started = time.perf_counter()
        output_text = ""
        parsed_result: HoldingsAnalysis | None = None
        status = 0
        config: Any = build_langsmith_metadata(operation="holdings-analysis")

        with langsmith_tracing_context(name="holdings-analysis", inputs={"question": question}):
            if self._structured_chain is not None:
                try:
                    structured = self._structured_chain.invoke(payload, config=cast(Any, config))
                    parsed_result = HoldingsAnalysis.model_validate(structured)
                    output_text = parsed_result.model_dump_json()
                    status = 1
                except Exception:
                    parsed_result = None

            if parsed_result is None:
                output_text = self.chain.invoke(payload, config=cast(Any, config))
                parsed_result = self._parse_analysis(output_text, question)
                status = 1

        latency_ms = int((time.perf_counter() - started) * 1000)
        self._log_usage(
            question=question, output_text=output_text, latency_ms=latency_ms, status=status
        )
        return parsed_result
