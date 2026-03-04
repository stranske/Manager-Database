"""Filing summary chain for 13F analysis."""

from __future__ import annotations

import json
import os
import sqlite3
import time
from contextlib import contextmanager
from datetime import date, datetime
from decimal import Decimal
from typing import Any, cast

from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from pydantic import BaseModel, Field, ValidationError

from scripts.langchain.injection_guard import check_prompt_injection
from tools.langchain_client import ClientInfo
from tools.llm_provider import build_langsmith_metadata


class FilingSummary(BaseModel):
    """Structured output for filing summaries."""

    manager_name: str
    filing_date: str
    total_positions: int
    total_aum_estimate: str = Field(description="Estimated AUM in human-readable format")
    key_positions: list[dict] = Field(description="Top-10 positions by value")
    notable_changes: list[str] = Field(description="Significant adds/exits/changes")
    sector_concentration: list[dict] = Field(description="Sector breakdown")
    risk_flags: list[str] = Field(description="QC warnings, e.g. large cash position")


FILING_SUMMARY_TEMPLATE = ChatPromptTemplate.from_messages(
    [
        (
            "system",
            """You are a financial analyst assistant specializing in 13F filing analysis.
Summarise the following 13F filing data for {manager_name}.
Focus on: key positions, notable changes from prior period, sector concentration, and risk flags.
Be precise with numbers. Do not speculate beyond the data provided.
Return JSON that matches this schema exactly:
{output_schema}""",
        ),
        (
            "human",
            """Filing date: {filing_date}
Period: {period_end}
Total positions: {total_positions}
Total estimated value: ${total_value_usd:,.2f}

Top 20 holdings by value:
{top_holdings_table}

Changes from prior filing:
{delta_summary}

Please provide a comprehensive summary.""",
        ),
    ]
)


@contextmanager
def langsmith_tracing_context(name: str, inputs: dict[str, Any] | None = None):
    """Best-effort LangSmith tracing context manager."""

    tracing_enabled = os.environ.get("LANGCHAIN_TRACING_V2", "").lower() == "true"
    has_api_key = bool(os.environ.get("LANGSMITH_API_KEY") or os.environ.get("LANGCHAIN_API_KEY"))
    if not (tracing_enabled and has_api_key):
        yield {"name": name, "inputs": inputs or {}}
        return

    try:
        from langsmith import tracing_context

        with tracing_context(enabled=True):
            yield {"name": name, "inputs": inputs or {}}
            return
    except Exception:
        pass

    yield {"name": name, "inputs": inputs or {}}


class FilingSummaryChain:
    def __init__(self, client_info: ClientInfo, db_conn: Any):
        self.llm: Any = client_info.client
        self.db = db_conn
        self._provider_label = client_info.provider_label
        self.chain = cast(Any, FILING_SUMMARY_TEMPLATE) | cast(Any, self.llm) | StrOutputParser()
        self._structured_chain = self._build_structured_chain()

    def _build_structured_chain(self):
        with_structured_output = getattr(self.llm, "with_structured_output", None)
        if not callable(with_structured_output):
            return None
        try:
            return cast(Any, FILING_SUMMARY_TEMPLATE) | cast(
                Any, with_structured_output(FilingSummary)
            )
        except Exception:
            return None

    @staticmethod
    def _is_sqlite_connection(conn: Any) -> bool:
        return isinstance(conn, sqlite3.Connection)

    @staticmethod
    def _placeholder(conn: Any) -> str:
        return "?" if FilingSummaryChain._is_sqlite_connection(conn) else "%s"

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
    def _format_currency_human(value: float) -> str:
        if value >= 1_000_000_000:
            return f"${value / 1_000_000_000:.2f}B"
        if value >= 1_000_000:
            return f"${value / 1_000_000:.2f}M"
        if value >= 1_000:
            return f"${value / 1_000:.2f}K"
        return f"${value:,.2f}"

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

    @staticmethod
    def _coerce_str_list(raw: Any) -> list[str]:
        if not isinstance(raw, list):
            return []
        return [str(item) for item in raw if item is not None]

    @staticmethod
    def _coerce_dict_list(raw: Any) -> list[dict[str, Any]]:
        if not isinstance(raw, list):
            return []
        result: list[dict[str, Any]] = []
        for item in raw:
            if isinstance(item, dict):
                result.append(item)
        return result

    @staticmethod
    def _format_holdings_table(holdings: list[dict[str, Any]]) -> str:
        if not holdings:
            return "(no holdings found)"

        header = "rank | issuer | cusip | shares | value_usd"
        divider = "-----|--------|-------|--------|----------"
        lines = [header, divider]
        for idx, item in enumerate(holdings[:20], start=1):
            issuer = str(item.get("name_of_issuer") or "").strip() or "UNKNOWN"
            cusip = str(item.get("cusip") or "").strip() or "N/A"
            shares = int(item.get("shares") or 0)
            value = float(item.get("value_usd") or 0)
            lines.append(f"{idx} | {issuer} | {cusip} | {shares:,} | {value:,.2f}")
        return "\n".join(lines)

    @staticmethod
    def _format_delta_summary(diffs: list[dict[str, Any]]) -> str:
        if not diffs:
            return "No prior-period changes available."

        buckets: dict[str, list[str]] = {
            "ADD": [],
            "EXIT": [],
            "INCREASE": [],
            "DECREASE": [],
            "OTHER": [],
        }

        for diff in diffs:
            delta_type = str(diff.get("delta_type") or "OTHER").upper()
            key = delta_type if delta_type in buckets else "OTHER"
            issuer = str(diff.get("name_of_issuer") or diff.get("cusip") or "UNKNOWN")
            prev_value = float(diff.get("value_prev") or 0)
            curr_value = float(diff.get("value_curr") or 0)
            buckets[key].append(f"{issuer} (${prev_value:,.0f} -> ${curr_value:,.0f})")

        lines: list[str] = []
        for key in ("ADD", "EXIT", "INCREASE", "DECREASE", "OTHER"):
            entries = buckets[key]
            if not entries:
                continue
            sample = "; ".join(entries[:10])
            suffix = "" if len(entries) <= 10 else f"; ... (+{len(entries) - 10} more)"
            lines.append(f"{key}: {sample}{suffix}")

        return "\n".join(lines) if lines else "No prior-period changes available."

    def _execute_fetchall(self, query: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
        cursor = self.db.cursor()
        cursor.execute(query, params)
        rows = cursor.fetchall()
        return self._cursor_rows_to_dicts(cursor, rows)

    def _execute_fetchone(self, query: str, params: tuple[Any, ...]) -> dict[str, Any] | None:
        cursor = self.db.cursor()
        cursor.execute(query, params)
        row = cursor.fetchone()
        if row is None:
            return None
        return self._cursor_rows_to_dicts(cursor, [row])[0]

    def _build_fallback_delta_summary(
        self, manager_id: int | None, period_end: Any, filing_id: int
    ) -> list[dict[str, Any]]:
        placeholder = self._placeholder(self.db)
        if manager_id is not None and period_end is not None:
            query = (
                "SELECT * FROM daily_diffs "
                f"WHERE manager_id = {placeholder} AND report_date = {placeholder} "
                "ORDER BY value_curr DESC"
            )
            try:
                diffs = self._execute_fetchall(query, (manager_id, period_end))
                if diffs:
                    return diffs
            except Exception:
                pass

        query = (
            f"SELECT * FROM daily_diffs WHERE filing_id = {placeholder} ORDER BY value_curr DESC"
        )
        try:
            return self._execute_fetchall(query, (filing_id,))
        except Exception:
            return []

    def _load_filing_data(self, filing_id: int) -> dict[str, Any]:
        """Load filing + holdings + deltas from database for prompt variables."""

        placeholder = self._placeholder(self.db)
        filing_query = f"SELECT * FROM filings WHERE filing_id = {placeholder}"
        filing = self._execute_fetchone(filing_query, (filing_id,))
        if not filing:
            raise ValueError(f"Filing {filing_id} not found")

        holdings_query = (
            "SELECT * FROM holdings "
            f"WHERE filing_id = {placeholder} "
            "ORDER BY value_usd DESC LIMIT 20"
        )
        top_holdings = self._execute_fetchall(holdings_query, (filing_id,))

        manager_id = filing.get("manager_id")
        manager_name = "Unknown Manager"
        if manager_id is not None:
            manager_query = f"SELECT name FROM managers WHERE manager_id = {placeholder}"
            try:
                manager_row = self._execute_fetchone(manager_query, (manager_id,))
                if manager_row and manager_row.get("name"):
                    manager_name = str(manager_row["name"])
            except Exception:
                manager_name = "Unknown Manager"

        period_end = filing.get("period_end")
        filing_date = filing.get("filed_date") or filing.get("filing_date") or period_end

        diffs = self._build_fallback_delta_summary(
            manager_id=manager_id, period_end=period_end, filing_id=filing_id
        )

        total_positions = int(filing.get("total_positions") or len(top_holdings))
        total_value_usd = filing.get("total_value_usd")
        if total_value_usd is None:
            total_value_usd = sum(float(item.get("value_usd") or 0) for item in top_holdings)

        top_holdings_table = self._format_holdings_table(top_holdings)
        delta_summary = self._format_delta_summary(diffs)

        return {
            "filing_id": filing_id,
            "manager_name": manager_name,
            "filing_date": str(filing_date or "unknown"),
            "period_end": str(period_end or "unknown"),
            "total_positions": total_positions,
            "total_value_usd": float(total_value_usd or 0),
            "top_holdings": top_holdings,
            "diffs": diffs,
            "top_holdings_table": top_holdings_table,
            "delta_summary": delta_summary,
            "output_schema": json.dumps(
                FilingSummary.model_json_schema(), default=self._json_default
            ),
        }

    def _guard_prompt_inputs(self, payload: dict[str, Any]) -> None:
        guard_targets = [
            payload.get("manager_name"),
            payload.get("filing_date"),
            payload.get("period_end"),
            payload.get("top_holdings_table"),
            payload.get("delta_summary"),
        ]
        for raw in guard_targets:
            result = check_prompt_injection(raw)
            if result["blocked"]:
                reason = result.get("reason") or "prompt injection detected"
                raise ValueError(f"Prompt injection blocked: {reason}")

    def _parse_summary_from_text(self, text: str, fallback_data: dict[str, Any]) -> FilingSummary:
        decoded_payload: dict[str, Any] = {}
        payload = self._extract_json_text(text)
        if payload:
            try:
                return FilingSummary.model_validate_json(payload)
            except ValidationError:
                pass
            except Exception:
                pass
            try:
                decoded = json.loads(payload)
                if isinstance(decoded, dict):
                    decoded_payload = decoded
            except Exception:
                decoded_payload = {}

        diffs = list(fallback_data.get("diffs") or [])
        notable_changes = []
        for diff in diffs[:5]:
            delta_type = str(diff.get("delta_type") or "CHANGE").upper()
            issuer = str(diff.get("name_of_issuer") or diff.get("cusip") or "UNKNOWN")
            notable_changes.append(delta_type + ": " + issuer)
        if not notable_changes:
            notable_changes = ["Unable to parse structured response; generated fallback summary."]

        top_positions = [
            {
                "name_of_issuer": item.get("name_of_issuer"),
                "cusip": item.get("cusip"),
                "shares": item.get("shares"),
                "value_usd": float(item.get("value_usd") or 0),
            }
            for item in fallback_data.get("top_holdings", [])[:10]
        ]
        parsed_key_positions = self._coerce_dict_list(decoded_payload.get("key_positions"))
        if parsed_key_positions:
            top_positions = parsed_key_positions

        sector_totals: dict[str, float] = {}
        for item in fallback_data.get("top_holdings", []):
            sector = str(item.get("sector") or "Unknown")
            value = float(item.get("value_usd") or 0)
            sector_totals[sector] = sector_totals.get(sector, 0.0) + value

        total_value = float(fallback_data.get("total_value_usd") or 0)
        top_10_value = sum(position["value_usd"] for position in top_positions[:10])
        top_1_weight = (
            (top_positions[0]["value_usd"] / total_value) if top_positions and total_value else 0
        )
        top_10_weight = (top_10_value / total_value) if total_value else 0
        sector_concentration = [
            {
                "sector": sector,
                "value_usd": value,
                "weight": (value / total_value) if total_value else 0.0,
            }
            for sector, value in sorted(
                sector_totals.items(), key=lambda entry: entry[1], reverse=True
            )
        ][:10]
        parsed_sector_concentration = self._coerce_dict_list(
            decoded_payload.get("sector_concentration")
        )
        if parsed_sector_concentration:
            sector_concentration = parsed_sector_concentration

        risk_flags: list[str] = []
        if top_1_weight >= 0.5:
            risk_flags.append("Top position exceeds 50% of reported value.")
        if top_10_weight >= 0.8 and len(top_positions) >= 5:
            risk_flags.append("Top-10 positions exceed 80% of reported value.")
        risk_flags.append("LLM response parsing fallback used.")
        parsed_risk_flags = self._coerce_str_list(decoded_payload.get("risk_flags"))
        if parsed_risk_flags:
            risk_flags = parsed_risk_flags

        parsed_notable_changes = self._coerce_str_list(decoded_payload.get("notable_changes"))
        if parsed_notable_changes:
            notable_changes = parsed_notable_changes

        total_positions = fallback_data.get("total_positions") or 0
        parsed_total_positions = decoded_payload.get("total_positions")
        if isinstance(parsed_total_positions, int):
            total_positions = parsed_total_positions

        total_aum_estimate = self._format_currency_human(total_value)
        parsed_total_aum_estimate = decoded_payload.get("total_aum_estimate")
        if isinstance(parsed_total_aum_estimate, str) and parsed_total_aum_estimate.strip():
            total_aum_estimate = parsed_total_aum_estimate

        manager_name = str(
            decoded_payload.get("manager_name")
            or fallback_data.get("manager_name")
            or "Unknown Manager"
        )
        filing_date = str(
            decoded_payload.get("filing_date") or fallback_data.get("filing_date") or "unknown"
        )
        return FilingSummary(
            manager_name=manager_name,
            filing_date=filing_date,
            total_positions=int(total_positions),
            total_aum_estimate=total_aum_estimate,
            key_positions=top_positions,
            notable_changes=notable_changes,
            sector_concentration=sector_concentration,
            risk_flags=risk_flags,
        )

    def _log_usage(self, *, filing_id: int, output_text: str, latency_ms: int, status: int) -> None:
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
            self.db.execute(
                insert_sql,
                (
                    "filing_summary_chain",
                    f"filing_id:{filing_id}",
                    int(status),
                    len(output_text.encode("utf-8")),
                    latency_ms,
                    0.0,
                ),
            )
            self.db.commit()
        except Exception:
            # Logging must not fail the chain.
            pass

    def run(self, filing_id: int) -> FilingSummary:
        """Generate a summary for a filing."""

        template_vars = self._load_filing_data(filing_id)
        self._guard_prompt_inputs(template_vars)

        started = time.perf_counter()
        parsed_result: FilingSummary | None = None
        output_text = ""
        status = 0
        config: Any = build_langsmith_metadata(operation="filing-summary")

        with langsmith_tracing_context(name="filing-summary", inputs={"filing_id": filing_id}):
            if self._structured_chain is not None:
                try:
                    structured = self._structured_chain.invoke(
                        template_vars, config=cast(Any, config)
                    )
                    parsed_result = FilingSummary.model_validate(structured)
                    output_text = parsed_result.model_dump_json()
                    status = 1
                except Exception:
                    parsed_result = None

            if parsed_result is None:
                output_text = self.chain.invoke(template_vars, config=cast(Any, config))
                parsed_result = self._parse_summary_from_text(output_text, template_vars)
                status = 1

        latency_ms = int((time.perf_counter() - started) * 1000)
        self._log_usage(
            filing_id=filing_id, output_text=output_text, latency_ms=latency_ms, status=status
        )
        return parsed_result

    def run_batch(self, filing_ids: list[int]) -> list[FilingSummary]:
        """Summarise multiple filings."""

        return [self.run(filing_id) for filing_id in filing_ids]
