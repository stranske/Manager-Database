"""Natural-language-to-SQL chain for analyst questions."""

from __future__ import annotations

import json
import re
import sqlite3
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

from adapters.base import connect_db
from llm.injection import guard_input
from tools.llm_provider import (
    build_langsmith_metadata,
    derive_langsmith_trace_url,
    extract_trace_id,
)

NL_QUERY_SYSTEM_PROMPT = """You are a SQL query generator for a financial manager database.
Given a natural language question, generate a PostgreSQL SELECT query.

IMPORTANT RULES:
1. Generate ONLY SELECT queries — never INSERT, UPDATE, DELETE, DROP, ALTER, or TRUNCATE.
2. Use only tables and columns from the schema below.
3. Always include appropriate WHERE clauses to limit results.
4. Add LIMIT clause (max 100 rows unless user specifies otherwise).
5. Use proper JOIN syntax for related tables.
6. Return the SQL query only, no explanation unless JSON output is explicitly requested.

Database schema:
{schema_ddl}
"""

_COMMENT_BLOCK_RE = re.compile(r"/\*.*?\*/", re.S)
_COMMENT_LINE_RE = re.compile(r"--.*?$", re.M)
_TABLE_REF_RE = re.compile(r'\b(?:from|join)\s+"?([a-zA-Z_][\w]*)"?', re.I)
_LIMIT_RE = re.compile(r"\blimit\s+\d+\b", re.I)
_DANGEROUS_KEYWORDS = {
    "insert",
    "update",
    "delete",
    "drop",
    "alter",
    "truncate",
    "execute",
    "copy",
    "grant",
    "revoke",
    "create",
}


class NLQueryResult(BaseModel):
    """Structured output for the NL query chain."""

    sql: str = Field(description="The generated SELECT SQL query")
    explanation: str = Field(default="", description="Brief explanation of the generated query")
    columns: list[str] = Field(default_factory=list, description="Expected result columns")


class NLQueryChain:
    """Translate a research question into a safe read-only query and answer."""

    def __init__(self, llm: Any | None = None, db_conn: Any | None = None) -> None:
        self.llm = llm
        self.db = db_conn
        self._schema_ddl = self._load_schema_ddl()
        self._known_tables = self._extract_known_tables(self._schema_ddl)

    def _load_schema_ddl(self) -> str:
        """Load public CREATE TABLE statements and omit internal bookkeeping tables."""
        schema_path = Path(__file__).resolve().parents[1] / "schema.sql"
        ddl_chunks: list[str] = []
        active_block: list[str] = []
        skip_block = False
        for raw_line in schema_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("--"):
                continue
            upper = line.upper()
            if upper.startswith("CREATE TABLE"):
                skip_block = "API_USAGE" in upper
                active_block = [] if skip_block else [line]
                continue
            if active_block:
                active_block.append(line)
                if line.endswith(");"):
                    ddl_chunks.append("\n".join(active_block))
                    active_block = []
                    skip_block = False
            elif not skip_block and upper.startswith("CREATE INDEX"):
                ddl_chunks.append(line)
        return "\n\n".join(ddl_chunks)

    def _extract_known_tables(self, schema_ddl: str) -> set[str]:
        return {
            match.group(1)
            for match in re.finditer(
                r"CREATE TABLE(?: IF NOT EXISTS)?\s+([a-zA-Z_][\w]*)", schema_ddl, re.I
            )
        }

    def _normalize_sql(self, sql: str) -> str:
        cleaned = _COMMENT_BLOCK_RE.sub(" ", sql)
        cleaned = _COMMENT_LINE_RE.sub(" ", cleaned)
        cleaned = cleaned.strip().rstrip(";").strip()
        if not _LIMIT_RE.search(cleaned):
            cleaned = f"{cleaned} LIMIT 100"
        return cleaned

    def _validate_sql(self, sql: str) -> tuple[bool, str | None]:
        """Validate SQL for read-only execution against known tables only."""
        normalized = self._normalize_sql(sql)
        lowered = normalized.lower()
        if not lowered.startswith("select"):
            return False, "Only SELECT queries are allowed"
        if ";" in normalized:
            return False, "Multiple SQL statements are not allowed"
        for keyword in _DANGEROUS_KEYWORDS:
            if re.search(rf"\b{keyword}\b", lowered):
                return False, f"Disallowed SQL keyword: {keyword.upper()}"
        table_refs = {match.group(1) for match in _TABLE_REF_RE.finditer(normalized)}
        unknown_tables = sorted(table_refs - self._known_tables)
        if unknown_tables:
            return False, f"Unknown tables referenced: {', '.join(unknown_tables)}"
        return True, None

    def _acquire_connection(self):
        if self.db is not None:
            return self.db, False
        return connect_db(), True

    def _execute_query(self, sql: str) -> list[dict[str, Any]]:
        """Execute a validated SQL query and return rows as dictionaries."""
        conn, should_close = self._acquire_connection()
        try:
            if not isinstance(conn, sqlite3.Connection):
                conn.execute("SET statement_timeout TO 10000")
            cursor = conn.execute(sql)
            description = cursor.description or []
            columns = [column[0] for column in description]
            rows = cursor.fetchall()
            return [dict(zip(columns, row, strict=False)) for row in rows]
        finally:
            if should_close:
                conn.close()

    def _format_results(self, results: list[dict[str, Any]], question: str) -> str:
        """Summarise result rows in a compact analyst-friendly format."""
        if not results:
            return f"No rows matched the question: {question}"
        if len(results) <= 10:
            columns = list(results[0].keys())
            header = " | ".join(columns)
            divider = " | ".join(["---"] * len(columns))
            body = [" | ".join(str(row.get(column, "")) for column in columns) for row in results]
            return "\n".join([f"Returned {len(results)} row(s).", header, divider, *body])
        sample = results[:3]
        column_summary = ", ".join(results[0].keys())
        return (
            f"Returned {len(results)} rows for '{question}'. "
            f"Columns: {column_summary}. Sample rows: {json.dumps(sample, default=str)}"
        )

    def _prompt_text(self, question: str) -> str:
        return (
            NL_QUERY_SYSTEM_PROMPT.format(schema_ddl=self._schema_ddl) + f"\nQuestion: {question}\n"
        )

    def _invoke_llm(self, prompt: str) -> tuple[str, str | None]:
        if self.llm is None:
            return "SELECT manager_id, name FROM managers ORDER BY name LIMIT 10", None

        config = build_langsmith_metadata(operation="nl-query")
        response = None
        if hasattr(self.llm, "invoke"):
            try:
                response = self.llm.invoke(prompt, config=config)
            except TypeError:
                response = self.llm.invoke(prompt)
        elif callable(self.llm):
            response = self.llm(prompt)
        else:
            raise TypeError("Configured LLM does not support invoke()")

        trace_url = derive_langsmith_trace_url(extract_trace_id(response))
        if isinstance(response, str):
            return response, trace_url
        if hasattr(response, "content"):
            return str(response.content), trace_url
        return str(response), trace_url

    def _parse_llm_result(self, raw_response: str) -> NLQueryResult:
        payload = raw_response.strip()
        if payload.startswith("{"):
            try:
                data = json.loads(payload)
                return NLQueryResult(**data)
            except Exception:
                pass
        return NLQueryResult(sql=payload, explanation="", columns=[])

    def run(self, question: str, context: dict[str, Any] | None = None) -> dict[str, Any]:
        """Run the NL-to-SQL pipeline end-to-end."""
        del context  # Reserved for future filters.
        guard_input(question)
        prompt = self._prompt_text(question)
        raw_response, trace_url = self._invoke_llm(prompt)
        parsed = self._parse_llm_result(raw_response)
        normalized_sql = self._normalize_sql(parsed.sql)
        is_safe, error_message = self._validate_sql(normalized_sql)
        if not is_safe:
            raise ValueError(error_message or "Unsafe SQL rejected")
        results = self._execute_query(normalized_sql)
        answer = self._format_results(results, question)
        return {
            "sql": normalized_sql,
            "results": results,
            "answer": answer,
            "trace_url": trace_url,
            "sources": [],
        }
