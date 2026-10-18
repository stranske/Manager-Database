"""Natural-language-to-SQL chain for analyst questions."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field
from sqlglot import exp, parse
from sqlglot.errors import OptimizeError, ParseError
from sqlglot.optimizer.scope import Scope, traverse_scope

from adapters.base import is_sqlite
from chains.utils import acquire_connection, guard_context_values
from llm.injection import guard_input
from tools.llm_provider import (
    build_langsmith_metadata,
    derive_langsmith_trace_url,
    extract_trace_id,
)

NL_QUERY_SYSTEM_PROMPT = """You are a SQL query generator for a financial manager database.
Given a natural language question, generate a {dialect_name} SELECT query.

IMPORTANT RULES:
1. Generate ONLY SELECT queries — never INSERT, UPDATE, DELETE, DROP, ALTER, or TRUNCATE.
2. Use only tables and columns from the schema below.
3. Always include appropriate WHERE clauses to limit results.
4. Add LIMIT clause (at most 100 rows).
5. Use proper JOIN syntax for related tables.
6. Return the SQL query only, no explanation unless JSON output is explicitly requested.

Database schema:
{schema_ddl}
"""

MAX_RESULT_ROWS = 100
_SQLITE_UNSUPPORTED_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"\bilike\b", re.I), "ILIKE is PostgreSQL-only; use LIKE for SQLite"),
    (re.compile(r"\bdate_trunc\s*\(", re.I), "date_trunc() is PostgreSQL-only"),
    (re.compile(r"::\s*[a-zA-Z_][\w]*", re.I), "PostgreSQL cast syntax is not valid SQLite"),
    (
        re.compile(r"\bto_(?:char|date|timestamp)\s*\(", re.I),
        "to_* date functions are PostgreSQL-only",
    ),
)


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
            match.group(1).lower()
            for match in re.finditer(
                r"CREATE TABLE(?: IF NOT EXISTS)?\s+([a-zA-Z_][\w]*)", schema_ddl, re.I
            )
        }

    def _parse_query(self, sql: str) -> exp.Query:
        """Parse exactly one analyzable, read-only query in the active dialect."""
        try:
            statements = parse(sql, read=self._connection_dialect())
        except ParseError as exc:
            raise ValueError("SQL could not be structurally analyzed") from exc
        if len(statements) != 1 or not isinstance(statements[0], (exp.Select, exp.SetOperation)):
            raise ValueError("Only one SELECT query is allowed")
        query = statements[0]
        for node in query.walk():
            if isinstance(node, (exp.DML, exp.DDL, exp.Command, exp.Into)):
                raise ValueError("Only read-only SELECT queries are allowed")
            if node.args.get("locks"):
                raise ValueError("Locking SELECT queries are not allowed")
        return query

    def _normalize_sql(self, sql: str) -> str:
        compatible, error = self._validate_sql_for_connection(sql, self.db)
        if not compatible:
            raise ValueError(error or "SQL dialect is incompatible with active database")
        query = self._parse_query(sql)
        limit = query.args.get("limit")
        if limit is not None:
            value = limit.expression
            if not isinstance(value, exp.Literal) or value.is_string or not value.this.isdecimal():
                raise ValueError("Unsupported outer LIMIT expression")
            count = min(int(value.this), MAX_RESULT_ROWS)
        else:
            count = MAX_RESULT_ROWS
        offset = query.args.get("offset")
        if offset is not None:
            value = offset.expression
            if not isinstance(value, exp.Literal) or value.is_string or not value.this.isdecimal():
                raise ValueError("Unsupported outer OFFSET expression")
        # Only the root LIMIT is replaced. Subquery limits retain their meaning.
        query.set("limit", exp.Limit(expression=exp.Literal.number(count)))
        return query.sql(dialect=self._connection_dialect())

    def _validate_sql(self, sql: str) -> tuple[bool, str | None]:
        """Walk each relation in its lexical scope before any database execution."""
        try:
            query = self._parse_query(self._normalize_sql(sql))
            table_refs: set[str] = set()
            for scope in traverse_scope(query):
                if not isinstance(scope.expression, (exp.Select, exp.SetOperation)):
                    raise ValueError("Unsupported SQL relation scope")
                for _alias, (_node, source) in scope.selected_sources.items():
                    if isinstance(source, Scope):
                        # CTE/derived queries are checked in their own scopes.
                        continue
                    if not isinstance(source, exp.Table) or not isinstance(
                        source.this, exp.Identifier
                    ):
                        raise ValueError("Unsupported SQL relation")
                    if source.db or source.catalog:
                        raise ValueError("Qualified table names are not allowed")
                    table_refs.add(source.name.lower())
            if not table_refs:
                return False, "SELECT queries must reference at least one known application table"
            unknown_tables = sorted(table_refs - self._known_tables)
            if unknown_tables:
                return False, f"Unknown tables referenced: {', '.join(unknown_tables)}"
        except (ValueError, OptimizeError, ParseError) as exc:
            return False, str(exc)
        return True, None

    def _connection_dialect(self, conn: Any | None = None) -> str:
        if is_sqlite(conn if conn is not None else self.db):
            return "sqlite"
        return "postgresql"

    def _dialect_prompt_label(self) -> str:
        if self._connection_dialect() == "sqlite":
            return "SQLite-compatible"
        return "PostgreSQL-compatible"

    def _validate_sql_for_connection(self, sql: str, conn: Any) -> tuple[bool, str | None]:
        """Reject dialect-specific SQL before it reaches the active connection."""
        if self._connection_dialect(conn) != "sqlite":
            return True, None
        for pattern, message in _SQLITE_UNSUPPORTED_PATTERNS:
            if pattern.search(sql):
                return False, message
        return True, None

    def _execute_query(self, sql: str) -> list[dict[str, Any]]:
        """Execute a validated SQL query and return rows as dictionaries."""
        conn, should_close = acquire_connection(self.db)
        try:
            is_valid_dialect, dialect_error = self._validate_sql_for_connection(sql, conn)
            if not is_valid_dialect:
                raise ValueError(
                    dialect_error or "SQL dialect is incompatible with active database"
                )
            if self._connection_dialect(conn) != "sqlite":
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

    def _context_prompt(self, context: dict[str, Any] | None) -> str:
        if not context:
            return ""
        filters: list[str] = []
        manager_ids = context.get("manager_ids")
        if manager_ids:
            filters.append(f"- Restrict results to manager_ids={manager_ids}")
        manager_name = context.get("manager_name")
        if manager_name:
            filters.append(f"- Restrict results to manager_name={manager_name}")
        filing_id = context.get("filing_id")
        if filing_id:
            filters.append(f"- Restrict results to filing_id={filing_id}")
        date_range = context.get("date_range")
        if date_range:
            filters.append(f"- Restrict results to date_range={date_range}")
        cusips = context.get("cusips")
        if cusips:
            filters.append(f"- Restrict results to cusips={cusips}")
        if not filters:
            return ""
        return "Context filters:\n" + "\n".join(filters) + "\n"

    def _prompt_text(self, question: str, context: dict[str, Any] | None = None) -> str:
        return (
            NL_QUERY_SYSTEM_PROMPT.format(
                dialect_name=self._dialect_prompt_label(),
                schema_ddl=self._schema_ddl,
            )
            + "\n"
            + self._context_prompt(context)
            + f"Question: {question}\n"
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
        guard_input(question)
        guard_context_values(context)
        prompt = self._prompt_text(question, context)
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
