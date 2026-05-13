"""Postgres-backed integration tests for chain query paths.

Set ``MGRDB_PG_TEST_URL`` to run these against a live Postgres database:

    MGRDB_PG_TEST_URL=postgresql://postgres:postgres@localhost:5432/postgres \
        pytest tests/test_chain_postgres_integration.py -v
"""

from __future__ import annotations

import os
from collections.abc import Generator
from datetime import date
from pathlib import Path
from typing import Any

import pytest
from langchain_core.runnables import RunnableLambda

from chains.holdings_analysis import HoldingsAnalysisChain
from chains.nl_query import NLQueryChain
from chains.rag_search import RAGSearchChain
from tools.langchain_client import ClientInfo

ROOT = Path(__file__).resolve().parents[1]
SCHEMA_SQL = ROOT / "schema.sql"

SEEDED_CUSIP = "037833100"
SEEDED_MANAGER_NAME = "Elliott"


class PgFixture:
    def __init__(self, conn: Any, seed: dict[str, int]) -> None:
        self.conn = conn
        self.seed = seed


class FakeResponse:
    def __init__(self, content: str) -> None:
        self.content = content
        self.response_metadata: dict[str, str] = {}


class FakeLLM:
    def __init__(self, response_text: str) -> None:
        self._response_text = response_text
        self.prompts: list[str] = []

    def invoke(self, prompt: str, config: Any = None) -> FakeResponse:
        self.prompts.append(prompt)
        return FakeResponse(self._response_text)


@pytest.fixture(scope="module")
def psycopg_module():
    return pytest.importorskip("psycopg")


@pytest.fixture(scope="module")
def pg_url() -> str:
    url = os.environ.get("MGRDB_PG_TEST_URL")
    if not url:
        pytest.skip("MGRDB_PG_TEST_URL not set; skipping Postgres chain integration tests")
    return url


@pytest.fixture(scope="module")
def pg_conn(pg_url: str, psycopg_module) -> Generator[PgFixture, None, None]:
    with psycopg_module.connect(pg_url, autocommit=False) as conn:
        _reset_public_schema(conn)
        _apply_schema_sql(conn)
        seed = _seed_chain_fixtures(conn)
        yield PgFixture(conn=conn, seed=seed)


def _split_sql_statements(sql: str) -> list[str]:
    stmts: list[str] = []
    buf: list[str] = []
    in_dollar_quote = False
    dollar_tag = ""
    i = 0
    n = len(sql)

    while i < n:
        ch = sql[i]
        if not in_dollar_quote:
            if ch == "-" and i + 1 < n and sql[i + 1] == "-":
                while i < n and sql[i] != "\n":
                    i += 1
                continue
            if ch == "$":
                j = i + 1
                while j < n and sql[j] != "$":
                    j += 1
                if j < n:
                    tag = sql[i : j + 1]
                    in_dollar_quote = True
                    dollar_tag = tag
                    buf.append(tag)
                    i = j + 1
                    continue
            if ch == ";":
                buf.append(";")
                stmt = "".join(buf).strip()
                if stmt:
                    stmts.append(stmt)
                buf = []
                i += 1
                continue
        else:
            if sql[i : i + len(dollar_tag)] == dollar_tag:
                buf.append(dollar_tag)
                i += len(dollar_tag)
                in_dollar_quote = False
                dollar_tag = ""
                continue

        buf.append(ch)
        i += 1

    remaining = "".join(buf).strip()
    if remaining:
        stmts.append(remaining)
    return stmts


def _reset_public_schema(conn: Any) -> None:
    with conn.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS public CASCADE")
        cur.execute("CREATE SCHEMA public")
        cur.execute("GRANT ALL ON SCHEMA public TO public")
    conn.commit()


def _apply_schema_sql(conn: Any) -> None:
    for idx, stmt in enumerate(_split_sql_statements(SCHEMA_SQL.read_text()), 1):
        try:
            with conn.cursor() as cur:
                cur.execute(stmt)
        except Exception as exc:
            conn.rollback()
            preview = stmt if len(stmt) <= 400 else stmt[:400] + "..."
            pytest.fail(
                f"schema.sql failed at statement {idx}: {type(exc).__name__}: {exc}\n{preview}"
            )
    conn.commit()


def _seed_chain_fixtures(conn: Any) -> dict[str, int]:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO managers(name, cik) VALUES (%s, %s) RETURNING manager_id",
            (SEEDED_MANAGER_NAME, "0001791786"),
        )
        manager_id = int(cur.fetchone()[0])
        cur.execute(
            """
            INSERT INTO filings(manager_id, type, period_end, filed_date, source, url, raw_key)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING filing_id
            """,
            (
                manager_id,
                "13F-HR",
                date(2026, 3, 31),
                date(2026, 4, 15),
                "sec",
                "https://example.com/filings/elliott-13f",
                "seed-elliott-2026-q1",
            ),
        )
        filing_id = int(cur.fetchone()[0])
        cur.execute(
            """
            INSERT INTO holdings(filing_id, cusip, name_of_issuer, shares, value_usd)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (filing_id, SEEDED_CUSIP, "Apple Inc.", 1000, 150000.0),
        )
        cur.execute(
            """
            INSERT INTO news_items(manager_id, published_at, source, headline, url, body_snippet)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                manager_id,
                "2026-04-16T08:00:00Z",
                "Reuters",
                "Elliott increases Apple stake",
                "https://example.com/news/elliott-apple",
                "Elliott disclosed a larger Apple position.",
            ),
        )
    conn.commit()
    return {"manager_id": manager_id, "filing_id": filing_id}


def test_rag_search_postgres(pg_conn: PgFixture) -> None:
    seed = pg_conn.seed
    _assert_to_regclass_reachable(pg_conn.conn)
    chain = RAGSearchChain(db_conn=pg_conn.conn)

    _context, sources = chain._structured_search(
        {
            "manager_ids": [seed["manager_id"]],
            "cusips": [],
            "keywords": [],
            "date_range": {"start": "2026-01-01", "end": "2026-12-31"},
        }
    )

    assert any(source.get("filing_id") == seed["filing_id"] for source in sources)


def test_nl_query_postgres(pg_conn: PgFixture) -> None:
    seed = pg_conn.seed
    llm = FakeLLM(
        '{"sql": "SELECT manager_id, name FROM managers ORDER BY manager_id", '
        '"columns": ["manager_id", "name"]}'
    )
    chain = NLQueryChain(llm=llm, db_conn=pg_conn.conn)

    result = chain.run("List all managers")

    assert result["results"] == [{"manager_id": seed["manager_id"], "name": SEEDED_MANAGER_NAME}]
    assert result["sql"].endswith("LIMIT 100")


def test_holdings_analysis_postgres(pg_conn: PgFixture) -> None:
    seed = pg_conn.seed
    client = ClientInfo(
        client=RunnableLambda(
            lambda _payload: {
                "thesis": "ok",
                "top_positions": [],
                "period_changes": [],
                "cross_manager_overlap": None,
                "concentration_metrics": {},
            }
        ),
        provider="test-provider",
        model="test-model",
    )
    chain = HoldingsAnalysisChain(client_info=client, db_conn=pg_conn.conn)

    result = chain.run(
        "Top positions",
        manager_ids=[seed["manager_id"]],
        date_range=(date(2026, 1, 1), date(2026, 12, 31)),
    )

    assert result.thesis == "ok"


def _assert_to_regclass_reachable(conn: Any) -> None:
    row = conn.execute("SELECT to_regclass(%s)", ("managers",)).fetchone()
    assert row and row[0] == "managers"
