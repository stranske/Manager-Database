"""Postgres-backed integration tests for alert persistence and dispatch paths.

Set ``MGRDB_PG_TEST_URL`` to run these against a live Postgres database:

    MGRDB_PG_TEST_URL=postgresql://postgres:postgres@localhost:5432/postgres \
        pytest tests/test_alert_postgres_integration.py -v
"""

from __future__ import annotations

import os
from collections.abc import Generator
from pathlib import Path
from typing import Any

import pytest

from alerts.channels import DeliveryResult, NotificationChannel
from alerts.db import (
    ensure_alert_tables,
    fetch_rule_by_id,
    insert_pending_alert,
    record_delivery_error,
    record_delivery_success,
    rule_from_row,
)
from alerts.integration import (
    build_new_filing_event,
    evaluate_and_record_new_filing_alerts,
    fire_alerts_for_event,
)
from alerts.models import FiredAlert

ROOT = Path(__file__).resolve().parents[1]
SCHEMA_SQL = ROOT / "schema.sql"


class PgAlertFixture:
    def __init__(self, conn: Any, *, manager_id: int, rule_id: int) -> None:
        self.conn = conn
        self.manager_id = manager_id
        self.rule_id = rule_id


class MockStreamlitChannel(NotificationChannel):
    channel_name = "streamlit"

    async def deliver(self, alert: FiredAlert) -> DeliveryResult:
        return DeliveryResult(success=True, channel=self.channel_name, skipped=False)


@pytest.fixture(scope="module")
def psycopg_module():
    return pytest.importorskip("psycopg")


@pytest.fixture(scope="module")
def pg_url() -> str:
    url = os.environ.get("MGRDB_PG_TEST_URL")
    if not url:
        pytest.skip("MGRDB_PG_TEST_URL not set; skipping Postgres alert integration tests")
    return url


@pytest.fixture(scope="module")
def pg_conn(pg_url: str, psycopg_module) -> Generator[PgAlertFixture, None, None]:
    with psycopg_module.connect(pg_url, autocommit=False) as conn:
        _reset_public_schema(conn)
        _apply_schema_sql(conn)
        fixture = _seed_alert_fixtures(conn)
        yield fixture


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


def _seed_alert_fixtures(conn: Any) -> PgAlertFixture:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO managers(name, cik) VALUES (%s, %s) RETURNING manager_id",
            ("Elliott", "0001791786"),
        )
        manager_id = int(cur.fetchone()[0])

    ensure_alert_tables(conn)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO alert_rules(
                name, event_type, condition_json, channels, enabled, manager_id, created_by
            ) VALUES (%s, %s, %s::jsonb, %s, %s, %s, %s)
            RETURNING rule_id
            """,
            (
                "New Filing Rule",
                "new_filing",
                '{"any_new_filing":true}',
                ["streamlit"],
                True,
                manager_id,
                "pytest",
            ),
        )
        rule_id = int(cur.fetchone()[0])
    conn.commit()
    return PgAlertFixture(conn=conn, manager_id=manager_id, rule_id=rule_id)


def _fired_alert(pg_conn: PgAlertFixture) -> FiredAlert:
    row = fetch_rule_by_id(pg_conn.conn, pg_conn.rule_id)
    assert row is not None
    return FiredAlert(
        rule=rule_from_row(row),
        event=build_new_filing_event(
            filing_id=1,
            manager_id=pg_conn.manager_id,
            filing_type="13F-HR",
            filed_date="2026-04-15",
        ),
        channels=["streamlit"],
    )


def test_ensure_alert_tables_creates_postgres_tables(pg_conn: PgAlertFixture):
    ensure_alert_tables(pg_conn.conn)

    rows = pg_conn.conn.execute(
        """
        SELECT table_name
          FROM information_schema.tables
         WHERE table_schema = 'public'
           AND table_name IN ('alert_rules', 'alert_history')
        """
    ).fetchall()

    assert {row[0] for row in rows} == {"alert_rules", "alert_history"}


def test_insert_pending_and_record_delivery_postgres(pg_conn: PgAlertFixture):
    alert_id = insert_pending_alert(pg_conn.conn, _fired_alert(pg_conn))

    record_delivery_success(pg_conn.conn, alert_id, "streamlit")
    record_delivery_error(pg_conn.conn, alert_id, "slack", "webhook unavailable")

    row = pg_conn.conn.execute(
        """
        SELECT delivered_channels, delivery_errors
          FROM alert_history
         WHERE alert_id = %s
        """,
        (alert_id,),
    ).fetchone()

    assert row is not None
    assert "streamlit" in row[0]
    assert row[1]["slack"] == "webhook unavailable"


def test_evaluate_and_record_new_filing_alerts_postgres(pg_conn: PgAlertFixture):
    alert_ids = evaluate_and_record_new_filing_alerts(
        pg_conn.conn,
        filing_id=1,
        manager_id=pg_conn.manager_id,
        filing_type="13F-HR",
        filed_date="2026-04-15",
    )

    assert alert_ids
    row = pg_conn.conn.execute(
        """
        SELECT rule_id, event_type, payload_json
          FROM alert_history
         WHERE alert_id = %s
        """,
        (alert_ids[0],),
    ).fetchone()

    assert row is not None
    assert row[0] == pg_conn.rule_id
    assert row[1] == "new_filing"
    assert row[2]["type"] == "13F-HR"


@pytest.mark.asyncio
async def test_fire_alerts_for_event_records_streamlit_channel(pg_conn: PgAlertFixture):
    alert_ids = await fire_alerts_for_event(
        pg_conn.conn,
        build_new_filing_event(
            filing_id=1,
            manager_id=pg_conn.manager_id,
            filing_type="13F-HR",
            filed_date="2026-04-15",
        ),
        channels={"streamlit": MockStreamlitChannel()},
    )

    assert alert_ids
    row = pg_conn.conn.execute(
        """
        SELECT delivered_channels
          FROM alert_history
         WHERE alert_id = %s
        """,
        (alert_ids[0],),
    ).fetchone()

    assert row is not None
    assert "streamlit" in row[0]
