"""Add opening-filing anchor and episode-aware uniqueness to activism_campaigns (#1924).

Revision ID: 024
Revises: 023
"""

from __future__ import annotations

from typing import Any

from adapters.base import is_postgres, is_sqlite, table_exists
from alembic import op
from etl.activism_campaign_flow import _unwrap_connection, upgrade_activism_campaign_episodes

revision = "024"
down_revision = "023"
branch_labels = None
depends_on = None


def upgrade() -> None:
    upgrade_activism_campaign_episodes(op.get_bind())


def downgrade() -> None:
    conn = _unwrap_connection(op.get_bind())
    if not table_exists(conn, "activism_campaigns"):
        return
    has_timeline = table_exists(conn, "activism_campaign_timeline")
    has_documents = table_exists(conn, "activism_documents")

    if is_sqlite(conn):
        existing = conn.execute(
            "SELECT campaign_id, manager_id, target_identifier, target_company, first_filed, "
            "last_filed, status, peak_ownership_pct, latest_ownership_pct, filing_count, "
            "event_count, latest_event_type, target_ticker, window_return, holding_period_days, "
            "return_computed_at, source_forms, data_quality_flags, computed_at "
            "FROM activism_campaigns ORDER BY campaign_id"
        ).fetchall()
        kept_by_pair: dict[tuple[int, str], tuple[Any, ...]] = {}
        remap: dict[int, int] = {}
        for row in existing:
            cid = int(row[0])
            pair = (int(row[1]), str(row[2]))
            if pair not in kept_by_pair:
                kept_by_pair[pair] = row
                remap[cid] = cid
            else:
                remap[cid] = int(kept_by_pair[pair][0])

        timeline_rows = (
            conn.execute(
                "SELECT timeline_id, campaign_id, filing_id, event_id, event_date, event_type, "
                "form_type, ownership_pct, summary, source_url "
                "FROM activism_campaign_timeline ORDER BY timeline_id"
            ).fetchall()
            if has_timeline
            else []
        )
        document_rows = (
            conn.execute(
                "SELECT document_id, campaign_id, filing_id, doc_type, source_url, raw_key, "
                "filed_date FROM activism_documents ORDER BY document_id"
            ).fetchall()
            if has_documents
            else []
        )
        if has_timeline and timeline_rows:
            conn.execute("DELETE FROM activism_campaign_timeline")
        if has_documents and document_rows:
            conn.execute("DELETE FROM activism_documents")

        conn.execute("DROP TABLE activism_campaigns")
        conn.execute("""CREATE TABLE activism_campaigns (
                campaign_id INTEGER PRIMARY KEY,
                manager_id BIGINT NOT NULL REFERENCES managers(manager_id),
                target_identifier TEXT NOT NULL,
                target_company TEXT NOT NULL,
                first_filed DATE NOT NULL,
                last_filed DATE NOT NULL,
                status TEXT NOT NULL,
                peak_ownership_pct NUMERIC(8, 4),
                latest_ownership_pct NUMERIC(8, 4),
                filing_count INTEGER NOT NULL DEFAULT '0',
                event_count INTEGER NOT NULL DEFAULT '0',
                latest_event_type TEXT,
                source_forms TEXT NOT NULL DEFAULT '[]',
                data_quality_flags TEXT NOT NULL DEFAULT '[]',
                computed_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                target_ticker TEXT,
                window_return NUMERIC(18, 8),
                holding_period_days INTEGER,
                return_computed_at DATETIME,
                CONSTRAINT uq_activism_campaign_manager_target UNIQUE (manager_id, target_identifier),
                CONSTRAINT ck_activism_campaigns_status CHECK (status IN ('active', 'monitoring', 'closed', 'unknown'))
            )""")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_activism_campaigns_manager ON activism_campaigns(manager_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_activism_campaigns_status ON activism_campaigns(status)"
        )
        if kept_by_pair:
            conn.executemany(
                "INSERT INTO activism_campaigns(campaign_id, manager_id, target_identifier, "
                "target_company, first_filed, last_filed, status, peak_ownership_pct, "
                "latest_ownership_pct, filing_count, event_count, latest_event_type, "
                "target_ticker, window_return, holding_period_days, return_computed_at, "
                "source_forms, data_quality_flags, computed_at) "
                f"VALUES ({', '.join(['?'] * 19)})",
                list(kept_by_pair.values()),
            )
        if has_timeline and timeline_rows:
            conn.executemany(
                "INSERT INTO activism_campaign_timeline(timeline_id, campaign_id, filing_id, "
                "event_id, event_date, event_type, form_type, ownership_pct, summary, source_url) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [(row[0], remap.get(int(row[1]), int(row[1])), *row[2:]) for row in timeline_rows],
            )
        if has_documents and document_rows:
            conn.executemany(
                "INSERT INTO activism_documents(document_id, campaign_id, filing_id, doc_type, "
                "source_url, raw_key, filed_date) VALUES (?, ?, ?, ?, ?, ?, ?)",
                [(row[0], remap.get(int(row[1]), int(row[1])), *row[2:]) for row in document_rows],
            )
        return

    if is_postgres(conn):
        existing = conn.execute(
            "SELECT campaign_id, manager_id, target_identifier FROM activism_campaigns ORDER BY campaign_id"
        ).fetchall()
        kept_ids: dict[tuple[int, str], int] = {}
        for cid_raw, mgr_raw, ident_raw in existing:
            cid = int(cid_raw)
            pair = (int(mgr_raw), str(ident_raw))
            if pair not in kept_ids:
                kept_ids[pair] = cid
            else:
                kept_cid = kept_ids[pair]
                if has_timeline:
                    conn.execute(
                        "UPDATE activism_campaign_timeline SET campaign_id = %s WHERE campaign_id = %s",
                        (kept_cid, cid),
                    )
                if has_documents:
                    conn.execute(
                        "UPDATE activism_documents SET campaign_id = %s WHERE campaign_id = %s",
                        (kept_cid, cid),
                    )
                conn.execute("DELETE FROM activism_campaigns WHERE campaign_id = %s", (cid,))
        for (conname,) in conn.execute(
            "SELECT con.conname FROM pg_constraint con "
            "JOIN pg_class c ON c.oid = con.conrelid "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = current_schema() AND c.relname = 'activism_campaigns' "
            "AND con.contype = 'u'"
        ).fetchall():
            conn.execute(f'ALTER TABLE activism_campaigns DROP CONSTRAINT "{conname}"')
        conn.execute("ALTER TABLE activism_campaigns DROP COLUMN IF EXISTS opening_filing_id")
        conn.execute(
            "ALTER TABLE activism_campaigns "
            "ADD CONSTRAINT uq_activism_campaign_manager_target "
            "UNIQUE (manager_id, target_identifier)"
        )
