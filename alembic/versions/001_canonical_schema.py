"""Initial canonical schema.

Matches schema.sql — the 7-table Manager Database Universe model
(managers, filings, holdings, news_items, documents, daily_diffs, api_usage)
plus two materialized views (monthly_usage, mv_daily_report).
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy import text

from alembic import op

# revision identifiers, used by Alembic.
revision = "001"
down_revision = None
branch_labels = None
depends_on = None


def _is_postgresql() -> bool:
    return op.get_bind().dialect.name == "postgresql"


def _array_or_text():
    """ARRAY(Text) on Postgres, plain Text on SQLite."""
    if _is_postgresql():
        return sa.ARRAY(sa.Text())
    return sa.Text()


def _json_type():
    """JSONB on Postgres, plain Text on SQLite."""
    if _is_postgresql():
        return sa.JSON()
    return sa.Text()


def upgrade() -> None:
    pg = _is_postgresql()

    if pg:
        op.execute("CREATE EXTENSION IF NOT EXISTS vector")
        op.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")

    # ── managers ──────────────────────────────────────────────
    op.create_table(
        "managers",
        sa.Column("manager_id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("aliases", _array_or_text(), server_default="{}"),
        sa.Column("jurisdictions", _array_or_text(), server_default="{}"),
        sa.Column("cik", sa.Text(), nullable=True),
        sa.Column("lei", sa.Text(), nullable=True),
        sa.Column("registry_ids", _json_type(), server_default="{}"),
        sa.Column("tags", _array_or_text(), server_default="{}"),
        sa.Column("quality_flags", _json_type(), server_default="{}"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=text("CURRENT_TIMESTAMP"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=text("CURRENT_TIMESTAMP"),
        ),
    )
    if pg:
        op.create_index(
            "idx_managers_cik_unique",
            "managers",
            ["cik"],
            unique=True,
            postgresql_where=text("cik IS NOT NULL"),
        )
        op.create_index("idx_managers_lei", "managers", ["lei"])

    # ── filings ───────────────────────────────────────────────
    op.create_table(
        "filings",
        sa.Column("filing_id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("manager_id", sa.BigInteger(), nullable=False),
        sa.Column("type", sa.Text(), nullable=False),
        sa.Column("period_end", sa.Date(), nullable=True),
        sa.Column("filed_date", sa.Date(), nullable=True),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("url", sa.Text(), nullable=True),
        sa.Column("raw_key", sa.Text(), nullable=True),
        sa.Column("parsed_payload", _json_type(), nullable=True),
        sa.Column("schema_version", sa.Integer(), server_default="1"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=text("CURRENT_TIMESTAMP"),
        ),
        sa.ForeignKeyConstraint(
            ["manager_id"], ["managers.manager_id"], name="fk_filings_manager_id"
        ),
    )
    op.create_index("idx_filings_manager_filed_date", "filings", ["manager_id", "filed_date"])
    op.create_index("idx_filings_manager_type", "filings", ["manager_id", "type"])

    # ── holdings ──────────────────────────────────────────────
    op.create_table(
        "holdings",
        sa.Column("holding_id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("filing_id", sa.BigInteger(), nullable=False),
        sa.Column("cusip", sa.Text(), nullable=True),
        sa.Column("isin", sa.Text(), nullable=True),
        sa.Column("name_of_issuer", sa.Text(), nullable=True),
        sa.Column("shares", sa.BigInteger(), nullable=True),
        sa.Column("value_usd", sa.Numeric(18, 2), nullable=True),
        sa.Column("delta_type", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=text("CURRENT_TIMESTAMP"),
        ),
        sa.ForeignKeyConstraint(["filing_id"], ["filings.filing_id"], name="fk_holdings_filing_id"),
    )
    op.create_index("idx_holdings_filing_id", "holdings", ["filing_id"])
    op.create_index("idx_holdings_cusip", "holdings", ["cusip"])

    # ── news_items ────────────────────────────────────────────
    op.create_table(
        "news_items",
        sa.Column("news_id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("manager_id", sa.BigInteger(), nullable=True),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("headline", sa.Text(), nullable=False),
        sa.Column("url", sa.Text(), nullable=True),
        sa.Column("body_snippet", sa.Text(), nullable=True),
        sa.Column("topics", _array_or_text(), server_default="{}"),
        sa.Column("confidence", sa.Float(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=text("CURRENT_TIMESTAMP"),
        ),
        sa.ForeignKeyConstraint(
            ["manager_id"], ["managers.manager_id"], name="fk_news_items_manager_id"
        ),
    )
    op.create_index(
        "idx_news_items_manager_published_at", "news_items", ["manager_id", "published_at"]
    )

    # ── documents ─────────────────────────────────────────────
    op.create_table(
        "documents",
        sa.Column("doc_id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("manager_id", sa.BigInteger(), nullable=True),
        sa.Column("kind", sa.Text(), nullable=False, server_default="note"),
        sa.Column("filename", sa.Text(), nullable=True),
        sa.Column("sha256", sa.Text(), nullable=True),
        sa.Column("text", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=text("CURRENT_TIMESTAMP"),
        ),
        sa.ForeignKeyConstraint(
            ["manager_id"], ["managers.manager_id"], name="fk_documents_manager_id"
        ),
    )
    if pg:
        op.execute("ALTER TABLE documents ADD COLUMN IF NOT EXISTS embedding vector(384)")
        op.create_index(
            "idx_documents_sha256_unique",
            "documents",
            ["sha256"],
            unique=True,
            postgresql_where=text("sha256 IS NOT NULL"),
        )

    # ── daily_diffs ───────────────────────────────────────────
    op.create_table(
        "daily_diffs",
        sa.Column("diff_id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("manager_id", sa.BigInteger(), nullable=False),
        sa.Column("report_date", sa.Date(), nullable=False),
        sa.Column("cusip", sa.Text(), nullable=False),
        sa.Column("name_of_issuer", sa.Text(), nullable=True),
        sa.Column("delta_type", sa.Text(), nullable=False),
        sa.Column("shares_prev", sa.BigInteger(), nullable=True),
        sa.Column("shares_curr", sa.BigInteger(), nullable=True),
        sa.Column("value_prev", sa.Numeric(18, 2), nullable=True),
        sa.Column("value_curr", sa.Numeric(18, 2), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=text("CURRENT_TIMESTAMP"),
        ),
        sa.ForeignKeyConstraint(
            ["manager_id"], ["managers.manager_id"], name="fk_daily_diffs_manager_id"
        ),
    )
    if pg:
        # Generated columns require raw DDL
        op.execute(
            "ALTER TABLE daily_diffs "
            "ADD COLUMN IF NOT EXISTS shares_delta bigint "
            "GENERATED ALWAYS AS (shares_curr - shares_prev) STORED"
        )
        op.execute(
            "ALTER TABLE daily_diffs "
            "ADD COLUMN IF NOT EXISTS value_delta numeric(18,2) "
            "GENERATED ALWAYS AS (value_curr - value_prev) STORED"
        )
    op.create_index(
        "idx_daily_diffs_report_date_manager", "daily_diffs", ["report_date", "manager_id"]
    )

    # ── api_usage ─────────────────────────────────────────────
    op.create_table(
        "api_usage",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column(
            "ts",
            sa.DateTime(timezone=True),
            server_default=text("CURRENT_TIMESTAMP"),
        ),
        sa.Column("source", sa.Text(), nullable=True),
        sa.Column("endpoint", sa.Text(), nullable=True),
        sa.Column("status", sa.Integer(), nullable=True),
        sa.Column("bytes", sa.Integer(), nullable=True),
        sa.Column("latency_ms", sa.Integer(), nullable=True),
        sa.Column("cost_usd", sa.Numeric(10, 4), nullable=True),
    )

    # ── materialized views (Postgres) / regular views (SQLite) ─
    if pg:
        op.execute("""
            CREATE MATERIALIZED VIEW monthly_usage AS
            SELECT date_trunc('month', ts) AS month,
                   source,
                   count(*) AS calls,
                   sum(bytes) AS mb,
                   sum(cost_usd) AS cost
            FROM api_usage
            GROUP BY 1, 2
        """)
        op.execute("""
            CREATE MATERIALIZED VIEW mv_daily_report AS
            SELECT
                d.report_date,
                m.manager_id,
                m.name           AS manager_name,
                d.cusip,
                d.name_of_issuer,
                d.delta_type,
                d.shares_prev,
                d.shares_curr,
                (d.shares_curr - d.shares_prev) AS shares_delta,
                d.value_prev,
                d.value_curr,
                (d.value_curr - d.value_prev)   AS value_delta
            FROM daily_diffs d
            JOIN managers m ON m.manager_id = d.manager_id
            ORDER BY d.report_date DESC, m.name, d.delta_type
        """)
        op.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS mv_daily_report_idx
            ON mv_daily_report (report_date, manager_id, cusip, delta_type)
        """)
    else:
        op.execute("""
            CREATE VIEW monthly_usage AS
            SELECT strftime('%Y-%m-01', ts) AS month,
                   source,
                   count(*) AS calls,
                   sum(bytes) AS mb,
                   sum(cost_usd) AS cost
            FROM api_usage
            GROUP BY 1, 2
        """)
        op.execute("""
            CREATE VIEW mv_daily_report AS
            SELECT
                d.report_date,
                m.manager_id,
                m.name           AS manager_name,
                d.cusip,
                d.name_of_issuer,
                d.delta_type,
                d.shares_prev,
                d.shares_curr,
                (d.shares_curr - d.shares_prev) AS shares_delta,
                d.value_prev,
                d.value_curr,
                (d.value_curr - d.value_prev)   AS value_delta
            FROM daily_diffs d
            JOIN managers m ON m.manager_id = d.manager_id
            ORDER BY d.report_date DESC, m.name, d.delta_type
        """)


def downgrade() -> None:
    if _is_postgresql():
        op.execute("DROP MATERIALIZED VIEW IF EXISTS mv_daily_report")
        op.execute("DROP MATERIALIZED VIEW IF EXISTS monthly_usage")
    else:
        op.execute("DROP VIEW IF EXISTS mv_daily_report")
        op.execute("DROP VIEW IF EXISTS monthly_usage")

    op.drop_table("api_usage")
    op.drop_table("daily_diffs")
    op.drop_table("documents")
    op.drop_table("news_items")
    op.drop_table("holdings")
    op.drop_table("filings")
    op.drop_table("managers")
