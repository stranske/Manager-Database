"""Widen api_usage.cost_usd and exclude LLM rows from monthly_usage.mb.

Revision ID: 026
Revises: 025
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "026"
down_revision = "025"
branch_labels = None
depends_on = None


def _is_postgresql() -> bool:
    return op.get_bind().dialect.name == "postgresql"


_MONTHLY_USAGE_MB_SUM = "sum(CASE WHEN source = 'langchain' OR bytes < 0 THEN 0 ELSE bytes END)"


def upgrade() -> None:
    if _is_postgresql():
        op.execute("DROP MATERIALIZED VIEW IF EXISTS monthly_usage")
        op.alter_column(
            "api_usage",
            "cost_usd",
            existing_type=sa.Numeric(10, 4),
            type_=sa.Numeric(12, 6),
            existing_nullable=True,
        )
        op.execute(f"""
            CREATE MATERIALIZED VIEW monthly_usage AS
            SELECT date_trunc('month', ts) AS month,
                   source,
                   count(*) AS calls,
                   {_MONTHLY_USAGE_MB_SUM} AS mb,
                   sum(cost_usd) AS cost
            FROM api_usage
            GROUP BY 1, 2
            """)
    else:
        op.execute("DROP VIEW IF EXISTS monthly_usage")
        op.execute(f"""
            CREATE VIEW monthly_usage AS
            SELECT strftime('%Y-%m-01', ts) AS month,
                   source,
                   count(*) AS calls,
                   {_MONTHLY_USAGE_MB_SUM} AS mb,
                   sum(cost_usd) AS cost
            FROM api_usage
            GROUP BY 1, 2
            """)


def downgrade() -> None:
    if _is_postgresql():
        op.execute("DROP MATERIALIZED VIEW IF EXISTS monthly_usage")
        op.alter_column(
            "api_usage",
            "cost_usd",
            existing_type=sa.Numeric(12, 6),
            type_=sa.Numeric(10, 4),
            existing_nullable=True,
        )
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
    else:
        op.execute("DROP VIEW IF EXISTS monthly_usage")
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
