"""Preserve decision-time source provenance for backtest positions.

Revision ID: 024
Revises: 023
"""

import sqlalchemy as sa

from alembic import op

revision = "024"
down_revision = "023"
branch_labels = None
depends_on = None


def upgrade() -> None:
    columns = {
        column["name"] for column in sa.inspect(op.get_bind()).get_columns("backtest_results")
    }
    # Nullable, without defaults or backfill: historical runs have unknown provenance.
    for name, column_type in (
        ("source_filing_id", sa.BigInteger()),
        ("source_holding_id", sa.BigInteger()),
        ("event_period", sa.Date()),
        ("source_knowledge_time", sa.DateTime(timezone=True)),
        ("applied_cutoff", sa.DateTime(timezone=True)),
    ):
        if name not in columns:
            op.add_column("backtest_results", sa.Column(name, column_type, nullable=True))


def downgrade() -> None:
    for name in (
        "applied_cutoff",
        "source_knowledge_time",
        "event_period",
        "source_holding_id",
        "source_filing_id",
    ):
        op.drop_column("backtest_results", name)
