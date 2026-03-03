"""Add conviction_scores table for per-filing manager concentration metrics.

Revision ID: 003
Revises: 002
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy import text

from alembic import op

revision = "003"
down_revision = "002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "conviction_scores",
        sa.Column("score_id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("manager_id", sa.BigInteger(), nullable=False),
        sa.Column("filing_id", sa.BigInteger(), nullable=False),
        sa.Column("cusip", sa.Text(), nullable=False),
        sa.Column("name_of_issuer", sa.Text(), nullable=True),
        sa.Column("shares", sa.BigInteger(), nullable=True),
        sa.Column("value_usd", sa.Numeric(16, 2), nullable=True),
        sa.Column("conviction_pct", sa.Numeric(8, 4), nullable=True),
        sa.Column("portfolio_weight", sa.Numeric(8, 6), nullable=True),
        sa.Column(
            "computed_at", sa.DateTime(timezone=True), server_default=text("CURRENT_TIMESTAMP")
        ),
        sa.ForeignKeyConstraint(
            ["manager_id"],
            ["managers.manager_id"],
            name="fk_conviction_scores_manager_id",
        ),
        sa.ForeignKeyConstraint(
            ["filing_id"],
            ["filings.filing_id"],
            name="fk_conviction_scores_filing_id",
        ),
        sa.UniqueConstraint("filing_id", "cusip", name="uq_conviction_scores_filing_cusip"),
    )

    op.create_index("idx_conviction_manager", "conviction_scores", ["manager_id"])
    op.create_index("idx_conviction_cusip", "conviction_scores", ["cusip"])
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_conviction_pct " "ON conviction_scores(conviction_pct DESC)"
    )


def downgrade() -> None:
    op.drop_index("idx_conviction_pct", table_name="conviction_scores")
    op.drop_index("idx_conviction_cusip", table_name="conviction_scores")
    op.drop_index("idx_conviction_manager", table_name="conviction_scores")
    op.drop_table("conviction_scores")
