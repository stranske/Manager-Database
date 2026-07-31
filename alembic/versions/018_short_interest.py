"""Add short-interest annotations for held issuers (#1470).

Revision ID: 018
Revises: 017
"""

import sqlalchemy as sa

from alembic import op

revision = "018"
down_revision = "017"
branch_labels = None
depends_on = None


def upgrade() -> None:
    sqlite_autoincrement_id = sa.BigInteger().with_variant(sa.Integer(), "sqlite")
    op.create_table(
        "short_interest",
        sa.Column("metric_id", sqlite_autoincrement_id, primary_key=True, autoincrement=True),
        sa.Column("ticker", sa.Text(), nullable=False),
        sa.Column("cusip", sa.Text(), nullable=True),
        sa.Column("short_interest", sa.Numeric(), nullable=True),
        sa.Column("float_shares", sa.Numeric(), nullable=True),
        sa.Column("short_interest_pct", sa.Numeric(), nullable=True),
        sa.Column("report_date", sa.Date(), nullable=False),
        sa.Column("source", sa.Text(), nullable=False, server_default="finra"),
        sa.Column(
            "ingested_at", sa.DateTime(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP")
        ),
        sa.UniqueConstraint(
            "ticker", "report_date", "source", name="uq_short_interest_ticker_date_source"
        ),
    )
    op.create_index("idx_short_interest_ticker_date", "short_interest", ["ticker", "report_date"])
    op.create_index("idx_short_interest_cusip_date", "short_interest", ["cusip", "report_date"])


def downgrade() -> None:
    op.drop_index("idx_short_interest_cusip_date", table_name="short_interest")
    op.drop_index("idx_short_interest_ticker_date", table_name="short_interest")
    op.drop_table("short_interest")
