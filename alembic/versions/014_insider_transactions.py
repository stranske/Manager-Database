"""Add insider_transactions table for Form-4 ingest (#1461).

Revision ID: 014
Revises: 013
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy import text

from alembic import op

revision = "014"
down_revision = "013"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "insider_transactions",
        sa.Column("txn_id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("issuer_cik", sa.Text(), nullable=False),
        sa.Column("ticker", sa.Text(), nullable=True),
        sa.Column("insider_name", sa.Text(), nullable=True),
        sa.Column("txn_code", sa.Text(), nullable=True),
        sa.Column("shares", sa.Numeric(), nullable=True),
        sa.Column("txn_date", sa.Date(), nullable=True),
        sa.Column("acquired_disposed", sa.Text(), nullable=True),
        sa.Column("cusip", sa.Text(), nullable=True),
        sa.Column(
            "ingested_at",
            sa.DateTime(timezone=True),
            server_default=text("CURRENT_TIMESTAMP"),
        ),
        sa.UniqueConstraint(
            "issuer_cik",
            "ticker",
            "insider_name",
            "txn_code",
            "shares",
            "txn_date",
            "acquired_disposed",
            name="uq_insider_transactions_natural_key",
        ),
    )
    op.create_index(
        "idx_insider_issuer_date",
        "insider_transactions",
        ["issuer_cik", "txn_date"],
    )
    op.create_index(
        "idx_insider_ticker_date",
        "insider_transactions",
        ["ticker", "txn_date"],
    )


def downgrade() -> None:
    op.drop_index("idx_insider_ticker_date", table_name="insider_transactions")
    op.drop_index("idx_insider_issuer_date", table_name="insider_transactions")
    op.drop_table("insider_transactions")
