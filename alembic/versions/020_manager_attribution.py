"""Add manager_attribution for position-level performance (#1465).

Revision ID: 020
Revises: 019
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy import text

from alembic import op

revision = "020"
down_revision = "019"
branch_labels = None
depends_on = None


def upgrade() -> None:
    sqlite_autoincrement_id = sa.BigInteger().with_variant(sa.Integer(), "sqlite")
    op.create_table(
        "manager_attribution",
        sa.Column("attribution_id", sqlite_autoincrement_id, autoincrement=True),
        sa.Column("manager_id", sa.BigInteger(), nullable=False),
        sa.Column("filing_id", sa.BigInteger(), nullable=True),
        sa.Column("disclosure_date", sa.Date(), nullable=False),
        sa.Column("as_of_date", sa.Date(), nullable=False),
        sa.Column("security_key", sa.Text(), nullable=False),
        sa.Column("ticker", sa.Text(), nullable=True),
        sa.Column("cusip", sa.Text(), nullable=True),
        sa.Column("name_of_issuer", sa.Text(), nullable=True),
        sa.Column("disclosure_price", sa.Float(), nullable=True),
        sa.Column("as_of_price", sa.Float(), nullable=True),
        sa.Column("position_return", sa.Float(), nullable=True),
        sa.Column("value_usd", sa.Float(), nullable=True),
        sa.Column("status", sa.Text(), nullable=False, server_default=text("'filled'")),
        sa.Column("skip_reason", sa.Text(), nullable=True),
        sa.Column(
            "computed_at",
            sa.DateTime(timezone=True),
            server_default=text("CURRENT_TIMESTAMP"),
        ),
        sa.PrimaryKeyConstraint("attribution_id", name=op.f("pk_manager_attribution")),
        sa.UniqueConstraint(
            "manager_id",
            "filing_id",
            "security_key",
            "as_of_date",
            name="uq_manager_attribution_period_security",
        ),
    )
    op.create_index(
        "idx_manager_attribution_manager",
        "manager_attribution",
        ["manager_id", "as_of_date"],
    )
    # SQLite and PostgreSQL both treat NULLs as distinct in a UNIQUE constraint, so the
    # composite key above cannot dedupe rows whose disclosure filing is unidentifiable.
    # Mirrors the partial-index pattern in 016_activism_campaigns.
    op.create_index(
        "uq_manager_attribution_no_filing",
        "manager_attribution",
        ["manager_id", "security_key", "as_of_date"],
        unique=True,
        sqlite_where=text("filing_id IS NULL"),
        postgresql_where=text("filing_id IS NULL"),
    )


def downgrade() -> None:
    op.drop_index("uq_manager_attribution_no_filing", table_name="manager_attribution")
    op.drop_index("idx_manager_attribution_manager", table_name="manager_attribution")
    op.drop_table("manager_attribution")
