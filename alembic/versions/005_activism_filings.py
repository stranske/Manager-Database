"""Add activism_filings table.

Revision ID: 005
Revises: 004
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy import text

from alembic import op

revision = "005"
down_revision = "004"
branch_labels = None
depends_on = None


def _is_postgresql() -> bool:
    return op.get_bind().dialect.name == "postgresql"


def _group_members_type():
    if _is_postgresql():
        return sa.ARRAY(sa.Text())
    return sa.Text()


def upgrade() -> None:
    op.create_table(
        "activism_filings",
        sa.Column("filing_id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("manager_id", sa.BigInteger(), nullable=False),
        sa.Column("filing_type", sa.Text(), nullable=False),
        sa.Column("subject_company", sa.Text(), nullable=False),
        sa.Column("subject_cusip", sa.Text(), nullable=True),
        sa.Column("ownership_pct", sa.Numeric(8, 4), nullable=True),
        sa.Column("shares", sa.BigInteger(), nullable=True),
        sa.Column("group_members", _group_members_type(), server_default="{}"),
        sa.Column("purpose_snippet", sa.Text(), nullable=True),
        sa.Column("filed_date", sa.Date(), nullable=False),
        sa.Column("url", sa.Text(), nullable=False),
        sa.Column("raw_key", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=text("CURRENT_TIMESTAMP"),
        ),
        sa.CheckConstraint(
            "filing_type IN ('SC 13D', 'SC 13D/A', 'SC 13G', 'SC 13G/A')",
            name="ck_activism_filings_type",
        ),
        sa.ForeignKeyConstraint(
            ["manager_id"], ["managers.manager_id"], name="fk_activism_filings_manager_id"
        ),
        sa.UniqueConstraint(
            "manager_id",
            "filing_type",
            "subject_cusip",
            "filed_date",
            name="uq_activism_filings_manager_type_cusip_filed_date",
        ),
    )

    op.create_index("idx_activism_manager", "activism_filings", ["manager_id"])
    op.create_index("idx_activism_cusip", "activism_filings", ["subject_cusip"])
    op.create_index("idx_activism_date", "activism_filings", [sa.text("filed_date DESC")])


def downgrade() -> None:
    op.drop_index("idx_activism_date", table_name="activism_filings")
    op.drop_index("idx_activism_cusip", table_name="activism_filings")
    op.drop_index("idx_activism_manager", table_name="activism_filings")
    op.drop_table("activism_filings")
