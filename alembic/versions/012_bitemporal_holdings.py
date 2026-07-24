"""Add bitemporal holdings columns and current view.

Revision ID: 012
Revises: 011
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy import text

from alembic import op

revision = "012"
down_revision = "011"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("holdings") as batch:
        batch.add_column(sa.Column("content_hash", sa.Text(), nullable=True))
        batch.add_column(
            sa.Column(
                "knowledge_time",
                sa.DateTime(timezone=True),
                nullable=False,
                server_default=sa.text("CURRENT_TIMESTAMP"),
            )
        )
        batch.add_column(sa.Column("superseded_at", sa.DateTime(timezone=True), nullable=True))
        batch.add_column(
            sa.Column("version", sa.Integer(), nullable=False, server_default=sa.text("1"))
        )

    op.execute(
        text(
            """
            UPDATE holdings
               SET knowledge_time = COALESCE(created_at, CURRENT_TIMESTAMP)
             WHERE knowledge_time IS NULL
            """
        )
    )

    op.execute(
        text(
            "CREATE INDEX IF NOT EXISTS idx_holdings_filing_knowledge "
            "ON holdings (filing_id, knowledge_time DESC, holding_id DESC)"
        )
    )
    op.execute(
        text(
            "CREATE INDEX IF NOT EXISTS idx_holdings_current_filing "
            "ON holdings (filing_id) WHERE superseded_at IS NULL"
        )
    )
    op.execute(
        text(
            """
            CREATE VIEW IF NOT EXISTS v_current_holdings AS
            SELECT *
            FROM holdings
            WHERE superseded_at IS NULL
            """
        )
    )


def downgrade() -> None:
    op.execute(text("DROP VIEW IF EXISTS v_current_holdings"))
    op.drop_index("idx_holdings_current_filing", table_name="holdings")
    op.drop_index("idx_holdings_filing_knowledge", table_name="holdings")
    with op.batch_alter_table("holdings") as batch:
        batch.drop_column("version")
        batch.drop_column("superseded_at")
        batch.drop_column("knowledge_time")
        batch.drop_column("content_hash")
