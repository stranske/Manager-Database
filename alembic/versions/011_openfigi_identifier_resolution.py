"""Add OpenFIGI identifier resolution cache.

Revision ID: 011
Revises: 010
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "011"
down_revision = "010"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "identifier_resolution_cache",
        sa.Column("cusip", sa.Text(), primary_key=True),
        sa.Column("ticker", sa.Text(), nullable=True),
        sa.Column("figi", sa.Text(), nullable=True),
        sa.Column("composite_figi", sa.Text(), nullable=True),
        sa.Column("share_class_figi", sa.Text(), nullable=True),
        sa.Column("isin", sa.Text(), nullable=True),
        sa.Column("lei", sa.Text(), nullable=True),
        sa.Column("name", sa.Text(), nullable=True),
        sa.Column("source", sa.Text(), nullable=False, server_default=sa.text("'openfigi'")),
        sa.Column(
            "resolved_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
    )
    op.create_table(
        "identifier_resolution_metrics",
        sa.Column("metric_id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("filing_id", sa.BigInteger(), nullable=True),
        sa.Column("total_cusips", sa.Integer(), nullable=False),
        sa.Column("unmapped_cusips", sa.Integer(), nullable=False),
        sa.Column("unmapped_cusip_rate", sa.Float(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
    )
    with op.batch_alter_table("holdings") as batch:
        batch.add_column(sa.Column("resolved_ticker", sa.Text(), nullable=True))
        batch.add_column(sa.Column("resolved_figi", sa.Text(), nullable=True))
        batch.add_column(sa.Column("resolved_lei", sa.Text(), nullable=True))
        batch.add_column(sa.Column("resolution_source", sa.Text(), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("holdings") as batch:
        batch.drop_column("resolution_source")
        batch.drop_column("resolved_lei")
        batch.drop_column("resolved_figi")
        batch.drop_column("resolved_ticker")
    op.drop_table("identifier_resolution_metrics")
    op.drop_table("identifier_resolution_cache")
