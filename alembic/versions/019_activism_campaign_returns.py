"""Add campaign return metrics backed by the existing free price adapter (#1467)."""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "019"
down_revision = "018"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("activism_campaigns", sa.Column("target_ticker", sa.Text(), nullable=True))
    op.add_column("activism_campaigns", sa.Column("window_return", sa.Float(), nullable=True))
    op.add_column(
        "activism_campaigns", sa.Column("holding_period_days", sa.Integer(), nullable=True)
    )
    op.add_column(
        "activism_campaigns",
        sa.Column("return_computed_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("activism_campaigns", "return_computed_at")
    op.drop_column("activism_campaigns", "holding_period_days")
    op.drop_column("activism_campaigns", "window_return")
    op.drop_column("activism_campaigns", "target_ticker")
