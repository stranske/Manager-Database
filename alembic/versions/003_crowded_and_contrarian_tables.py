"""Add crowded_trades and contrarian_signals tables.

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


def _is_postgresql() -> bool:
    return op.get_bind().dialect.name == "postgresql"


def _manager_ids_type():
    if _is_postgresql():
        return sa.ARRAY(sa.BigInteger())
    return sa.Text()


def upgrade() -> None:
    op.create_table(
        "crowded_trades",
        sa.Column("crowd_id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("cusip", sa.Text(), nullable=False),
        sa.Column("name_of_issuer", sa.Text(), nullable=True),
        sa.Column("manager_count", sa.Integer(), nullable=False),
        sa.Column("manager_ids", _manager_ids_type(), nullable=False),
        sa.Column("total_value_usd", sa.Numeric(18, 2), nullable=True),
        sa.Column("avg_conviction_pct", sa.Numeric(8, 4), nullable=True),
        sa.Column("max_conviction_pct", sa.Numeric(8, 4), nullable=True),
        sa.Column("report_date", sa.Date(), nullable=False),
        sa.Column(
            "computed_at",
            sa.DateTime(timezone=True),
            server_default=text("CURRENT_TIMESTAMP"),
        ),
        sa.UniqueConstraint("cusip", "report_date", name="uq_crowded_trades_cusip_report_date"),
    )

    op.create_index("idx_crowded_date", "crowded_trades", [sa.text("report_date DESC")])
    op.create_index("idx_crowded_count", "crowded_trades", [sa.text("manager_count DESC")])

    op.create_table(
        "contrarian_signals",
        sa.Column("signal_id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("manager_id", sa.BigInteger(), nullable=False),
        sa.Column("cusip", sa.Text(), nullable=False),
        sa.Column("name_of_issuer", sa.Text(), nullable=True),
        sa.Column("direction", sa.Text(), nullable=False),
        sa.Column("consensus_direction", sa.Text(), nullable=False),
        sa.Column("manager_delta_shares", sa.BigInteger(), nullable=True),
        sa.Column("manager_delta_value", sa.Numeric(16, 2), nullable=True),
        sa.Column("consensus_count", sa.Integer(), nullable=True),
        sa.Column("report_date", sa.Date(), nullable=False),
        sa.Column(
            "detected_at",
            sa.DateTime(timezone=True),
            server_default=text("CURRENT_TIMESTAMP"),
        ),
        sa.CheckConstraint(
            "direction IN ('BUY', 'SELL', 'INCREASE', 'DECREASE')",
            name="ck_contrarian_signals_direction",
        ),
        sa.CheckConstraint(
            "consensus_direction IN ('BUY', 'SELL', 'INCREASE', 'DECREASE', 'HOLD')",
            name="ck_contrarian_signals_consensus_direction",
        ),
        sa.ForeignKeyConstraint(
            ["manager_id"], ["managers.manager_id"], name="fk_contrarian_signals_manager_id"
        ),
        sa.UniqueConstraint(
            "manager_id",
            "cusip",
            "report_date",
            name="uq_contrarian_signals_manager_cusip_report_date",
        ),
    )

    op.create_index("idx_contrarian_manager", "contrarian_signals", ["manager_id"])
    op.create_index("idx_contrarian_date", "contrarian_signals", [sa.text("report_date DESC")])


def downgrade() -> None:
    op.drop_index("idx_contrarian_date", table_name="contrarian_signals")
    op.drop_index("idx_contrarian_manager", table_name="contrarian_signals")
    op.drop_table("contrarian_signals")

    op.drop_index("idx_crowded_count", table_name="crowded_trades")
    op.drop_index("idx_crowded_date", table_name="crowded_trades")
    op.drop_table("crowded_trades")
