"""Align alert history schema with canonical alert tables.

Revision ID: 008
Revises: 007
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "008"
down_revision = "007"
branch_labels = None
depends_on = None


def _is_postgresql() -> bool:
    return op.get_bind().dialect.name == "postgresql"


def upgrade() -> None:
    if _is_postgresql():
        op.drop_column("alert_history", "rule_name")
        return

    with op.batch_alter_table("alert_history", recreate="always") as batch_op:
        batch_op.drop_column("rule_name")


def downgrade() -> None:
    op.add_column(
        "alert_history",
        sa.Column("rule_name", sa.Text(), nullable=False, server_default=""),
    )
    op.execute("""
        UPDATE alert_history
        SET rule_name = COALESCE(
            (SELECT name FROM alert_rules WHERE alert_rules.rule_id = alert_history.rule_id),
            ''
        )
        """)
