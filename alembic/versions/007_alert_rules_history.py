"""Add alert rules and alert history tables.

Revision ID: 007
Revises: 006
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy import text
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "007"
down_revision = "006"
branch_labels = None
depends_on = None


def _is_postgresql() -> bool:
    return op.get_bind().dialect.name == "postgresql"


def _array_or_text():
    if _is_postgresql():
        return sa.ARRAY(sa.Text())
    return sa.Text()


def _json_type():
    if _is_postgresql():
        return postgresql.JSONB(astext_type=sa.Text())
    return sa.Text()


def upgrade() -> None:
    pg = _is_postgresql()

    op.create_table(
        "alert_rules",
        sa.Column("rule_id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("event_type", sa.Text(), nullable=False),
        sa.Column("condition_json", _json_type(), nullable=False, server_default="{}"),
        sa.Column(
            "channels",
            _array_or_text(),
            nullable=False,
            server_default=text("ARRAY['streamlit']") if pg else '["streamlit"]',
        ),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("manager_id", sa.BigInteger(), nullable=True),
        sa.Column("created_by", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=text("CURRENT_TIMESTAMP"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=text("CURRENT_TIMESTAMP"),
        ),
        sa.CheckConstraint(
            "event_type IN ("
            "'new_filing', 'large_delta', 'news_spike', 'crowded_trade_change', "
            "'contrarian_signal', 'missing_filing', 'etl_failure', 'activism_event'"
            ")",
            name="ck_alert_rules_event_type",
        ),
        sa.ForeignKeyConstraint(
            ["manager_id"], ["managers.manager_id"], name="fk_alert_rules_manager_id"
        ),
    )
    if pg:
        op.create_index(
            "idx_alert_rules_event",
            "alert_rules",
            ["event_type"],
            postgresql_where=text("enabled = true"),
        )
    else:
        op.create_index("idx_alert_rules_event", "alert_rules", ["event_type"])

    op.create_table(
        "alert_history",
        sa.Column("alert_id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("rule_id", sa.BigInteger(), nullable=False),
        sa.Column("rule_name", sa.Text(), nullable=False),
        sa.Column(
            "fired_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=text("CURRENT_TIMESTAMP"),
        ),
        sa.Column("event_type", sa.Text(), nullable=False),
        sa.Column("payload_json", _json_type(), nullable=False),
        sa.Column(
            "delivered_channels",
            _array_or_text(),
            nullable=False,
            server_default=text("ARRAY[]::text[]") if pg else "[]",
        ),
        sa.Column("delivery_errors", _json_type(), nullable=True),
        sa.Column("acknowledged", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("acknowledged_by", sa.Text(), nullable=True),
        sa.Column("acknowledged_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(
            ["rule_id"], ["alert_rules.rule_id"], name="fk_alert_history_rule_id"
        ),
    )
    if pg:
        op.create_index(
            "idx_alert_history_unack",
            "alert_history",
            [sa.text("fired_at DESC")],
            postgresql_where=text("acknowledged = false"),
        )
    else:
        op.create_index("idx_alert_history_unack", "alert_history", ["fired_at"])
    op.create_index("idx_alert_history_rule", "alert_history", ["rule_id"])


def downgrade() -> None:
    op.drop_index("idx_alert_history_rule", table_name="alert_history")
    op.drop_index("idx_alert_history_unack", table_name="alert_history")
    op.drop_table("alert_history")
    op.drop_index("idx_alert_rules_event", table_name="alert_rules")
    op.drop_table("alert_rules")
