"""Add activism_events table.

Revision ID: 006
Revises: 005
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy import text

from alembic import op

revision = "006"
down_revision = "005"
branch_labels = None
depends_on = None


def _where_clause(sql: str) -> dict[str, sa.TextClause]:
    clause = text(sql)
    if op.get_bind().dialect.name == "postgresql":
        return {"postgresql_where": clause}
    return {"sqlite_where": clause}


def upgrade() -> None:
    op.create_table(
        "activism_events",
        sa.Column("event_id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("manager_id", sa.BigInteger(), nullable=False),
        sa.Column("filing_id", sa.BigInteger(), nullable=False),
        sa.Column("event_type", sa.Text(), nullable=False),
        sa.Column("subject_company", sa.Text(), nullable=False),
        sa.Column("subject_cusip", sa.Text(), nullable=True),
        sa.Column("ownership_pct", sa.Numeric(8, 4), nullable=True),
        sa.Column("previous_pct", sa.Numeric(8, 4), nullable=True),
        sa.Column("delta_pct", sa.Numeric(8, 4), nullable=True),
        sa.Column("threshold_crossed", sa.Numeric(8, 4), nullable=True),
        sa.Column(
            "detected_at",
            sa.DateTime(timezone=True),
            server_default=text("CURRENT_TIMESTAMP"),
        ),
        sa.CheckConstraint(
            "event_type IN ("
            "'initial_stake', 'threshold_crossing', 'stake_increase', 'stake_decrease', "
            "'group_formation', 'amendment', 'form_upgrade', 'form_downgrade'"
            ")",
            name="ck_activism_events_type",
        ),
        sa.ForeignKeyConstraint(
            ["manager_id"], ["managers.manager_id"], name="fk_activism_events_manager_id"
        ),
        sa.ForeignKeyConstraint(
            ["filing_id"], ["activism_filings.filing_id"], name="fk_activism_events_filing_id"
        ),
    )

    op.create_index("idx_activism_events_manager", "activism_events", ["manager_id"])
    op.create_index("idx_activism_events_type", "activism_events", ["event_type"])
    op.create_index("idx_activism_events_date", "activism_events", [sa.text("detected_at DESC")])
    op.create_index("idx_activism_events_cusip", "activism_events", ["subject_cusip"])
    op.create_index(
        "idx_activism_events_unique_base",
        "activism_events",
        ["manager_id", "filing_id", "event_type"],
        unique=True,
        **_where_clause("threshold_crossed IS NULL"),
    )
    op.create_index(
        "idx_activism_events_unique_threshold",
        "activism_events",
        ["manager_id", "filing_id", "event_type", "threshold_crossed"],
        unique=True,
        **_where_clause("threshold_crossed IS NOT NULL"),
    )


def downgrade() -> None:
    op.drop_index("idx_activism_events_unique_threshold", table_name="activism_events")
    op.drop_index("idx_activism_events_unique_base", table_name="activism_events")
    op.drop_index("idx_activism_events_cusip", table_name="activism_events")
    op.drop_index("idx_activism_events_date", table_name="activism_events")
    op.drop_index("idx_activism_events_type", table_name="activism_events")
    op.drop_index("idx_activism_events_manager", table_name="activism_events")
    op.drop_table("activism_events")
