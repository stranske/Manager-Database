"""Close schema.sql and Alembic Postgres parity gaps.

Revision ID: 010
Revises: 009
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "010"
down_revision = "009"
branch_labels = None
depends_on = None


def _is_postgresql() -> bool:
    return op.get_bind().dialect.name == "postgresql"


def upgrade() -> None:
    if _is_postgresql():
        op.execute(
            "CREATE INDEX IF NOT EXISTS idx_news_items_topics_gin "
            "ON news_items USING GIN (topics)"
        )
        op.alter_column(
            "chat_feedback",
            "created_at",
            existing_type=sa.DateTime(timezone=True),
            nullable=False,
            existing_server_default=sa.text("CURRENT_TIMESTAMP"),
        )


def downgrade() -> None:
    if _is_postgresql():
        op.alter_column(
            "chat_feedback",
            "created_at",
            existing_type=sa.DateTime(timezone=True),
            nullable=True,
            existing_server_default=sa.text("CURRENT_TIMESTAMP"),
        )
        op.execute("DROP INDEX IF EXISTS idx_news_items_topics_gin")
