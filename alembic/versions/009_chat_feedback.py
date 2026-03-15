"""Add chat feedback persistence table.

Revision ID: 009
Revises: 008
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy import text

from alembic import op

revision = "009"
down_revision = "008"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "chat_feedback",
        sa.Column("feedback_id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("response_id", sa.Text(), nullable=False),
        sa.Column("rating", sa.Integer(), nullable=False),
        sa.Column("comment", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=True,
            server_default=text("CURRENT_TIMESTAMP"),
        ),
        sa.CheckConstraint("rating BETWEEN 1 AND 5", name="ck_chat_feedback_rating_range"),
    )
    op.create_index("idx_chat_feedback_response_id", "chat_feedback", ["response_id"])


def downgrade() -> None:
    op.drop_index("idx_chat_feedback_response_id", table_name="chat_feedback")
    op.drop_table("chat_feedback")
