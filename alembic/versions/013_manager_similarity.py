"""Add manager similarity table.

Revision ID: 013
Revises: 012
"""

import sqlalchemy as sa

from alembic import op

revision = "013"
down_revision = "012"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "manager_similarity",
        sa.Column(
            "manager_id_a", sa.BigInteger(), sa.ForeignKey("managers.manager_id"), nullable=False
        ),
        sa.Column(
            "manager_id_b", sa.BigInteger(), sa.ForeignKey("managers.manager_id"), nullable=False
        ),
        sa.Column("jaccard", sa.REAL(), nullable=False),
        sa.Column("overlap_count", sa.Integer(), nullable=False),
        sa.Column("union_count", sa.Integer(), nullable=False),
        sa.Column(
            "computed_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.PrimaryKeyConstraint("manager_id_a", "manager_id_b"),
        sa.CheckConstraint("manager_id_a < manager_id_b"),
    )
    op.create_index("idx_manager_similarity_a", "manager_similarity", ["manager_id_a"])
    op.create_index("idx_manager_similarity_b", "manager_similarity", ["manager_id_b"])


def downgrade() -> None:
    op.drop_table("manager_similarity")
