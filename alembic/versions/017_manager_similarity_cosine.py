"""Store an embedding-cosine score alongside Jaccard similarity.

Revision ID: 017
Revises: 016
"""

import sqlalchemy as sa

from alembic import op

revision = "017"
down_revision = "016"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("manager_similarity", sa.Column("cosine", sa.REAL(), nullable=True))


def downgrade() -> None:
    op.drop_column("manager_similarity", "cosine")
