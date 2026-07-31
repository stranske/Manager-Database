"""Store an embedding-cosine score alongside Jaccard similarity.

Revision ID: 016
Revises: 015
"""

import sqlalchemy as sa

from alembic import op

revision = "016"
down_revision = "015"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("manager_similarity", sa.Column("cosine", sa.REAL(), nullable=True))


def downgrade() -> None:
    op.drop_column("manager_similarity", "cosine")
