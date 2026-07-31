"""Add activism document references for campaign archive profiles (#1468)."""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "021"
down_revision = "020"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "activism_documents",
        sa.Column("document_id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column(
            "campaign_id",
            sa.Integer(),
            sa.ForeignKey("activism_campaigns.campaign_id"),
            nullable=False,
        ),
        sa.Column(
            "filing_id", sa.Integer(), sa.ForeignKey("activism_filings.filing_id"), nullable=False
        ),
        sa.Column("doc_type", sa.Text(), nullable=False),
        sa.Column("source_url", sa.Text(), nullable=True),
        sa.Column("raw_key", sa.Text(), nullable=True),
        sa.Column("filed_date", sa.Date(), nullable=False),
        sa.UniqueConstraint("campaign_id", "filing_id", "doc_type", "source_url"),
    )
    op.create_index(
        "idx_activism_documents_campaign", "activism_documents", ["campaign_id", "filed_date"]
    )


def downgrade() -> None:
    op.drop_index("idx_activism_documents_campaign", table_name="activism_documents")
    op.drop_table("activism_documents")
