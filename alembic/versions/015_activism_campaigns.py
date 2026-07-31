"""Add materialized activism campaign summaries and timelines.

Revision ID: 015
"""

from alembic import op
import sqlalchemy as sa

revision = "015"
down_revision = "014"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "activism_campaigns",
        sa.Column("campaign_id", sa.BigInteger(), primary_key=True),
        sa.Column("manager_id", sa.BigInteger(), sa.ForeignKey("managers.manager_id"), nullable=False),
        sa.Column("target_identifier", sa.Text(), nullable=False),
        sa.Column("target_company", sa.Text(), nullable=False),
        sa.Column("first_filed", sa.Date(), nullable=False),
        sa.Column("last_filed", sa.Date(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("peak_ownership_pct", sa.Numeric(8, 4)),
        sa.Column("latest_ownership_pct", sa.Numeric(8, 4)),
        sa.Column("filing_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("event_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("latest_event_type", sa.Text()),
        sa.Column("source_forms", sa.Text(), nullable=False, server_default="[]"),
        sa.Column("data_quality_flags", sa.Text(), nullable=False, server_default="[]"),
        sa.Column("computed_at", sa.DateTime(), nullable=False, server_default=sa.func.current_timestamp()),
        sa.UniqueConstraint("manager_id", "target_identifier", name="uq_activism_campaign_manager_target"),
    )
    op.create_index("idx_activism_campaigns_manager", "activism_campaigns", ["manager_id"])
    op.create_index("idx_activism_campaigns_status", "activism_campaigns", ["status"])
    op.create_table(
        "activism_campaign_timeline",
        sa.Column("timeline_id", sa.BigInteger(), primary_key=True),
        sa.Column("campaign_id", sa.BigInteger(), sa.ForeignKey("activism_campaigns.campaign_id"), nullable=False),
        sa.Column("filing_id", sa.BigInteger(), sa.ForeignKey("activism_filings.filing_id"), nullable=False),
        sa.Column("event_id", sa.BigInteger(), sa.ForeignKey("activism_events.event_id")),
        sa.Column("event_date", sa.Date(), nullable=False),
        sa.Column("event_type", sa.Text(), nullable=False),
        sa.Column("form_type", sa.Text(), nullable=False),
        sa.Column("ownership_pct", sa.Numeric(8, 4)),
        sa.Column("summary", sa.Text(), nullable=False),
        sa.Column("source_url", sa.Text()),
        sa.UniqueConstraint("campaign_id", "filing_id", "event_id", name="uq_activism_campaign_timeline_source"),
    )
    op.create_index("idx_activism_campaign_timeline_campaign", "activism_campaign_timeline", ["campaign_id", "event_date"])


def downgrade() -> None:
    op.drop_index("idx_activism_campaign_timeline_campaign", table_name="activism_campaign_timeline")
    op.drop_table("activism_campaign_timeline")
    op.drop_index("idx_activism_campaigns_status", table_name="activism_campaigns")
    op.drop_index("idx_activism_campaigns_manager", table_name="activism_campaigns")
    op.drop_table("activism_campaigns")
