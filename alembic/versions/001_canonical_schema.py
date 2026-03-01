"""Initial canonical schema."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "001"
down_revision = None
branch_labels = None
depends_on = None


def _is_postgresql() -> bool:
    return op.get_bind().dialect.name == "postgresql"


def upgrade() -> None:
    if _is_postgresql():
        op.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")
        op.execute("CREATE EXTENSION IF NOT EXISTS vector")

    op.create_table(
        "managers",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("cik", sa.String(length=10), nullable=False),
        sa.Column("role", sa.Text(), nullable=True),
        sa.Column("department", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.UniqueConstraint("cik", name="uq_managers_cik"),
    )
    op.create_table(
        "manager_aliases",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("manager_id", sa.Integer(), nullable=False),
        sa.Column("alias", sa.Text(), nullable=False),
        sa.ForeignKeyConstraint(
            ["manager_id"], ["managers.id"], name="fk_manager_aliases_manager_id"
        ),
        sa.UniqueConstraint("manager_id", "alias", name="uq_manager_aliases_manager_alias"),
    )
    op.create_table(
        "manager_jurisdictions",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("manager_id", sa.Integer(), nullable=False),
        sa.Column("jurisdiction", sa.Text(), nullable=False),
        sa.ForeignKeyConstraint(
            ["manager_id"], ["managers.id"], name="fk_manager_jurisdictions_manager_id"
        ),
        sa.UniqueConstraint(
            "manager_id",
            "jurisdiction",
            name="uq_manager_jurisdictions_manager_jurisdiction",
        ),
    )
    op.create_table(
        "manager_tags",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("manager_id", sa.Integer(), nullable=False),
        sa.Column("tag", sa.Text(), nullable=False),
        sa.ForeignKeyConstraint(["manager_id"], ["managers.id"], name="fk_manager_tags_manager_id"),
        sa.UniqueConstraint("manager_id", "tag", name="uq_manager_tags_manager_tag"),
    )
    op.create_table(
        "filings",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("manager_id", sa.Integer(), nullable=False),
        sa.Column("accession", sa.Text(), nullable=False),
        sa.Column("filed", sa.Date(), nullable=False),
        sa.Column("form_type", sa.Text(), nullable=False, server_default=sa.text("'13F-HR'")),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.ForeignKeyConstraint(["manager_id"], ["managers.id"], name="fk_filings_manager_id"),
        sa.UniqueConstraint("accession", name="uq_filings_accession"),
    )
    op.create_table(
        "holdings",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("filing_id", sa.Integer(), nullable=False),
        sa.Column("manager_id", sa.Integer(), nullable=False),
        sa.Column("name_of_issuer", sa.Text(), nullable=True),
        sa.Column("cusip", sa.Text(), nullable=False),
        sa.Column("value", sa.BigInteger(), nullable=True),
        sa.Column("ssh_prnamt", sa.BigInteger(), nullable=True),
        sa.ForeignKeyConstraint(["filing_id"], ["filings.id"], name="fk_holdings_filing_id"),
        sa.ForeignKeyConstraint(["manager_id"], ["managers.id"], name="fk_holdings_manager_id"),
        sa.UniqueConstraint("filing_id", "cusip", name="uq_holdings_filing_cusip"),
    )
    op.create_table(
        "api_usage",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column(
            "ts",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.Column("source", sa.Text(), nullable=True),
        sa.Column("endpoint", sa.Text(), nullable=True),
        sa.Column("status", sa.Integer(), nullable=True),
        sa.Column("bytes", sa.Integer(), nullable=True),
        sa.Column("latency_ms", sa.Integer(), nullable=True),
        sa.Column("cost_usd", sa.Numeric(10, 4), nullable=True),
    )

    if _is_postgresql():
        op.execute("""
            CREATE MATERIALIZED VIEW monthly_usage AS
            SELECT date_trunc('month', ts) AS month,
                   source,
                   count(*) AS calls,
                   sum(bytes) AS mb,
                   sum(cost_usd) AS cost
            FROM api_usage
            GROUP BY 1,2
            """)
        op.execute("""
            CREATE MATERIALIZED VIEW manager_holdings_summary AS
            SELECT m.id AS manager_id,
                   m.name AS manager_name,
                   max(f.filed) AS latest_filed,
                   count(h.id) AS holdings_count,
                   coalesce(sum(h.value), 0) AS total_value
            FROM managers m
            LEFT JOIN filings f ON f.manager_id = m.id
            LEFT JOIN holdings h ON h.filing_id = f.id
            GROUP BY m.id, m.name
            """)
    else:
        op.execute("""
            CREATE VIEW monthly_usage AS
            SELECT strftime('%Y-%m-01', ts) AS month,
                   source,
                   count(*) AS calls,
                   sum(bytes) AS mb,
                   sum(cost_usd) AS cost
            FROM api_usage
            GROUP BY 1,2
            """)
        op.execute("""
            CREATE VIEW manager_holdings_summary AS
            SELECT m.id AS manager_id,
                   m.name AS manager_name,
                   max(f.filed) AS latest_filed,
                   count(h.id) AS holdings_count,
                   coalesce(sum(h.value), 0) AS total_value
            FROM managers m
            LEFT JOIN filings f ON f.manager_id = m.id
            LEFT JOIN holdings h ON h.filing_id = f.id
            GROUP BY m.id, m.name
            """)


def downgrade() -> None:
    if _is_postgresql():
        op.execute("DROP MATERIALIZED VIEW IF EXISTS manager_holdings_summary")
        op.execute("DROP MATERIALIZED VIEW IF EXISTS monthly_usage")
    else:
        op.execute("DROP VIEW IF EXISTS manager_holdings_summary")
        op.execute("DROP VIEW IF EXISTS monthly_usage")

    op.drop_table("api_usage")
    op.drop_table("holdings")
    op.drop_table("filings")
    op.drop_table("manager_tags")
    op.drop_table("manager_jurisdictions")
    op.drop_table("manager_aliases")
    op.drop_table("managers")
