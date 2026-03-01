"""Add unique partial index on filings.raw_key.

Without this constraint, ON CONFLICT DO NOTHING in edgar_flow.py has no
conflict target on Postgres and every INSERT succeeds — producing duplicate
filings on re-runs.  The SQLite fallback already defines raw_key as UNIQUE
inline, so this migration brings Postgres into alignment.

Revision ID: 002
Revises: 001
"""

from __future__ import annotations

from sqlalchemy import text

from alembic import op

revision = "002"
down_revision = "001"
branch_labels = None
depends_on = None


def _is_postgresql() -> bool:
    return op.get_bind().dialect.name == "postgresql"


def upgrade() -> None:
    if _is_postgresql():
        op.create_index(
            "idx_filings_raw_key_unique",
            "filings",
            ["raw_key"],
            unique=True,
            postgresql_where=text("raw_key IS NOT NULL"),
        )
    else:
        # SQLite: recreating the index is harmless; the inline UNIQUE already
        # covers this, but we add an explicit index for parity.
        op.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_filings_raw_key_unique "
            "ON filings (raw_key) WHERE raw_key IS NOT NULL"
        )


def downgrade() -> None:
    op.drop_index("idx_filings_raw_key_unique", table_name="filings")
