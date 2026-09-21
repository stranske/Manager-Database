"""Add exact object provenance and durable manager-erasure manifests."""

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "023"
down_revision = "022"
branch_labels = None
depends_on = None


def _ambiguous_keys_type():
    if op.get_bind().dialect.name == "postgresql":
        return postgresql.JSONB(astext_type=sa.Text())
    return sa.JSON()


def upgrade() -> None:
    inspector = sa.inspect(op.get_bind())
    filing_columns = {column["name"] for column in inspector.get_columns("filings")}
    if "storage_key" not in filing_columns:
        op.add_column("filings", sa.Column("storage_key", sa.Text(), nullable=True))

    if not inspector.has_table("manager_deletion_operations"):
        op.create_table(
            "manager_deletion_operations",
            sa.Column("operation_id", sa.Text(), nullable=False),
            sa.Column("manager_id", sa.BigInteger(), nullable=False),
            sa.Column("state", sa.Text(), nullable=False),
            sa.Column(
                "ambiguous_keys",
                _ambiguous_keys_type(),
                nullable=False,
                server_default=(
                    sa.text("'[]'::jsonb")
                    if op.get_bind().dialect.name == "postgresql"
                    else sa.text("'[]'")
                ),
            ),
            sa.Column("error", sa.Text(), nullable=True),
            sa.Column(
                "created_at",
                sa.DateTime(timezone=True),
                nullable=False,
                server_default=sa.func.now(),
            ),
            sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
            sa.PrimaryKeyConstraint("operation_id"),
        )
    inspector = sa.inspect(op.get_bind())
    if not inspector.has_table("manager_deletion_objects"):
        op.create_table(
            "manager_deletion_objects",
            sa.Column("operation_id", sa.Text(), nullable=False),
            sa.Column("bucket", sa.Text(), nullable=False),
            sa.Column("object_key", sa.Text(), nullable=False),
            sa.Column("state", sa.Text(), nullable=False, server_default="pending"),
            sa.Column("error", sa.Text(), nullable=True),
            sa.ForeignKeyConstraint(
                ["operation_id"],
                ["manager_deletion_operations.operation_id"],
                ondelete="CASCADE",
            ),
            sa.PrimaryKeyConstraint("operation_id", "bucket", "object_key"),
        )


def downgrade() -> None:
    inspector = sa.inspect(op.get_bind())
    if inspector.has_table("manager_deletion_objects"):
        op.drop_table("manager_deletion_objects")
    inspector = sa.inspect(op.get_bind())
    if inspector.has_table("manager_deletion_operations"):
        op.drop_table("manager_deletion_operations")
    # ``upgrade`` may encounter a storage_key column provisioned by schema.sql.
    # Alembic cannot determine column ownership after the fact, so preserve this
    # nullable provenance column rather than risk deleting pre-existing values.
