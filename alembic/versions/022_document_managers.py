"""Preserve explicit manager associations for deduplicated documents (#1669)."""

import sqlalchemy as sa

from alembic import op

revision = "022"
down_revision = "021"
branch_labels = None
depends_on = None


def upgrade() -> None:
    inspector = sa.inspect(op.get_bind())
    if not inspector.has_table("document_managers"):
        op.create_table(
            "document_managers",
            sa.Column("doc_id", sa.BigInteger(), nullable=False),
            sa.Column("manager_id", sa.BigInteger(), nullable=False),
            sa.ForeignKeyConstraint(
                ["doc_id"],
                ["documents.doc_id"],
                name=op.f("fk_document_managers_doc_id_documents"),
                ondelete="CASCADE",
            ),
            sa.ForeignKeyConstraint(
                ["manager_id"],
                ["managers.manager_id"],
                name=op.f("fk_document_managers_manager_id_managers"),
                ondelete="CASCADE",
            ),
            sa.PrimaryKeyConstraint("doc_id", "manager_id", name=op.f("pk_document_managers")),
        )
    indexes = {index["name"] for index in inspector.get_indexes("document_managers")}
    if "idx_document_managers_manager" not in indexes:
        op.create_index(
            "idx_document_managers_manager", "document_managers", ["manager_id", "doc_id"]
        )
    op.execute(
        "INSERT INTO document_managers (doc_id, manager_id) "
        "SELECT doc_id, manager_id FROM documents WHERE manager_id IS NOT NULL "
        "ON CONFLICT (doc_id, manager_id) DO NOTHING"
    )


def downgrade() -> None:
    op.drop_index("idx_document_managers_manager", table_name="document_managers")
    op.drop_table("document_managers")
