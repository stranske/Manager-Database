"""Seed deterministic local data used by readiness smoke checks."""

from __future__ import annotations

import os
import sqlite3
from collections.abc import Callable

from adapters.base import connect_db

READINESS_DOC_TEXT = "Readiness smoke deterministic fact: manager universe bootstrap is healthy."
READINESS_DOC_FILENAME = "readiness-smoke-note.txt"
READINESS_MANAGER_CIK = "0001791786"


def _resolve_seeded_manager_id(cik: str) -> int | None:
    """Return a manager ID for ``cik`` when the managers table is available."""
    conn = connect_db()
    try:
        row = None
        if isinstance(conn, sqlite3.Connection):
            row = conn.execute(
                "SELECT manager_id FROM managers WHERE cik = ? LIMIT 1",
                (cik,),
            ).fetchone()
        else:
            row = conn.execute(
                "SELECT manager_id FROM managers WHERE cik = %s LIMIT 1",
                (cik,),
            ).fetchone()
        return int(row[0]) if row else None
    finally:
        conn.close()


def seed_readiness_data(
    seed_managers_fn: Callable[[], int] | None = None,
    store_document_fn: Callable[..., int] | None = None,
    resolve_manager_id_fn: Callable[[str], int | None] | None = None,
) -> int:
    """Seed baseline managers and one deterministic local research document."""
    # Keep embeddings deterministic and lightweight in local/docker runs.
    os.environ.setdefault("USE_SIMPLE_EMBED", "1")

    if seed_managers_fn is None:
        from scripts.seed_managers import seed_managers

        seed_managers_fn = seed_managers
    if store_document_fn is None:
        from embeddings import store_document

        store_document_fn = store_document
    if resolve_manager_id_fn is None:
        resolve_manager_id_fn = _resolve_seeded_manager_id

    seed_managers_fn()
    manager_id = resolve_manager_id_fn(READINESS_MANAGER_CIK)
    return store_document_fn(
        READINESS_DOC_TEXT,
        manager_id=manager_id,
        kind="note",
        filename=READINESS_DOC_FILENAME,
    )


def main() -> None:
    doc_id = seed_readiness_data()
    print(f"Seeded readiness document doc_id={doc_id}")


if __name__ == "__main__":
    main()
