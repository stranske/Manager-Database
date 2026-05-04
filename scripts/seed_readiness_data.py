"""Seed deterministic local data used by readiness smoke checks."""

from __future__ import annotations

import os

READINESS_DOC_TEXT = "Readiness smoke deterministic fact: manager universe bootstrap is healthy."
READINESS_DOC_FILENAME = "readiness-smoke-note.txt"
seed_managers = None
store_document = None


def seed_readiness_data() -> int:
    """Seed baseline managers and one deterministic local research document."""
    global seed_managers, store_document
    if seed_managers is None:
        from scripts.seed_managers import seed_managers as imported_seed_managers

        seed_managers = imported_seed_managers
    if store_document is None:
        from embeddings import store_document as imported_store_document

        store_document = imported_store_document

    # Keep embeddings deterministic and lightweight in local/docker runs.
    os.environ.setdefault("USE_SIMPLE_EMBED", "1")
    seed_managers()
    return store_document(
        READINESS_DOC_TEXT,
        kind="note",
        filename=READINESS_DOC_FILENAME,
    )


def main() -> None:
    doc_id = seed_readiness_data()
    print(f"Seeded readiness document doc_id={doc_id}")


if __name__ == "__main__":
    main()
