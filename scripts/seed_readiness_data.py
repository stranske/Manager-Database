"""Seed deterministic local data used by readiness smoke checks."""

from __future__ import annotations

import os
from collections.abc import Callable

READINESS_DOC_TEXT = "Readiness smoke deterministic fact: manager universe bootstrap is healthy."
READINESS_DOC_FILENAME = "readiness-smoke-note.txt"


def seed_readiness_data(
    seed_managers_fn: Callable[[], int] | None = None,
    store_document_fn: Callable[..., int] | None = None,
) -> int:
    """Seed baseline managers and one deterministic local research document."""
    if seed_managers_fn is None:
        from scripts.seed_managers import seed_managers as seed_managers_fn
    if store_document_fn is None:
        from embeddings import store_document as store_document_fn

    # Keep embeddings deterministic and lightweight in local/docker runs.
    os.environ.setdefault("USE_SIMPLE_EMBED", "1")
    seed_managers_fn()
    return store_document_fn(
        READINESS_DOC_TEXT,
        kind="note",
        filename=READINESS_DOC_FILENAME,
    )


def main() -> None:
    doc_id = seed_readiness_data()
    print(f"Seeded readiness document doc_id={doc_id}")


if __name__ == "__main__":
    main()
