"""Seed the database with baseline manager records."""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from adapters.base import connect_db  # noqa: E402

SEED_MANAGERS = [
    {
        "name": "Elliott Investment Management L.P.",
        "cik": "0001791786",
        "jurisdictions": ["us"],
        "tags": ["activist", "multi-strategy"],
        "aliases": ["Elliott Management"],
    },
    {
        "name": "SIR Capital Management L.P.",
        "cik": "0001434997",
        "jurisdictions": ["us"],
        "tags": ["energy", "hedge-fund"],
        "aliases": ["Standard Investment Research"],
    },
]


def _ensure_sqlite_schema(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS managers (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            cik TEXT,
            aliases TEXT NOT NULL DEFAULT '[]',
            jurisdictions TEXT NOT NULL DEFAULT '[]',
            tags TEXT NOT NULL DEFAULT '[]',
            registry_ids TEXT NOT NULL DEFAULT '{}',
            quality_flags TEXT NOT NULL DEFAULT '[]',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_managers_cik ON managers(cik)")


def _sqlite_values(values: list[str]) -> str:
    import json

    return json.dumps(values)


def _upsert_sqlite_manager(conn: sqlite3.Connection, manager: dict[str, Any]) -> bool:
    cik = str(manager["cik"])
    existed = conn.execute("SELECT 1 FROM managers WHERE cik = ?", (cik,)).fetchone() is not None
    conn.execute(
        """
        INSERT INTO managers (name, cik, aliases, jurisdictions, tags, updated_at)
        VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(cik) DO UPDATE
        SET name = excluded.name,
            aliases = excluded.aliases,
            jurisdictions = excluded.jurisdictions,
            tags = excluded.tags,
            updated_at = CURRENT_TIMESTAMP
        """,
        (
            manager["name"],
            cik,
            _sqlite_values(manager["aliases"]),
            _sqlite_values(manager["jurisdictions"]),
            _sqlite_values(manager["tags"]),
        ),
    )
    return not existed


def _upsert_postgres_manager(conn, manager: dict[str, Any]) -> bool:
    row = conn.execute(
        """
        INSERT INTO managers (name, cik, aliases, jurisdictions, tags)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (cik) WHERE cik IS NOT NULL DO UPDATE
        SET name = EXCLUDED.name,
            aliases = EXCLUDED.aliases,
            jurisdictions = EXCLUDED.jurisdictions,
            tags = EXCLUDED.tags,
            updated_at = now()
        RETURNING (xmax = 0) AS inserted
        """,
        (
            manager["name"],
            manager["cik"],
            manager["aliases"],
            manager["jurisdictions"],
            manager["tags"],
        ),
    ).fetchone()
    return bool(row and row[0])


def seed_managers() -> int:
    inserted = 0
    conn = connect_db()
    try:
        if isinstance(conn, sqlite3.Connection):
            _ensure_sqlite_schema(conn)
            for manager in SEED_MANAGERS:
                if _upsert_sqlite_manager(conn, manager):
                    inserted += 1
            conn.commit()
        else:
            with conn.transaction():
                for manager in SEED_MANAGERS:
                    if _upsert_postgres_manager(conn, manager):
                        inserted += 1
    finally:
        conn.close()
    return inserted


def main() -> None:
    count = seed_managers()
    print(f"Inserted {count} new manager records.")


if __name__ == "__main__":
    main()
