"""Seed the database with baseline manager records."""

from __future__ import annotations

import os

import psycopg

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


def _db_url() -> str:
    return os.getenv("DB_URL", "postgresql://postgres:postgres@localhost:5432/postgres")


def seed_managers() -> int:
    inserted = 0
    with psycopg.connect(_db_url()) as conn:
        with conn.cursor() as cur:
            for manager in SEED_MANAGERS:
                cur.execute(
                    """
                    INSERT INTO managers (name, cik, aliases, jurisdictions, tags)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (cik) WHERE cik IS NOT NULL DO UPDATE
                    SET name = EXCLUDED.name,
                        aliases = EXCLUDED.aliases,
                        jurisdictions = EXCLUDED.jurisdictions,
                        tags = EXCLUDED.tags,
                        updated_at = now()
                    RETURNING manager_id
                    """,
                    (
                        manager["name"],
                        manager["cik"],
                        manager["aliases"],
                        manager["jurisdictions"],
                        manager["tags"],
                    ),
                )
                cur.fetchone()
                inserted += 1
        conn.commit()
    return inserted


def main() -> None:
    count = seed_managers()
    print(f"Seeded {count} manager records.")


if __name__ == "__main__":
    main()
