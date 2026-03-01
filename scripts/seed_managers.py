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
                    INSERT INTO managers (name, cik, role, department)
                    VALUES (%s, %s, NULL, NULL)
                    ON CONFLICT (cik) DO UPDATE
                    SET name = EXCLUDED.name
                    RETURNING id
                    """,
                    (manager["name"], manager["cik"]),
                )
                manager_id = cur.fetchone()[0]

                for alias in manager["aliases"]:
                    cur.execute(
                        """
                        INSERT INTO manager_aliases (manager_id, alias)
                        VALUES (%s, %s)
                        ON CONFLICT (manager_id, alias) DO NOTHING
                        """,
                        (manager_id, alias),
                    )
                for jurisdiction in manager["jurisdictions"]:
                    cur.execute(
                        """
                        INSERT INTO manager_jurisdictions (manager_id, jurisdiction)
                        VALUES (%s, %s)
                        ON CONFLICT (manager_id, jurisdiction) DO NOTHING
                        """,
                        (manager_id, jurisdiction),
                    )
                for tag in manager["tags"]:
                    cur.execute(
                        """
                        INSERT INTO manager_tags (manager_id, tag)
                        VALUES (%s, %s)
                        ON CONFLICT (manager_id, tag) DO NOTHING
                        """,
                        (manager_id, tag),
                    )
                inserted += 1
        conn.commit()
    return inserted


def main() -> None:
    count = seed_managers()
    print(f"Seeded {count} manager records.")


if __name__ == "__main__":
    main()
