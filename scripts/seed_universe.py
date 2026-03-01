#!/usr/bin/env python
"""Seed the manager universe from JSON or CSV input."""

from __future__ import annotations

import argparse
import csv
import json
import sqlite3
from pathlib import Path
from typing import Any

from adapters.base import connect_db

DEFAULT_ROLE = "Manager"


def _normalize_cik(raw: Any) -> str:
    cik = "" if raw is None else str(raw).strip()
    if not cik:
        return ""
    digits = "".join(ch for ch in cik if ch.isdigit())
    if not digits:
        return ""
    return digits.zfill(10)


def _load_records(path: Path) -> list[dict[str, Any]]:
    suffix = path.suffix.lower()
    if suffix == ".json":
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, list):
            raise ValueError("JSON input must be an array of manager records")
        return [row for row in data if isinstance(row, dict)]

    if suffix == ".csv":
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            return [dict(row) for row in reader]

    raise ValueError("Unsupported input file type. Use .json or .csv")


def _ensure_universe_schema(conn: Any) -> None:
    if isinstance(conn, sqlite3.Connection):
        conn.execute("""
            CREATE TABLE IF NOT EXISTS managers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                role TEXT NOT NULL,
                department TEXT,
                cik TEXT,
                jurisdiction TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """)
        columns = {row[1] for row in conn.execute("PRAGMA table_info(managers)").fetchall()}
        if "cik" not in columns:
            conn.execute("ALTER TABLE managers ADD COLUMN cik TEXT")
        if "jurisdiction" not in columns:
            conn.execute("ALTER TABLE managers ADD COLUMN jurisdiction TEXT")
        if "created_at" not in columns:
            conn.execute("ALTER TABLE managers ADD COLUMN created_at TIMESTAMP")
            conn.execute(
                "UPDATE managers SET created_at = CURRENT_TIMESTAMP WHERE created_at IS NULL"
            )
        if "updated_at" not in columns:
            conn.execute("ALTER TABLE managers ADD COLUMN updated_at TIMESTAMP")
            conn.execute(
                "UPDATE managers SET updated_at = CURRENT_TIMESTAMP WHERE updated_at IS NULL"
            )
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_managers_cik_unique ON managers(cik)")
        conn.commit()
        return

    conn.execute("""
        CREATE TABLE IF NOT EXISTS managers (
            id bigserial PRIMARY KEY,
            name text NOT NULL,
            role text NOT NULL,
            department text,
            cik text,
            jurisdiction text,
            created_at timestamptz DEFAULT now(),
            updated_at timestamptz DEFAULT now()
        )
        """)
    conn.execute("ALTER TABLE managers ADD COLUMN IF NOT EXISTS cik text")
    conn.execute("ALTER TABLE managers ADD COLUMN IF NOT EXISTS jurisdiction text")
    conn.execute(
        "ALTER TABLE managers ADD COLUMN IF NOT EXISTS created_at timestamptz DEFAULT now()"
    )
    conn.execute(
        "ALTER TABLE managers ADD COLUMN IF NOT EXISTS updated_at timestamptz DEFAULT now()"
    )
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_managers_cik_unique ON managers(cik)")


def _existing_ciks(conn: Any) -> set[str]:
    rows = conn.execute(
        "SELECT cik FROM managers WHERE cik IS NOT NULL AND TRIM(cik) != ''"
    ).fetchall()
    return {str(row[0]).strip() for row in rows if row and row[0] is not None}


def seed_universe(file_path: Path, *, dry_run: bool = False) -> tuple[int, int, int]:
    records = _load_records(file_path)
    conn = connect_db()
    created = 0
    updated = 0
    skipped = 0
    try:
        _ensure_universe_schema(conn)
        known_ciks = _existing_ciks(conn)

        for idx, record in enumerate(records):
            name = str(record.get("name", "")).strip()
            cik = _normalize_cik(record.get("cik"))
            jurisdiction = str(record.get("jurisdiction", "")).strip().lower() or None

            if not name or not cik or not jurisdiction:
                skipped += 1
                print(f"Skipping record {idx}: requires name, cik, jurisdiction")
                continue

            if cik in known_ciks:
                updated += 1
            else:
                created += 1
                known_ciks.add(cik)

            if dry_run:
                continue

            if isinstance(conn, sqlite3.Connection):
                conn.execute(
                    """
                    INSERT INTO managers(name, role, cik, jurisdiction, updated_at)
                    VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(cik)
                    DO UPDATE SET
                        name = excluded.name,
                        jurisdiction = excluded.jurisdiction,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    (name, DEFAULT_ROLE, cik, jurisdiction),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO managers(name, role, cik, jurisdiction, updated_at)
                    VALUES (%s, %s, %s, %s, now())
                    ON CONFLICT(cik)
                    DO UPDATE SET
                        name = EXCLUDED.name,
                        jurisdiction = EXCLUDED.jurisdiction,
                        updated_at = now()
                    """,
                    (name, DEFAULT_ROLE, cik, jurisdiction),
                )

        if dry_run:
            print("Dry run complete. No rows written.")
            return created, updated, skipped

        conn.commit()
        return created, updated, skipped
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed manager universe records")
    parser.add_argument("--file", required=True, help="Path to .json or .csv input file")
    parser.add_argument("--dry-run", action="store_true", help="Preview changes without writing")
    args = parser.parse_args()

    file_path = Path(args.file)
    if not file_path.exists():
        raise SystemExit(f"Input file not found: {file_path}")

    created, updated, skipped = seed_universe(file_path, dry_run=args.dry_run)
    print(f"Created: {created}")
    print(f"Updated: {updated}")
    print(f"Skipped: {skipped}")


if __name__ == "__main__":
    main()
