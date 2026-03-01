#!/usr/bin/env python
"""Resolve official EDGAR names and append them as manager aliases."""

from __future__ import annotations

import argparse
import asyncio
import json
import sqlite3
from dataclasses import dataclass
from typing import Any

import httpx

from adapters.base import connect_db, tracked_call

EDGAR_BASE_URL = "https://data.sec.gov"
DEFAULT_USER_AGENT = "manager-intel/0.1 (resolve-aliases)"


@dataclass
class ManagerRow:
    manager_id: int
    name: str
    cik: str
    aliases_raw: Any


def _normalize_cik(raw: Any) -> str:
    cik = "" if raw is None else str(raw).strip()
    if not cik:
        return ""
    digits = "".join(ch for ch in cik if ch.isdigit())
    if not digits:
        return ""
    return digits.zfill(10)


def _normalize_name(value: str) -> str:
    return " ".join(value.strip().lower().split())


def _parse_aliases(raw: Any) -> list[str]:
    if raw is None:
        return []
    if isinstance(raw, list):
        return [str(item).strip() for item in raw if str(item).strip()]
    if isinstance(raw, tuple):
        return [str(item).strip() for item in raw if str(item).strip()]
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            return [text]
        if isinstance(parsed, list):
            return [str(item).strip() for item in parsed if str(item).strip()]
        return []
    return []


def _manager_id_column(conn: Any) -> str:
    if isinstance(conn, sqlite3.Connection):
        columns = {str(row[1]).lower() for row in conn.execute("PRAGMA table_info(managers)")}
        if "id" in columns:
            return "id"
        if "manager_id" in columns:
            return "manager_id"
        raise RuntimeError("managers table must include an id or manager_id column")

    rows = conn.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = 'managers'
        """).fetchall()
    columns = {str(row[0]).lower() for row in rows if row and row[0] is not None}
    if "id" in columns:
        return "id"
    if "manager_id" in columns:
        return "manager_id"
    raise RuntimeError("managers table must include an id or manager_id column")


def _fetch_managers_with_cik(conn: Any) -> tuple[str, list[ManagerRow]]:
    id_column = _manager_id_column(conn)
    rows = conn.execute(f"""
        SELECT {id_column}, name, cik, aliases
        FROM managers
        WHERE cik IS NOT NULL AND TRIM(cik) != ''
        ORDER BY {id_column}
        """).fetchall()
    managers = [
        ManagerRow(
            manager_id=int(row[0]),
            name=str(row[1] or "").strip(),
            cik=_normalize_cik(row[2]),
            aliases_raw=row[3] if len(row) > 3 else None,
        )
        for row in rows
    ]
    return id_column, [manager for manager in managers if manager.cik and manager.name]


def _encode_aliases_for_db(conn: Any, aliases: list[str]) -> Any:
    if isinstance(conn, sqlite3.Connection):
        return json.dumps(aliases)
    return aliases


def _update_aliases(conn: Any, id_column: str, manager_id: int, aliases: list[str]) -> None:
    placeholder = "?" if isinstance(conn, sqlite3.Connection) else "%s"
    conn.execute(
        f"UPDATE managers SET aliases = {placeholder}, updated_at = CURRENT_TIMESTAMP "
        f"WHERE {id_column} = {placeholder}",
        (_encode_aliases_for_db(conn, aliases), manager_id),
    )


async def fetch_edgar_official_name(
    client: httpx.AsyncClient, cik: str, *, user_agent: str = DEFAULT_USER_AGENT
) -> str | None:
    url = f"{EDGAR_BASE_URL}/submissions/CIK{cik}.json"
    headers = {"User-Agent": user_agent, "Accept": "application/json"}
    async with tracked_call("edgar", url) as log:
        response = await client.get(url, headers=headers)
        log(response)
    response.raise_for_status()
    payload = response.json()
    name = str(payload.get("name") or "").strip()
    return name or None


async def resolve_aliases(
    *,
    dry_run: bool = False,
    db_path: str | None = None,
    user_agent: str = DEFAULT_USER_AGENT,
) -> tuple[int, int, int]:
    conn = connect_db(db_path)
    checked = 0
    alias_additions = 0
    errors = 0

    try:
        id_column, managers = _fetch_managers_with_cik(conn)
        async with httpx.AsyncClient(timeout=30.0) as client:
            for manager in managers:
                checked += 1
                try:
                    official_name = await fetch_edgar_official_name(
                        client, manager.cik, user_agent=user_agent
                    )
                except Exception as exc:
                    errors += 1
                    print(f"EDGAR lookup failed for CIK {manager.cik}: {exc}")
                    continue

                if not official_name:
                    continue
                if _normalize_name(official_name) == _normalize_name(manager.name):
                    continue

                aliases = _parse_aliases(manager.aliases_raw)
                known = {
                    _normalize_name(manager.name),
                    *(_normalize_name(alias) for alias in aliases),
                }
                if _normalize_name(official_name) in known:
                    continue

                updated_aliases = aliases + [official_name]
                alias_additions += 1
                print(
                    f"Added alias for manager {manager.manager_id} "
                    f"(CIK {manager.cik}): {official_name}"
                )
                if not dry_run:
                    _update_aliases(conn, id_column, manager.manager_id, updated_aliases)

        if not dry_run:
            conn.commit()
    finally:
        conn.close()

    return checked, alias_additions, errors


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Resolve manager aliases from EDGAR submissions data"
    )
    parser.add_argument("--dry-run", action="store_true", help="Preview alias changes only")
    parser.add_argument("--db-path", help="Optional database path override")
    parser.add_argument(
        "--user-agent",
        default=DEFAULT_USER_AGENT,
        help="User-Agent for SEC EDGAR requests",
    )
    args = parser.parse_args()

    checked, alias_additions, errors = asyncio.run(
        resolve_aliases(
            dry_run=args.dry_run,
            db_path=args.db_path,
            user_agent=args.user_agent.strip() or DEFAULT_USER_AGENT,
        )
    )
    if args.dry_run:
        print("Dry run complete. No rows written.")
    print(f"Checked managers: {checked}")
    print(f"Aliases added: {alias_additions}")
    print(f"Lookup errors: {errors}")


if __name__ == "__main__":
    main()
