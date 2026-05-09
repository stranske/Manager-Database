# Dialect Portability Audit

Issue: #980

Generated: 2026-05-09

## Summary

The Manager-Database runtime can use SQLite through `DB_PATH` or Postgres through
`DB_URL`. This audit records production modules that still contain SQLite-only
DDL or inspection tokens on paths that can share the same backend-flexible
connection surface. The paired gate, `scripts/check_dialect_portability.py`,
fails CI when a new non-test module combines `connect_db()` with an unaudited
SQLite-only token.

Already-covered dispositions:

- `adapters/base.py` `tracked_call` telemetry was made dialect-aware by PR #976.
- `etl/edgar_flow.py` EDGAR persistence was made dialect-aware by PR #977.
- `etl/summariser_flow.py` now uses `daily_diffs` and backend-aware placeholders.
- `schema.sql` bootstrap ordering is covered by PR #978 and the schema idempotence gate.

## Audit Commands

```bash
rg -n "AUTOINCREMENT|INSERT OR IGNORE|PRAGMA table_(?:info|xinfo)|execute(?:many)?\\([^#\\n]*(?:SELECT|INSERT|UPDATE|DELETE|VALUES|WHERE)[^#\\n]*\\?|\\bdaily_diff\\b|\\bd\\.change\\b" adapters etl chains alerts api ui llm scripts embeddings.py diff_holdings.py
rg -n "connect_db\(" adapters etl chains alerts api ui llm scripts embeddings.py diff_holdings.py
python scripts/check_dialect_portability.py
```

## Disposition Table

| Surface | Classification | Evidence | Failure Mode Or Justification | Disposition |
| --- | --- | --- | --- | --- |
| `embeddings.py` | dialect-aware | SQLite table setup, column introspection, and duplicate handling avoid audited SQLite-only tokens; Postgres paths use `bigserial`, `information_schema`-free inserts/searches, `%s` placeholders, and `ON CONFLICT`. | Follow-up #1005 resolved: `python scripts/check_dialect_portability.py --no-allowlist embeddings.py` now passes. | Allowed by gate without an allowlist entry. |
| `chains/holdings_analysis.py` | dialect-aware | Usage log table setup branches for SQLite/Postgres; inserts use backend-aware placeholders. | Follow-up #1006 resolved: `python scripts/check_dialect_portability.py --no-allowlist chains/holdings_analysis.py` now passes. | Allowed by gate without an allowlist entry. |
| `chains/filing_summary.py` | dialect-aware | Usage log table setup branches for SQLite/Postgres; inserts use backend-aware placeholders. | Follow-up #1006 resolved: `python scripts/check_dialect_portability.py --no-allowlist chains/filing_summary.py` now passes. | Allowed by gate without an allowlist entry. |
| `etl/activism_detection.py` | postgres-incompatible | `AUTOINCREMENT`, `INSERT OR IGNORE` | Activism event persistence uses SQLite DDL/upsert syntax on an ETL path that should not assume SQLite. | Follow-up #1007: `dialect_portability_audit etl-activism`. |
| `etl/activism_flow.py` | postgres-incompatible | `AUTOINCREMENT`; `connect_db(DB_PATH)` | Legacy filing setup is SQLite-only and separate from the EDGAR dialect-aware fix. | Follow-up #1007: `dialect_portability_audit etl-activism`. |
| `etl/conviction_flow.py` | postgres-incompatible | multiple `AUTOINCREMENT`; `connect_db()` | Conviction score/crowding/signal setup uses SQLite DDL in a backend-flexible ETL flow. | Follow-up #1008: `dialect_portability_audit etl-conviction`. |
| `etl/ingest_flow.py` | postgres-incompatible | `PRAGMA table_info`, `AUTOINCREMENT`; `connect_db(db_path or DB_PATH)` | Ingest setup still introspects and creates local tables with SQLite-only syntax. | Follow-up #1008: `dialect_portability_audit etl-ingest`. |
| `etl/evaluation_flow.py` | postgres-incompatible | multiple `AUTOINCREMENT`, unconditional `?` placeholders; `connect_db()` | Evaluation dataset and holding/diff setup use SQLite DDL and SQLite placeholders without a Postgres branch. | Follow-up #1008: `dialect_portability_audit etl-evaluation`. |
| `etl/daily_diff_flow.py` | dialect-aware | `AUTOINCREMENT`; `connect_db()` | `_ensure_daily_diffs_table` branches on `sqlite3.Connection`; Postgres requires migrations and fails fast if missing. | Allowed by gate. |
| `etl/digest_flow.py` | dialect-aware | `PRAGMA table_xinfo`; `connect_db(db_path)` | `_columns` first branches on `sqlite3.Connection`; Postgres uses `information_schema.columns`. | Allowed by gate. |
| `etl/edgar_flow.py` | dialect-aware | `PRAGMA table_xinfo`; `connect_db(DB_PATH)` | `_columns`, placeholders, table setup, filing upsert, and holding inserts branch for SQLite vs Postgres after PR #977. | Fixed by PR #977; allowed by gate. |
| `alerts/db.py` | postgres-incompatible | `PRAGMA table_info`, `AUTOINCREMENT` | Alert schema setup and introspection are SQLite-only while API alert routes use `connect_db()`. | Follow-up #1009: `dialect_portability_audit alerts`. |
| `api/chat.py` | postgres-incompatible | `PRAGMA table_info`, `AUTOINCREMENT`; `connect_db()` | Manager introspection and feedback table setup use SQLite-only SQL in the FastAPI app. | Follow-up #1009: `dialect_portability_audit api`. |
| `api/managers.py` | postgres-incompatible | `AUTOINCREMENT`, `PRAGMA table_info`; `connect_db()` | Manager bootstrap and column introspection use SQLite-only SQL in API routes. | Follow-up #1009: `dialect_portability_audit api`. |
| `api/search.py` | postgres-incompatible | `PRAGMA table_info`; imports `connect_db` | Dynamic table introspection is SQLite-specific in an API search route. | Follow-up #1009: `dialect_portability_audit api`. |
| `api/signals.py` | postgres-incompatible | `PRAGMA table_info`; `connect_db()` | Signals routes inspect manager columns through SQLite PRAGMA only. | Follow-up #1009: `dialect_portability_audit api`. |
| `llm/cost_tracking.py` | postgres-incompatible | `AUTOINCREMENT`; `connect_db()` | Cost tracking table setup uses SQLite DDL despite arbitrary backend selection. | Follow-up #1010: `dialect_portability_audit llm-cost`. |
| `scripts/seed_universe.py` | postgres-incompatible | `AUTOINCREMENT`, `PRAGMA table_info`; `connect_db()` | Seed helper setup and introspection are SQLite-only but can be run against configured DB. | Follow-up #1010: `dialect_portability_audit scripts`. |
| `scripts/resolve_aliases.py` | postgres-incompatible | `PRAGMA table_info`; `connect_db(db_path)` | Alias resolution introspection is SQLite-specific and should branch before Postgres usage. | Follow-up #1010: `dialect_portability_audit scripts`. |

## Gate Policy

`scripts/check_dialect_portability.py` is intentionally conservative:

- It scans production Python surfaces only.
- It only flags modules that define or import `connect_db`.
- It rejects unaudited `AUTOINCREMENT`, `INSERT OR IGNORE`, `PRAGMA table_info`, `PRAGMA table_xinfo`, direct `execute` / `executemany` calls with unconditional SQLite `?` placeholders, and legacy `daily_diff` / `d.change` schema names.
- Existing entries in this report are mirrored in the script allowlist so CI stays green while follow-up issues drain the debt.

New code should either use dialect-aware helpers, branch on `sqlite3.Connection`,
or update this report with a `sqlite-only-by-design` rationale before adding an
allowlist entry.
