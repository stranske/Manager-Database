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
| `etl/activism_detection.py` | dialect-aware | Event table setup and duplicate-safe inserts branch across SQLite/Postgres without SQLite-only tokens. | Follow-up #1007 resolved: `python scripts/check_dialect_portability.py --no-allowlist etl/activism_detection.py etl/activism_flow.py` now passes. | Allowed by gate without an allowlist entry. |
| `etl/activism_flow.py` | dialect-aware | Filing table setup uses SQLite `INTEGER PRIMARY KEY` and Postgres `BIGSERIAL`/array-aware DDL; inserts and lookups use backend-aware placeholders. | Follow-up #1007 resolved: `python scripts/check_dialect_portability.py --no-allowlist etl/activism_detection.py etl/activism_flow.py` now passes. | Allowed by gate without an allowlist entry. |
| `etl/conviction_flow.py` | dialect-aware | SQLite setup avoids audited tokens; Postgres checks canonical schema presence before use. | Follow-up #1008 resolved: `python scripts/check_dialect_portability.py --no-allowlist etl/conviction_flow.py` now passes. | Allowed by gate without an allowlist entry. |
| `etl/ingest_flow.py` | dialect-aware | Table setup branches for SQLite/Postgres; column detection uses cursor metadata; inserts use backend-aware placeholders/upserts. | Follow-up #1008 resolved: `python scripts/check_dialect_portability.py --no-allowlist etl/ingest_flow.py` now passes. | Allowed by gate without an allowlist entry. |
| `etl/evaluation_flow.py` | dialect-aware | Evaluation logging has SQLite/Postgres table setup branches; live local seed keeps SQLite fixture behavior without audited tokens. | Follow-up #1008 resolved: `python scripts/check_dialect_portability.py --no-allowlist etl/evaluation_flow.py` now passes. | Allowed by gate without an allowlist entry. |
| `etl/daily_diff_flow.py` | dialect-aware | `AUTOINCREMENT`; `connect_db()` | `_ensure_daily_diffs_table` branches on `sqlite3.Connection`; Postgres requires migrations and fails fast if missing. | Allowed by gate. |
| `etl/digest_flow.py` | dialect-aware | `PRAGMA table_xinfo`; `connect_db(db_path)` | `_columns` first branches on `sqlite3.Connection`; Postgres uses `information_schema.columns`. | Allowed by gate. |
| `etl/edgar_flow.py` | dialect-aware | `PRAGMA table_xinfo`; `connect_db(DB_PATH)` | `_columns`, placeholders, table setup, filing upsert, and holding inserts branch for SQLite vs Postgres after PR #977. | Fixed by PR #977; allowed by gate. |
| `alerts/db.py` | dialect-aware | SQLite alert setup uses portable `INTEGER PRIMARY KEY`, table-valued column introspection, and Postgres-specific `bigserial`/`jsonb`/array DDL. | Follow-up #1009 resolved: `python scripts/check_dialect_portability.py --no-allowlist alerts/db.py api/chat.py api/managers.py api/search.py api/signals.py` now passes. | Allowed by gate without an allowlist entry. |
| `api/chat.py` | dialect-aware | Manager column detection branches through SQLite table-valued introspection vs Postgres `information_schema`; feedback storage uses SQLite `INTEGER PRIMARY KEY` and requires migrated Postgres `chat_feedback`. | Follow-up #1009 resolved. | Allowed by gate without an allowlist entry. |
| `api/managers.py` | dialect-aware | Manager bootstrap uses SQLite `INTEGER PRIMARY KEY`, SQLite column detection avoids audited PRAGMA tokens, and Postgres schema extension remains explicit. | Follow-up #1009 resolved. | Allowed by gate without an allowlist entry. |
| `api/search.py` | dialect-aware | Dynamic column discovery branches between SQLite table-valued introspection and Postgres `information_schema.columns`. | Follow-up #1009 resolved. | Allowed by gate without an allowlist entry. |
| `api/signals.py` | dialect-aware | Signals manager-id resolution branches through SQLite table-valued introspection and canonical Postgres `manager_id`. | Follow-up #1009 resolved. | Allowed by gate without an allowlist entry. |
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
