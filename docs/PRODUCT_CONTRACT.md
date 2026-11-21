# Product contract — stranske/Manager-Database
_First draft generated 2026-09-20 from the audit scorecard; the repo owns this file from now on. A PR that adds a user-facing route, command or page adds a line here. The audit's Phase 1.5 scores every line below and prints any surface not listed as UNSCORED._

## Purpose
Track investment managers, filings, holdings, news and research Q&A over an indexed document corpus.

## Primary journey
Register manager → ingest documents and filings → search managers/documents → research corpus → review dashboard and daily report.

## Core functions
| id | a user can … and sees … | entry point | probe (how to exercise it; vary these determinants) | status 2026-09-20 |
|---|---|---|---|---|
| MDB-1 | an analyst can add a manager and sees a manager ID in the list | `POST /managers` | POST two managers then `GET /managers`; compare IDs and names | WORKS |
| MDB-2 | an analyst can find managers and sees filtered, paginated list and detail | `GET /managers`; Search UI | create two managers; compare unfiltered list, detail, `search` and `name` filters | WORKS |
| MDB-3 | an analyst can record and query holdings or filings and sees stored positions, diffs and trends | EDGAR ETL; signals/activism API; Dashboard/Daily Report UI | ingest holdings/filings then compare signals and trends | NOT-EXERCISED |
| MDB-4 | an analyst can search managers and documents and sees ranked linked snippets | `GET /api/search?q=…`; Search UI | query `Elliott`; inspect manager and document results | WORKS |
| MDB-5 | an analyst can research the corpus and sees a grounded answer with sources | `POST /api/chat`; Research UI | seed snippet; configure provider; query it and inspect answer/context/sources | WORKS |
| MDB-6 | an analyst can view dashboard analytics and daily report and sees DB-backed KPI and filing trends | dashboard/daily-report UI; offline web page | compare demo SQLite with seed-only SQLite; inspect `load_delta()` and UI tests | PARTIAL |
| MDB-7 | an analyst can upload documents and sees stored, embedded documents linked to a manager | Upload UI; ingest APIs | upload with and without selected manager; inspect document_managers association and search | NOT-EXERCISED |
| MDB-8 | an analyst can manage alerts and sees rules, inbox, acknowledgements and statistics | Alerts UI | create/edit rule; vary matching/nonmatching events; inspect inbox, acknowledgement and stats | NOT-EXERCISED |

## Known gaps at draft time
- MDB-6: demo database renders a synthetic filing trend, but a seed-only database lacks the filings schema and dashboard trend fails.

## Dated coverage evidence
- **MDB-2 (verified 2026-11-21):** `pytest tests/baseline/test_coverage_manifest.py::test_manager_search_baseline_records_observed_filter_reads` exercises `GET /managers` against the 8-row `richer` baseline seed. `search=management` returns IDs `[1, 3, 4]`, `search=rentech` matches ID `[5]` via alias `"RenTech"`, `name=t` returns IDs `[3, 7]`, and the combined probe `GET /managers?search=management&name=t&unused_control=ignored` narrows to `[3]` (`TCI Fund Management`, `total=1`) with observed runtime SQL reads `{"endpoint", "search", "name"}` (`unused_control` excluded; whitespace-only `search`/`name` register only `{"endpoint"}`). Bypassed-key controls confirm that bypassing `name` widens results to `[1, 3, 4]` and omits `"name"` from `read_keys`, while bypassing `search` widens results to `[3, 7]` and omits `"search"` from `read_keys`. Compatibility checks confirm `GET /managers?jurisdiction=us&tag=activist` (`[1, 4]`), `GET /managers/4`, `GET /managers/stats` (`total_managers=8`), and `8/8` exercised + `8` observed runtime parameter reads in `docs/reports/baseline-coverage.md`.
