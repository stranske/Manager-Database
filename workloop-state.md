# Workloop State

## 2026-05-09T22:12:00Z - opener lane selected issue #1007 activism dialect work

- Automation: `pd-workloop-resume` (codex opener lane).
- Source repo: `stranske/Manager-Database`.
- Source issue: `#1007` (`Make activism ETL SQL dialect-aware`, `priority:normal`, `repo-review-approved`).
- Selection:
  - ACTION A succeeded from the neutral Code workspace.
  - Full priority discovery ran across supported repos using `priority:high`, `priority:normal`, and `priority:low`.
  - Skipped high-priority `stranske/Workflows#2073` as an auth-expiry operational alert.
  - Skipped high-priority `stranske/Inv-Man-Intake#381` because merged PR `#400` already serves it and the issue is open only pending verifier disposition.
  - Skipped normal-priority items already served by merged/open PRs, including `stranske/Counter_Risk#476`, `stranske/Manager-Database#910`, `stranske/Manager-Database#1006`, and prior Trend issues.
  - Initial cap-health reported `total_opener_owned=3`, `raw_cap_reached=false`, `normal_cap_reached=false`, and no cap blocker; `stranske/Manager-Database#1017` needed dispatch evidence below cap.
  - `opener-repair-infra-stalls.py --json` added `agent:retry` and dispatched Gate Followups for `#1017`.
  - Fresh cap-health reported `total_opener_owned=3`, `drainable_count=3`, `non_drainable_count=0`, `raw_cap_reached=false`, `normal_cap_reached=false`.
- Implementation:
  - Updated `etl/activism_detection.py` so activism event table setup uses SQLite `INTEGER PRIMARY KEY` and Postgres `BIGSERIAL`/`DOUBLE PRECISION`/`TIMESTAMPTZ` DDL branches, and duplicate-safe event inserts use backend placeholders with `ON CONFLICT DO NOTHING`.
  - Updated `etl/activism_flow.py` so activism filing table setup uses SQLite/Postgres DDL branches, including Postgres `TEXT[]` group members, and preserves backend-aware insert/update paths.
  - Removed `etl/activism_detection.py` and `etl/activism_flow.py` from `scripts/check_dialect_portability.py` allowlist.
  - Updated `docs/reports/dialect_portability_audit.md` to mark follow-up `#1007` resolved.
  - Added `tests/test_activism_dialect_portability.py` with strict Postgres fake coverage for table setup, event inserts, and filing upserts.
- Validation:
  - `python scripts/check_dialect_portability.py --no-allowlist etl/activism_detection.py etl/activism_flow.py` -> passed.
  - `pytest tests/test_activism_dialect_portability.py tests/test_activism_detection.py tests/test_activism_adapter.py tests/test_dialect_portability_gate.py --no-cov` -> 29 passed, 8 existing warnings.
  - `ruff check etl/activism_detection.py etl/activism_flow.py tests/test_activism_dialect_portability.py scripts/check_dialect_portability.py` -> passed.
  - `black --target-version py312 --check etl/activism_detection.py etl/activism_flow.py tests/test_activism_dialect_portability.py scripts/check_dialect_portability.py` -> passed.
  - `git diff --check` -> passed.
- Branch: `codex/issue-1007-activism-etl-dialect`.
- Commit: `203da91` (`Issue #1007: make activism ETL dialect-aware`).
- PR: `#1018` (`https://github.com/stranske/Manager-Database/pull/1018`), opened ready-for-review with `agent:codex`, `agents:keepalive`, and `autofix`.
- Relay:
  - `pr_opened active.source_repo=stranske/Manager-Database active.source_issue=1007 active.source_pr=1018 active.next_action=wait_for_keepalive`
- Next action: keepalive owns CI/check follow-up for PR `#1018`.

## 2026-05-09T21:08:00Z - opener lane implementing issue #1006

- Automation: `pd-workloop-resume` (codex opener lane).
- Source repo: `stranske/Manager-Database`.
- Source issue: `#1006` (`Make chain persistence tables dialect-aware`, `priority:normal`, `repo-review-approved`).
- Selection:
  - ACTION A succeeded from the neutral Code workspace.
  - Full fleet priority discovery ran across `priority:high`, `priority:normal`, and `priority:low`.
  - Skipped `Workflows#2073` as an operational auth-expiry alert.
  - Skipped `Inv-Man-Intake#381`, `Counter_Risk#476`, `Manager-Database#910`, and served Trend issues because each already has a merged/open PR pending verifier or closer work.
  - Cap health after infra-repair preflight was below cap: `total_opener_owned=2`, `drainable_count=2`, `non_drainable_count=0`, `raw_cap_reached=false`, `normal_cap_reached=false`.
  - No existing PR was found for `Manager-Database#1006`.
- Implementation in progress:
  - Branch: `codex/issue-1006-chain-persistence-dialect`.
  - Replaced SQLite-only chain usage-log DDL with explicit SQLite/Postgres branches in `chains/filing_summary.py` and `chains/holdings_analysis.py`.
  - Removed both chain files from `scripts/check_dialect_portability.py` allowlist and updated `docs/reports/dialect_portability_audit.md`.
  - Added `tests/test_chain_dialect_portability.py` with strict Postgres fake coverage for both chain usage-log paths.
- Validation so far:
  - `python scripts/check_dialect_portability.py --no-allowlist chains/holdings_analysis.py chains/filing_summary.py` -> passed.
  - `pytest tests/test_chain_dialect_portability.py tests/test_filing_summary_chain.py tests/test_holdings_analysis_chain.py tests/test_dialect_portability_gate.py --no-cov` -> 43 passed, 7 existing warnings.
  - `ruff check chains/filing_summary.py chains/holdings_analysis.py tests/test_chain_dialect_portability.py scripts/check_dialect_portability.py` -> passed.
  - `black --target-version py312 --check chains/filing_summary.py chains/holdings_analysis.py tests/test_chain_dialect_portability.py scripts/check_dialect_portability.py` -> passed.
  - `git diff --check` -> passed.
- Commit/push:
  - Commit `758a042` (`Issue #1006: make chain usage logs dialect-aware`) pushed to `codex/issue-1006-chain-persistence-dialect`.
- PR:
  - Opened ready-for-review PR `#1017`: `https://github.com/stranske/Manager-Database/pull/1017`.
  - PR labels verified: `agent:codex`, `agents:keepalive`, `autofix`; `isDraft=false`.
- Relay:
  - `pr_opened active.source_repo=stranske/Manager-Database active.source_issue=1006 active.source_pr=1017 active.next_action=wait_for_keepalive`.
- Next action: keepalive owns CI/check follow-up for PR `#1017`.

## 2026-05-09T16:24:12Z - closer lane addressed PR #1012 review threads

- Automation: `imi-merge-verify-closer` (codex closer lane).
- Source repo: `stranske/Manager-Database`.
- Source issue: `#1000`.
- Source PR: `#1012` (`Issue #1000: Align rate limit docs with shipped API`), branch `codex/issue-1000-rate-limit-contract`.
- Batch sweep:
  - No safe terminal sweep action was available.
  - `stranske/Inv-Man-Intake#401/#404` was already merged, had `verify:compare`, had durable all-provider PASS, and the source issue was already closed.
  - `stranske/Trend_Model_Project#5172/#5260` had green Gate checks and resolved/outdated review threads, but remained `UNSTABLE` due to the non-required `claude-review` failure and was deferred.
  - Maintenance PRs in Workflows/Portable-Alpha were excluded; remaining verifier non-PASS lanes were deferred.
- Selected lane evidence:
  - PR `#1012` was open, non-draft, `MERGEABLE/CLEAN`, and all current CI/Gate checks were green.
  - GraphQL review-thread audit found three unresolved Copilot threads:
    - Document that delegated `POST /api/chat/*` routes share the chat limiter.
    - Add those routes to `tests/test_rate_limit_contract.py`.
    - Anchor the docs path in the doc-contract test to the repo root.
- Action:
  - Updated `docs/api_rate_limiting.md` to list `POST /api/chat/filing-summary`, `/holdings-analysis`, `/query`, and `/search` as session-keyed chat-handler routes.
  - Extended `tests/test_rate_limit_contract.py` to assert those routes emit no rate-limit headers.
  - Changed the docs contract read to `Path(__file__).resolve().parents[1]`.
- Validation:
  - `python -m pytest tests/test_rate_limit_contract.py --no-cov` -> 13 passed.
  - `python -m pytest tests/test_chat_api.py tests/test_manager_api.py tests/test_data_api.py tests/test_rate_limit_contract.py --no-cov` -> 96 passed.
  - `python -m ruff check tests/test_rate_limit_contract.py` -> passed.
  - `python -m black --target-version py312 --check tests/test_rate_limit_contract.py` -> passed.
  - `git diff --check` -> passed.
- Next action: push this review-thread fix to PR `#1012`, resolve the three Copilot review threads, then re-check fresh CI before merge/verify-label sequencing.

## 2026-05-09T16:05:58Z - opener lane opened issue #1000 rate-limit contract PR

- Automation: `pd-workloop-resume` (codex opener lane).
- Source repo: `stranske/Manager-Database`.
- Source issue: `#1000` (`Reconcile docs/api_rate_limiting.md contract with FastAPI implementation across all documented endpoints`, `priority:normal`, `repo-review-approved`).
- Selection:
  - ACTION A succeeded from the neutral Code workspace.
  - Full fleet priority discovery ran across supported repos.
  - Skipped high-priority `Workflows#2073` as an operational auth-expiry alert.
  - Skipped high-priority `Inv-Man-Intake#381` because prior lane state indicates it is already served by PR `#400` and is verifier/closer work.
  - Cap health after repair was below cap: `total_opener_owned=1`, `drainable_count=1`, `non_drainable_count=0`, `raw_cap_reached=false`, `normal_cap_reached=false`.
  - No open Manager-Database PR existed for issue `#1000`.
- Implementation:
  - Chose the issue-permitted doc-rewrite resolution rather than adding a broad new cross-endpoint limiter.
  - Rewrote `docs/api_rate_limiting.md` to describe shipped behavior: a session-keyed 10 requests per 60 seconds limiter only on `POST /api/chat` and `POST /api/chat/feedback`, no global per-IP limiter, and no retry/quota response headers.
  - Added `tests/test_rate_limit_contract.py` covering chat, manager, data, health, bare 429 behavior, and the documentation/header contract.
  - Added a dated `docs/api_changes.md` entry.
- Validation:
  - `pytest tests/test_rate_limit_contract.py` passed: 9 passed.
  - `pytest tests/test_chat_api.py tests/test_manager_api.py tests/test_data_api.py tests/test_rate_limit_contract.py` passed: 92 passed.
  - `git diff --check` passed.
- Branch: `codex/issue-1000-rate-limit-contract`.
- PR: `#1012` (`https://github.com/stranske/Manager-Database/pull/1012`), opened ready-for-review with `agent:codex`, `agents:keepalive`, and `autofix`.
- Relay:
  - `pr_opened active.source_repo=stranske/Manager-Database active.source_issue=1000 active.source_pr=1012 active.next_action=wait_for_keepalive`
- Post-open repair:
  - Immediate cap-health initially classified `#1012` as `needs-dispatch-evidence` because early Gate/Gate Followups runs cancelled before runner evidence appeared.
  - Ran `opener-repair-infra-stalls.py --json`; it added `agent:retry` and dispatched Gate Followups for `#1012`.
  - Fresh cap-health at `2026-05-09T16:07:49Z` reported `#1012` as `draining` with active Gate and Agents Gate Followups runs; fleet totals were `total_opener_owned=2`, `drainable_count=2`, `non_drainable_count=0`, `raw_cap_reached=false`, `normal_cap_reached=false`.
- Next action: keepalive owns CI/check follow-up for PR `#1012`.
