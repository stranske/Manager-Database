Part of #1750. Keep the broader coverage initiative open.

Dashboard database errors had uncovered fallback paths. Ten regressions prove that three real pandas/SQLite missing-schema reads preserve empty-result column schemas and seven domain-query failures preserve arguments and release connections. Production files are unchanged.

## Tasks

- [x] Select the next executable gap from 500-commit repair-history and current coverage ranking.
- [x] Add ten named dashboard read-failure cases and prove each with a real private production mutation.
- [x] Preserve the complete current baseline/candidate outcome and coverage comparison, raw logs/JUnit/commands/source hashes and replay-reuse refusal.

## Acceptance criteria

- [x] Ten cases: intended assertion RED exit1 → byte-identical restored GREEN exit0, no errors/skips.
- [x] Archived full suite: 1959 → 1969 PASS, identical 24 existing UI-auth FAIL / 21 SKIP. Both commands exit1; every old outcome and covered line is preserved. No full-suite PASS claim.
- [x] Archived statement coverage 86.98721085% → 87.12698302% (12447 → 12467 / 14309), same 117 source files / 82 excluded lines. Branch coverage was not measured.
- [x] Archived full Black (409 files), focused Ruff and diff check pass. All 96 raw archive members and three current source/test/driver SHA256 bindings verify.

## Current reconciliation

Reviewed commits `2b8f7b6`, `1d7f8eb` and `a801e76`. The immutable archive verification passes on the current checkout. The historical full-suite counts exclude the subsequently added verifier tests; no new full-suite result or coverage measurement is claimed.

This round adds a real subprocess test of the current replay driver. It checks all ten fresh intended RED1/restored GREEN0 pairs against their raw JUnit, command, source and restoration receipts, then confirms output reuse exits1 without changing receipts or caller source/test/driver bytes.

Validation:

- `pytest tests/test_dashboard_read_failure_evidence.py tests/test_dashboard_loader_failures.py -m "not slow" -q`: 25 passed.
- Focused Ruff for both test files and the replay driver: passed.
- `git diff --check`: passed.
- Full Black: all 410 current files passed serial checks using Black 26.10.0's real single-file formatter and normal CLI source discovery. Its worker dispatch stalled locally. After populating a dedicated cache with these checks, `BLACK_CACHE_DIR=/tmp/dashboard-black-1770-current black --check --line-length 100 --exclude '(\.workflows-lib|node_modules)' .` exited0 with 410 files unchanged.

Proof and reproducible driver: `docs/evidence/issue-1750-dashboard-read-failures/`. The archive, manifest and three bound source/test/driver files are unchanged.

## PR update blocker

The GitHub connector rejected the PR-body update: "MCP tool call requires approval, but approval policy is never". This file records the verified checkbox reconciliation for application by an authorized run. The remote PR body was not updated. PR #1770 was verified open and ready for review.

The primary checkout's `.git` directory is read-only, so staging failed before a local commit could be created there. The tested changes are committed in `/tmp/dashboard-replay-1770-commit`, with a transferable patch at `/tmp/dashboard-replay-1770.patch`. The primary working-tree edits remain available for the automation runner to commit.

Hosted CI/reviews remain outside this local verification. No live PostgreSQL, real browser, deployment, provider PASS or whole-source 90% acceptance is claimed.
