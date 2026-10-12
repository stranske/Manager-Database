Part of #1750. Keep the broader coverage initiative open.

<!-- meta:issue:1750 -->
> **Source:** Issue #1750

Dashboard database errors had uncovered fallback paths. Ten new regressions prove that three real pandas/SQLite missing-schema reads preserve empty-result column schemas and seven domain-query failures preserve arguments and release connections. Production files are unchanged.

## Tasks

- [x] Select the next executable gap from 500-commit repair-history and current coverage ranking.
- [x] Add ten named dashboard read-failure cases and prove each with a real private production mutation.
- [x] Preserve the complete current baseline/candidate outcome and coverage comparison, raw logs/JUnit/commands/source hashes and replay-reuse refusal.

## Acceptance Criteria

- [x] Ten cases: intended assertion RED exit1 → byte-identical restored GREEN exit0, no errors/skips.
- [x] Full suite: 1959 → 1969 PASS, identical 24 existing UI-auth FAIL / 21 SKIP. Both commands exit1; every old outcome and covered line is preserved. No full-suite PASS claim.
- [x] Statement coverage 86.98721085% → 87.12698302% (12447 → 12467 / 14309), same 117 source files / 82 excluded lines. Branch coverage was not measured.
- [x] Full Black (409 files), focused Ruff and diff check pass. All 96 raw archive members and three current source/test/driver SHA256 bindings verify.

The suite counts, coverage percentages, and 409-file Black receipt above describe the archived baseline/candidate experiment. Current independent evidence checks verify those receipts; current full Black checks 412 files. Subsequent replay controls are outside that historical comparison.

Proof and reproducible driver: `docs/evidence/issue-1750-dashboard-read-failures/`. Current baseline also covers final merged1769 test/replay nodes absent from its older full-suite comparison; actual1769 CONCERNS/CONCERNS remains unchanged.

Hosted CI/reviews are pending. Keepalive owns those checks; closer requires complete expected topology, all active review-thread bodies, zero active findings, unchanged-head seven-minute floor, guarded merge and actual compare. No live PostgreSQL, real browser, deployment, provider PASS or whole-source 90% acceptance is claimed.


<!-- This is an auto-generated comment: release notes by coderabbit.ai -->
## Summary by CodeRabbit

* **Tests**
  * Added coverage for dashboard data-read failures, verifying that loaders return empty results with the expected columns and close database connections.
  * Added checks for fallback behavior and the integrity of archived validation evidence.
* **Documentation**
  * Added evidence and validation records for dashboard read-failure cases, including test comparisons, reproduction details, and documented limitations.
<!-- end of auto-generated comment: release notes by coderabbit.ai -->
