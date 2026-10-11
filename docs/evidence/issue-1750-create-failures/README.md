# Manager create/import failure boundaries for #1750

This test-only logical chunk starts at main `78501cdd97fe953949a1a3fe1bb2593d08ad217c`, after merged #1764/#1765. The broader coverage initiative remains open below 90%. Production bytes, coverage source universe, exclusions, thresholds and skip policy are unchanged.

The fresh last-500-commit ranking over api/etl/services/adapters/utils selects api/managers.py first: 19 repair-subject proxy matches, 61 touches and 15 uncovered statements. The reproducible proxy uses whole-word fix/bug/repair/regression/correct/fail/error matches, not independently verified escaped incidents. The raw log and complete ranking are retained.

Four named cases use actual SQLite constraints/triggers and independent database observers. They require unrelated schema-integrity and non-CIK insert failures to become sanitized HTTPException 503 responses; universe-import write failure has the same contract. Failed calls never invalidate the manager cache, preserve the seeded manager and close exactly one endpoint connection. The fourth case simulates missing readback after real committed insertion, requiring the payload response and independently persisted values. Only the readback is controlled; it does not model real concurrent deletion. No real PostgreSQL or deployed HTTP transport is claimed.

## Observed validation

- Matched complete suite baseline: 1871 PASS / 24 FAIL / 21 SKIP; candidate: 1875 PASS / the same 24 FAIL / 21 SKIP. Exactly four new passing nodes, zero changed old outcomes. Both full commands exit1. All 24 unchanged failures are tests/test_ui_auth.py; this is not a complete-suite PASS.
- Coverage 12371 -> 12376 of the same 14243 statements: 86.85670153759742% -> 86.8918065014393%. Same 82 exclusions and full source universe; zero covered-line regressions. api/managers.py gains lines1079/1096/1106/1524/1525 (811 -> 816 of826 statements).
- Four actual function-scoped production mutations each produce exactly one named JUnit failure with REDexit1; exact byte restoration gives GREENexit0. A private Git archive plus current test overlay keeps caller source/test bytes unchanged. Reusing output raises FileExistsError before mutation, with all25proof-member hashes unchanged.
- Focused four cases PASS; full Black397files, focused Ruff, both replay entry points together under mypy, and git diff --check pass. Local Python3.12.2/dependencies are the same for both suites; hosted CI parity is not claimed.

## Reproduce

Use Python3.12+ with repository test dependencies (including declared defusedxml):

```sh
python3 -m pytest tests/test_manager_create_failure_boundaries.py -q -o addopts=
python3 docs/evidence/issue-1750-create-failures/replay_create_failures.py --output /tmp/new-manager-create-proof
python3 -m pytest tests -q --cov --cov-report=json:coverage.json
black --check --line-length 100 --exclude '(\.workflows-lib|node_modules)' .
ruff check tests/test_manager_create_failure_boundaries.py docs/evidence/issue-1750-create-failures/replay_create_failures.py
git diff --check
```

validation.tar.gz retains complete raw suites/coverage/JUnit, command receipts, all eight mutation phases, reuse guard, source/test/replay bytes and ranking. manifest.json binds every member and current deliverable file by SHA256. Historical completed chunks and provider verdicts are unchanged. Matching Codex keepalive owns hosted CI and review; Reviewed Repo Merge Verify Closer owns complete expected check topology, active review bodies/zero-active gate, seven-minute latest-activity floor, guarded merge and actual verify:compare/component disposition. Source1750 stays OPEN below90%.
