# Manager patch boundary coverage for #1750

One test-only logical chunk on current main `abf863032fc570211e9eb7692bb98bbacabe66c8`. The broad source remains open below 90%. No production, workflow, source-universe, threshold, exclusion or skip-policy changes.

The last 500 commits touching api/etl/services/adapters/utils rank `api/managers.py` first: 31 repair-subject proxy matches and 61 touches. These subjects are a proxy, not independently verified escaped defects. Fresh whole-suite coverage shows 19 missing statements in that module; this chunk safely covers four write guards (469, 706, 1797, 1867).

Four named tests in `tests/test_manager_patch_boundaries.py` protect an empty update's no-SQL result, whitespace-only name rejection before connecting, and the ordinary/tag patch endpoints returning 404 when the manager is removed between the committed update and readback. The latter two use real SQLite writes/deletes and prove exactly-once connection cleanup. No live PostgreSQL, hosted deployment or real production concurrency scheduling is claimed; readback timing is deliberately controlled.

## Observed validation

- Full matched suite: 1867 PASS / 24 FAIL / 21 SKIP baseline; 1871 PASS / the same 24 FAIL / 21 SKIP candidate. Exactly four new passing nodes; zero changed pre-existing outcomes. Both full commands exit 1. Existing UI-auth failures remain visible in the raw JUnit and comparison.json; this is not a whole-suite PASS.
- Coverage 12367 -> 12371 of the same 14243 statements, 86.8286175665239% -> 86.85670153759742%; same 82 excluded lines and complete source universe; no covered-line regression. Manager module 807 -> 811 of 826 statements. No 90% completion claim.
- Four function-scoped actual production mutations each produce one named JUnit failure (RED exit 1), then exact byte restoration produces GREEN exit 0. The replay uses a private Git archive with the candidate test overlay and never mutates caller sources. All eight phase receipts retain argv/cwd/exits, raw console/JUnit and source/test/replay hashes.
- Focused patch/write-failure/cache/legacy-decoding suite: 25 PASS.

## Reproduce

Use Python 3.12 or newer with this repository's test dependencies:

```sh
python3 -m pytest tests/test_manager_patch_boundaries.py -q -o addopts=
python3 docs/evidence/issue-1750-patch-boundaries/replay.py --output /tmp/new-manager-patch-proof
python3 -m pytest tests -q --cov --cov-report=json:coverage.json
```

Replay refuses an existing output path before archiving or mutation. Each private source restore runs in finally, then TemporaryDirectory removes the copy. Caller source/test identity is checked on completion. Fixed shell-free interpreter/test argv invokes the selected node; user-selected output is an individual junitxml operand.

`validation.tar.gz` preserves raw matched suites/coverage/JUnit/receipts, all mutation pairs, focused JUnit, complete ranking output and the evaluated production source. `manifest.json` binds every member and current test/replay/comparison by SHA256. Historical completed chunks and provider dispositions are unchanged. Matching Codex keepalive owns hosted CI/review; closer owns current-head expected topology, active review bodies/zero-active gate, seven-minute latest-push floor, guarded merge and actual compare/component disposition.
