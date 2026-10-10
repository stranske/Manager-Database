# Manager legacy-schema coverage

Related to #1750. This is the next bounded test-only chunk after merged #1760,
which covered JSON decoding. The initiative stays open below 90%.

Fresh last-500-commit selection ranks `api/managers.py` first: nine repair-subject
proxy commits, 16 touches, 32 uncovered statements. This is a history proxy,
not a measured escaped-defect count. `selection.json` retains the ranking.

Four tests exercise real in-memory SQLite connections and production migration
code: minimal legacy tables gain identifiers/JSON defaults without row loss;
missing creation/update timestamps are backfilled while the other historical
value survives; a second migration preserves rows/schema and enforces CIK
uniqueness. Production code is unchanged from main SHA b3f80503.

## Executable checks

```bash
python3 -m pytest tests/test_manager_legacy_schema.py -q -o addopts=
python3 docs/evidence/issue-1750-legacy-schema/replay_schema_boundaries.py --output /tmp/new-manager-schema-proof
python3 -m pytest tests -q --cov --cov-report=json:coverage.json --junitxml=results.xml
python3 -m black --check .
python3 -m ruff check tests/test_manager_legacy_schema.py docs/evidence/issue-1750-legacy-schema/replay_schema_boundaries.py
git diff --check
```

Run the replay in a dedicated worktree with the bound source bytes and a fresh
output path. It temporarily mutates that worktree's production module, uses
named-node JUnit to require exactly one assertion failure for RED and one pass
for GREEN, and restores the original bytes in a finally block. Timeout/parse/
selector errors are incomplete proof, never success. It does not claim safety
against concurrent writers or process termination outside Python cleanup.

All four final production mutations produced RED exit 1 and byte-identical
restoration GREEN exit 0: wrong migrated JSON default, omitted creation backfill,
omitted update backfill, and omitted SQLite unique index. Test and replay-driver
hashes remained unchanged. Earlier selector-ambiguity attempts stopped before
claiming complete proof; their partial receipts are preserved separately.

## Matched complete-suite results

Baseline: 1,813 passed, 24 failed, 21 skipped. Candidate: 1,817 passed, the same
24 failed and 21 skipped. Every old test outcome is identical. Both commands
exit 1 because of the independently existing UI-auth failures; this is not a
whole-suite PASS. `comparison.json` lists every failing node.

Coverage: 12,297 to 12,304 covered lines, 86.7696867061812% to
86.81907987581145%, with the same 115 source files, 14,172 statements and 82
excluded lines. The seven newly covered lines are all in `api/managers.py`;
no previously covered line regressed. Source SHA256 remains
`401a4591cd8965308f5ccffaf2d95144f2fd190a114e223cf3bccd77c0ff8245`.
Whole-repository Black checked 387 files; focused Ruff and whitespace checks pass.

`validation.tar.gz` retains lossless full-suite consoles, JUnit, coverage,
interpreter/platform, ranking, original production/test/replay bytes, and every
mutation phase. `archive-index.json` binds all 49 members and the archive itself.
Exact argv/cwd/exits and caller/source hashes appear in final mutation receipts.
The archived suite output paths are the original absolute evidence paths.

These measurements use the available Anaconda Python 3.12 interpreter,
pytest 9.1.1 and coverage 7.16.0; the repository pins coverage 7.16.2. No hosted
CI environment parity, PostgreSQL execution, deployment, provider acceptance
or repository-wide 90% claim is made. Keepalive owns fresh CI and reviews;
closer owns complete exact-head check topology, threads, seven-minute floor,
guarded merge, actual comparison and bounded chunk disposition.
