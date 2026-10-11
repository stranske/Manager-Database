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
python3 -m pytest tests/test_manager_patch_boundaries.py -q -o addopts= -m "not slow"
python3 docs/evidence/issue-1750-patch-boundaries/replay_patch_boundaries.py --output /tmp/new-manager-patch-proof
python3 -m pytest tests -q --cov --cov-report=json:coverage.json
```

Replay refuses an existing output path before archiving or mutation. Each private source restore runs in finally, then TemporaryDirectory removes the copy. Caller source/test identity is checked on completion. Fixed shell-free interpreter/test argv invokes the selected node; user-selected output is an individual junitxml operand.

`validation.tar.gz` preserves raw matched suites/coverage/JUnit/receipts, all mutation pairs, focused JUnit, complete ranking output and the evaluated production source. `manifest.json` binds every member and current test/replay/comparison by SHA256. Historical completed chunks and provider dispositions are unchanged. Matching Codex keepalive owns hosted CI/review; closer owns current-head expected topology, active review bodies/zero-active gate, seven-minute latest-push floor, guarded merge and actual compare/component disposition.

## Acceptance verification follow-up

The four named cases now also verify that an empty update leaves a seeded manager and
the connection's transaction state unchanged. Before injecting the readback deletion,
an independent SQLite reader observes the exact committed name/tag update. Both
readback cases still require one endpoint connection, exactly one close, a closed
connection rejecting further SQL, and a separately verified empty manager table.
The fixture's seed, observer and verification connections are explicitly closed too.

Final focused validation: 25 passed, exit 0, with `-m "not slow"`. Targeted coverage
for `api/managers.py` reports 49% for these four focused suites and confirms executed
lines 469, 706, 1797 and 1867. This narrow run measures boundary execution; its
`--cov-fail-under=0` avoids applying the repository-wide threshold to a partial suite.
The whole-suite percentages above belong to the original validation snapshot; the
whole suite was not rerun for this assertion-only follow-up.

Reproduce the final focused check:

```sh
python3 -m pytest tests/test_manager_patch_boundaries.py tests/test_manager_write_failures.py tests/test_manager_cache.py tests/test_manager_legacy_decoding.py -q -m "not slow" --cov=api.managers --cov-report=term-missing --cov-fail-under=0
black --check --line-length 100 --exclude '(\.workflows-lib|node_modules)' .
ruff check tests/test_manager_patch_boundaries.py docs/evidence/issue-1750-patch-boundaries/replay_patch_boundaries.py
git diff --check
```

`acceptance-validation.tar.gz` retains the final focused console/JUnit/coverage,
command receipts, formatting/lint checks and all four fresh RED1/restored GREEN0
mutation pairs. The original archive is unchanged. `manifest.json` preserves its
original file bindings as `historical_files_sha256`, binds the current files under
`files_sha256`, and binds the follow-up archive and every member separately.
Black's directory mode stalled in this sandbox, so all 395 Python files were first
checked individually using a fresh `BLACK_CACHE_DIR` under `/tmp`; the required
directory command was then checked with that verified cache. The archive records
every individual exit and the final directory command's result.

Acceptance criteria verified against the final tests and retained evidence:

- [x] **Tests**
  - [x] Expanded coverage for manager updates, including empty updates and validation of blank names.
  - [x] Added checks for missing-manager responses when a manager is removed during patch readback, including tag patches.
  - [x] Verified database connections are closed and database state remains consistent in these scenarios.
- [x] **Documentation**
  - [x] Added validation records and reproduction guidance for the manager patch-boundary tests.

Delivery blocker for this follow-up: `.git` is mounted read-only in the agent
sandbox, and staging fails while creating `.git/index.lock`. No commit or push
was created. The GitHub connector also rejected the blocker comment and
`needs-human` label because they require approval and this run's policy is
`never`. A runner with Git metadata write access must stage and commit the test,
this README, the manifest and `acceptance-validation.tar.gz`. PR #1764 was
verified open and ready for review; its remote head was unchanged by this run.

## Mypy entry-point recovery

The entry point is now `replay_patch_boundaries.py` to avoid mypy discovering two modules named `replay`. Its bytes retain SHA256 `42995b1b758d04565ab6bbb7dfe5a6e86d0adf02630da956433881836629bf46`. Historical archive contents and historical bindings are unchanged. The exact full command passes module discovery locally but exits 1 on unrelated local installed-stub/type errors; hosted confirmation remains pending. Both replay files typecheck together successfully, and all four manager patch tests pass. Full logs and commands are in `mypy-recovery/`.
