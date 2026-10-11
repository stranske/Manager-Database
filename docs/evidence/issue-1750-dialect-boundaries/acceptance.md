# PostgreSQL adapter acceptance follow-up

The original six recording-connection tests pass alongside eleven additional checks:
six adapter checks and five replay/evidence checks. Production code, the original
six tests, the replay driver, and the original evidence archive remain unchanged.
This run uses Python 3.14.8; the original full-suite comparison used Python 3.12.2.

The additional adapter checks exercise filtered count queries with absent, NULL,
and populated results; populated inserts with native arrays and a serialized JSONB
parameter; and updates whose current-row lookup returns no result. They check full
SQL, parameter order, and payload preservation. The recording connection executes
no SQL against a database.

The replay checks reject an existing output directory before archiving and reject
dirty caller production bytes before starting test children. They also verify
restoration after a timeout or malformed JUnit result, preservation of caller
source/tests, and absence of a success receipt for incomplete runs. The evidence
check verifies all 59 original archive members and all four original input hashes.

## Verified acceptance checklist

- [x] **Tests**
  - [x] Added checks for PostgreSQL adapter queries, parameter handling, JSONB updates,
    and missing-result behavior using a recording connection.
  - [x] Added replay checks for six database-adapter test cases; these do not use a
    live database or deployed endpoint.
- [x] **Documentation**
  - [x] Added evidence reports, reproduction details, and archived test artifacts.
    The report records improved passing test results, unchanged production code,
    and the broader issue's open status.

## Results and limits

- Focused suite: **17 passed**, including the six original cases and eleven added
  checks. All pytest verification uses `-m "not slow"`.
- Actual replay: six named mutation failures (exit 1) and six exact-byte restored
  passes (exit 0). Caller source/test hashes are unchanged.
- Black: **401 files unchanged**, with the required repository-wide check. Black's
  multi-file worker execution hung in this sandbox. An archived local runtime shim
  executes each formatting job inline and returns completed futures; Black still
  checks every file with its normal formatting rules. The repository adds no
  formatter configuration or runtime changes.
- Focused Ruff and `git diff --check` pass. Focused mypy passes with
  `--follow-imports=silent`. The initial mypy run also inspected imported production
  modules and reported existing errors at `api/cache.py:85` and
  `api/managers.py:1426`; its failing log and receipt are retained.
- The original archived matched full suite improved **1,875 to 1,881 passes**,
  with the same 24 UI/auth failures and 21 skips. Its coverage improved from
  **86.8918065% to 86.9479744%**. These are historical measurements, not a full-suite
  or coverage rerun for this follow-up. No new global percentage is claimed.
- Broader **#1750 remains OPEN below 90%**. No live PostgreSQL, deployed endpoint,
  concurrency, hosted-check, or merge result is claimed. GitHub was unreachable
  during this run, so PR readiness could not be verified remotely. The workspace's
  `.git` directory is read-only; delivery uses a commit created in a writable local
  Git repository and exported as a bundle. The workspace branch cannot be advanced
  in this environment.

## Reproduce

```sh
python3 -m pytest tests/test_manager_dialect_boundaries.py tests/test_manager_dialect_contracts.py tests/test_manager_dialect_replay_guard.py -q -o addopts= -m "not slow"
PYTEST_ADDOPTS="-m 'not slow'" python3 docs/evidence/issue-1750-dialect-boundaries/replay_dialect_boundaries.py --output /tmp/new-manager-dialect-acceptance-proof
python3 -m ruff check tests/test_manager_dialect_boundaries.py tests/test_manager_dialect_contracts.py tests/test_manager_dialect_replay_guard.py docs/evidence/issue-1750-dialect-boundaries/replay_dialect_boundaries.py
python3 -m mypy --follow-imports=silent tests/test_manager_dialect_boundaries.py tests/test_manager_dialect_contracts.py tests/test_manager_dialect_replay_guard.py docs/evidence/issue-1750-dialect-boundaries/replay_dialect_boundaries.py
black --check --line-length 100 --exclude '(\.workflows-lib|node_modules)' .
git diff --check
```

Use a fresh replay output path. If Black hangs in the same sandbox, extract
`black-runtime/sitecustomize.py` from `acceptance-validation.tar.gz` into a local
temporary directory and set `PYTHONPATH` to that directory for the Black command.
The recorded Black receipt gives the exact environment override used here.

`acceptance-validation.tar.gz` retains focused console/JUnit output, check logs,
the failed initial mypy log, argv/cwd/exit receipts, all twelve replay phase
logs/JUnit/receipts, replay controls, and the two added test snapshots.
`acceptance-manifest.json` binds every archive member and the current test,
production, replay, and original evidence inputs. The original `validation.tar.gz`,
`manifest.json`, and `comparison.json` remain the full-suite and coverage evidence.
