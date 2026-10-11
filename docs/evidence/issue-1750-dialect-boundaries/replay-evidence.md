# Replay evidence rejection follow-up

The previous two commits already implemented the recording-connection adapter
checks and replay restoration safeguards. Their 17 focused cases pass, and both
historical archives verify against every member and current-input hash in their
manifests. The PR body omitted the acceptance checklist. Updating it through the
GitHub connector was blocked: the connector requires approval, while this run's
approval policy is `never`. The verified reconciliation is recorded below.

This follow-up adds 16 checks for well-formed but invalid evidence, plus missing
JUnit files. Each of eight defects is exercised in both the mutation (red) and
restored (green) phase: missing JUnit, missing case, duplicate case, wrong case
name, an error, a skip, an incorrect failure result, and an incorrect process exit.
When testing green-phase rejection, the preceding red evidence must first pass.
Every rejection restores private source bytes, preserves caller source/test bytes,
stops further children, and leaves no success receipt or success message.

Production, the six original boundary tests, the replay driver, and both historical
evidence sets are unchanged. The new tests reuse the isolated caller/archive
fixtures. They execute no SQL against a live database or deployed endpoint.

## Verified acceptance reconciliation

- [x] **Tests**
  - [x] Added focused checks for database query parameters, count results, inserts,
    updates, and missing data, using a recording connection rather than a live database.
  - [x] Added safeguards that verify replay checks restore changes and reject
    mismatched or incomplete evidence.
  - [x] Replay checks cover six database-adapter cases; no live database or
    deployed endpoint is involved.
- [x] **Documentation**
  - [x] Added verification results, reproduction instructions, and archived
    evidence. The reports note unchanged production code, limitations in broader
    test results, and that the related issue remains open.

## Results and reproduction

- Focused tests: **33 passed**, comprising 17 existing checks and 16 new checks.
- Fresh actual replay: all six named mutation failures exit 1; each byte-identical
  restoration passes with exit 0. Caller input hashes remain unchanged.
- Original evidence: **59 members / four current inputs** verified. Previous
  acceptance evidence: **49 members / ten current inputs** verified.
- Required repository-wide Black check: **402 files unchanged**. The ordinary
  invocation timed out after 60 seconds in this sandbox. The same command passes
  with the previously archived inline executor shim for Black's worker threads.
  This changes task scheduling only; Black's formatting rules remain in use.
  Both command receipts and the shim are archived.
- Focused Ruff and mypy (`--follow-imports=silent`), and `git diff --check` pass.
- Interpreter: Python 3.14.8. The original Python 3.12.2 full-suite results remain
  historical: 1,875 to 1,881 passes, unchanged 24 UI/auth failures and 21 skips;
  coverage 86.8918065% to 86.9479744%. This follow-up does not rerun the full suite
  or measure a new global coverage percentage. **#1750 remains OPEN below 90%.**

```sh
python3 -m pytest tests/test_manager_dialect_boundaries.py tests/test_manager_dialect_contracts.py tests/test_manager_dialect_replay_guard.py tests/test_manager_dialect_replay_evidence.py -q -o addopts= -m "not slow"
PYTEST_ADDOPTS="-m 'not slow'" python3 docs/evidence/issue-1750-dialect-boundaries/replay_dialect_boundaries.py --output /tmp/new-manager-dialect-evidence-proof
black --check --line-length 100 --exclude '(\.workflows-lib|node_modules)' .
python3 -m ruff check tests/test_manager_dialect_boundaries.py tests/test_manager_dialect_contracts.py tests/test_manager_dialect_replay_guard.py tests/test_manager_dialect_replay_evidence.py docs/evidence/issue-1750-dialect-boundaries/replay_dialect_boundaries.py
python3 -m mypy --follow-imports=silent tests/test_manager_dialect_boundaries.py tests/test_manager_dialect_contracts.py tests/test_manager_dialect_replay_guard.py tests/test_manager_dialect_replay_evidence.py docs/evidence/issue-1750-dialect-boundaries/replay_dialect_boundaries.py
git diff --check
```

Use a fresh replay output path. If Black's worker threads hang in this sandbox,
extract `black-runtime/sitecustomize.py` from the new archive into a temporary
directory and set `PYTHONPATH` to that directory for the same Black command.

`replay-evidence-validation.tar.gz` contains console/JUnit output, command and
process receipts, all twelve actual replay phase logs/JUnit/receipts, controls,
the new test snapshot, and the historical archive verification script.
`replay-evidence-manifest.json` binds every member and the current inputs. The
original manifests and archives continue to describe their original runs.

The workspace's `.git` directory is read-only. Delivery therefore uses a commit
in a writable temporary Git repository and an exported
`manager-dialect-replay-evidence.bundle`; the workspace branch cannot be advanced
here. No hosted-check, live PostgreSQL, deployed endpoint, concurrency, merge,
or broader issue closure result is claimed.
