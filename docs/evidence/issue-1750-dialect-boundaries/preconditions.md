# Mutation preconditions and interrupted children

Review of commits `d812e3b`, `b85e382`, and `b190853` found that the acceptance
items were already implemented. Before this follow-up, all 51 focused checks
passed. All four historical archives and their current-input bindings verify:
59 members / four inputs, 49 / ten, 67 / twelve, and eight / five, respectively.

The additional 30 checks cover two remaining safeguard paths across all six
adapter cases: interrupted mutation-phase children and invalid mutation plans.
Child-launch errors and keyboard interruptions must restore private source bytes,
preserve caller source/test bytes, stop subsequent children, and omit both the
interrupted phase receipt and the overall success receipt/message. Earlier pairs
return valid simulated results so later-case interruptions are also exercised.
Missing functions, missing mutation anchors, and ambiguous anchors must fail
before any child starts or any proof file is written.

These are isolated driver tests with simulated child results. They execute no SQL
and do not rerun actual production mutations. Production, the original six tests,
the replay driver, and historical evidence remain byte-identical.

## Verified acceptance reconciliation

- [x] **Tests**
  - [x] Added focused checks for database query parameters, count results, inserts,
    updates, and missing data, using a recording connection rather than a live database.
  - [x] Added replay checks for six database-adapter cases, including validation of
    expected test results and restoration of source files.
  - [x] Added safeguards for interrupted or invalid replay runs, mismatched inputs,
    and incomplete evidence; validation failures cannot produce a success result.
- [x] **Documentation**
  - [x] Added verification results, reproduction instructions, and archived evidence.
    Reports record unchanged production code, broader test limitations, and the
    related issue's open status.

The PR-body update was rejected by the GitHub connector because approval is
required and this run's approval policy is `never`. Remote checkboxes remain
unchanged. The proposed reconciled body is preserved inside the new archive.

## Verification and reproduction

Python 3.14.8 verification: **81 focused checks pass**, including the 30 new cases,
with slow tests excluded. Focused Ruff and mypy over all seven test/replay modules
(`--follow-imports=silent`) pass, as does `git diff --check`. The new Python file
was formatted with Black at line length 100. The required repository-wide Black
check reports **404 files unchanged**. Its ordinary invocation timed out after
45 seconds; the identical command passes with the previously archived inline
executor shim. The shim changes worker scheduling; Black retains its formatting
rules. Both command receipts, logs, and the shim are archived.

```sh
python3 -m pytest tests/test_manager_dialect_boundaries.py tests/test_manager_dialect_contracts.py tests/test_manager_dialect_replay_guard.py tests/test_manager_dialect_replay_evidence.py tests/test_manager_dialect_replay_interruptions.py tests/test_manager_dialect_replay_preconditions.py -q -o addopts= -m "not slow"
black --check --line-length 100 --exclude '(\.workflows-lib|node_modules)' .
python3 -m ruff check tests/test_manager_dialect_replay_preconditions.py
python3 -m mypy --follow-imports=silent tests/test_manager_dialect_boundaries.py tests/test_manager_dialect_contracts.py tests/test_manager_dialect_replay_guard.py tests/test_manager_dialect_replay_evidence.py tests/test_manager_dialect_replay_interruptions.py tests/test_manager_dialect_replay_preconditions.py docs/evidence/issue-1750-dialect-boundaries/replay_dialect_boundaries.py
git diff --check
```

`preconditions-validation.tar.gz` preserves console/JUnit output, command/process
receipts, historical verification results, the new test snapshot, and the proposed
PR-body reconciliation. `preconditions-manifest.json` binds each member and current
input. For a Black worker hang, extract `black-runtime/sitecustomize.py` into a
temporary directory and set `PYTHONPATH` to that directory for the same command.

The workspace's `.git` is read-only. A source/test commit is created in a writable
temporary checkout and exported to `/tmp/manager-dialect-replay-preconditions.bundle`;
the workspace branch cannot be advanced here. PR #1767 remains open and ready for
review. No hosted-check, live PostgreSQL, deployed endpoint, concurrency, merge,
or issue closure result is claimed.

The original Python 3.12.2 full-suite results remain historical: 1,875 to 1,881
passes, unchanged 24 UI/auth failures and 21 skips, and coverage 86.8918065% to
86.9479744%. This follow-up does not rerun the full suite or claim a new global
coverage percentage. **#1750 remains OPEN below 90%.**
