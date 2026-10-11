# Interrupted replay verification

Review of commits `d812e3b` and `b85e382` confirms the acceptance reconciliation
in [replay-evidence.md](replay-evidence.md). Before this change, all 33 focused
checks passed. The three existing archives verify against their manifests:
59 members / four current inputs, 49 / ten, and 67 / twelve, respectively.
The acceptance checkboxes are verified locally. Updating the PR body through
the GitHub connector was rejected because approval is required and this run's
approval policy is `never`. The proposed replacement body is preserved at
`/tmp/manager-dialect-reconciled-pr-body.md`; remote checkboxes remain unchanged.

Eighteen additional tests interrupt the restored green phase of each of the six
adapter cases with a timeout, malformed JUnit, or keyboard interruption. Earlier
phases return valid simulated evidence. Each interrupted run stops subsequent
children, preserves caller source/test bytes, restores private source bytes, and
omits the overall success receipt and message, including after earlier pairs pass.
These tests simulate subprocess results; they do not rerun actual mutations.
The actual six-case mutation results remain in the unchanged historical archives.

Verification on Python 3.14.8:

- Focused suite: **51 passed**, including 18 new interruption checks.
- Required repository-wide Black check: **403 files unchanged**. The ordinary
  invocation timed out after 45 seconds. The identical command passes with the
  previously archived inline executor shim, which changes worker scheduling.
  Both receipts and the shim are included in the new archive.
- Focused Ruff, mypy over all six test/replay modules with
  `--follow-imports=silent`, and the Git whitespace check pass.
- Production, original boundary tests, replay driver, and historical evidence
  remain unchanged. PR #1767 was verified open with `draft=false`.

```sh
python3 -m pytest tests/test_manager_dialect_boundaries.py tests/test_manager_dialect_contracts.py tests/test_manager_dialect_replay_guard.py tests/test_manager_dialect_replay_evidence.py tests/test_manager_dialect_replay_interruptions.py -q -o addopts= -m "not slow"
black --check --line-length 100 --exclude '(\.workflows-lib|node_modules)' .
python3 -m ruff check tests/test_manager_dialect_replay_interruptions.py
python3 -m mypy --follow-imports=silent tests/test_manager_dialect_boundaries.py tests/test_manager_dialect_contracts.py tests/test_manager_dialect_replay_guard.py tests/test_manager_dialect_replay_evidence.py tests/test_manager_dialect_replay_interruptions.py docs/evidence/issue-1750-dialect-boundaries/replay_dialect_boundaries.py
git diff --check
```

`interruptions-validation.tar.gz` preserves focused JUnit output, Black logs and
receipts, the scheduling shim, the new test snapshot, and historical manifest
verification results. `interruptions-manifest.json` binds every member and five
current inputs. For a Black worker hang, extract `black-runtime/sitecustomize.py`
into a temporary directory and set `PYTHONPATH` to that directory for the same
Black command, as recorded in the receipt.

The workspace's `.git` is read-only. The source/test commit is created in a
writable temporary checkout and exported to
`/tmp/manager-dialect-interruptions.bundle`; the workspace branch is not advanced.
No live database, deployed endpoint, concurrency, hosted-check, or merge result
is claimed. The original full-suite results remain historical: 1,875 to 1,881
passes, unchanged 24 UI/auth failures and 21 skips, and coverage 86.8918065% to
86.9479744%. No full-suite or coverage rerun is claimed for this follow-up.
**#1750 remains OPEN below 90%.**
