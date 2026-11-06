# Issue #1885 closer repair (PR #1893)

Checked 2026-11-06 (America/Chicago). Closes
[#1885](https://github.com/stranske/Manager-Database/issues/1885).

## Scope vs historical sync PR #1884

- **#1884 / #1885 (historical):** Workflows dev-tool sync and the dashboard review-recovery validator admitted only formatter pins; a regenerated consumer sync PR was the follow-up closure path.
- **#1892 / #1891 (current acceptance):** The branch validator and receipts prove **six-tool exact-pin** historical-input validation (mypy, pytest, pytest-cov, black, ruff, coverage) with successor digests and deliberate-break evidence. Post-merge Evidence/sync-core capture for **#1891** (or its linked successor) still depends on `main` and the controller workflow; it is **not** asserted in this branch commit.

## Validator digest

| Field | SHA-256 |
| --- | --- |
| `docs/evidence/issue-1885/previous-validator.py.txt` (archived) | `0d5880682352899c8341a02dd4b1dc6fa5d0d3c4b8b72efb965679abaa42cc34` |
| `tests/test_dashboard_read_failure_evidence.py` (current) | `a75ef95ebc44129c685422014896438586e7a5170cceaa1fb4be8e9896155799` |

Recorded in `docs/evidence/issue-1885/synced-tool-pin-successor.json` (`current_validator_sha256` only; `previous_validator_sha256` unchanged).

## Dependency setup validation

Command: `python scripts/validate_dependency_test_setup.py`

Receipt: [validate_dependency_test_setup.txt](issue-1885/validate_dependency_test_setup.txt) — ends with `✓ All validation checks passed!`

## Focused pytest

Command (Python **3.13**, external venv — see receipt header; avoids repo-local `.venv313`):

```text
python -m pytest tests/test_validate_dependency_test_setup.py tests/test_dashboard_read_failure_evidence.py -q
```

Receipt: [focused-pytest.txt](issue-1885/focused-pytest.txt) — **47 passed**, exit 0, including parametrized `test_review_recovery_accepts_synced_dev_tool_pin_bumps[False]` and `[True]`.

**PR review thread `discussion_r4492669575` (focused receipt):** Reply on GitHub with a pointer to this refreshed [focused-pytest.txt](issue-1885/focused-pytest.txt); the file records interpreter path, `repo_head`, full output, and exit status.

## Deliberate break → restore (`test_review_recovery_accepts_synced_dev_tool_pin_bumps`)

Temporary break: merged **formatter-only** `normalized_dashboard_project` and the **archived** `validate_review_recovery_current_inputs` from `docs/evidence/issue-1885/previous-validator.py.txt`, with current `SYNC_PR_1884_BUMPS` / parametrized test retained; **`docs/evidence/issue-1868/python-floor-successor.json`** `current_validator_sha256` was set to the temporary file digest, then restored. **`docs/evidence/issue-1885/synced-tool-pin-successor.json` was not edited** (pin successor still binds the archived six-tool predecessor bytes).

| Phase | Receipt | Exit | Failure locus |
| --- | --- | ---: | --- |
| RED (formatter-only) | [deliberate-break-pin-bump-red.txt](issue-1885/deliberate-break-pin-bump-red.txt) | 1 | `validate_review_recovery_current_inputs` → `assert current == historical` |
| GREEN (restored six-tool validator) | [deliberate-break-pin-bump-green.txt](issue-1885/deliberate-break-pin-bump-green.txt) | 0 | — |

## Full repository test command (documented `python -m pytest -q`)

Per [README_bootstrap.md](../../README_bootstrap.md) (editable install, then `pytest -q`; module form equivalent):

```text
python -m pip install -e ".[dev,app]"
python -m pytest -q
```

Environment: external venv `/private/tmp/mgrdb-pr1893-py313` with **Python 3.13.13** (no `.venv313` under the checkout).

Receipt: [full-pytest-q-py313-external-venv.txt](issue-1885/full-pytest-q-py313-external-venv.txt) — exit 0; **nightly-marked tests included** (dialect gate not suppressed).

Older attempt omitting nightly markers remains historical only: [full-pytest-not-nightly-py313.txt](issue-1885/full-pytest-not-nightly-py313.txt).

## Regenerated sync PR / sync-core (not completed in this push)

Issue acceptance requires `maint-68-sync-consumer-repos.yml` to regenerate closed sync PR #1884 after this fix merges, then `gh run view <run-id>` on the named Python CI job for that successor PR, and a sync-core round with the pin-conflict finding absent. That workflow reads consumer `main`; it cannot be satisfied on branch `claude/issue-1885` alone.

**#1891 Evidence check / sync-core (post-merge, still required):** After merge to `main`, use the existing controller/sync workflow to regenerate the #1891 target as needed; capture `gh run view <actual-run-id>` showing the named **Evidence** check and target head, and retain the next sync-core classification **without** the pin-conflict finding. This step is **out of scope for a coding agent** until merge and workflow availability.
