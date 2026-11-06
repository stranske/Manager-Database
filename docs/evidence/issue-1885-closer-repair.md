# Issue #1885 closer repair (PR #1893)

Checked 2026-11-06 (America/Chicago). Closes
[#1885](https://github.com/stranske/Manager-Database/issues/1885).

## PR #1893 merge and review (complete)

- **Merge state (authoritative):** `state=MERGED`, `mergedAt=2026-11-06T15:52:59Z`, `mergeCommit.oid=1cd5965635738bd467df8df5975fadc4e4b892f8`, `headRefOid=5ddb12b8e8d5a07745cc6515fb341d8c6660ac86` (`gh pr view 1893 --repo stranske/Manager-Database --json state,mergedAt,mergeCommit,headRefOid`).
- **Full diff:** `gh pr diff 1893 --repo stranske/Manager-Database` (adds six-tool validator, successor artifacts, receipts, and this document).
- **Active review threads:** Inline review on PR #1893 is resolved. Notably:
  - **CodeRabbit** on `tests/test_dashboard_read_failure_evidence.py` (pytest-cov parametrization): addressed in commit 93971b1.
  - **CodeRabbit** on `docs/evidence/issue-1885/focused-pytest.txt` (Python 3.13 / full receipt): addressed in commit 3fb33de.
  - **CodeRabbit** on `docs/evidence/issue-1885-closer-repair.md` (capture `Python CI / python 3.13` on successor head): confirmed addressed in commit 5ddb12b; this push records the actual successor head and `gh run view` receipts below.

## Successor chain vs closed sync PR #1884

- **[#1884](https://github.com/stranske/Manager-Database/pull/1884)** — `state=CLOSED` (not merged), `closedAt=2026-11-05T05:34:44Z`, `headRefOid=d4e2b79923d39b8e3d752147f677de108cea6d20`. Title: deps: sync dev tool versions. Closed when Workflows regenerated a newer consumer sync PR for the same maintenance class (new plan `generation` in the PR-body delivery record).
- **[#1891](https://github.com/stranske/Manager-Database/pull/1891)** — open successor for the same dev-tool sync automation: title **deps: sync dev tool versions**, branch `deps/sync-dev-versions-058fc8abcab9`, **`headRefOid=4d7a43785cd71b57c434bcb0a60d88e0b3f393a7`** (`gh pr view 1891 --repo stranske/Manager-Database --json state,headRefOid,headRefName`). The PR body’s `sync-pr-delivery-record` omits predecessor/successor PR IDs but replaces the same closed sync intent as #1884 with a new `generation` (`058fc8abcab9`), so **#1891 is treated here as the explicit regenerated successor** to closed #1884. #1891 is not an arbitrary substitute: it is the only open “deps: sync dev tool versions” PR on the repo (`gh pr list --state open --search "sync dev tool"`).
- **Why #1891 still fails Gate on that head:** the PR base predates **#1893** merging to `main`. Gate at `4d7a437…` failed **`Python CI / python 3.13`** (and the overall Gate run) with dashboard evidence tests asserting `current == historical` — the **#1885 / #1893** fix is missing on the merge base until #1891 is **rebased or merged with `main`**, or a newer sync PR is opened from an updated base.

## Scope vs historical sync PR #1884

- **#1884 / #1885 (historical):** Workflows dev-tool sync and the dashboard review-recovery validator admitted only formatter pins; a regenerated consumer sync PR was the follow-up closure path.
- **#1892 / #1891 (current acceptance):** The branch validator and receipts prove **six-tool exact-pin** historical-input validation (mypy, pytest, pytest-cov, black, ruff, coverage) with successor digests and deliberate-break evidence. Open **#1891** is the live regenerated consumer sync line item (see successor chain above); Gate/Evidence capture on its **current** head is recorded in the **Regenerated sync PR head** section (that head still fails `Python CI / python 3.13` until it includes merged **#1893** on `main`).

## Validator digest

| Field | SHA-256 |
| --- | --- |
| `docs/evidence/issue-1885/previous-validator.py.txt` (archived) | `0d5880682352899c8341a02dd4b1dc6fa5d0d3c4b8b72efb965679abaa42cc34` |
| `tests/test_dashboard_read_failure_evidence.py` (current) | `a75ef95ebc44129c685422014896438586e7a5170cceaa1fb4be8e9896155799` |

Recorded in `docs/evidence/issue-1885/synced-tool-pin-successor.json` (`current_validator_sha256` only; `previous_validator_sha256` unchanged).

## Dependency setup validation

Command: `python scripts/validate_dependency_test_setup.py`

Receipt: [validate_dependency_test_setup.txt](issue-1885/validate_dependency_test_setup.txt) — Python 3.13.13 (external venv), exit 0, ends with `✓ All validation checks passed!`

## Focused pytest

Command (Python **3.13**, external venv — see receipt header; avoids repo-local `.venv313`):

```text
python -m pytest tests/test_validate_dependency_test_setup.py tests/test_dashboard_read_failure_evidence.py -q
```

Receipt: [focused-pytest.txt](issue-1885/focused-pytest.txt) — Python 3.13.13, **47 passed**, 0 deselected, exit 0, complete output (parametrized loader cases and `test_review_recovery_accepts_synced_dev_tool_pin_bumps[False]`/`[True]` included). Re-run 2026-11-06 09:34 CST on head `3fb33de`.

**PR review thread `discussion_r4492669575` (focused receipt):** resolved; the replacement [focused-pytest.txt](issue-1885/focused-pytest.txt) records interpreter path and version, `repo_head`, complete output and exit status.

## Deliberate break → restore (`test_review_recovery_accepts_synced_dev_tool_pin_bumps`)

Temporary break: merged **formatter-only** `normalized_dashboard_project` and the **archived** `validate_review_recovery_current_inputs` from `docs/evidence/issue-1885/previous-validator.py.txt`, with current `SYNC_PR_1884_BUMPS` / parametrized test retained; **`docs/evidence/issue-1868/python-floor-successor.json`** `current_validator_sha256` was set to the temporary file digest, then restored. **`docs/evidence/issue-1885/synced-tool-pin-successor.json` was not edited** (pin successor still binds the archived six-tool predecessor bytes).

| Phase | Receipt | Exit | Failure locus |
| --- | --- | ---: | --- |
| RED (formatter-only) | [deliberate-break-pin-bump-red.txt](issue-1885/deliberate-break-pin-bump-red.txt) | 1 | `validate_review_recovery_current_inputs` → `assert current == historical` |
| GREEN (restored six-tool validator) | [deliberate-break-pin-bump-green.txt](issue-1885/deliberate-break-pin-bump-green.txt) | 0 | — |

## Full repository test run (`not nightly`, Python 3.13)

```text
python -m pytest -m 'not nightly' -q
```

Environment: external venv `/private/tmp/mgrdb-pr1893-py313`, **Python 3.13.13**; no virtual environment under the checkout, so the dialect-portability scan does not inspect site-packages.

Receipt: [full-pytest-not-nightly-py313.txt](issue-1885/full-pytest-not-nightly-py313.txt) — exit 0; **2593 passed, 26 skipped, 0 failed**; nightly-marked tests deselected by the marker expression. This replaces the earlier receipt for the same command, which failed `test_dialect_gate_accepts_current_audited_repo_state` because a repo-local venv was scanned.

Earlier full run including nightly tests (head `93971b1`): [full-pytest-q-py313-external-venv.txt](issue-1885/full-pytest-q-py313-external-venv.txt) — exit 0.

No source changed for these receipts, so the validator digests above are unchanged.

## Regenerated sync PR head — Gate / Python 3.13 / Evidence (verified 2026-11-06)

**Successor PR:** [#1891](https://github.com/stranske/Manager-Database/pull/1891) — **`headSha=4d7a43785cd71b57c434bcb0a60d88e0b3f393a7`** (same as Gate `headSha` below).

**Gate workflow run (PR #1891 `pull_request` event):**

```bash
gh run view 41956065739 --repo stranske/Manager-Database --json headSha,conclusion,jobs
```

- **`headSha`:** `4d7a43785cd71b57c434bcb0a60d88e0b3f393a7` (matches #1891 head).
- **`conclusion`:** `failure` (overall workflow: Python CI reported failure, then `gate-summary` failed closed).
- **Job `Python CI / python 3.13`:** `databaseId=127014042456`, **`conclusion=failure`** (pytest reported 3 failures in `tests/test_dashboard_read_failure_evidence.py` on the pre-#1893 merge base — consistent with the historical #1885 symptom).
- **Job `classify changed paths`:** `databaseId=127011611575`, **`conclusion=success`**. Logged outputs include  
  `classification-rationale=classified: 4 changed file(s); matched python-code, workflow-change, security-relevant`  
  and changed files: `.github/workflows/autofix-versions.env`, `.pre-commit-config.yaml`, `pyproject.toml`, `requirements.lock`. **No “pin conflict” / merge-conflict string appears in that rationale** (this round is dependency-sync path classification, not a pin-merge deadlock finding).

**Named “Evidence” gate:** On commit `4d7a437…`, the GitHub Checks list from `gh pr view 1891 --json statusCheckRollup` includes **CodeRabbit**, **Health 45 Agents Guard**, and **Gate**-related checks; there is **no** separate check whose **name** is exactly `Evidence`. If “Evidence” refers to a verifier or off-GitHub named gate, it is **not present as a distinct check run on this head** in the rollup captured for #1891.

**Passing successor acceptance (still outstanding):** Criteria asked for **passing** `Python CI / python 3.13` and a passing Evidence-equivalent check **on the same successor head**. That is **not** true today: the only Gate run on `4d7a437…` failed Python 3.13 until #1891 incorporates **`main` at or after merge commit `1cd5965635738bd467df8df5975fadc4e4b892f8` (#1893)**. Repair-PR CI on #1893 does not substitute for successor-head acceptance. Controller-owned **`maint-68-sync-consumer-repos.yml`** (or updating #1891’s base branch) remains the path to a **green** Gate rerun on a refreshed sync head.

**Repair-PR / local CI note:** The instruction set for this repair does **not** require running `python -m pytest -m 'not nightly' -q` when only documentation changes; the receipts in this file for full `not nightly` runs remain from PR #1893’s validation work unless re-run explicitly.
