# Issue #1917 local Form-4 CLI deliberate-break evidence

Keep this line in PR #1921 body so `scripts/check_deliberate_break.py` can execute
the named gate (fallback parsing misses `Named test` without a colon):

```html
<!-- deliberate-break: test=tests/test_insider_flow.py::test_local_form4_command_persists_and_annotates test-file=tests/test_insider_flow.py break-file=etl/insider_flow.py -->
```

This records falsification proof for the local ingestion command in
`etl/insider_flow.py` (`run_form4_ingest` → `ingest_insider_for_issuers`). The named
gate is `test_local_form4_command_persists_and_annotates`, which exercises
`main()`, stubbed fetching, seeded SQLite, full run-summary counters,
`api.signals` readback, per-issuer fetch failure tolerance, and rerun idempotency.

## Named gate on restored code

Verified locally on 2026-11-16 (keepalive round 2, branch `cursor/issue-1917`):

```console
$ python3 -m pytest tests/test_insider_flow.py::test_local_form4_command_persists_and_annotates -q
.                                                                        [100%]
1 passed

$ python3 -m pytest tests/test_insider_flow.py -q
..........                                                               [100%]
10 passed

$ python3 scripts/check_deliberate_break.py --base "$(git merge-base origin/main HEAD)" --head HEAD --pr-body-file docs/verification/issue-1917-marker-snippet.txt
{"verdict": "PASS", "reason": "head-passed-base-failed", ...}
```

Unit gate documenting the surgical break (no ingestion call):

```console
$ python3 -m pytest tests/test_insider_flow.py::test_local_form4_skip_ingest_documents_deliberate_break_regression -q
.                                                                        [100%]
1 passed
```

## Deliberate break

In `etl/insider_flow.py`, `run_form4_ingest` temporarily replaced the
`ingest_insider_for_issuers` call with a zero-row stub so the CLI resolved
issuers but never fetched or persisted Form-4 transactions.

Running the named gate failed for the expected reason:

```console
$ python3 -m pytest tests/test_insider_flow.py::test_local_form4_command_persists_and_annotates -q
F                                                                        [100%]
=========================== short test summary info ============================
FAILED tests/test_insider_flow.py::test_local_form4_command_persists_and_annotates
  Differing items:
  {'rows_inserted': 0} != {'rows_inserted': 3}
  {'fetch_errors': 0} != {'fetch_errors': 1}
```

The break left `insider_transactions` empty and dropped the MSFT fetch error
from the summary, showing the named test detects absent ingestion and lost
per-issuer error accounting.

## Revert and restoration

The ingestion call was restored in commit `f981e20` (break at `6aedec2`). The named gate and full
`test_insider_flow.py` module pass on the restored tree (see outputs below).

Branch history retains the disposable break commit immediately before the
restoration commit so the PR diff shows the mutation and its revert.

## Acceptance criteria reconciliation (2026-11-16 keepalive)

All acceptance criteria are satisfied on the current branch:

| Criterion | Evidence |
| --- | --- |
| Named local CLI gate | `test_local_form4_command_persists_and_annotates` |
| Deliberate break → revert | Commits `6aedec2` (break) and `f981e20` (revert) |
| Full module | `tests/test_insider_flow.py` |
| Deliberate-break unit gate (skip ingest) | `test_local_form4_skip_ingest_documents_deliberate_break_regression` |
| Deliberate-break unit gate (abort on error) | `test_local_form4_strict_fetch_error_documents_deliberate_break_regression` |
| README local invocation | `README.md` → **Local Form 4 ingestion** |

### Keepalive local verification

Executed on worktree head `cursor/issue-1917` after restoring ingestion:

```console
$ python3 -m pytest tests/test_insider_flow.py::test_local_form4_command_persists_and_annotates -q
.                                                                        [100%]
1 passed

$ python3 -m pytest tests/test_insider_flow.py -q
..........                                                               [100%]
10 passed
```

At commit `6aedec2` (ingestion skipped), the named gate failed as expected:

```console
$ python3 -m pytest tests/test_insider_flow.py::test_local_form4_command_persists_and_annotates -q
F                                                                        [100%]
FAILED tests/test_insider_flow.py::test_local_form4_command_persists_and_annotates
  Differing items:
  {'rows_inserted': 0} != {'rows_inserted': 3}
  {'fetch_errors': 0} != {'fetch_errors': 1}
```