# Dashboard read-failure coverage — source #1750

Base `e2116a0bee72caf257b3513145d2f86c24e33958`. Ranking of500 commits selects ui/dashboard.py:5 repair-subject proxy matches/8 touches/104 uncovered statements. api/managers.py ranks first but its sole schema-add line 330 is structurally unreachable under the current helper; no bypass was introduced to manufacture coverage. The proxy is not an escaped-incident count.

Ten new named parameter cases cover three real pandas/SQLite missing-schema errors and seven domain-query exceptions. They assert empty DataFrames, SQL fallback column schemas, exact domain query arguments and connection release. Production ui/dashboard.py and all other covered production files are unchanged.

Current full baseline/candidate: {'passed': 1959, 'skipped': 21, 'failure': 24} → {'passed': 1969, 'skipped': 21, 'failure': 24}; ten new PASS nodes, no removed/changed old outcomes. Both commands exit1 with the identical 24 UI-auth failures/21 skips. Coverage 86.98721085% → 87.12698302%, 12447 → 12467 covered statements / 14309 statements, same 117 source files / 82 excluded lines, no covered-line regression. This measurement does not include branches (`branch_coverage=False`). The baseline also supplies the final merged #1769 test/replay nodes missing from its older fullsuite comparison; its actual CONCERNS/CONCERNS provider verdict remains unchanged.

Each of ten private production mutations replaces exactly one error fallback with a nonempty result. Exact new test fails on `assert result.empty` (RED exit 1; one failure / no errors or skips), then passes after byte-identical restoration (GREEN exit 0). Raw original/mutant source, logs, JUnit, commands/cwd/exits and hashes are in validation.tar.gz. Replay output reuse was refused before mutation, with all prior evidence and caller production bytes unchanged. The replay uses committed production from `git archive HEAD` and overlays the current test; uncommitted production is refused. It never mutates the calling checkout.

Reproduce from this committed checkout:

```sh
/opt/anaconda3/bin/python3 -m pytest tests/test_dashboard_loader_failures.py -q -o addopts=
/opt/anaconda3/bin/python3 docs/evidence/issue-1750-dashboard-read-failures/replay.py --output /tmp/new-dashboard-proof
/opt/anaconda3/bin/python3 -m pytest tests -q --cov --cov-report=json:coverage.json
/opt/anaconda3/bin/python3 -m black --check .
/opt/anaconda3/bin/python3 -m ruff check tests/test_dashboard_loader_failures.py docs/evidence/issue-1750-dashboard-read-failures/replay.py
git diff --check
```

FullBlack409 files/focusedRuff/diff-check passed. manifest.json binds all 96 raw members and three current source/test/driver files. No live PostgreSQL, real browser, deployment, hosted parity, providerPASS or whole-source 90% completion is claimed. Keep broad #1750 open. Matching keeper owns hosted CI/reviews; closer owns exact-head complete checks, full review-thread gate, seven-minute floor, guarded merge and actual comparison.
