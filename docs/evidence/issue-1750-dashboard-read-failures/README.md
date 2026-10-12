# Dashboard read-failure coverage — source #1750

Base `e2116a0bee72caf257b3513145d2f86c24e33958`. Ranking of500 commits selects ui/dashboard.py:5 repair-subject proxy matches/8 touches/104 uncovered statements. api/managers.py ranks first but its sole schema-add line 330 is structurally unreachable under the current helper; no bypass was introduced to manufacture coverage. The proxy is not an escaped-incident count.

Ten new named parameter cases cover three real pandas/SQLite missing-schema errors and seven domain-query exceptions. They assert empty DataFrames, SQL fallback column schemas, exact domain query arguments and connection release. Production ui/dashboard.py and all other covered production files are unchanged.

Archived full baseline/candidate: {'passed': 1959, 'skipped': 21, 'failure': 24} → {'passed': 1969, 'skipped': 21, 'failure': 24}; ten new PASS nodes, no removed/changed old outcomes. Both commands exit1 with the identical 24 UI-auth failures/21 skips. Coverage 86.98721085% → 87.12698302%, 12447 → 12467 covered statements / 14309 statements, same 117 source files / 82 excluded lines, no covered-line regression. This measurement does not include branches (`branch_coverage=False`). The baseline also supplies the final merged #1769 test/replay nodes missing from its older fullsuite comparison; its actual CONCERNS/CONCERNS provider verdict remains unchanged.

Each of ten private production mutations replaces exactly one error fallback with a nonempty result. Exact new test fails on `assert result.empty` (RED exit 1; one failure / no errors or skips), then passes after byte-identical restoration (GREEN exit 0). Raw original/mutant source, logs, JUnit, commands/cwd/exits and hashes are in validation.tar.gz. Replay output reuse was refused before mutation, with all prior evidence and caller production bytes unchanged. The replay uses committed production from `git archive HEAD` and overlays the current test; uncommitted production is refused. It never mutates the calling checkout.

Activate a Python environment that satisfies the project requirements, then reproduce from this committed checkout:

```sh
python -m pytest tests/test_dashboard_loader_failures.py -q -o addopts=
python docs/evidence/issue-1750-dashboard-read-failures/dashboard_read_failure_replay.py --output /tmp/new-dashboard-proof
python -m pytest tests -q --cov --cov-report=json:coverage.json
python -m black --check .
python -m ruff check tests/test_dashboard_loader_failures.py docs/evidence/issue-1750-dashboard-read-failures/dashboard_read_failure_replay.py
git diff --check
```

FullBlack409 files/focusedRuff/diff-check passed. manifest.json binds all 96 raw members and three current source/test/driver files. No live PostgreSQL, real browser, deployment, hosted parity, providerPASS or whole-source 90% completion is claimed. Keep broad #1750 open. Matching keeper owns hosted CI/reviews; closer owns exact-head complete checks, full review-thread gate, seven-minute floor, guarded merge and actual comparison.

Acceptance evidence is now checked by `tests/test_dashboard_read_failure_evidence.py`:

- [x] Ten named cases have the intended assertion RED exit1 and restored GREEN exit0, with no errors or skips. Each archived mutant changes only its selected production fallback; all original/restored source hashes match.
- [x] Archived full suites have 1959 → 1969 PASS, identical 24 existing UI-auth FAIL / 21 SKIP, both exit1, and preserve every old outcome and covered line. No full-suite PASS claim.
- [x] Archived statement coverage is 86.98721085% → 87.12698302% (12447 → 12467 / 14309), with the same 117 source files / 82 excluded lines. Branch coverage was not measured.
- [x] The historical Black receipt verifies 409 files; current full Black, focused Ruff and diff checks pass. All 96 raw archive members and three current source/test/driver SHA256 bindings verify.

These historical counts exclude the subsequent evidence and replay verification tests. The added controls do not change the archived full-suite comparison or its coverage measurement.

Run the independent checks with `pytest tests/test_dashboard_read_failure_evidence.py tests/test_dashboard_loader_failures.py -m "not slow"`. They also verify all 96 raw archive members, the archive itself, three current source/test/driver SHA256 bindings, the output-reuse refusal, and the historical Black receipt for 409 files. The archive and its manifest are unchanged.

Local revalidation on Python 3.14.8: 24 focused cases pass; a fresh private replay produces all ten intended RED1/restored GREEN0 pairs, and reuse is refused without changing any receipt or caller source. Focused Ruff and diff checks pass. Full Black checks all 410 current files successfully using Black 26.10.0's single-file formatter sequentially with the required line length and exclusions; its usual asynchronous dispatch stalled here. The local full-suite rerun also stalled after three cases and was interrupted, so no new full-suite result or coverage comparison is claimed. The new targeted loader run reports 21% for `ui/dashboard.py`; it exercises only these ten failure cases and does not reproduce the archived full-suite coverage target.

## Current review recovery

See [review-recovery-20261012](review-recovery-20261012/README.md) for32focusedPASS, current ten-case replay, timeout/rejection controls, unique driver module and baseline-equivalent exact mypy result. Historical archive members and original input bindings remain preserved; current driver bindings are separate.

## Replay phase guard revalidation

`tests/test_dashboard_read_failure_phase_guards.py` adds 22 rejection cases for interrupted, empty and killed pytest processes, reversed RED/GREEN exit codes, contradictory suite counts, a case outside its counted suite, and failures/errors/skips outside the selected case. They ensure an otherwise valid assertion receipt cannot override these phase controls.

The following run passes all 62 focused cases, including a fresh ten-mutation replay and output-reuse refusal:

```sh
pytest tests/test_dashboard_read_failure_phase_guards.py tests/test_dashboard_read_failure_replay.py tests/test_dashboard_loader_failures.py tests/test_dashboard_read_failure_evidence.py -m "not slow"
```

Both raw archives, manifests, production, driver and loader tests remain byte-identical. Full Black 26.10.0 checks all 412 current files at line length 100 with the required exclusions using its normal CLI and formatter with synchronous executor completion. The asynchronous dispatch stalls in this sandbox; its checks were not skipped. Focused Ruff and diff checks pass. No new full-suite run or coverage measurement is claimed.

Six further phase controls exercise private production restoration when RED or GREEN
times out, exits with no tests, or emits malformed JUnit. Each checks restoration before
temporary-tree cleanup, unchanged caller bytes, and absence of completed controls or a
PASS report. The 28 phase-guard cases pass. The preceding 62-case validation also
passes, including a fresh ten-pair production replay. Full Black checks all 412 current
files with the required CLI after sequential formatting checks populate a writable
temporary cache; focused Ruff and diff checks pass. Archived suite counts and coverage
remain historical. PR-body checkbox reconciliation was rejected because connector
writes require approval and this run's approval policy is `never`.

Two additional replay controls verify that an existing output directory is refused
before archive creation or mutation, and that a mismatch between committed production
and caller bytes is refused before any pytest phase starts. Both preserve caller
bytes, prior receipts and private production, and neither publishes a PASS report.
All 70 focused cases pass, including the 30 phase controls and a fresh ten-pair replay.
Current full Black checks 412 files using the writable
temporary cache; focused Ruff and diff checks pass. Targeted loader coverage remains
21%; the archived full-suite statement coverage comparison remains historical.
PR #1770 is open and ready for review. Reconciliation of its four verified acceptance
checkboxes was again rejected by the GitHub connector's approval requirement under
the `never` approval policy; a remote PR-body update still requires a permitted run.
