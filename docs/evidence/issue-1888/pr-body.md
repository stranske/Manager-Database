### Keepalive: OFF
<!-- meta:issue:1888 -->

### Source Issue #1888: Raise test coverage toward 90% (round 6): finish EDGAR and re-measure CI coverage for the #1806 targets

Base: main
Head: codex/issue-1888

Source: https://github.com/stranske/Manager-Database/issues/1888

<!-- auto-status-summary:start -->
## Automated Status Summary
#### Scope
Three repair attempts by cursor, codex and claude produced no PR, so another repair on #1806 is unlikely to land. The remainder is a clearly scoped piece: the one ranked target #1808 skipped (etl/edgar_flow.py, 26 uncovered units) plus the biggest remaining gaps, with the CI before/after numbers recorded properly.
Source issue #1806; merged PR #1808 delivered: PR #1808 added one test-only module with 8 focused tests for helpers in digest_flow, signals, search, daily_report and asic, with no production changes. It does not cover EDGAR, and no CI coverage reduction is shown against the ~180-unit target.

#### Tasks
- [ ] Re-rank targets by the repo's coverage-gap ranking (escaped-defect priority, then churn, then uncovered mass) from the CI Gate coverage artifact on current main. Record the main-branch figure for uncovered statements+branches as the exact base.
- [x] Add tests for etl/edgar_flow.py (26 uncovered units), or write a concrete per-target explanation of why a branch cannot be meaningfully covered.
- [x] Cover the largest remaining gaps among the 149 units left after #1808: api/search.py (52), ui/daily_report.py (35), api/signals.py (27). Prefer behaviour-level tests over helper-level ones.
- [x] Strengthen the weak assertions in tests/test_issue_1806_coverage.py: headline_markdown escaping with an exact expected string, and exact error messages in the digest guard tests. Move the universal_search guard checks out of the parametrized rank/distance test.
- [x] For every new test, run a real break-and-revert (break the covered code, confirm the test fails, revert, confirm it passes) and paste the commands and outputs into the PR body.
- [x] Check for the negative-cache pattern (`cache.get(key) is None` used as 'not yet cached') in any code touched. Fix any instance with a minimal diff and explain it in the PR body.

#### Acceptance Criteria
- [ ] The PR body quotes the CI Gate coverage uncovered statements+branches for the targets on main before and after, taken from CI artifacts, with the reduction at or above ~180 units across the ranked targets. Each target that falls short has a written explanation.
- [x] etl/edgar_flow.py has new tests that execute its uncovered branches, or the PR body explains each branch that could not be covered.
- [x] The PR body contains the break-and-revert command output for every new test function (fail when broken, pass when reverted).
- [x] Any production change is limited to a real bug found by a test and is explained in the PR body. Otherwise the diff is test-only.
- [ ] `pytest` passes in CI.
<!-- auto-status-summary:end -->

<details>

<summary>Full Issue Text</summary>



<!-- closer-core/v1 followup-of=stranske/Manager-Database#1806@1808 -->
## Why
Three repair attempts by cursor, codex and claude produced no PR, so another repair on #1806 is unlikely to land. The remainder is a clearly scoped piece: the one ranked target #1808 skipped (etl/edgar_flow.py, 26 uncovered units) plus the biggest remaining gaps, with the CI before/after numbers recorded properly.
Source issue #1806; merged PR #1808 delivered: PR #1808 added one test-only module with 8 focused tests for helpers in digest_flow, signals, search, daily_report and asic, with no production changes. It does not cover EDGAR, and no CI coverage reduction is shown against the ~180-unit target.

## Scope
- Re-rank targets by the repo's coverage-gap ranking (escaped-defect priority, then churn, then uncovered mass) from the CI Gate coverage artifact on current main. Record the main-branch figure for uncovered statements+branches as the exact base.
- Add tests for etl/edgar_flow.py (26 uncovered units), or write a concrete per-target explanation of why a branch cannot be meaningfully covered.
- Cover the largest remaining gaps among the 149 units left after #1808: api/search.py (52), ui/daily_report.py (35), api/signals.py (27). Prefer behaviour-level tests over helper-level ones.
- Strengthen the weak assertions in tests/test_issue_1806_coverage.py: headline_markdown escaping with an exact expected string, and exact error messages in the digest guard tests. Move the universal_search guard checks out of the parametrized rank/distance test.
- For every new test, run a real break-and-revert (break the covered code, confirm the test fails, revert, confirm it passes) and paste the commands and outputs into the PR body.
- Check for the negative-cache pattern (`cache.get(key) is None` used as 'not yet cached') in any code touched. Fix any instance with a minimal diff and explain it in the PR body.

## Non-Goals
- Do not redo work that PR #1808 already delivered.

## Tasks
- [ ] Re-rank targets by the repo's coverage-gap ranking (escaped-defect priority, then churn, then uncovered mass) from the CI Gate coverage artifact on current main. Record the main-branch figure for uncovered statements+branches as the exact base.
- [x] Add tests for etl/edgar_flow.py (26 uncovered units), or write a concrete per-target explanation of why a branch cannot be meaningfully covered.
- [x] Cover the largest remaining gaps among the 149 units left after #1808: api/search.py (52), ui/daily_report.py (35), api/signals.py (27). Prefer behaviour-level tests over helper-level ones.
- [x] Strengthen the weak assertions in tests/test_issue_1806_coverage.py: headline_markdown escaping with an exact expected string, and exact error messages in the digest guard tests. Move the universal_search guard checks out of the parametrized rank/distance test.
- [x] For every new test, run a real break-and-revert (break the covered code, confirm the test fails, revert, confirm it passes) and paste the commands and outputs into the PR body.
- [x] Check for the negative-cache pattern (`cache.get(key) is None` used as 'not yet cached') in any code touched. Fix any instance with a minimal diff and explain it in the PR body.

## Acceptance Criteria
- [ ] The PR body quotes the CI Gate coverage uncovered statements+branches for the targets on main before and after, taken from CI artifacts, with the reduction at or above ~180 units across the ranked targets. Each target that falls short has a written explanation.
- [x] etl/edgar_flow.py has new tests that execute its uncovered branches, or the PR body explains each branch that could not be covered.
- [x] The PR body contains the break-and-revert command output for every new test function (fail when broken, pass when reverted).
- [x] Any production change is limited to a real bug found by a test and is explained in the PR body. Otherwise the diff is test-only.
- [ ] `pytest` passes in CI.

## Implementation Notes
Verifier report (excerpt):

```
 Completeness: scores within 1 point (avg 3.0/10, range 3.0-3.0)
- Quality: scores within 1 point (avg 7.5/10, range 7.0-8.0)
- Testing: scores within 1 point (avg 6.0/10, range 6.0-6.0)
- Risks: scores within 1 point (avg 9.0/10, range 9.0-9.0)

### Disagreement
No major disagreements detected.

### Unique Insights
- openai: The issue’s primary quantitative acceptance criterion is not met or demonstrated. The PR body explicitly states that it does not establish the required approximately 180 whole-CI uncovered statement-and-branch reduction, and reports 149 uncovered units still remaining across the ranked targets.; EDGAR (`etl/edgar_flow.py`) is named as a target but is not exercised by the new module. The acceptance criteria permit an uncovered target only when the PR explains why it could not be meaningfully covered; the PR instead says EDGAR remains for later work and provides no such justification.; The issue requires break-and-revert proof for every new test. The PR body claims a later eight-target causal-proof audit, but the required underlying artifacts/logs are unavailable in the supplied evidence. Therefore that explicit deliverable cannot be verified, and a PASS is not supportable.; The added tests provide useful focused coverage but do not substantiate the issue’s whole-CI coverage objective. The local per-file focused-suite percentages are explicitly not a substitute for the required CI-level reduction.
- anthropic: The issue's core quantitative acceptance criterion (targets' uncovered statements+branches down by ~180 or more, or each unreachable target explained) is not met. The PR body admits it 'does not establish the issue body's roughly 180 CI-unit reduction'. A later audit puts the whole-CI uncovered count for the targets at 149 total (digest 3, signals 27, search 52, daily-report 35, EDGAR 26, ASIC 6), with no exact-base comparison.; etl/edgar_flow.py, a ranked target, is not exercised at all by the added module (26 uncovered units remain). Nothing shows it cannot meaningfully be tested, so the alternative of explaining each target that could not be covered is also unmet.; The PR body reports local focused-suite percentages rather than the CI Gate coverage figures the issue says the PR is judged on. The PR's own audit says the exact-base whole-CI artifact was never bound, and the Gate coverage artifacts I could see were partially truncated or unavailable, so I cannot verify any CI-level improvement.; The break-and-revert proof was initially given for only one test (search). A later audit added causal proof for all eight test functions, which is a good outcome. The audit text itself says the criterion is only partially addressed, and the ~180-unit gap still blocks it.; The tests are small, but several assertions are weak. The `'<x>' not in headline_markdown` check and the loose `in` substring checks would pass on many broken variants. The `universal_search` assertions are bundled into a parametrized rank/distance test, which mixes concerns and repeats the same calls for every parameter set.; The diff itself is correct. It adds one test file with no production changes, which matches the low-blast-radius requirement, and the code is clean and readable.

### 🔍 LangSmith Traces
- [openai](https://smith.langchain.com/r/lc_run--01a14acf-4ef4-7630-961c-969e5200bbdd-0)
- [anthropic](https://smith.langchain.com/r/lc_run--01a14acf-6d6c-7ed0-8ae3-9799d7a04f6b-0)

---

Workflow state fingerprint for Agents Verifier. Do not edit.
```



</details>

—
PR created automatically to engage Codex.

## Verified implementation and validation

Tests and evidence only; no production or protected workflow changes. EDGAR tests
cover ingestion, reverse-order amendments, schemas, watermarks, idempotency,
deletions and persistence. Search/signal/report tests cover legacy and vector
results, annotations, page sorting/filtering and export behavior. PostgreSQL is
modeled; live-server validation is not claimed.

This follow-up adds five news-loader/headline cases. Matched targeted report
coverage rises **92% to 93%**, with missing statements+branches **29 to 25**.
Affected suites: **173 passed, 1 existing nightly skip**. Targeted coverage table:
search **97%**, signals **91%**, EDGAR **98%**, report **93%**. Black's required
repository check passes (452 files unchanged); Ruff passes. Commands and raw reports
are in [the evidence README](docs/evidence/issue-1888/README.md#news-loading-and-negative-cache-audit-2026-11-04).

Negative-cache audit completed across EDGAR, search, signals, report, digest and
ASIC. No `cache.get(key) is None` misuse was found. Search's `deduped` only stores
`SearchResult` objects; signals' nullable ticker mapping is populated once and
never treats `None` as a reason to query again. Report caching is delegated to
Streamlit. New tests confirm empty/error results are reused for the same date and
that a different date queries again. No production fix is warranted.

## Coverage accounting and remaining requirements

The main Gate coverage artifact is `13265760076` (`gate-coverage-3.13-1`) from
[run 41702885246](https://github.com/stranske/Manager-Database/actions/runs/41702885246)
at `a7f4e626bb4b8359a64c56a56211218e41b6020a`. Artifact transfer is blocked by the
sandbox; a returned ZIP reference does not verify its JSON. Shell GitHub access
still fails. The required exact CI base and CI ranking remain unchecked.
[ranking.json](docs/evidence/issue-1888/ranking.json) is a provisional local ranking
using repair-subject matches and churn, not verified escaped-defect counts.

Earlier matched local whole-suite coverage (not CI artifact credit):

| Target | Main missing statements+branches | Prior candidate | Reduction |
| --- | ---: | ---: | ---: |
| EDGAR | 16+16=32 | 9+7=16 | 16 |
| Daily report | 20+15=35 | 16+15=31 | 4 |
| Signals | 9+4=13 | 4+2=6 | 7 |
| Search | 25+27=52 | 10+12=22 | 30 |
| Total | 132 | 75 | 57 |

Later EDGAR-focused tests reduce local missing units from **19 to 6**. A later
matched full-suite comparison for the search/report/signal additions reduces
**62 to 33** against the preceding PR head. This round's report-focused comparison
reduces **29 to 25**. These different baselines/selections must not be added.
The full-suite comparison still has three identical existing failures before and
after: a chat dependency timeout, edgartools writing its home directory, and a
WASM server socket blocked by the sandbox. Full logs and counts are in the README.
No successful CI pytest result or approximately 180-unit CI reduction is claimed.

Per-target shortfalls: the four-target local main base is only 132 units, so a
180-unit reduction is unattainable within that measured base. EDGAR retains the
scheduled CLI entry point and logging paths in the focused run. Search retains
optional imports, score guards, and optional-schema/null branches. Signals retains
optional imports and guards in the targeted selection. Report retains compatibility
error/empty-value guards, login/entry-point wiring, and iteration/sort arcs.
These are residual coverage paths, not assertions that they cannot be tested.
[The README](docs/evidence/issue-1888/README.md) records the specific measurements
and remaining branches for each stage. Current-main CI measurement and candidate
CI validation remain required before acceptance can be marked complete.

## Break-and-revert proof for every new function

All 31 receipts have red exit **1** and restored exit **0**. Mutations edit actual
runtime source in disposable `/tmp` checkouts; original/restored hashes match.
Commands and exact output excerpts follow. Each linked red/green file retains
complete output and the environment/cwd; each linked diff records the source edit.
[Replay runner](docs/evidence/issue-1888/mutations/replay.py) and
[receipts](docs/evidence/issue-1888/mutations/receipts.json).

### 1. `test_manager_lookup_rejects_missing_or_deleted_manager`

Source: `etl/edgar_flow.py`; [mutation diff](docs/evidence/issue-1888/mutations/test_manager_lookup_rejects_missing_or_deleted_manager.diff).

Broken: [complete output](docs/evidence/issue-1888/mutations/test_manager_lookup_rejects_missing_or_deleted_manager-red.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_edgar_flow.py::test_manager_lookup_rejects_missing_or_deleted_manager -m 'not slow' --tb=short --disable-warnings
```

```text
FAILED tests/test_edgar_flow.py::test_manager_lookup_rejects_missing_or_deleted_manager[None-rows0] - AssertionError: assert 99 is None
1 failed, 5 passed, 8 warnings in 1.70s
exit: 1
```

Restored: [complete output](docs/evidence/issue-1888/mutations/test_manager_lookup_rejects_missing_or_deleted_manager-green.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_edgar_flow.py::test_manager_lookup_rejects_missing_or_deleted_manager -m 'not slow' --tb=short --disable-warnings
```

```text
6 passed, 8 warnings in 1.37s
exit: 0
```

### 2. `test_watermark_unavailable_without_filing_history`

Source: `etl/edgar_flow.py`; [mutation diff](docs/evidence/issue-1888/mutations/test_watermark_unavailable_without_filing_history.diff).

Broken: [complete output](docs/evidence/issue-1888/mutations/test_watermark_unavailable_without_filing_history-red.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_edgar_flow.py::test_watermark_unavailable_without_filing_history -m 'not slow' --tb=short --disable-warnings
```

```text
FAILED tests/test_edgar_flow.py::test_watermark_unavailable_without_filing_history[None] - AssertionError: assert '2000-01-01' is None
2 failed, 1 passed, 8 warnings in 1.67s
exit: 1
```

Restored: [complete output](docs/evidence/issue-1888/mutations/test_watermark_unavailable_without_filing_history-green.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_edgar_flow.py::test_watermark_unavailable_without_filing_history -m 'not slow' --tb=short --disable-warnings
```

```text
3 passed, 8 warnings in 1.61s
exit: 0
```

### 3. `test_holdings_resolution_failure_preserves_raw_rows`

Source: `etl/edgar_flow.py`; [mutation diff](docs/evidence/issue-1888/mutations/test_holdings_resolution_failure_preserves_raw_rows.diff).

Broken: [complete output](docs/evidence/issue-1888/mutations/test_holdings_resolution_failure_preserves_raw_rows-red.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_edgar_flow.py::test_holdings_resolution_failure_preserves_raw_rows -m 'not slow' --tb=short --disable-warnings
```

```text
FAILED tests/test_edgar_flow.py::test_holdings_resolution_failure_preserves_raw_rows - RuntimeError: resolver unavailable
1 failed, 8 warnings in 2.51s
exit: 1
```

Restored: [complete output](docs/evidence/issue-1888/mutations/test_holdings_resolution_failure_preserves_raw_rows-green.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_edgar_flow.py::test_holdings_resolution_failure_preserves_raw_rows -m 'not slow' --tb=short --disable-warnings
```

```text
1 passed, 8 warnings in 1.39s
exit: 0
```

### 4. `test_empty_holdings_do_not_delete_existing_rows`

Source: `etl/edgar_flow.py`; [mutation diff](docs/evidence/issue-1888/mutations/test_empty_holdings_do_not_delete_existing_rows.diff).

Broken: [complete output](docs/evidence/issue-1888/mutations/test_empty_holdings_do_not_delete_existing_rows-red.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_edgar_flow.py::test_empty_holdings_do_not_delete_existing_rows -m 'not slow' --tb=short --disable-warnings
```

```text
FAILED tests/test_edgar_flow.py::test_empty_holdings_do_not_delete_existing_rows - AssertionError: assert [] == [('KEEP',)]
1 failed, 8 warnings in 1.84s
exit: 1
```

Restored: [complete output](docs/evidence/issue-1888/mutations/test_empty_holdings_do_not_delete_existing_rows-green.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_edgar_flow.py::test_empty_holdings_do_not_delete_existing_rows -m 'not slow' --tb=short --disable-warnings
```

```text
1 passed, 8 warnings in 1.88s
exit: 0
```

### 5. `test_bitemporal_unowned_filing_rerun_preserves_version`

Source: `etl/edgar_flow.py`; [mutation diff](docs/evidence/issue-1888/mutations/test_bitemporal_unowned_filing_rerun_preserves_version.diff).

Broken: [complete output](docs/evidence/issue-1888/mutations/test_bitemporal_unowned_filing_rerun_preserves_version-red.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_edgar_flow.py::test_bitemporal_unowned_filing_rerun_preserves_version -m 'not slow' --tb=short --disable-warnings
```

```text
FAILED tests/test_edgar_flow.py::test_bitemporal_unowned_filing_rerun_preserves_version - AssertionError: assert [('AAA', 12, ...4.0, 2, None)] == [('AAA', 12, 34.0, 1, None)]
1 failed, 8 warnings in 2.36s
exit: 1
```

Restored: [complete output](docs/evidence/issue-1888/mutations/test_bitemporal_unowned_filing_rerun_preserves_version-green.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_edgar_flow.py::test_bitemporal_unowned_filing_rerun_preserves_version -m 'not slow' --tb=short --disable-warnings
```

```text
1 passed, 8 warnings in 1.43s
exit: 0
```

### 6. `test_deletion_during_second_download_keeps_first_filing`

Source: `etl/edgar_flow.py`; [mutation diff](docs/evidence/issue-1888/mutations/test_deletion_during_second_download_keeps_first_filing.diff).

Broken: [complete output](docs/evidence/issue-1888/mutations/test_deletion_during_second_download_keeps_first_filing-red.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_edgar_flow.py::test_deletion_during_second_download_keeps_first_filing -m 'not slow' --tb=short --disable-warnings
```

```text
FAILED tests/test_edgar_flow.py::test_deletion_during_second_download_keeps_first_filing - AssertionError: assert [] == [{'nameOfIssu...shPrnamt': 1}]
1 failed, 8 warnings in 1.98s
exit: 1
```

Restored: [complete output](docs/evidence/issue-1888/mutations/test_deletion_during_second_download_keeps_first_filing-green.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_edgar_flow.py::test_deletion_during_second_download_keeps_first_filing -m 'not slow' --tb=short --disable-warnings
```

```text
1 passed, 8 warnings in 1.42s
exit: 0
```

### 7. `test_binary_filing_without_manager_skips_text_index`

Source: `etl/edgar_flow.py`; [mutation diff](docs/evidence/issue-1888/mutations/test_binary_filing_without_manager_skips_text_index.diff).

Broken: [complete output](docs/evidence/issue-1888/mutations/test_binary_filing_without_manager_skips_text_index-red.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_edgar_flow.py::test_binary_filing_without_manager_skips_text_index -m 'not slow' --tb=short --disable-warnings
```

```text
FAILED tests/test_edgar_flow.py::test_binary_filing_without_manager_skips_text_index - AssertionError: expected call not found.
1 failed, 8 warnings in 1.78s
exit: 1
```

Restored: [complete output](docs/evidence/issue-1888/mutations/test_binary_filing_without_manager_skips_text_index-green.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_edgar_flow.py::test_binary_filing_without_manager_skips_text_index -m 'not slow' --tb=short --disable-warnings
```

```text
1 passed, 8 warnings in 2.28s
exit: 0
```

### 8. `test_search_guards_return_before_accessing_database`

Source: `api/search.py`; [mutation diff](docs/evidence/issue-1888/mutations/test_search_guards_return_before_accessing_database.diff).

Broken: [complete output](docs/evidence/issue-1888/mutations/test_search_guards_return_before_accessing_database-red.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1806_coverage.py::test_search_guards_return_before_accessing_database -m 'not slow' --tb=short --disable-warnings
```

```text
FAILED tests/test_issue_1806_coverage.py::test_search_guards_return_before_accessing_database - assert [None] == []
1 failed, 8 warnings in 1.97s
exit: 1
```

Restored: [complete output](docs/evidence/issue-1888/mutations/test_search_guards_return_before_accessing_database-green.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1806_coverage.py::test_search_guards_return_before_accessing_database -m 'not slow' --tb=short --disable-warnings
```

```text
1 passed, 8 warnings in 2.10s
exit: 0
```

### 9. `test_digest_delivery_configuration_guards`

Source: `etl/digest_flow.py`; [mutation diff](docs/evidence/issue-1888/mutations/test_digest_delivery_configuration_guards.diff).

Broken: [complete output](docs/evidence/issue-1888/mutations/test_digest_delivery_configuration_guards-red.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1806_coverage.py::test_digest_delivery_configuration_guards -m 'not slow' --tb=short --disable-warnings
```

```text
FAILED tests/test_issue_1806_coverage.py::test_digest_delivery_configuration_guards - AssertionError: assert 'DIGEST_EMAIL...t configured!' == 'DIGEST_EMAIL...ot configured'
1 failed, 8 warnings in 1.96s
exit: 1
```

Restored: [complete output](docs/evidence/issue-1888/mutations/test_digest_delivery_configuration_guards-green.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1806_coverage.py::test_digest_delivery_configuration_guards -m 'not slow' --tb=short --disable-warnings
```

```text
1 passed, 8 warnings in 1.92s
exit: 0
```

### 10. `test_daily_report_formatters_cover_empty_and_directional_values`

Source: `ui/daily_report.py`; [mutation diff](docs/evidence/issue-1888/mutations/test_daily_report_formatters_cover_empty_and_directional_values.diff).

Broken: [complete output](docs/evidence/issue-1888/mutations/test_daily_report_formatters_cover_empty_and_directional_values-red.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1806_coverage.py::test_daily_report_formatters_cover_empty_and_directional_values -m 'not slow' --tb=short --disable-warnings
```

```text
FAILED tests/test_issue_1806_coverage.py::test_daily_report_formatters_cover_empty_and_directional_values - AssertionError: assert '&lt;x&gt;!' == '&lt;x&gt;'
1 failed, 8 warnings in 1.95s
exit: 1
```

Restored: [complete output](docs/evidence/issue-1888/mutations/test_daily_report_formatters_cover_empty_and_directional_values-green.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1806_coverage.py::test_daily_report_formatters_cover_empty_and_directional_values -m 'not slow' --tb=short --disable-warnings
```

```text
1 passed, 8 warnings in 1.99s
exit: 0
```

### 11. `test_search_legacy_holdings_return_filing_groups_and_holdings`

Source: `api/search.py`; [mutation diff](docs/evidence/issue-1888/mutations/test_search_legacy_holdings_return_filing_groups_and_holdings.diff).

Broken: [complete output](docs/evidence/issue-1888/mutations/test_search_legacy_holdings_return_filing_groups_and_holdings-red.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_search_legacy_holdings_return_filing_groups_and_holdings -m 'not slow' --tb=short --disable-warnings
```

```text
FAILED tests/test_issue_1888_coverage.py::test_search_legacy_holdings_return_filing_groups_and_holdings - AssertionError: assert [('filing', 1...'2024-05-01')] == [('filing', 1...'2024-05-01')]
1 failed, 6 warnings in 0.81s
exit: 1
```

Restored: [complete output](docs/evidence/issue-1888/mutations/test_search_legacy_holdings_return_filing_groups_and_holdings-green.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_search_legacy_holdings_return_filing_groups_and_holdings -m 'not slow' --tb=short --disable-warnings
```

```text
1 passed, 6 warnings in 0.82s
exit: 0
```

### 12. `test_search_vector_documents_keep_shared_managers_and_deduplicate`

Source: `api/search.py`; [mutation diff](docs/evidence/issue-1888/mutations/test_search_vector_documents_keep_shared_managers_and_deduplicate.diff).

Broken: [complete output](docs/evidence/issue-1888/mutations/test_search_vector_documents_keep_shared_managers_and_deduplicate-red.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_search_vector_documents_keep_shared_managers_and_deduplicate -m 'not slow' --tb=short --disable-warnings
```

```text
FAILED tests/test_issue_1888_coverage.py::test_search_vector_documents_keep_shared_managers_and_deduplicate - AssertionError: assert [(1, None, 'm...only result')] == [(1, None, 'm...only result')]
1 failed, 6 warnings in 0.82s
exit: 1
```

Restored: [complete output](docs/evidence/issue-1888/mutations/test_search_vector_documents_keep_shared_managers_and_deduplicate-green.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_search_vector_documents_keep_shared_managers_and_deduplicate -m 'not slow' --tb=short --disable-warnings
```

```text
1 passed, 6 warnings in 0.83s
exit: 0
```

### 13. `test_search_postgres_activism_preserves_filing_context`

Source: `api/search.py`; [mutation diff](docs/evidence/issue-1888/mutations/test_search_postgres_activism_preserves_filing_context.diff).

Broken: [complete output](docs/evidence/issue-1888/mutations/test_search_postgres_activism_preserves_filing_context-red.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_search_postgres_activism_preserves_filing_context -m 'not slow' --tb=short --disable-warnings
```

```text
FAILED tests/test_issue_1888_coverage.py::test_search_postgres_activism_preserves_filing_context - AssertionError: assert {'entity_type... (5.5%)', ...} == {'entity_type... (5.5%)', ...}
1 failed, 6 warnings in 0.81s
exit: 1
```

Restored: [complete output](docs/evidence/issue-1888/mutations/test_search_postgres_activism_preserves_filing_context-green.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_search_postgres_activism_preserves_filing_context -m 'not slow' --tb=short --disable-warnings
```

```text
1 passed, 6 warnings in 0.89s
exit: 0
```

### 14. `test_conviction_annotation_survives_unavailable_holdings`

Source: `api/signals.py`; [mutation diff](docs/evidence/issue-1888/mutations/test_conviction_annotation_survives_unavailable_holdings.diff).

Broken: [complete output](docs/evidence/issue-1888/mutations/test_conviction_annotation_survives_unavailable_holdings-red.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_conviction_annotation_survives_unavailable_holdings -m 'not slow' --tb=short --disable-warnings
```

```text
FAILED tests/test_issue_1888_coverage.py::test_conviction_annotation_survives_unavailable_holdings - AssertionError: assert [{'cusip': 'A...': 50.0, ...}] == [{'cusip': 'A...': 50.0, ...}]
1 failed, 6 warnings in 0.91s
exit: 1
```

Restored: [complete output](docs/evidence/issue-1888/mutations/test_conviction_annotation_survives_unavailable_holdings-green.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_conviction_annotation_survives_unavailable_holdings -m 'not slow' --tb=short --disable-warnings
```

```text
1 passed, 6 warnings in 0.98s
exit: 0
```

### 15. `test_report_signal_query_failure_returns_empty_and_closes`

Source: `ui/daily_report.py`; [mutation diff](docs/evidence/issue-1888/mutations/test_report_signal_query_failure_returns_empty_and_closes.diff).

Broken: [complete output](docs/evidence/issue-1888/mutations/test_report_signal_query_failure_returns_empty_and_closes-red.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_report_signal_query_failure_returns_empty_and_closes -m 'not slow' --tb=short --disable-warnings
```

```text
FAILED tests/test_issue_1888_coverage.py::test_report_signal_query_failure_returns_empty_and_closes[load_crowded_trades-query_crowded_trades] - sqlite3.OperationalError: database unavailable
2 failed, 6 warnings in 1.17s
exit: 1
```

Restored: [complete output](docs/evidence/issue-1888/mutations/test_report_signal_query_failure_returns_empty_and_closes-green.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_report_signal_query_failure_returns_empty_and_closes -m 'not slow' --tb=short --disable-warnings
```

```text
2 passed, 6 warnings in 0.90s
exit: 0
```

### 16. `test_watermark_without_source_column_is_scoped_to_manager`

Source: `etl/edgar_flow.py`; [mutation diff](docs/evidence/issue-1888/mutations/test_watermark_without_source_column_is_scoped_to_manager.diff).

Broken: [complete output](docs/evidence/issue-1888/mutations/test_watermark_without_source_column_is_scoped_to_manager-red.txt).

```sh
PYTHONPATH= PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.8/x64/bin/python -m pytest tests/test_edgar_flow.py::test_watermark_without_source_column_is_scoped_to_manager -m 'not slow' --tb=short --disable-warnings
```

```text
FAILED tests/test_edgar_flow.py::test_watermark_without_source_column_is_scoped_to_manager - AssertionError: assert '2025-01-01' == '2024-05-01'
1 failed, 8 warnings in 0.94s
exit: 1
```

Restored: [complete output](docs/evidence/issue-1888/mutations/test_watermark_without_source_column_is_scoped_to_manager-green.txt).

```sh
PYTHONPATH= PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.8/x64/bin/python -m pytest tests/test_edgar_flow.py::test_watermark_without_source_column_is_scoped_to_manager -m 'not slow' --tb=short --disable-warnings
```

```text
1 passed, 8 warnings in 0.79s
exit: 0
```

### 17. `test_upsert_without_raw_key_leaves_legacy_table_unchanged`

Source: `etl/edgar_flow.py`; [mutation diff](docs/evidence/issue-1888/mutations/test_upsert_without_raw_key_leaves_legacy_table_unchanged.diff).

Broken: [complete output](docs/evidence/issue-1888/mutations/test_upsert_without_raw_key_leaves_legacy_table_unchanged-red.txt).

```sh
PYTHONPATH= PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.8/x64/bin/python -m pytest tests/test_edgar_flow.py::test_upsert_without_raw_key_leaves_legacy_table_unchanged -m 'not slow' --tb=short --disable-warnings
```

```text
FAILED tests/test_edgar_flow.py::test_upsert_without_raw_key_leaves_legacy_table_unchanged[filing_id INTEGER PRIMARY KEY] - AssertionError: assert 99 == 0
2 failed, 8 warnings in 0.96s
exit: 1
```

Restored: [complete output](docs/evidence/issue-1888/mutations/test_upsert_without_raw_key_leaves_legacy_table_unchanged-green.txt).

```sh
PYTHONPATH= PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.8/x64/bin/python -m pytest tests/test_edgar_flow.py::test_upsert_without_raw_key_leaves_legacy_table_unchanged -m 'not slow' --tb=short --disable-warnings
```

```text
2 passed, 8 warnings in 0.79s
exit: 0
```

### 18. `test_upsert_ignored_by_trigger_returns_existing_id_or_zero`

Source: `etl/edgar_flow.py`; [mutation diff](docs/evidence/issue-1888/mutations/test_upsert_ignored_by_trigger_returns_existing_id_or_zero.diff).

Broken: [complete output](docs/evidence/issue-1888/mutations/test_upsert_ignored_by_trigger_returns_existing_id_or_zero-red.txt).

```sh
PYTHONPATH= PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.8/x64/bin/python -m pytest tests/test_edgar_flow.py::test_upsert_ignored_by_trigger_returns_existing_id_or_zero -m 'not slow' --tb=short --disable-warnings
```

```text
FAILED tests/test_edgar_flow.py::test_upsert_ignored_by_trigger_returns_existing_id_or_zero[None] - AssertionError: assert 99 == ((None or 0))
2 failed, 8 warnings in 0.96s
exit: 1
```

Restored: [complete output](docs/evidence/issue-1888/mutations/test_upsert_ignored_by_trigger_returns_existing_id_or_zero-green.txt).

```sh
PYTHONPATH= PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.8/x64/bin/python -m pytest tests/test_edgar_flow.py::test_upsert_ignored_by_trigger_returns_existing_id_or_zero -m 'not slow' --tb=short --disable-warnings
```

```text
2 passed, 8 warnings in 0.80s
exit: 0
```

### 19. `test_holding_insert_without_recognized_columns_preserves_legacy_rows`

Source: `etl/edgar_flow.py`; [mutation diff](docs/evidence/issue-1888/mutations/test_holding_insert_without_recognized_columns_preserves_legacy_rows.diff).

Broken: [complete output](docs/evidence/issue-1888/mutations/test_holding_insert_without_recognized_columns_preserves_legacy_rows-red.txt).

```sh
PYTHONPATH= PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.8/x64/bin/python -m pytest tests/test_edgar_flow.py::test_holding_insert_without_recognized_columns_preserves_legacy_rows -m 'not slow' --tb=short --disable-warnings
```

```text
FAILED tests/test_edgar_flow.py::test_holding_insert_without_recognized_columns_preserves_legacy_rows - RuntimeError: schema rejected
1 failed, 8 warnings in 0.98s
exit: 1
```

Restored: [complete output](docs/evidence/issue-1888/mutations/test_holding_insert_without_recognized_columns_preserves_legacy_rows-green.txt).

```sh
PYTHONPATH= PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.8/x64/bin/python -m pytest tests/test_edgar_flow.py::test_holding_insert_without_recognized_columns_preserves_legacy_rows -m 'not slow' --tb=short --disable-warnings
```

```text
1 passed, 8 warnings in 0.79s
exit: 0
```

### 20. `test_postgres_upsert_preserves_storage_key`

Source: `etl/edgar_flow.py`; [mutation diff](docs/evidence/issue-1888/mutations/test_postgres_upsert_preserves_storage_key.diff).

Broken: [complete output](docs/evidence/issue-1888/mutations/test_postgres_upsert_preserves_storage_key-red.txt).

```sh
PYTHONPATH= PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.8/x64/bin/python -m pytest tests/test_edgar_flow.py::test_postgres_upsert_preserves_storage_key -m 'not slow' --tb=short --disable-warnings
```

```text
FAILED tests/test_edgar_flow.py::test_postgres_upsert_preserves_storage_key[None-0] - AssertionError: assert 'storage_key = EXCLUDED.storage_key' in 'INSERT INTO filings(manager_id, type, filed_date, source, raw_key, parsed_payload, storage_key) VALUES (%s, %s, %s, %...= EXCLUDED.filed_date, parsed_payload = EXCLUDED.parsed_payload, storage_key = filings.storage_key RETURNING filing_id'
3 failed, 8 warnings in 0.99s
exit: 1
```

Restored: [complete output](docs/evidence/issue-1888/mutations/test_postgres_upsert_preserves_storage_key-green.txt).

```sh
PYTHONPATH= PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.8/x64/bin/python -m pytest tests/test_edgar_flow.py::test_postgres_upsert_preserves_storage_key -m 'not slow' --tb=short --disable-warnings
```

```text
3 passed, 8 warnings in 0.81s
exit: 0
```

### 21. `test_reverse_order_original_keeps_amendment_current`

Source: `etl/edgar_flow.py`; [mutation diff](docs/evidence/issue-1888/mutations/test_reverse_order_original_keeps_amendment_current.diff).

Broken: [complete output](docs/evidence/issue-1888/mutations/test_reverse_order_original_keeps_amendment_current-red.txt).

```sh
PYTHONPATH= PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.8/x64/bin/python -m pytest tests/test_edgar_flow.py::test_reverse_order_original_keeps_amendment_current -m 'not slow' --tb=short --disable-warnings
```

```text
FAILED tests/test_edgar_flow.py::test_reverse_order_original_keeps_amendment_current - assert [(10, 250, 0,..., 1, None, 1)] == [(10, 250, 1,...100, 0, 1, 1)]
1 failed, 8 warnings in 0.95s
exit: 1
```

Restored: [complete output](docs/evidence/issue-1888/mutations/test_reverse_order_original_keeps_amendment_current-green.txt).

```sh
PYTHONPATH= PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.8/x64/bin/python -m pytest tests/test_edgar_flow.py::test_reverse_order_original_keeps_amendment_current -m 'not slow' --tb=short --disable-warnings
```

```text
1 passed, 8 warnings in 0.79s
exit: 0
```

### 22. `test_report_page_sorts_value_and_filters_news_exports`

Source: `ui/daily_report.py`; [mutation diff](docs/evidence/issue-1888/mutations/test_report_page_sorts_value_and_filters_news_exports.diff).

Broken: [complete output](docs/evidence/issue-1888/mutations/test_report_page_sorts_value_and_filters_news_exports-red.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.10/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_report_page_sorts_value_and_filters_news_exports -m 'not slow' --tb=short --disable-warnings
```

```text
FAILED tests/test_issue_1888_coverage.py::test_report_page_sorts_value_and_filters_news_exports[merger] - AssertionError: assert ['Headline1', 'Headline2'] == ['Headline1']
2 failed, 12 warnings in 1.33s
exit: 1
```

Restored: [complete output](docs/evidence/issue-1888/mutations/test_report_page_sorts_value_and_filters_news_exports-green.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.10/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_report_page_sorts_value_and_filters_news_exports -m 'not slow' --tb=short --disable-warnings
```

```text
2 passed, 12 warnings in 1.32s
exit: 0
```

### 23. `test_report_diff_unexpected_error_propagates_and_closes`

Source: `ui/daily_report.py`; [mutation diff](docs/evidence/issue-1888/mutations/test_report_diff_unexpected_error_propagates_and_closes.diff).

Broken: [complete output](docs/evidence/issue-1888/mutations/test_report_diff_unexpected_error_propagates_and_closes-red.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.10/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_report_diff_unexpected_error_propagates_and_closes -m 'not slow' --tb=short --disable-warnings
```

```text
FAILED tests/test_issue_1888_coverage.py::test_report_diff_unexpected_error_propagates_and_closes - AssertionError: Expected 'close' to be called once. Called 0 times.
1 failed, 6 warnings in 0.58s
exit: 1
```

Restored: [complete output](docs/evidence/issue-1888/mutations/test_report_diff_unexpected_error_propagates_and_closes-green.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.10/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_report_diff_unexpected_error_propagates_and_closes -m 'not slow' --tb=short --disable-warnings
```

```text
1 passed, 6 warnings in 0.54s
exit: 0
```

### 24. `test_report_latest_date_ignores_missing_sources`

Source: `ui/daily_report.py`; [mutation diff](docs/evidence/issue-1888/mutations/test_report_latest_date_ignores_missing_sources.diff).

Broken: [complete output](docs/evidence/issue-1888/mutations/test_report_latest_date_ignores_missing_sources-red.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.10/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_report_latest_date_ignores_missing_sources -m 'not slow' --tb=short --disable-warnings
```

```text
FAILED tests/test_issue_1888_coverage.py::test_report_latest_date_ignores_missing_sources - pandas.errors.DatabaseError: Execution failed on sql 'SELECT MAX(report_date) AS report_date FROM daily_diffs': no such table: daily_diffs
1 failed, 6 warnings in 0.62s
exit: 1
```

Restored: [complete output](docs/evidence/issue-1888/mutations/test_report_latest_date_ignores_missing_sources-green.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.10/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_report_latest_date_ignores_missing_sources -m 'not slow' --tb=short --disable-warnings
```

```text
1 passed, 6 warnings in 0.53s
exit: 0
```

### 25. `test_crowded_query_preserves_native_dates_and_tolerates_invalid_ids`

Source: `api/signals.py`; [mutation diff](docs/evidence/issue-1888/mutations/test_crowded_query_preserves_native_dates_and_tolerates_invalid_ids.diff).

Broken: [complete output](docs/evidence/issue-1888/mutations/test_crowded_query_preserves_native_dates_and_tolerates_invalid_ids-red.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.10/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_crowded_query_preserves_native_dates_and_tolerates_invalid_ids -m 'not slow' --tb=short --disable-warnings
```

```text
FAILED tests/test_issue_1888_coverage.py::test_crowded_query_preserves_native_dates_and_tolerates_invalid_ids["Alpha"-names0] - AssertionError: assert [{'cusip': 'A...Alpha'], ...}] == [{'cusip': 'A...es': [], ...}]
3 failed, 1 passed, 6 warnings in 0.54s
exit: 1
```

Restored: [complete output](docs/evidence/issue-1888/mutations/test_crowded_query_preserves_native_dates_and_tolerates_invalid_ids-green.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.10/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_crowded_query_preserves_native_dates_and_tolerates_invalid_ids -m 'not slow' --tb=short --disable-warnings
```

```text
4 passed, 6 warnings in 0.52s
exit: 0
```

### 26. `test_search_sqlite_vector_failure_preserves_text_results`

Source: `api/search.py`; [mutation diff](docs/evidence/issue-1888/mutations/test_search_sqlite_vector_failure_preserves_text_results.diff).

Broken: [complete output](docs/evidence/issue-1888/mutations/test_search_sqlite_vector_failure_preserves_text_results-red.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.10/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_search_sqlite_vector_failure_preserves_text_results -m 'not slow' --tb=short --disable-warnings
```

```text
FAILED tests/test_issue_1888_coverage.py::test_search_sqlite_vector_failure_preserves_text_results[embedding] - OSError: vector index unavailable
1 failed, 1 passed, 6 warnings in 0.59s
exit: 1
```

Restored: [complete output](docs/evidence/issue-1888/mutations/test_search_sqlite_vector_failure_preserves_text_results-green.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.10/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_search_sqlite_vector_failure_preserves_text_results -m 'not slow' --tb=short --disable-warnings
```

```text
2 passed, 6 warnings in 0.52s
exit: 0
```

### 27. `test_search_postgres_ignores_vector_hits_without_document_id`

Source: `api/search.py`; [mutation diff](docs/evidence/issue-1888/mutations/test_search_postgres_ignores_vector_hits_without_document_id.diff).

Broken: [complete output](docs/evidence/issue-1888/mutations/test_search_postgres_ignores_vector_hits_without_document_id-red.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.10/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_search_postgres_ignores_vector_hits_without_document_id -m 'not slow' --tb=short --disable-warnings
```

```text
FAILED tests/test_issue_1888_coverage.py::test_search_postgres_ignores_vector_hits_without_document_id - ValueError: missing document ID
1 failed, 6 warnings in 0.54s
exit: 1
```

Restored: [complete output](docs/evidence/issue-1888/mutations/test_search_postgres_ignores_vector_hits_without_document_id-green.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.10/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_search_postgres_ignores_vector_hits_without_document_id -m 'not slow' --tb=short --disable-warnings
```

```text
1 passed, 6 warnings in 0.53s
exit: 0
```

### 28. `test_report_postgres_news_caches_results_by_date_and_closes`

Source: `ui/daily_report.py`; [mutation diff](docs/evidence/issue-1888/mutations/test_report_postgres_news_caches_results_by_date_and_closes.diff).

Broken: [complete output](docs/evidence/issue-1888/mutations/test_report_postgres_news_caches_results_by_date_and_closes-red.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.8/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_report_postgres_news_caches_results_by_date_and_closes -m 'not slow' --tb=short --disable-warnings
```

```text
FAILED tests/test_issue_1888_coverage.py::test_report_postgres_news_caches_results_by_date_and_closes[populated] - AssertionError: assert 'WHERE n.published_at::date = %s' in 'SELECT n.headline, n.url, n.published_at, n.source, n.topics, n.confidence, m.name AS manager_name FROM news_items n LEFT JOIN managers m ON m.manager_id = n.manager_id WHERE n.published_at::date = ? ORDER BY n.published_at DESC LIMIT 50'
3 failed, 6 warnings in 0.57s
exit: 1
```

Restored: [complete output](docs/evidence/issue-1888/mutations/test_report_postgres_news_caches_results_by_date_and_closes-green.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.8/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_report_postgres_news_caches_results_by_date_and_closes -m 'not slow' --tb=short --disable-warnings
```

```text
3 passed, 6 warnings in 0.55s
exit: 0
```

### 29. `test_report_whitespace_news_url_keeps_escaped_headline`

Source: `ui/daily_report.py`; [mutation diff](docs/evidence/issue-1888/mutations/test_report_whitespace_news_url_keeps_escaped_headline.diff).

Broken: [complete output](docs/evidence/issue-1888/mutations/test_report_whitespace_news_url_keeps_escaped_headline-red.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.8/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_report_whitespace_news_url_keeps_escaped_headline -m 'not slow' --tb=short --disable-warnings
```

```text
FAILED tests/test_issue_1888_coverage.py::test_report_whitespace_news_url_keeps_escaped_headline[ ] - AssertionError: assert '\\[Desk\\] \...;x&gt; &amp;!' == '\\[Desk\\] \...t;x&gt; &amp;'
2 failed, 6 warnings in 0.54s
exit: 1
```

Restored: [complete output](docs/evidence/issue-1888/mutations/test_report_whitespace_news_url_keeps_escaped_headline-green.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.8/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_report_whitespace_news_url_keeps_escaped_headline -m 'not slow' --tb=short --disable-warnings
```

```text
2 passed, 6 warnings in 0.53s
exit: 0
```

## Date navigation follow-up (2026-11-04)

Two additional test functions (three cases) verify that choosing an older report
or pressing the latest-report button keeps displayed rows, exported rows, and CSV
filenames in sync, using the real SQLite loaders. A rejected login must stop before
date widgets, tabs, or database access. No production change was needed. The tested
report surface still has no negative-cache sentinel misuse.

Matched local report-suite coverage against preceding head `63569b1` increases
**93% to 96%**; missing statements+branches decrease **12+13=25 to 8+7=15**.
Before: **38 passed**. After: **41 passed**. The affected EDGAR/search/signal/report
suites pass: **176 passed, 1 existing nightly skip**. These are local incremental
measurements, not CI artifact credit or amounts to add to earlier comparisons.
[Commands and raw reports](docs/evidence/issue-1888/README.md#date-navigation-follow-up-2026-11-04).

The live checklist reconciliation was attempted before adding these tests. All 22
checkboxes are retained here; 16 have verified implementation/evidence, and six
remain open for exact CI artifact accounting or CI validation. Publishing the body,
adding `needs-human`, and posting the blocker comment were each rejected with
`MCP tool call requires approval, but approval policy is never`. No live checkbox
update is claimed. Git staging in the primary checkout fails with read-only
`.git/index.lock`; a source commit and patch are preserved in a writable local clone.

Current main was rechecked through the GitHub connector at
`a7f4e626bb4b8359a64c56a56211218e41b6020a`; artifact `13265760076` still belongs to
that main SHA. Direct shell access to GitHub fails, and the downloadable file
reference does not establish the artifact JSON contents. The first task remains
blocked. The prior published head `63569b1` has successful
[Gate run 41774281794](https://github.com/stranske/Manager-Database/actions/runs/41774281794),
including the Pytest steps for Python 3.13 (job `126391914723`) and 3.14
(job `126391914767`). That result predates these local additions; CI for the new
commit remains pending, so its acceptance checkboxes stay open.

### 30. `test_report_date_navigation_keeps_display_and_export_in_sync`

Source: `ui/daily_report.py`; [mutation diff](docs/evidence/issue-1888/mutations/test_report_date_navigation_keeps_display_and_export_in_sync.diff).

Broken: [complete output](docs/evidence/issue-1888/mutations/test_report_date_navigation_keeps_display_and_export_in_sync-red.txt).

```sh
PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_report_date_navigation_keeps_display_and_export_in_sync -m 'not slow' --tb=short --disable-warnings
```

```text
FAILED tests/test_issue_1888_coverage.py::test_report_date_navigation_keeps_display_and_export_in_sync[False] - AssertionError: assert ['AAA', 'BBB'] == ['OLDER']
FAILED tests/test_issue_1888_coverage.py::test_report_date_navigation_keeps_display_and_export_in_sync[True] - AssertionError: assert ['OLDER'] == ['AAA', 'BBB']
2 failed, 12 warnings in 2.38s
exit: 1
```

Restored: [complete output](docs/evidence/issue-1888/mutations/test_report_date_navigation_keeps_display_and_export_in_sync-green.txt).

```sh
PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_report_date_navigation_keeps_display_and_export_in_sync -m 'not slow' --tb=short --disable-warnings
```

```text
2 passed, 12 warnings in 2.42s
exit: 0
```

### 31. `test_report_login_stop_prevents_database_access`

Source: `ui/daily_report.py`; [mutation diff](docs/evidence/issue-1888/mutations/test_report_login_stop_prevents_database_access.diff).

Broken: [complete output](docs/evidence/issue-1888/mutations/test_report_login_stop_prevents_database_access-red.txt).

```sh
PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_report_login_stop_prevents_database_access -m 'not slow' --tb=short --disable-warnings
```

```text
FAILED tests/test_issue_1888_coverage.py::test_report_login_stop_prevents_database_access - AssertionError: database accessed before login
1 failed, 6 warnings in 1.10s
exit: 1
```

Restored: [complete output](docs/evidence/issue-1888/mutations/test_report_login_stop_prevents_database_access-green.txt).

```sh
PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_report_login_stop_prevents_database_access -m 'not slow' --tb=short --disable-warnings
```

```text
1 passed, 6 warnings in 0.93s
exit: 0
```

### Full-suite limit and artifact handoff

The full local command `PYTHONPATH=/tmp/issue1888-runtime timeout --signal=INT 240
pytest tests/ -m 'not slow' --tb=short --disable-warnings` exited **124** after
reporting **2 failed, 2527 passed, 28 skipped** and interrupting Streamlit's local
script runner. The failures match previously recorded chat dependency timing and
edgartools' read-only `/home/runner/.edgar` directory errors. No existing test was
changed or disabled. [Complete output](docs/evidence/issue-1888/navigation-full-pytest.txt).
The successful 176-test affected-suite run is separate from this incomplete run.

[Verified CI metadata](docs/evidence/issue-1888/navigation-ci-metadata.json)
identifies the prior published head's coverage artifacts: `13296162876` for
Python 3.13 and `13296791087` for 3.14. These are handoff identifiers, not inspected
coverage contents or CI validation of the new tests. PR #1889 remains open and
ready for review (`draft=false`).
