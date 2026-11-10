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



—
PR created automatically to engage Codex.

## Verified implementation and validation

Tests and evidence only; no production or protected workflow changes. EDGAR tests
cover ingestion, reverse-order amendments, schemas, watermarks, idempotency,
deletions and persistence. Search/signal/report tests cover legacy and vector
results, annotations, page sorting/filtering and export behavior. PostgreSQL is
modeled; live-server validation is not claimed.

The latest follow-up adds two report behavior test functions (four cases). Real
SQLite loaders verify unknown versus zero deltas, exact display/CSV agreement,
blank topics, incomplete signal identifiers and cached empty activism results
with connection cleanup. Matched targeted coverage rises **97% to 99%**, missing
statements+branches **13 to 2**; only the script entry point remains. Related
suites pass **187 tests, 2 existing nightly skips**. Black passes (452 files
unchanged); Ruff passes. These are local measurements, not CI artifact credit.
See [the evidence README](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/README.md#report-missing-values-and-optional-activism-follow-up-2026-11-04)
for raw JSON, commands and source mutation proof. Candidate head `64fcbc71` passed
Gate run `41839886184` on Python 3.13 and 3.14; see [run details](https://github.com/stranske/Manager-Database/actions/runs/41839886184).

Negative-cache audit completed across EDGAR, search, signals, report, digest and
ASIC. No `cache.get(key) is None` misuse was found. Search's `deduped` only stores
`SearchResult` objects; signals' nullable ticker mapping is populated once and
never treats `None` as a reason to query again. Report caching is delegated to
Streamlit. New tests confirm empty/error results are reused for the same date and
that a different date queries again. No production fix is warranted.

## Coverage accounting and remaining requirements

The main Gate coverage artifact is `13265760076` (`gate-coverage-3.13-1`) from
[run 41702885246](https://github.com/stranske/Manager-Database/actions/runs/41702885246)
at `a7f4e626bb4b8359a64c56a56211218e41b6020a`. Artifacts were successfully downloaded
and verified for both Python 3.13 and 3.14, with identical coverage counts.
Candidate Gate run [41839886184](https://github.com/stranske/Manager-Database/actions/runs/41839886184)
at `64fcbc71d7fc93c292aff58a57d19d63b2de2b98` also succeeded with verified
coverage artifacts for Python 3.13 and 3.14.

Verified CI artifact-based uncovered units (statements+branches):

| Target | Baseline (41702885246) | Candidate (41839886184) | Reduction |
| --- | ---: | ---: | ---: |
| EDGAR | 15+14 = 29 | 2+1 = 3 | 26 |
| Daily report | 20+15 = 35 | 1+1 = 2 | 33 |
| Signals | 9+4 = 13 | 2+0 = 2 | 11 |
| Search | 25+27 = 52 | 4+2 = 6 | 46 |
| **Total** | **130** | **13** | **117** |

[ranking.json](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/ranking.json)
contains the verified CI-based ranking using the repository's documented 500-source-commit
repair-subject proxy, then source touches, then verified CI uncovered units. The proxy
is not a verified escaped-defect count. The baseline contains 130 uncovered units
across the four targets, so a reduction of ~180 units is not attainable within this
measured scope; the mathematical bound is 130 units maximum. This criterion remains
unmet pending an explicit scope decision.

Verified CI Gate artifact comparison (Python 3.13 and 3.14):

| Target | Main missing statements+branches | Candidate | Reduction |
| --- | ---: | ---: | ---: |
| EDGAR | 15+14 = 29 | 2+1 = 3 | 26 |
| Daily report | 20+15 = 35 | 1+1 = 2 | 33 |
| Signals | 9+4 = 13 | 2+0 = 2 | 11 |
| Search | 25+27 = 52 | 4+2 = 6 | 46 |
| **Total** | **130** | **13** | **117** |

The baseline contains 130 uncovered units (statements+branches) across the four
targets. A reduction of ~180 units is not attainable within this measured scope;
the mathematical bound is 130 units maximum. This criterion remains unmet pending
an explicit scope decision. Per-target shortfall explanations are in [ranking.json](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/ranking.json).

The Gate runs succeeded: baseline [41702885246](https://github.com/stranske/Manager-Database/actions/runs/41702885246)
at `a7f4e626` and candidate [41839886184](https://github.com/stranske/Manager-Database/actions/runs/41839886184)
at `64fcbc71`. Both Python 3.13 and 3.14 report identical counts.

Per-target shortfalls: the four-target local main base is only 132 units, so a
180-unit reduction is unattainable within that measured base. EDGAR retains the
scheduled CLI entry point and logging paths in the focused run. Search retains
optional imports, score guards, and optional-schema/null branches. Signals retains
optional imports and guards in the targeted selection. Report retains compatibility
error/empty-value guards, the script entry point, and an incomplete annotation
branch. Login and date-navigation wiring are covered by the preceding tests.
These are residual coverage paths, not assertions that they cannot be tested.
[The README](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/README.md)
records the specific measurements and remaining branches for each stage.
Verified CI measurement and ranking are now documented above.

## Break-and-revert proof for every new function

All 39 receipts have red exit **1** and restored exit **0**. Mutations edit actual
runtime source in disposable `/tmp` checkouts; original/restored hashes match.
Commands and exact output excerpts follow. Each linked red/green file retains
complete output and the environment/cwd; each linked diff records the source edit.
[Replay runner](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/replay.py) and
[receipts](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/receipts.json).

### 1. `test_manager_lookup_rejects_missing_or_deleted_manager`

Source: `etl/edgar_flow.py`; [mutation diff](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_manager_lookup_rejects_missing_or_deleted_manager.diff).

Broken: [complete output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_manager_lookup_rejects_missing_or_deleted_manager-red.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_edgar_flow.py::test_manager_lookup_rejects_missing_or_deleted_manager -m 'not slow' --tb=short --disable-warnings
```

```text
FAILED tests/test_edgar_flow.py::test_manager_lookup_rejects_missing_or_deleted_manager[None-rows0] - AssertionError: assert 99 is None
1 failed, 5 passed, 8 warnings in 1.70s
exit: 1
```

Restored: [complete output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_manager_lookup_rejects_missing_or_deleted_manager-green.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_edgar_flow.py::test_manager_lookup_rejects_missing_or_deleted_manager -m 'not slow' --tb=short --disable-warnings
```

```text
6 passed, 8 warnings in 1.37s
exit: 0
```

### 2. `test_watermark_unavailable_without_filing_history`

Source: `etl/edgar_flow.py`; [mutation diff](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_watermark_unavailable_without_filing_history.diff).

Broken: [complete output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_watermark_unavailable_without_filing_history-red.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_edgar_flow.py::test_watermark_unavailable_without_filing_history -m 'not slow' --tb=short --disable-warnings
```

```text
FAILED tests/test_edgar_flow.py::test_watermark_unavailable_without_filing_history[None] - AssertionError: assert '2000-01-01' is None
2 failed, 1 passed, 8 warnings in 1.67s
exit: 1
```

Restored: [complete output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_watermark_unavailable_without_filing_history-green.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_edgar_flow.py::test_watermark_unavailable_without_filing_history -m 'not slow' --tb=short --disable-warnings
```

```text
3 passed, 8 warnings in 1.61s
exit: 0
```

### 3. `test_holdings_resolution_failure_preserves_raw_rows`

Source: `etl/edgar_flow.py`; [mutation diff](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_holdings_resolution_failure_preserves_raw_rows.diff).

Broken: [complete output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_holdings_resolution_failure_preserves_raw_rows-red.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_edgar_flow.py::test_holdings_resolution_failure_preserves_raw_rows -m 'not slow' --tb=short --disable-warnings
```

```text
FAILED tests/test_edgar_flow.py::test_holdings_resolution_failure_preserves_raw_rows - RuntimeError: resolver unavailable
1 failed, 8 warnings in 2.51s
exit: 1
```

Restored: [complete output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_holdings_resolution_failure_preserves_raw_rows-green.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_edgar_flow.py::test_holdings_resolution_failure_preserves_raw_rows -m 'not slow' --tb=short --disable-warnings
```

```text
1 passed, 8 warnings in 1.39s
exit: 0
```

### 4. `test_empty_holdings_do_not_delete_existing_rows`

Source: `etl/edgar_flow.py`; [mutation diff](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_empty_holdings_do_not_delete_existing_rows.diff).

Broken: [complete output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_empty_holdings_do_not_delete_existing_rows-red.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_edgar_flow.py::test_empty_holdings_do_not_delete_existing_rows -m 'not slow' --tb=short --disable-warnings
```

```text
FAILED tests/test_edgar_flow.py::test_empty_holdings_do_not_delete_existing_rows - AssertionError: assert [] == [('KEEP',)]
1 failed, 8 warnings in 1.84s
exit: 1
```

Restored: [complete output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_empty_holdings_do_not_delete_existing_rows-green.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_edgar_flow.py::test_empty_holdings_do_not_delete_existing_rows -m 'not slow' --tb=short --disable-warnings
```

```text
1 passed, 8 warnings in 1.88s
exit: 0
```

### 5. `test_bitemporal_unowned_filing_rerun_preserves_version`

Source: `etl/edgar_flow.py`; [mutation diff](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_bitemporal_unowned_filing_rerun_preserves_version.diff).

Broken: [complete output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_bitemporal_unowned_filing_rerun_preserves_version-red.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_edgar_flow.py::test_bitemporal_unowned_filing_rerun_preserves_version -m 'not slow' --tb=short --disable-warnings
```

```text
FAILED tests/test_edgar_flow.py::test_bitemporal_unowned_filing_rerun_preserves_version - AssertionError: assert [('AAA', 12, ...4.0, 2, None)] == [('AAA', 12, 34.0, 1, None)]
1 failed, 8 warnings in 2.36s
exit: 1
```

Restored: [complete output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_bitemporal_unowned_filing_rerun_preserves_version-green.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_edgar_flow.py::test_bitemporal_unowned_filing_rerun_preserves_version -m 'not slow' --tb=short --disable-warnings
```

```text
1 passed, 8 warnings in 1.43s
exit: 0
```

### 6. `test_deletion_during_second_download_keeps_first_filing`

Source: `etl/edgar_flow.py`; [mutation diff](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_deletion_during_second_download_keeps_first_filing.diff).

Broken: [complete output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_deletion_during_second_download_keeps_first_filing-red.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_edgar_flow.py::test_deletion_during_second_download_keeps_first_filing -m 'not slow' --tb=short --disable-warnings
```

```text
FAILED tests/test_edgar_flow.py::test_deletion_during_second_download_keeps_first_filing - AssertionError: assert [] == [{'nameOfIssu...shPrnamt': 1}]
1 failed, 8 warnings in 1.98s
exit: 1
```

Restored: [complete output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_deletion_during_second_download_keeps_first_filing-green.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_edgar_flow.py::test_deletion_during_second_download_keeps_first_filing -m 'not slow' --tb=short --disable-warnings
```

```text
1 passed, 8 warnings in 1.42s
exit: 0
```

### 7. `test_binary_filing_without_manager_skips_text_index`

Source: `etl/edgar_flow.py`; [mutation diff](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_binary_filing_without_manager_skips_text_index.diff).

Broken: [complete output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_binary_filing_without_manager_skips_text_index-red.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_edgar_flow.py::test_binary_filing_without_manager_skips_text_index -m 'not slow' --tb=short --disable-warnings
```

```text
FAILED tests/test_edgar_flow.py::test_binary_filing_without_manager_skips_text_index - AssertionError: expected call not found.
1 failed, 8 warnings in 1.78s
exit: 1
```

Restored: [complete output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_binary_filing_without_manager_skips_text_index-green.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_edgar_flow.py::test_binary_filing_without_manager_skips_text_index -m 'not slow' --tb=short --disable-warnings
```

```text
1 passed, 8 warnings in 2.28s
exit: 0
```

### 8. `test_search_guards_return_before_accessing_database`

Source: `api/search.py`; [mutation diff](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_search_guards_return_before_accessing_database.diff).

Broken: [complete output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_search_guards_return_before_accessing_database-red.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1806_coverage.py::test_search_guards_return_before_accessing_database -m 'not slow' --tb=short --disable-warnings
```

```text
FAILED tests/test_issue_1806_coverage.py::test_search_guards_return_before_accessing_database - assert [None] == []
1 failed, 8 warnings in 1.97s
exit: 1
```

Restored: [complete output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_search_guards_return_before_accessing_database-green.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1806_coverage.py::test_search_guards_return_before_accessing_database -m 'not slow' --tb=short --disable-warnings
```

```text
1 passed, 8 warnings in 2.10s
exit: 0
```

### 9. `test_digest_delivery_configuration_guards`

Source: `etl/digest_flow.py`; [mutation diff](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_digest_delivery_configuration_guards.diff).

Broken: [complete output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_digest_delivery_configuration_guards-red.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1806_coverage.py::test_digest_delivery_configuration_guards -m 'not slow' --tb=short --disable-warnings
```

```text
FAILED tests/test_issue_1806_coverage.py::test_digest_delivery_configuration_guards - AssertionError: assert 'DIGEST_EMAIL...t configured!' == 'DIGEST_EMAIL...ot configured'
1 failed, 8 warnings in 1.96s
exit: 1
```

Restored: [complete output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_digest_delivery_configuration_guards-green.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1806_coverage.py::test_digest_delivery_configuration_guards -m 'not slow' --tb=short --disable-warnings
```

```text
1 passed, 8 warnings in 1.92s
exit: 0
```

### 10. `test_daily_report_formatters_cover_empty_and_directional_values`

Source: `ui/daily_report.py`; [mutation diff](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_daily_report_formatters_cover_empty_and_directional_values.diff).

Broken: [complete output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_daily_report_formatters_cover_empty_and_directional_values-red.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1806_coverage.py::test_daily_report_formatters_cover_empty_and_directional_values -m 'not slow' --tb=short --disable-warnings
```

```text
FAILED tests/test_issue_1806_coverage.py::test_daily_report_formatters_cover_empty_and_directional_values - AssertionError: assert '&lt;x&gt;!' == '&lt;x&gt;'
1 failed, 8 warnings in 1.95s
exit: 1
```

Restored: [complete output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_daily_report_formatters_cover_empty_and_directional_values-green.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1806_coverage.py::test_daily_report_formatters_cover_empty_and_directional_values -m 'not slow' --tb=short --disable-warnings
```

```text
1 passed, 8 warnings in 1.99s
exit: 0
```

### 11. `test_search_legacy_holdings_return_filing_groups_and_holdings`

Source: `api/search.py`; [mutation diff](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_search_legacy_holdings_return_filing_groups_and_holdings.diff).

Broken: [complete output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_search_legacy_holdings_return_filing_groups_and_holdings-red.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_search_legacy_holdings_return_filing_groups_and_holdings -m 'not slow' --tb=short --disable-warnings
```

```text
FAILED tests/test_issue_1888_coverage.py::test_search_legacy_holdings_return_filing_groups_and_holdings - AssertionError: assert [('filing', 1...'2024-05-01')] == [('filing', 1...'2024-05-01')]
1 failed, 6 warnings in 0.81s
exit: 1
```

Restored: [complete output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_search_legacy_holdings_return_filing_groups_and_holdings-green.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_search_legacy_holdings_return_filing_groups_and_holdings -m 'not slow' --tb=short --disable-warnings
```

```text
1 passed, 6 warnings in 0.82s
exit: 0
```

### 12. `test_search_vector_documents_keep_shared_managers_and_deduplicate`

Source: `api/search.py`; [mutation diff](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_search_vector_documents_keep_shared_managers_and_deduplicate.diff).

Broken: [complete output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_search_vector_documents_keep_shared_managers_and_deduplicate-red.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_search_vector_documents_keep_shared_managers_and_deduplicate -m 'not slow' --tb=short --disable-warnings
```

```text
FAILED tests/test_issue_1888_coverage.py::test_search_vector_documents_keep_shared_managers_and_deduplicate - AssertionError: assert [(1, None, 'm...only result')] == [(1, None, 'm...only result')]
1 failed, 6 warnings in 0.82s
exit: 1
```

Restored: [complete output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_search_vector_documents_keep_shared_managers_and_deduplicate-green.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_search_vector_documents_keep_shared_managers_and_deduplicate -m 'not slow' --tb=short --disable-warnings
```

```text
1 passed, 6 warnings in 0.83s
exit: 0
```

### 13. `test_search_postgres_activism_preserves_filing_context`

Source: `api/search.py`; [mutation diff](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_search_postgres_activism_preserves_filing_context.diff).

Broken: [complete output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_search_postgres_activism_preserves_filing_context-red.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_search_postgres_activism_preserves_filing_context -m 'not slow' --tb=short --disable-warnings
```

```text
FAILED tests/test_issue_1888_coverage.py::test_search_postgres_activism_preserves_filing_context - AssertionError: assert {'entity_type... (5.5%)', ...} == {'entity_type... (5.5%)', ...}
1 failed, 6 warnings in 0.81s
exit: 1
```

Restored: [complete output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_search_postgres_activism_preserves_filing_context-green.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_search_postgres_activism_preserves_filing_context -m 'not slow' --tb=short --disable-warnings
```

```text
1 passed, 6 warnings in 0.89s
exit: 0
```

### 14. `test_conviction_annotation_survives_unavailable_holdings`

Source: `api/signals.py`; [mutation diff](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_conviction_annotation_survives_unavailable_holdings.diff).

Broken: [complete output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_conviction_annotation_survives_unavailable_holdings-red.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_conviction_annotation_survives_unavailable_holdings -m 'not slow' --tb=short --disable-warnings
```

```text
FAILED tests/test_issue_1888_coverage.py::test_conviction_annotation_survives_unavailable_holdings - AssertionError: assert [{'cusip': 'A...': 50.0, ...}] == [{'cusip': 'A...': 50.0, ...}]
1 failed, 6 warnings in 0.91s
exit: 1
```

Restored: [complete output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_conviction_annotation_survives_unavailable_holdings-green.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_conviction_annotation_survives_unavailable_holdings -m 'not slow' --tb=short --disable-warnings
```

```text
1 passed, 6 warnings in 0.98s
exit: 0
```

### 15. `test_report_signal_query_failure_returns_empty_and_closes`

Source: `ui/daily_report.py`; [mutation diff](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_report_signal_query_failure_returns_empty_and_closes.diff).

Broken: [complete output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_report_signal_query_failure_returns_empty_and_closes-red.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_report_signal_query_failure_returns_empty_and_closes -m 'not slow' --tb=short --disable-warnings
```

```text
FAILED tests/test_issue_1888_coverage.py::test_report_signal_query_failure_returns_empty_and_closes[load_crowded_trades-query_crowded_trades] - sqlite3.OperationalError: database unavailable
2 failed, 6 warnings in 1.17s
exit: 1
```

Restored: [complete output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_report_signal_query_failure_returns_empty_and_closes-green.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_report_signal_query_failure_returns_empty_and_closes -m 'not slow' --tb=short --disable-warnings
```

```text
2 passed, 6 warnings in 0.90s
exit: 0
```

### 16. `test_watermark_without_source_column_is_scoped_to_manager`

Source: `etl/edgar_flow.py`; [mutation diff](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_watermark_without_source_column_is_scoped_to_manager.diff).

Broken: [complete output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_watermark_without_source_column_is_scoped_to_manager-red.txt).

```sh
PYTHONPATH= PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.8/x64/bin/python -m pytest tests/test_edgar_flow.py::test_watermark_without_source_column_is_scoped_to_manager -m 'not slow' --tb=short --disable-warnings
```

```text
FAILED tests/test_edgar_flow.py::test_watermark_without_source_column_is_scoped_to_manager - AssertionError: assert '2025-01-01' == '2024-05-01'
1 failed, 8 warnings in 0.94s
exit: 1
```

Restored: [complete output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_watermark_without_source_column_is_scoped_to_manager-green.txt).

```sh
PYTHONPATH= PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.8/x64/bin/python -m pytest tests/test_edgar_flow.py::test_watermark_without_source_column_is_scoped_to_manager -m 'not slow' --tb=short --disable-warnings
```

```text
1 passed, 8 warnings in 0.79s
exit: 0
```

### 17. `test_upsert_without_raw_key_leaves_legacy_table_unchanged`

Source: `etl/edgar_flow.py`; [mutation diff](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_upsert_without_raw_key_leaves_legacy_table_unchanged.diff).

Broken: [complete output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_upsert_without_raw_key_leaves_legacy_table_unchanged-red.txt).

```sh
PYTHONPATH= PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.8/x64/bin/python -m pytest tests/test_edgar_flow.py::test_upsert_without_raw_key_leaves_legacy_table_unchanged -m 'not slow' --tb=short --disable-warnings
```

```text
FAILED tests/test_edgar_flow.py::test_upsert_without_raw_key_leaves_legacy_table_unchanged[filing_id INTEGER PRIMARY KEY] - AssertionError: assert 99 == 0
2 failed, 8 warnings in 0.96s
exit: 1
```

Restored: [complete output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_upsert_without_raw_key_leaves_legacy_table_unchanged-green.txt).

```sh
PYTHONPATH= PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.8/x64/bin/python -m pytest tests/test_edgar_flow.py::test_upsert_without_raw_key_leaves_legacy_table_unchanged -m 'not slow' --tb=short --disable-warnings
```

```text
2 passed, 8 warnings in 0.79s
exit: 0
```

### 18. `test_upsert_ignored_by_trigger_returns_existing_id_or_zero`

Source: `etl/edgar_flow.py`; [mutation diff](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_upsert_ignored_by_trigger_returns_existing_id_or_zero.diff).

Broken: [complete output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_upsert_ignored_by_trigger_returns_existing_id_or_zero-red.txt).

```sh
PYTHONPATH= PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.8/x64/bin/python -m pytest tests/test_edgar_flow.py::test_upsert_ignored_by_trigger_returns_existing_id_or_zero -m 'not slow' --tb=short --disable-warnings
```

```text
FAILED tests/test_edgar_flow.py::test_upsert_ignored_by_trigger_returns_existing_id_or_zero[None] - AssertionError: assert 99 == ((None or 0))
2 failed, 8 warnings in 0.96s
exit: 1
```

Restored: [complete output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_upsert_ignored_by_trigger_returns_existing_id_or_zero-green.txt).

```sh
PYTHONPATH= PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.8/x64/bin/python -m pytest tests/test_edgar_flow.py::test_upsert_ignored_by_trigger_returns_existing_id_or_zero -m 'not slow' --tb=short --disable-warnings
```

```text
2 passed, 8 warnings in 0.80s
exit: 0
```

### 19. `test_holding_insert_without_recognized_columns_preserves_legacy_rows`

Source: `etl/edgar_flow.py`; [mutation diff](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_holding_insert_without_recognized_columns_preserves_legacy_rows.diff).

Broken: [complete output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_holding_insert_without_recognized_columns_preserves_legacy_rows-red.txt).

```sh
PYTHONPATH= PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.8/x64/bin/python -m pytest tests/test_edgar_flow.py::test_holding_insert_without_recognized_columns_preserves_legacy_rows -m 'not slow' --tb=short --disable-warnings
```

```text
FAILED tests/test_edgar_flow.py::test_holding_insert_without_recognized_columns_preserves_legacy_rows - RuntimeError: schema rejected
1 failed, 8 warnings in 0.98s
exit: 1
```

Restored: [complete output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_holding_insert_without_recognized_columns_preserves_legacy_rows-green.txt).

```sh
PYTHONPATH= PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.8/x64/bin/python -m pytest tests/test_edgar_flow.py::test_holding_insert_without_recognized_columns_preserves_legacy_rows -m 'not slow' --tb=short --disable-warnings
```

```text
1 passed, 8 warnings in 0.79s
exit: 0
```

### 20. `test_postgres_upsert_preserves_storage_key`

Source: `etl/edgar_flow.py`; [mutation diff](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_postgres_upsert_preserves_storage_key.diff).

Broken: [complete output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_postgres_upsert_preserves_storage_key-red.txt).

```sh
PYTHONPATH= PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.8/x64/bin/python -m pytest tests/test_edgar_flow.py::test_postgres_upsert_preserves_storage_key -m 'not slow' --tb=short --disable-warnings
```

```text
FAILED tests/test_edgar_flow.py::test_postgres_upsert_preserves_storage_key[None-0] - AssertionError: assert 'storage_key = EXCLUDED.storage_key' in 'INSERT INTO filings(manager_id, type, filed_date, source, raw_key, parsed_payload, storage_key) VALUES (%s, %s, %s, %...= EXCLUDED.filed_date, parsed_payload = EXCLUDED.parsed_payload, storage_key = filings.storage_key RETURNING filing_id'
3 failed, 8 warnings in 0.99s
exit: 1
```

Restored: [complete output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_postgres_upsert_preserves_storage_key-green.txt).

```sh
PYTHONPATH= PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.8/x64/bin/python -m pytest tests/test_edgar_flow.py::test_postgres_upsert_preserves_storage_key -m 'not slow' --tb=short --disable-warnings
```

```text
3 passed, 8 warnings in 0.81s
exit: 0
```

### 21. `test_reverse_order_original_keeps_amendment_current`

Source: `etl/edgar_flow.py`; [mutation diff](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_reverse_order_original_keeps_amendment_current.diff).

Broken: [complete output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_reverse_order_original_keeps_amendment_current-red.txt).

```sh
PYTHONPATH= PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.8/x64/bin/python -m pytest tests/test_edgar_flow.py::test_reverse_order_original_keeps_amendment_current -m 'not slow' --tb=short --disable-warnings
```

```text
FAILED tests/test_edgar_flow.py::test_reverse_order_original_keeps_amendment_current - assert [(10, 250, 0,..., 1, None, 1)] == [(10, 250, 1,...100, 0, 1, 1)]
1 failed, 8 warnings in 0.95s
exit: 1
```

Restored: [complete output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_reverse_order_original_keeps_amendment_current-green.txt).

```sh
PYTHONPATH= PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.8/x64/bin/python -m pytest tests/test_edgar_flow.py::test_reverse_order_original_keeps_amendment_current -m 'not slow' --tb=short --disable-warnings
```

```text
1 passed, 8 warnings in 0.79s
exit: 0
```

### 22. `test_report_page_sorts_value_and_filters_news_exports`

Source: `ui/daily_report.py`; [mutation diff](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_report_page_sorts_value_and_filters_news_exports.diff).

Broken: [complete output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_report_page_sorts_value_and_filters_news_exports-red.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.10/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_report_page_sorts_value_and_filters_news_exports -m 'not slow' --tb=short --disable-warnings
```

```text
FAILED tests/test_issue_1888_coverage.py::test_report_page_sorts_value_and_filters_news_exports[merger] - AssertionError: assert ['Headline1', 'Headline2'] == ['Headline1']
2 failed, 12 warnings in 1.33s
exit: 1
```

Restored: [complete output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_report_page_sorts_value_and_filters_news_exports-green.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.10/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_report_page_sorts_value_and_filters_news_exports -m 'not slow' --tb=short --disable-warnings
```

```text
2 passed, 12 warnings in 1.32s
exit: 0
```

### 23. `test_report_diff_unexpected_error_propagates_and_closes`

Source: `ui/daily_report.py`; [mutation diff](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_report_diff_unexpected_error_propagates_and_closes.diff).

Broken: [complete output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_report_diff_unexpected_error_propagates_and_closes-red.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.10/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_report_diff_unexpected_error_propagates_and_closes -m 'not slow' --tb=short --disable-warnings
```

```text
FAILED tests/test_issue_1888_coverage.py::test_report_diff_unexpected_error_propagates_and_closes - AssertionError: Expected 'close' to be called once. Called 0 times.
1 failed, 6 warnings in 0.58s
exit: 1
```

Restored: [complete output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_report_diff_unexpected_error_propagates_and_closes-green.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.10/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_report_diff_unexpected_error_propagates_and_closes -m 'not slow' --tb=short --disable-warnings
```

```text
1 passed, 6 warnings in 0.54s
exit: 0
```

### 24. `test_report_latest_date_ignores_missing_sources`

Source: `ui/daily_report.py`; [mutation diff](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_report_latest_date_ignores_missing_sources.diff).

Broken: [complete output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_report_latest_date_ignores_missing_sources-red.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.10/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_report_latest_date_ignores_missing_sources -m 'not slow' --tb=short --disable-warnings
```

```text
FAILED tests/test_issue_1888_coverage.py::test_report_latest_date_ignores_missing_sources - pandas.errors.DatabaseError: Execution failed on sql 'SELECT MAX(report_date) AS report_date FROM daily_diffs': no such table: daily_diffs
1 failed, 6 warnings in 0.62s
exit: 1
```

Restored: [complete output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_report_latest_date_ignores_missing_sources-green.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.10/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_report_latest_date_ignores_missing_sources -m 'not slow' --tb=short --disable-warnings
```

```text
1 passed, 6 warnings in 0.53s
exit: 0
```

### 25. `test_crowded_query_preserves_native_dates_and_tolerates_invalid_ids`

Source: `api/signals.py`; [mutation diff](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_crowded_query_preserves_native_dates_and_tolerates_invalid_ids.diff).

Broken: [complete output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_crowded_query_preserves_native_dates_and_tolerates_invalid_ids-red.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.10/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_crowded_query_preserves_native_dates_and_tolerates_invalid_ids -m 'not slow' --tb=short --disable-warnings
```

```text
FAILED tests/test_issue_1888_coverage.py::test_crowded_query_preserves_native_dates_and_tolerates_invalid_ids["Alpha"-names0] - AssertionError: assert [{'cusip': 'A...Alpha'], ...}] == [{'cusip': 'A...es': [], ...}]
3 failed, 1 passed, 6 warnings in 0.54s
exit: 1
```

Restored: [complete output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_crowded_query_preserves_native_dates_and_tolerates_invalid_ids-green.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.10/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_crowded_query_preserves_native_dates_and_tolerates_invalid_ids -m 'not slow' --tb=short --disable-warnings
```

```text
4 passed, 6 warnings in 0.52s
exit: 0
```

### 26. `test_search_sqlite_vector_failure_preserves_text_results`

Source: `api/search.py`; [mutation diff](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_search_sqlite_vector_failure_preserves_text_results.diff).

Broken: [complete output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_search_sqlite_vector_failure_preserves_text_results-red.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.10/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_search_sqlite_vector_failure_preserves_text_results -m 'not slow' --tb=short --disable-warnings
```

```text
FAILED tests/test_issue_1888_coverage.py::test_search_sqlite_vector_failure_preserves_text_results[embedding] - OSError: vector index unavailable
1 failed, 1 passed, 6 warnings in 0.59s
exit: 1
```

Restored: [complete output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_search_sqlite_vector_failure_preserves_text_results-green.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.10/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_search_sqlite_vector_failure_preserves_text_results -m 'not slow' --tb=short --disable-warnings
```

```text
2 passed, 6 warnings in 0.52s
exit: 0
```

### 27. `test_search_postgres_ignores_vector_hits_without_document_id`

Source: `api/search.py`; [mutation diff](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_search_postgres_ignores_vector_hits_without_document_id.diff).

Broken: [complete output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_search_postgres_ignores_vector_hits_without_document_id-red.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.10/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_search_postgres_ignores_vector_hits_without_document_id -m 'not slow' --tb=short --disable-warnings
```

```text
FAILED tests/test_issue_1888_coverage.py::test_search_postgres_ignores_vector_hits_without_document_id - ValueError: missing document ID
1 failed, 6 warnings in 0.54s
exit: 1
```

Restored: [complete output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_search_postgres_ignores_vector_hits_without_document_id-green.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.10/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_search_postgres_ignores_vector_hits_without_document_id -m 'not slow' --tb=short --disable-warnings
```

```text
1 passed, 6 warnings in 0.53s
exit: 0
```

### 28. `test_report_postgres_news_caches_results_by_date_and_closes`

Source: `ui/daily_report.py`; [mutation diff](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_report_postgres_news_caches_results_by_date_and_closes.diff).

Broken: [complete output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_report_postgres_news_caches_results_by_date_and_closes-red.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.8/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_report_postgres_news_caches_results_by_date_and_closes -m 'not slow' --tb=short --disable-warnings
```

```text
FAILED tests/test_issue_1888_coverage.py::test_report_postgres_news_caches_results_by_date_and_closes[populated] - AssertionError: assert 'WHERE n.published_at::date = %s' in 'SELECT n.headline, n.url, n.published_at, n.source, n.topics, n.confidence, m.name AS manager_name FROM news_items n LEFT JOIN managers m ON m.manager_id = n.manager_id WHERE n.published_at::date = ? ORDER BY n.published_at DESC LIMIT 50'
3 failed, 6 warnings in 0.57s
exit: 1
```

Restored: [complete output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_report_postgres_news_caches_results_by_date_and_closes-green.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.8/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_report_postgres_news_caches_results_by_date_and_closes -m 'not slow' --tb=short --disable-warnings
```

```text
3 passed, 6 warnings in 0.55s
exit: 0
```

### 29. `test_report_whitespace_news_url_keeps_escaped_headline`

Source: `ui/daily_report.py`; [mutation diff](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_report_whitespace_news_url_keeps_escaped_headline.diff).

Broken: [complete output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_report_whitespace_news_url_keeps_escaped_headline-red.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.8/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_report_whitespace_news_url_keeps_escaped_headline -m 'not slow' --tb=short --disable-warnings
```

```text
FAILED tests/test_issue_1888_coverage.py::test_report_whitespace_news_url_keeps_escaped_headline[ ] - AssertionError: assert '\\[Desk\\] \...;x&gt; &amp;!' == '\\[Desk\\] \...t;x&gt; &amp;'
2 failed, 6 warnings in 0.54s
exit: 1
```

Restored: [complete output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_report_whitespace_news_url_keeps_escaped_headline-green.txt).

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
[Commands and raw reports](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/README.md#date-navigation-follow-up-2026-11-04).

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

Source: `ui/daily_report.py`; [mutation diff](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_report_date_navigation_keeps_display_and_export_in_sync.diff).

Broken: [complete output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_report_date_navigation_keeps_display_and_export_in_sync-red.txt).

```sh
PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_report_date_navigation_keeps_display_and_export_in_sync -m 'not slow' --tb=short --disable-warnings
```

```text
FAILED tests/test_issue_1888_coverage.py::test_report_date_navigation_keeps_display_and_export_in_sync[False] - AssertionError: assert ['AAA', 'BBB'] == ['OLDER']
FAILED tests/test_issue_1888_coverage.py::test_report_date_navigation_keeps_display_and_export_in_sync[True] - AssertionError: assert ['OLDER'] == ['AAA', 'BBB']
2 failed, 12 warnings in 2.38s
exit: 1
```

Restored: [complete output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_report_date_navigation_keeps_display_and_export_in_sync-green.txt).

```sh
PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_report_date_navigation_keeps_display_and_export_in_sync -m 'not slow' --tb=short --disable-warnings
```

```text
2 passed, 12 warnings in 2.42s
exit: 0
```

### 31. `test_report_login_stop_prevents_database_access`

Source: `ui/daily_report.py`; [mutation diff](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_report_login_stop_prevents_database_access.diff).

Broken: [complete output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_report_login_stop_prevents_database_access-red.txt).

```sh
PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_report_login_stop_prevents_database_access -m 'not slow' --tb=short --disable-warnings
```

```text
FAILED tests/test_issue_1888_coverage.py::test_report_login_stop_prevents_database_access - AssertionError: database accessed before login
1 failed, 6 warnings in 1.10s
exit: 1
```

Restored: [complete output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_report_login_stop_prevents_database_access-green.txt).

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
changed or disabled. [Complete output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/navigation-full-pytest.txt).
The successful 176-test affected-suite run is separate from this incomplete run.

[Verified CI metadata](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/navigation-ci-metadata.json)
identifies the prior published head's coverage artifacts: `13296162876` for
Python 3.13 and `13296791087` for 3.14. These are handoff identifiers, not inspected
coverage contents or CI validation of the new tests. PR #1889 remains open and
ready for review (`draft=false`).

### 32. `test_report_nearest_date_caches_empty_and_populated_results`

Source: `ui/daily_report.py`; [mutation diff](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_report_nearest_date_caches_empty_and_populated_results.diff).

Broken: [complete output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_report_nearest_date_caches_empty_and_populated_results-red.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.10/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_report_nearest_date_caches_empty_and_populated_results -m 'not slow' --tb=short --disable-warnings
```

```text
2 failed, 2 passed, 6 warnings in 0.99s
exit: 1
```

Restored: [complete output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_report_nearest_date_caches_empty_and_populated_results-green.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.10/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_report_nearest_date_caches_empty_and_populated_results -m 'not slow' --tb=short --disable-warnings
```

```text
4 passed, 6 warnings in 0.95s
exit: 0
```

### 33. `test_report_latest_date_query_errors_close_connections_and_are_retried`

Source: `ui/daily_report.py`; [mutation diff](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_report_latest_date_query_errors_close_connections_and_are_retried.diff).

Broken: [complete output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_report_latest_date_query_errors_close_connections_and_are_retried-red.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.10/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_report_latest_date_query_errors_close_connections_and_are_retried -m 'not slow' --tb=short --disable-warnings
```

```text
1 failed, 6 warnings in 0.95s
exit: 1
```

Restored: [complete output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_report_latest_date_query_errors_close_connections_and_are_retried-green.txt).

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.10/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_report_latest_date_query_errors_close_connections_and_are_retried -m 'not slow' --tb=short --disable-warnings
```

```text
1 passed, 6 warnings in 0.88s
exit: 0
```

## Date-lookup follow-up and publication state

The new SQLite date lookup checks verify that cached `None` is reused after the
connection closes, and that equal-distance choices prefer the newer date. A
separate error test verifies exact operational errors, connection closure, and
retry rather than caching a transient failure. Both functions have real source
mutation receipts above. No production bug or production edit is claimed.

Published head `c1a5b73198727c351a7b0bc4c3ce603ca5196323` passed the Python 3.13
and 3.14 Pytest steps in [Gate 41780939810](https://github.com/stranske/Manager-Database/actions/runs/41780939810).
Its Python 3.13 coverage artifact is `13299645909`; Python 3.14 is `13299486485`.
Artifact contents remain unread. This CI result predates these five local cases,
so the final-candidate CI checkbox remains open. Current main is still
`a7f4e626bb4b8359a64c56a56211218e41b6020a`; transfer of its artifact still fails.
The exact CI base/ranking and ~180-unit comparison therefore remain unverified.

PR body publication was rejected because the tool requires approval while this
run's approval policy is `never`. The primary Git metadata mount is read-only.
The tested source change is preserved in `/tmp/issue1888-date-lookup-proof` with a
patch in the evidence directory. This prepared body reconciles verified tasks;
it does not claim the live PR checkboxes were updated.


## Partial search schema follow-up

Two SQLite behavior tests assert complete filing and activism results with missing
optional schema metadata. No production change. Matched local search coverage is
**95% to 96%**, missing statements+branches **8+12=20 to 7+8=15** (59 to 61 passing
tests). Broader affected suites: **183 passed, 1 existing nightly skip**. Black's
required repository check (452 files) and Ruff pass. Negative-cache audit remains
clear. Main is still `a7f4e626`; artifact transfer fails with `URLError`, so the
exact CI ranking/base and ~180-unit comparison remain open. The primary `.git`
mount is read-only; a writable-clone commit and patch preserve this follow-up.

### 34. `test_search_partial_sqlite_schema_keeps_filings_searchable`

Red: [complete output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_search_partial_sqlite_schema_keeps_filings_searchable-red.txt).

```sh
PYTHONPATH= PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.8/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_search_partial_sqlite_schema_keeps_filings_searchable -m 'not slow' --tb=short --disable-warnings
```

```text
1 failed, 6 warnings in 0.57s
exit: 1
```

Green: [complete output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_search_partial_sqlite_schema_keeps_filings_searchable-green.txt).

```sh
PYTHONPATH= PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.8/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_search_partial_sqlite_schema_keeps_filings_searchable -m 'not slow' --tb=short --disable-warnings
```

```text
1 passed, 6 warnings in 0.55s
exit: 0
```

### 35. `test_search_activism_without_manager_table_preserves_subject_context`

Red: [complete output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_search_activism_without_manager_table_preserves_subject_context-red.txt).

```sh
PYTHONPATH= PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.8/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_search_activism_without_manager_table_preserves_subject_context -m 'not slow' --tb=short --disable-warnings
```

```text
1 failed, 6 warnings in 0.57s
exit: 1
```

Green: [complete output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_search_activism_without_manager_table_preserves_subject_context-green.txt).

```sh
PYTHONPATH= PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.8/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_search_activism_without_manager_table_preserves_subject_context -m 'not slow' --tb=short --disable-warnings
```

```text
1 passed, 6 warnings in 0.55s
exit: 0
```



Publication attempt for this follow-up: updating PR #1889's prepared body, adding
`needs-human`, and posting the blocker comment were each rejected:
`MCP tool call requires approval, but approval policy is never`.
No live body, label, or comment update was published by this run.


## Nullable ticker negative-cache audit

Completed the scoped negative-cache audit with a behavior test using real SQLite
holdings and annotations. Null/missing ticker entries preserve CUSIP annotations
without a second holdings lookup. No production bug or production change. A source
mutation adds the negative-cache requery and fails on two extra SQL queries;
restoration passes. Matched targeted suites: **45 to 46 passed**, signals **84%**
unchanged; this adds no coverage reduction. Broader suites: **184 passed, 1 existing
nightly skip**. Required Black repository check (452 files) and Ruff pass.

Current main remains `a7f4e626`; downloading its CI artifact into the sandbox still
fails with `URLError`. Exact CI base/ranking and the ~180-unit comparison remain
unchecked. This new local test has no candidate CI result yet. Primary Git metadata
is read-only; a writable-clone commit and patch preserve the follow-up.

### 36. `test_conviction_nullable_ticker_lookup_preserves_annotations_without_requery`

Source mutation: [diff](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_conviction_nullable_ticker_lookup_preserves_annotations_without_requery.diff).
Broken: [complete output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_conviction_nullable_ticker_lookup_preserves_annotations_without_requery-red.txt).
Restored: [complete output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_conviction_nullable_ticker_lookup_preserves_annotations_without_requery-green.txt).
The same command runs with broken and restored source:

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_conviction_nullable_ticker_lookup_preserves_annotations_without_requery -m 'not slow' --tb=short --disable-warnings
```

```text
Broken:
Left contains 2 more items, first extra item: 'SELECT cusip, resolved_ticker FROM holdings WHERE filing_id = 2'
1 failed, 6 warnings in 0.92s
exit: 1

Restored:
1 passed, 6 warnings in 0.94s
exit: 0
```


Publication for the nullable-ticker follow-up was rejected for all three actions:
PR body update, `needs-human` label and blocker comment. Each returned
`MCP tool call requires approval, but approval policy is never`. The prepared body
and evidence remain local. PR #1889 is verified open with `isDraft=false`.


## PostgreSQL nullable identifier follow-up

Two cases assert complete holdings search results and SQL bind counts with absent
identifier columns or null identifier values. Search targeted coverage rises
**97% to 98%**, uncovered statements+branches **11 to 7**, with **79 passed**.
Related suites: **198 passed, 2 existing nightly skips**. Required repository
Black check (452 files) and Ruff pass. PostgreSQL execution is modeled. No
production change; scoped negative-cache audit remains clear. Exact CI base,
ranking and ~180-unit comparison remain blocked by artifact transfer (`URLError`).
This local addition has no final-candidate CI result yet.

### 37. `test_search_postgres_holdings_without_identifiers_keep_cusip_context`

Source: `api/search.py`; [mutation diff](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_search_postgres_holdings_without_identifiers_keep_cusip_context.diff).
[Broken output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_search_postgres_holdings_without_identifiers_keep_cusip_context-red.txt),
[restored output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_search_postgres_holdings_without_identifiers_keep_cusip_context-green.txt).
Same command against deliberately broken then restored source:

```sh
PYTHONPATH= PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_search_postgres_holdings_without_identifiers_keep_cusip_context -m 'not slow' --tb=short --disable-warnings
```

```text
Broken: snippet 'CUSIP: CUSIP | Resolved: None' != 'CUSIP: CUSIP'
2 failed, 6 warnings in 0.80s
exit: 1

Restored:
2 passed, 6 warnings in 0.78s
exit: 0
```

Publication of this follow-up body, `needs-human` label, and blocker comment was
rejected: `MCP tool call requires approval, but approval policy is never`.
The writable clone independently passes Black (452 files) and 79 search cases.
PR #1889 remains open and ready for review; the local prepared body is not a
claim that live PR metadata was updated.

Full local suite completed with **3 failed, 2645 passed, 28 skipped** in 258.45s:

```sh
PYTHONPATH=/tmp/issue1888-runtime timeout 300 pytest tests/ -m 'not slow' --tb=short --disable-warnings
```

Exit **1**. Failures are the same three already recorded before this follow-up:
chat dependency errors timing out (`test_coverage_round_1842`), edgartools attempting
to write `/home/runner/.edgar` (`test_edgar_integration`), and the sandbox refusing
a local socket (`test_wasm_static_routes`). Full output: `holdings-full-pytest.txt`.
No existing test was skipped, disabled, or changed to accommodate these failures.
This does not satisfy the CI pytest acceptance criterion.


## Empty database metadata follow-up (2026-11-04)

This extends an existing test function with one parametrized case; no production
files or new test functions are added. A SQLite connection subclass models an
empty database-list response. Full text results must remain available when vector
search has no usable database path. Local coverage eliminates the `656->663` arc.
Remaining search misses are the optional Pydantic import fallback (28-31), the
optional psycopg-import arc (23->26), and the blank `_score_result` guard (93).
The public search entry point rejects blank queries before calling the scorer;
this last guard cannot be reached through normal public search behavior.

The [source mutation](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/metadata-fallback.diff) deliberately
drops text results when metadata is empty. The same command was run against
broken then restored source in `/tmp/issue1888-metadata-commit`:

```sh
PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.10/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_search_sqlite_vector_failure_preserves_text_results -m 'not slow' --tb=short --disable-warnings
```

```text
Broken:
FAILED tests/test_issue_1888_coverage.py::test_search_sqlite_vector_failure_preserves_text_results[empty_database_list] - AssertionError: assert [] == [{'entity_typ... 'memo', ...}]
1 failed, 2 passed, 6 warnings in 0.92s
exit: 1

Restored:
3 passed, 6 warnings in 0.90s
exit: 0
```

[Full failing output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/metadata-fallback-red.txt),
[full restored output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/metadata-fallback-green.txt), and
[restoration receipt](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/metadata-fallback-receipt.json).
The original 37 function receipts remain intact.

Reconciliation reviewed all eleven branch commits through `49f5341`: the prepared
checkboxes reflect verified EDGAR and broader search/signal/report tests, exact
assertions, mutation proof, and the completed negative-cache audit. No runtime
diff exists. Gate [41815491478](https://github.com/stranske/Manager-Database/actions/runs/41815491478)
passed pytest for the published `49f5341` head on Python 3.13 and 3.14; see
[metadata](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/metadata-ci-status.json). That result predates
this added case, so candidate CI remains unchecked.

The main artifact transfer still fails with `URLError`; exact CI baseline/ranking
and the approximately 180-unit comparison remain unchecked. The approval system
rejected the PR body update, `needs-human` label, and blocker comment with
`MCP tool call requires approval, but approval policy is never`. The live PR body
was not reconciled. Primary Git metadata is read-only (`.git/index.lock` creation
fails); the test change and evidence are preserved in a writable-clone commit.

### 38. `test_report_page_preserves_unknown_deltas_and_skips_incomplete_signals`

Source: `ui/daily_report.py`; [mutation diff](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_report_page_preserves_unknown_deltas_and_skips_incomplete_signals.diff).

Broken: [complete output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_report_page_preserves_unknown_deltas_and_skips_incomplete_signals-red.txt).

```sh
PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_report_page_preserves_unknown_deltas_and_skips_incomplete_signals -m 'not slow' --tb=short --disable-warnings
```

```text
3 failed, 14 warnings in 1.93s
exit: 1
```

Restored: [complete output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_report_page_preserves_unknown_deltas_and_skips_incomplete_signals-green.txt).

```sh
PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_report_page_preserves_unknown_deltas_and_skips_incomplete_signals -m 'not slow' --tb=short --disable-warnings
```

```text
3 passed, 14 warnings in 1.75s
exit: 0
```

### 39. `test_report_missing_activism_table_caches_empty_result_and_closes`

Source: `ui/daily_report.py`; [mutation diff](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_report_missing_activism_table_caches_empty_result_and_closes.diff).

Broken: [complete output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_report_missing_activism_table_caches_empty_result_and_closes-red.txt).

```sh
PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_report_missing_activism_table_caches_empty_result_and_closes -m 'not slow' --tb=short --disable-warnings
```

```text
1 failed, 6 warnings in 0.84s
exit: 1
```

Restored: [complete output](https://github.com/stranske/Manager-Database/blob/64fcbc71d7fc93c292aff58a57d19d63b2de2b98/docs/evidence/issue-1888/mutations/test_report_missing_activism_table_caches_empty_result_and_closes-green.txt).

```sh
PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_report_missing_activism_table_caches_empty_result_and_closes -m 'not slow' --tb=short --disable-warnings
```

```text
1 passed, 6 warnings in 0.73s
exit: 0
```
