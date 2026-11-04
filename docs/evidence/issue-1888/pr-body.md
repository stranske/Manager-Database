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
- [ ] Cover the largest remaining gaps among the 149 units left after #1808: api/search.py (52), ui/daily_report.py (35), api/signals.py (27). Prefer behaviour-level tests over helper-level ones.
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

## Verified implementation

This PR changes tests and evidence only. The existing EDGAR checks cover deletion,
raw persistence, idempotency and unowned filings. Six follow-up functions (10 cases)
cover reverse-order amendments, legacy watermark/schema behavior, ignored inserts,
and PostgreSQL storage-key SQL generation. PostgreSQL execution is mocked.

Local follow-up validation: EDGAR **47 passed, 1 skipped**, affected suites
**99 passed, 1 skipped**, bitemporal suite **20 passed, 1 skipped**. Black's required
repository check passes (452 files), using the event-loop shim documented in the
evidence README; Ruff passes for both changed Python files. Existing nightly skips
remain. No production code or protected files changed. The negative-cache pattern
was absent in the EDGAR code inspected.

### Coverage accounting

The required CI-artifact baseline and ranking remain unverified: the previous run
identified main artifact `13265760076` for run `41702885246` at
`a7f4e626bb4b8359a64c56a56211218e41b6020a`, but its bytes could not be transferred
through the restricted sandbox. `ranking.json` is explicitly a provisional repair-
subject/churn ranking based on local coverage. No CI reduction or ~180-unit claim
is made. Current-main artifact verification and CI pytest remain open tasks.

Earlier whole-suite local measurements (not CI artifacts):

| Target | Main missing statements+branches | Earlier candidate | Reduction |
| --- | ---: | ---: | ---: |
| EDGAR | 16+16=32 | 9+7=16 | 16 |
| Daily report | 20+15=35 | 16+15=31 | 4 |
| Signals | 9+4=13 | 4+2=6 | 7 |
| Search | 25+27=52 | 10+12=22 | 30 |
| Total | 132 | 75 | 57 |

The follow-up EDGAR-only run improves **94% to 98%**: missing statements+branches
fall from **12+7=19** to **5+1=6**, a 13-unit local reduction. This targeted delta
must not be added to the earlier whole-suite total. Raw reports and commands are
in [the evidence README](docs/evidence/issue-1888/README.md). Remaining focused
misses are logging proxy forwarding and the scheduled CLI entry point. Schema
fallback, watermark and reverse-order history gaps are now covered. The README
retains per-target explanations for search, signals and daily-report shortfalls.
The measured earlier local base contains only 132 units across the four main
targets; a 180-unit reduction cannot be achieved within that measured base.

### Break-and-revert proof for every new function

The following are exact command and terminal-output excerpts from each real
break-and-revert run. Full stdout, source mutation diffs, original/restored hashes,
and exit codes are checked in under `docs/evidence/issue-1888/mutations/`.
The two strengthened existing functions are included as well.

<details>
<summary>tests/test_edgar_flow.py::test_manager_lookup_rejects_missing_or_deleted_manager</summary>

Mutation: [exact diff](docs/evidence/issue-1888/mutations/test_manager_lookup_rejects_missing_or_deleted_manager.diff).

red: [complete output](docs/evidence/issue-1888/mutations/test_manager_lookup_rejects_missing_or_deleted_manager-red.txt)

```text
cwd: /tmp/issue1888-mutations
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_edgar_flow.py::test_manager_lookup_rejects_missing_or_deleted_manager -m 'not slow' --tb=short --disable-warnings
E   AssertionError: assert 99 is None
E    +  where 99 = <function _manager_id_for_cik at 0x7119adc6c9e0>(<sqlite3.Connection object at 0x7119b0727790>, '0')
E    +    where <function _manager_id_for_cik at 0x7119adc6c9e0> = flow._manager_id_for_cik
FAILED tests/test_edgar_flow.py::test_manager_lookup_rejects_missing_or_deleted_manager[None-rows0] - AssertionError: assert 99 is None
 +  where 99 = <function _manager_id_for_cik at 0x7119adc6c9e0>(<sqlite3.Connection object at 0x7119b0727790>, '0')
 +    where <function _manager_id_for_cik at 0x7119adc6c9e0> = flow._manager_id_for_cik
1 failed, 5 passed, 8 warnings in 1.70s

exit: 1
```

green: [complete output](docs/evidence/issue-1888/mutations/test_manager_lookup_rejects_missing_or_deleted_manager-green.txt)

```text
cwd: /tmp/issue1888-mutations
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_edgar_flow.py::test_manager_lookup_rejects_missing_or_deleted_manager -m 'not slow' --tb=short --disable-warnings
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_edgar_flow.py::test_manager_lookup_rejects_missing_or_deleted_manager -m 'not slow' --tb=short --disable-warnings
......                                                                   [100%]
6 passed, 8 warnings in 1.37s

exit: 0
```

</details>

<details>
<summary>tests/test_edgar_flow.py::test_watermark_unavailable_without_filing_history</summary>

Mutation: [exact diff](docs/evidence/issue-1888/mutations/test_watermark_unavailable_without_filing_history.diff).

red: [complete output](docs/evidence/issue-1888/mutations/test_watermark_unavailable_without_filing_history-red.txt)

```text
cwd: /tmp/issue1888-mutations
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_edgar_flow.py::test_watermark_unavailable_without_filing_history -m 'not slow' --tb=short --disable-warnings
E   AssertionError: assert '2000-01-01' is None
E    +  where '2000-01-01' = <function _latest_filed_date_for_cik at 0x7476d6065380>('missing')
E    +    where <function _latest_filed_date_for_cik at 0x7476d6065380> = flow._latest_filed_date_for_cik
E   AssertionError: assert '2000-01-01' is None
FAILED tests/test_edgar_flow.py::test_watermark_unavailable_without_filing_history[None] - AssertionError: assert '2000-01-01' is None
 +  where '2000-01-01' = <function _latest_filed_date_for_cik at 0x7476d6065380>('missing')
 +    where <function _latest_filed_date_for_cik at 0x7476d6065380> = flow._latest_filed_date_for_cik
FAILED tests/test_edgar_flow.py::test_watermark_unavailable_without_filing_history[manager_id INTEGER] - AssertionError: assert '2000-01-01' is None
 +  where '2000-01-01' = <function _latest_filed_date_for_cik at 0x7476d6065380>('missing')
 +    where <function _latest_filed_date_for_cik at 0x7476d6065380> = flow._latest_filed_date_for_cik
2 failed, 1 passed, 8 warnings in 1.67s

exit: 1
```

green: [complete output](docs/evidence/issue-1888/mutations/test_watermark_unavailable_without_filing_history-green.txt)

```text
cwd: /tmp/issue1888-mutations
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_edgar_flow.py::test_watermark_unavailable_without_filing_history -m 'not slow' --tb=short --disable-warnings
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_edgar_flow.py::test_watermark_unavailable_without_filing_history -m 'not slow' --tb=short --disable-warnings
...                                                                      [100%]
3 passed, 8 warnings in 1.61s

exit: 0
```

</details>

<details>
<summary>tests/test_edgar_flow.py::test_holdings_resolution_failure_preserves_raw_rows</summary>

Mutation: [exact diff](docs/evidence/issue-1888/mutations/test_holdings_resolution_failure_preserves_raw_rows.diff).

red: [complete output](docs/evidence/issue-1888/mutations/test_holdings_resolution_failure_preserves_raw_rows-red.txt)

```text
cwd: /tmp/issue1888-mutations
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_edgar_flow.py::test_holdings_resolution_failure_preserves_raw_rows -m 'not slow' --tb=short --disable-warnings
E   RuntimeError: resolver unavailable
FAILED tests/test_edgar_flow.py::test_holdings_resolution_failure_preserves_raw_rows - RuntimeError: resolver unavailable
1 failed, 8 warnings in 2.51s

exit: 1
```

green: [complete output](docs/evidence/issue-1888/mutations/test_holdings_resolution_failure_preserves_raw_rows-green.txt)

```text
cwd: /tmp/issue1888-mutations
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_edgar_flow.py::test_holdings_resolution_failure_preserves_raw_rows -m 'not slow' --tb=short --disable-warnings
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_edgar_flow.py::test_holdings_resolution_failure_preserves_raw_rows -m 'not slow' --tb=short --disable-warnings
.                                                                        [100%]
1 passed, 8 warnings in 1.39s

exit: 0
```

</details>

<details>
<summary>tests/test_edgar_flow.py::test_empty_holdings_do_not_delete_existing_rows</summary>

Mutation: [exact diff](docs/evidence/issue-1888/mutations/test_empty_holdings_do_not_delete_existing_rows.diff).

red: [complete output](docs/evidence/issue-1888/mutations/test_empty_holdings_do_not_delete_existing_rows-red.txt)

```text
cwd: /tmp/issue1888-mutations
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_edgar_flow.py::test_empty_holdings_do_not_delete_existing_rows -m 'not slow' --tb=short --disable-warnings
E   AssertionError: assert [] == [('KEEP',)]
E
E     Right contains one more item: ('KEEP',)
E
FAILED tests/test_edgar_flow.py::test_empty_holdings_do_not_delete_existing_rows - AssertionError: assert [] == [('KEEP',)]

  Right contains one more item: ('KEEP',)

  Full diff:
  + []
  - [
  -     (
  -         'KEEP',
  -     ),
  - ]
1 failed, 8 warnings in 1.84s

exit: 1
```

green: [complete output](docs/evidence/issue-1888/mutations/test_empty_holdings_do_not_delete_existing_rows-green.txt)

```text
cwd: /tmp/issue1888-mutations
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_edgar_flow.py::test_empty_holdings_do_not_delete_existing_rows -m 'not slow' --tb=short --disable-warnings
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_edgar_flow.py::test_empty_holdings_do_not_delete_existing_rows -m 'not slow' --tb=short --disable-warnings
.                                                                        [100%]
1 passed, 8 warnings in 1.88s

exit: 0
```

</details>

<details>
<summary>tests/test_edgar_flow.py::test_bitemporal_unowned_filing_rerun_preserves_version</summary>

Mutation: [exact diff](docs/evidence/issue-1888/mutations/test_bitemporal_unowned_filing_rerun_preserves_version.diff).

red: [complete output](docs/evidence/issue-1888/mutations/test_bitemporal_unowned_filing_rerun_preserves_version-red.txt)

```text
cwd: /tmp/issue1888-mutations
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_edgar_flow.py::test_bitemporal_unowned_filing_rerun_preserves_version -m 'not slow' --tb=short --disable-warnings
E   AssertionError: assert [('AAA', 12, ...4.0, 2, None)] == [('AAA', 12, 34.0, 1, None)]
E
E     At index 0 diff: ('AAA', 12, 34.0, 1, '2026-11-04T00:53:15Z') != ('AAA', 12, 34.0, 1, None)
E     Left contains one more item: ('AAA', 12, 34.0, 2, None)
FAILED tests/test_edgar_flow.py::test_bitemporal_unowned_filing_rerun_preserves_version - AssertionError: assert [('AAA', 12, ...4.0, 2, None)] == [('AAA', 12, 34.0, 1, None)]

  At index 0 diff: ('AAA', 12, 34.0, 1, '2026-11-04T00:53:15Z') != ('AAA', 12, 34.0, 1, None)
  Left contains one more item: ('AAA', 12, 34.0, 2, None)

  Full diff:
    [
        (
            'AAA',
            12,
            34.0,
            1,
  +         '2026-11-04T00:53:15Z',
  +     ),
  +     (
  +         'AAA',
  +         12,
  +         34.0,
  +         2,
            None,
        ),
    ]
1 failed, 8 warnings in 2.36s

exit: 1
```

green: [complete output](docs/evidence/issue-1888/mutations/test_bitemporal_unowned_filing_rerun_preserves_version-green.txt)

```text
cwd: /tmp/issue1888-mutations
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_edgar_flow.py::test_bitemporal_unowned_filing_rerun_preserves_version -m 'not slow' --tb=short --disable-warnings
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_edgar_flow.py::test_bitemporal_unowned_filing_rerun_preserves_version -m 'not slow' --tb=short --disable-warnings
.                                                                        [100%]
1 passed, 8 warnings in 1.43s

exit: 0
```

</details>

<details>
<summary>tests/test_edgar_flow.py::test_deletion_during_second_download_keeps_first_filing</summary>

Mutation: [exact diff](docs/evidence/issue-1888/mutations/test_deletion_during_second_download_keeps_first_filing.diff).

red: [complete output](docs/evidence/issue-1888/mutations/test_deletion_during_second_download_keeps_first_filing-red.txt)

```text
cwd: /tmp/issue1888-mutations
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_edgar_flow.py::test_deletion_during_second_download_keeps_first_filing -m 'not slow' --tb=short --disable-warnings
E   AssertionError: assert [] == [{'nameOfIssu...shPrnamt': 1}]
E
E     Right contains one more item: {'nameOfIssuer': 'Corp', 'cusip': 'AAA', 'value': 1, 'sshPrnamt': 1}
E
FAILED tests/test_edgar_flow.py::test_deletion_during_second_download_keeps_first_filing - AssertionError: assert [] == [{'nameOfIssu...shPrnamt': 1}]

  Right contains one more item: {'nameOfIssuer': 'Corp', 'cusip': 'AAA', 'value': 1, 'sshPrnamt': 1}

  Full diff:
  + []
  - [
  -     {
  -         'nameOfIssuer': 'Corp',
  -         'cusip': 'AAA',
  -         'value': 1,
  -         'sshPrnamt': 1,
  -     },
  - ]
1 failed, 8 warnings in 1.98s

exit: 1
```

green: [complete output](docs/evidence/issue-1888/mutations/test_deletion_during_second_download_keeps_first_filing-green.txt)

```text
cwd: /tmp/issue1888-mutations
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_edgar_flow.py::test_deletion_during_second_download_keeps_first_filing -m 'not slow' --tb=short --disable-warnings
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_edgar_flow.py::test_deletion_during_second_download_keeps_first_filing -m 'not slow' --tb=short --disable-warnings
.                                                                        [100%]
1 passed, 8 warnings in 1.42s

exit: 0
```

</details>

<details>
<summary>tests/test_edgar_flow.py::test_binary_filing_without_manager_skips_text_index</summary>

Mutation: [exact diff](docs/evidence/issue-1888/mutations/test_binary_filing_without_manager_skips_text_index.diff).

red: [complete output](docs/evidence/issue-1888/mutations/test_binary_filing_without_manager_skips_text_index-red.txt)

```text
cwd: /tmp/issue1888-mutations
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_edgar_flow.py::test_binary_filing_without_manager_skips_text_index -m 'not slow' --tb=short --disable-warnings
E   AssertionError: expected call not found.
E   Expected: mock(Bucket='filings', Key='raw/edgar/6eb820e0f9762c61_unknown.xml', Body=b'<xml/>', ServerSideEncryption='AES256')
E     Actual: mock(Bucket='filings', Key='raw/edgar/6eb820e0f9762c61_incorrect.xml', Body=b'<xml/>', ServerSideEncryption='AES256')
FAILED tests/test_edgar_flow.py::test_binary_filing_without_manager_skips_text_index - AssertionError: expected call not found.
Expected: mock(Bucket='filings', Key='raw/edgar/6eb820e0f9762c61_unknown.xml', Body=b'<xml/>', ServerSideEncryption='AES256')
  Actual: mock(Bucket='filings', Key='raw/edgar/6eb820e0f9762c61_incorrect.xml', Body=b'<xml/>', ServerSideEncryption='AES256')
1 failed, 8 warnings in 1.78s

exit: 1
```

green: [complete output](docs/evidence/issue-1888/mutations/test_binary_filing_without_manager_skips_text_index-green.txt)

```text
cwd: /tmp/issue1888-mutations
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_edgar_flow.py::test_binary_filing_without_manager_skips_text_index -m 'not slow' --tb=short --disable-warnings
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_edgar_flow.py::test_binary_filing_without_manager_skips_text_index -m 'not slow' --tb=short --disable-warnings
.                                                                        [100%]
1 passed, 8 warnings in 2.28s

exit: 0
```

</details>

<details>
<summary>tests/test_issue_1806_coverage.py::test_search_guards_return_before_accessing_database</summary>

Mutation: [exact diff](docs/evidence/issue-1888/mutations/test_search_guards_return_before_accessing_database.diff).

red: [complete output](docs/evidence/issue-1888/mutations/test_search_guards_return_before_accessing_database-red.txt)

```text
cwd: /tmp/issue1888-mutations
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1806_coverage.py::test_search_guards_return_before_accessing_database -m 'not slow' --tb=short --disable-warnings
E   assert [None] == []
E
E     Left contains one more item: None
E
FAILED tests/test_issue_1806_coverage.py::test_search_guards_return_before_accessing_database - assert [None] == []

  Left contains one more item: None

  Full diff:
  - []
  + [
  +     None,
  + ]
1 failed, 8 warnings in 1.97s

exit: 1
```

green: [complete output](docs/evidence/issue-1888/mutations/test_search_guards_return_before_accessing_database-green.txt)

```text
cwd: /tmp/issue1888-mutations
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1806_coverage.py::test_search_guards_return_before_accessing_database -m 'not slow' --tb=short --disable-warnings
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1806_coverage.py::test_search_guards_return_before_accessing_database -m 'not slow' --tb=short --disable-warnings
.                                                                        [100%]
1 passed, 8 warnings in 2.10s

exit: 0
```

</details>

<details>
<summary>tests/test_issue_1806_coverage.py::test_digest_delivery_configuration_guards</summary>

Mutation: [exact diff](docs/evidence/issue-1888/mutations/test_digest_delivery_configuration_guards.diff).

red: [complete output](docs/evidence/issue-1888/mutations/test_digest_delivery_configuration_guards-red.txt)

```text
cwd: /tmp/issue1888-mutations
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1806_coverage.py::test_digest_delivery_configuration_guards -m 'not slow' --tb=short --disable-warnings
E   AssertionError: assert 'DIGEST_EMAIL...t configured!' == 'DIGEST_EMAIL...ot configured'
E
E     - DIGEST_EMAIL_FROM is not configured
E     + DIGEST_EMAIL_FROM is not configured!
FAILED tests/test_issue_1806_coverage.py::test_digest_delivery_configuration_guards - AssertionError: assert 'DIGEST_EMAIL...t configured!' == 'DIGEST_EMAIL...ot configured'

  - DIGEST_EMAIL_FROM is not configured
  + DIGEST_EMAIL_FROM is not configured!
  ?                                    +
1 failed, 8 warnings in 1.96s

exit: 1
```

green: [complete output](docs/evidence/issue-1888/mutations/test_digest_delivery_configuration_guards-green.txt)

```text
cwd: /tmp/issue1888-mutations
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1806_coverage.py::test_digest_delivery_configuration_guards -m 'not slow' --tb=short --disable-warnings
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1806_coverage.py::test_digest_delivery_configuration_guards -m 'not slow' --tb=short --disable-warnings
.                                                                        [100%]
1 passed, 8 warnings in 1.92s

exit: 0
```

</details>

<details>
<summary>tests/test_issue_1806_coverage.py::test_daily_report_formatters_cover_empty_and_directional_values</summary>

Mutation: [exact diff](docs/evidence/issue-1888/mutations/test_daily_report_formatters_cover_empty_and_directional_values.diff).

red: [complete output](docs/evidence/issue-1888/mutations/test_daily_report_formatters_cover_empty_and_directional_values-red.txt)

```text
cwd: /tmp/issue1888-mutations
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1806_coverage.py::test_daily_report_formatters_cover_empty_and_directional_values -m 'not slow' --tb=short --disable-warnings
E   AssertionError: assert '&lt;x&gt;!' == '&lt;x&gt;'
E
E     - &lt;x&gt;
E     + &lt;x&gt;!
FAILED tests/test_issue_1806_coverage.py::test_daily_report_formatters_cover_empty_and_directional_values - AssertionError: assert '&lt;x&gt;!' == '&lt;x&gt;'

  - &lt;x&gt;
  + &lt;x&gt;!
  ?          +
1 failed, 8 warnings in 1.95s

exit: 1
```

green: [complete output](docs/evidence/issue-1888/mutations/test_daily_report_formatters_cover_empty_and_directional_values-green.txt)

```text
cwd: /tmp/issue1888-mutations
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1806_coverage.py::test_daily_report_formatters_cover_empty_and_directional_values -m 'not slow' --tb=short --disable-warnings
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1806_coverage.py::test_daily_report_formatters_cover_empty_and_directional_values -m 'not slow' --tb=short --disable-warnings
.                                                                        [100%]
1 passed, 8 warnings in 1.99s

exit: 0
```

</details>

<details>
<summary>tests/test_issue_1888_coverage.py::test_search_legacy_holdings_return_filing_groups_and_holdings</summary>

Mutation: [exact diff](docs/evidence/issue-1888/mutations/test_search_legacy_holdings_return_filing_groups_and_holdings.diff).

red: [complete output](docs/evidence/issue-1888/mutations/test_search_legacy_holdings_return_filing_groups_and_holdings-red.txt)

```text
cwd: /tmp/issue1888-mutations
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_search_legacy_holdings_return_filing_groups_and_holdings -m 'not slow' --tb=short --disable-warnings
E   AssertionError: assert [('filing', 1...'2024-05-01')] == [('filing', 1...'2024-05-01')]
E
E     At index 0 diff: ('filing', 1, '123', 'Filing needle-1', '3 holdings; filed 2024-05-01', '2024-05-01') != ('filing', 1, '123', 'Filing needle-1', '2 holdings; filed 2024-05-01', '2024-05-01')
E
FAILED tests/test_issue_1888_coverage.py::test_search_legacy_holdings_return_filing_groups_and_holdings - AssertionError: assert [('filing', 1...'2024-05-01')] == [('filing', 1...'2024-05-01')]

  At index 0 diff: ('filing', 1, '123', 'Filing needle-1', '3 holdings; filed 2024-05-01', '2024-05-01') != ('filing', 1, '123', 'Filing needle-1', '2 holdings; filed 2024-05-01', '2024-05-01')

  Full diff:
    [
        (
            'filing',
            1,
            '123',
            'Filing needle-1',
  -         '2 holdings; filed 2024-05-01',
  ?          ^
  +         '3 holdings; filed 2024-05-01',
  ?          ^
            '2024-05-01',
        ),
        (
            'holding',
            1,
            '123',
            'needle Corp (AAA)',
            'Accession needle-1 filed 2024-05-01',
            '2024-05-01',
        ),
    ]
1 failed, 6 warnings in 0.81s

exit: 1
```

green: [complete output](docs/evidence/issue-1888/mutations/test_search_legacy_holdings_return_filing_groups_and_holdings-green.txt)

```text
cwd: /tmp/issue1888-mutations
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_search_legacy_holdings_return_filing_groups_and_holdings -m 'not slow' --tb=short --disable-warnings
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_search_legacy_holdings_return_filing_groups_and_holdings -m 'not slow' --tb=short --disable-warnings
.                                                                        [100%]
1 passed, 6 warnings in 0.82s

exit: 0
```

</details>

<details>
<summary>tests/test_issue_1888_coverage.py::test_search_vector_documents_keep_shared_managers_and_deduplicate</summary>

Mutation: [exact diff](docs/evidence/issue-1888/mutations/test_search_vector_documents_keep_shared_managers_and_deduplicate.diff).

red: [complete output](docs/evidence/issue-1888/mutations/test_search_vector_documents_keep_shared_managers_and_deduplicate-red.txt)

```text
cwd: /tmp/issue1888-mutations
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_search_vector_documents_keep_shared_managers_and_deduplicate -m 'not slow' --tb=short --disable-warnings
E   AssertionError: assert [(1, None, 'm...only result')] == [(1, None, 'm...only result')]
E
E     At index 1 diff: (2, 'Alpha / Beta', 'vector only result', 'vector only result') != (2, 'Alpha, Beta', 'vector only result', 'vector only result')
E
FAILED tests/test_issue_1888_coverage.py::test_search_vector_documents_keep_shared_managers_and_deduplicate - AssertionError: assert [(1, None, 'm...only result')] == [(1, None, 'm...only result')]

  At index 1 diff: (2, 'Alpha / Beta', 'vector only result', 'vector only result') != (2, 'Alpha, Beta', 'vector only result', 'vector only result')

  Full diff:
    [
        (
            1,
            None,
            'memo',
            'needle memo',
        ),
        (
            2,
  -         'Alpha, Beta',
  ?               ^
  +         'Alpha / Beta',
  ?               ^^
            'vector only result',
            'vector only result',
        ),
    ]
1 failed, 6 warnings in 0.82s

exit: 1
```

green: [complete output](docs/evidence/issue-1888/mutations/test_search_vector_documents_keep_shared_managers_and_deduplicate-green.txt)

```text
cwd: /tmp/issue1888-mutations
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_search_vector_documents_keep_shared_managers_and_deduplicate -m 'not slow' --tb=short --disable-warnings
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_search_vector_documents_keep_shared_managers_and_deduplicate -m 'not slow' --tb=short --disable-warnings
.                                                                        [100%]
1 passed, 6 warnings in 0.83s

exit: 0
```

</details>

<details>
<summary>tests/test_issue_1888_coverage.py::test_search_postgres_activism_preserves_filing_context</summary>

Mutation: [exact diff](docs/evidence/issue-1888/mutations/test_search_postgres_activism_preserves_filing_context.diff).

red: [complete output](docs/evidence/issue-1888/mutations/test_search_postgres_activism_preserves_filing_context-red.txt)

```text
cwd: /tmp/issue1888-mutations
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_search_postgres_activism_preserves_filing_context -m 'not slow' --tb=short --disable-warnings
E   AssertionError: assert {'entity_type... (5.5%)', ...} == {'entity_type... (5.5%)', ...}
E
E     Omitting 7 identical items, use -vv to show
E     Differing items:
FAILED tests/test_issue_1888_coverage.py::test_search_postgres_activism_preserves_filing_context - AssertionError: assert {'entity_type... (5.5%)', ...} == {'entity_type... (5.5%)', ...}

  Omitting 7 identical items, use -vv to show
  Differing items:
  {'snippet': 'SC 13D | Wrong: CUSIP | Filed: 2024-05-01'} != {'snippet': 'SC 13D | CUSIP: CUSIP | Filed: 2024-05-01'}

  Full diff:
    {
        'entity_type': 'filing',
        'entity_id': 12,
        'manager_name': 'Alpha',
        'headline': '13D Filing: Alpha -> Target (5.5%)',
  -     'snippet': 'SC 13D | CUSIP: CUSIP | Filed: 2024-05-01',
  ?                               -------
  +     'snippet': 'SC 13D | Wrong: CUSIP | Filed: 2024-05-01',
  ?                         +++++++
  -     'relevance': 0.8666666667 ± 8.7e-07,
  ?                             ^^^^^^^^^^
  +     'relevance': 0.8666666666666667,
  ?                             ^^^^^^
        'url': 'https://example.test/12',
        'timestamp': '2024-05-01',
    }
1 failed, 6 warnings in 0.81s

exit: 1
```

green: [complete output](docs/evidence/issue-1888/mutations/test_search_postgres_activism_preserves_filing_context-green.txt)

```text
cwd: /tmp/issue1888-mutations
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_search_postgres_activism_preserves_filing_context -m 'not slow' --tb=short --disable-warnings
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_search_postgres_activism_preserves_filing_context -m 'not slow' --tb=short --disable-warnings
.                                                                        [100%]
1 passed, 6 warnings in 0.89s

exit: 0
```

</details>

<details>
<summary>tests/test_issue_1888_coverage.py::test_conviction_annotation_survives_unavailable_holdings</summary>

Mutation: [exact diff](docs/evidence/issue-1888/mutations/test_conviction_annotation_survives_unavailable_holdings.diff).

red: [complete output](docs/evidence/issue-1888/mutations/test_conviction_annotation_survives_unavailable_holdings-red.txt)

```text
cwd: /tmp/issue1888-mutations
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_conviction_annotation_survives_unavailable_holdings -m 'not slow' --tb=short --disable-warnings
E   AssertionError: assert [{'cusip': 'A...': 50.0, ...}] == [{'cusip': 'A...': 50.0, ...}]
E
E     At index 0 diff: {'cusip': 'AAA', 'name_of_issuer': 'Issuer', 'value_usd': 300.0, 'conviction_pct': 50.0, 'portfolio_weight': 0.5, 'insider_net_direction': None, 'short_interest_pct': None, 'short_interest_report_date': None, 'short_interest_source': None} != {'cusip': 'AAA', 'name_of_issuer': 'Issuer', 'value_usd': 300.0, 'conviction_pct': 50.0, 'portfolio_weight': 0.5, 'insider_net_direction': 'BUY', 'short_interest_pct': None, 'short_interest_report_date': None, 'short_interest_source': None}
E
FAILED tests/test_issue_1888_coverage.py::test_conviction_annotation_survives_unavailable_holdings - AssertionError: assert [{'cusip': 'A...': 50.0, ...}] == [{'cusip': 'A...': 50.0, ...}]

  At index 0 diff: {'cusip': 'AAA', 'name_of_issuer': 'Issuer', 'value_usd': 300.0, 'conviction_pct': 50.0, 'portfolio_weight': 0.5, 'insider_net_direction': None, 'short_interest_pct': None, 'short_interest_report_date': None, 'short_interest_source': None} != {'cusip': 'AAA', 'name_of_issuer': 'Issuer', 'value_usd': 300.0, 'conviction_pct': 50.0, 'portfolio_weight': 0.5, 'insider_net_direction': 'BUY', 'short_interest_pct': None, 'short_interest_report_date': None, 'short_interest_source': None}

  Full diff:
    [
        {
            'cusip': 'AAA',
            'name_of_issuer': 'Issuer',
            'value_usd': 300.0,
            'conviction_pct': 50.0,
            'portfolio_weight': 0.5,
  -         'insider_net_direction': 'BUY',
  ?                                  ^^^^^
  +         'insider_net_direction': None,
  ?                                  ^^^^
            'short_interest_pct': None,
            'short_interest_report_date': None,
            'short_interest_source': None,
        },
    ]
1 failed, 6 warnings in 0.91s

exit: 1
```

green: [complete output](docs/evidence/issue-1888/mutations/test_conviction_annotation_survives_unavailable_holdings-green.txt)

```text
cwd: /tmp/issue1888-mutations
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_conviction_annotation_survives_unavailable_holdings -m 'not slow' --tb=short --disable-warnings
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_conviction_annotation_survives_unavailable_holdings -m 'not slow' --tb=short --disable-warnings
.                                                                        [100%]
1 passed, 6 warnings in 0.98s

exit: 0
```

</details>

<details>
<summary>tests/test_issue_1888_coverage.py::test_report_signal_query_failure_returns_empty_and_closes</summary>

Mutation: [exact diff](docs/evidence/issue-1888/mutations/test_report_signal_query_failure_returns_empty_and_closes.diff).

red: [complete output](docs/evidence/issue-1888/mutations/test_report_signal_query_failure_returns_empty_and_closes-red.txt)

```text
cwd: /tmp/issue1888-mutations
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_report_signal_query_failure_returns_empty_and_closes -m 'not slow' --tb=short --disable-warnings
E   sqlite3.OperationalError: database unavailable
E   sqlite3.OperationalError: database unavailable
FAILED tests/test_issue_1888_coverage.py::test_report_signal_query_failure_returns_empty_and_closes[load_crowded_trades-query_crowded_trades] - sqlite3.OperationalError: database unavailable
FAILED tests/test_issue_1888_coverage.py::test_report_signal_query_failure_returns_empty_and_closes[load_contrarian_signals-query_contrarian_signals] - sqlite3.OperationalError: database unavailable
2 failed, 6 warnings in 1.17s

exit: 1
```

green: [complete output](docs/evidence/issue-1888/mutations/test_report_signal_query_failure_returns_empty_and_closes-green.txt)

```text
cwd: /tmp/issue1888-mutations
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_report_signal_query_failure_returns_empty_and_closes -m 'not slow' --tb=short --disable-warnings
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_report_signal_query_failure_returns_empty_and_closes -m 'not slow' --tb=short --disable-warnings
..                                                                       [100%]
2 passed, 6 warnings in 0.90s

exit: 0
```

</details>

<details>
<summary>tests/test_edgar_flow.py::test_watermark_without_source_column_is_scoped_to_manager</summary>

Mutation: [exact diff](docs/evidence/issue-1888/mutations/test_watermark_without_source_column_is_scoped_to_manager.diff).

red: [complete output](docs/evidence/issue-1888/mutations/test_watermark_without_source_column_is_scoped_to_manager-red.txt)

```text
cwd: /tmp/issue1888-edgar-mutations
PYTHONPATH= PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.8/x64/bin/python -m pytest tests/test_edgar_flow.py::test_watermark_without_source_column_is_scoped_to_manager -m 'not slow' --tb=short --disable-warnings
E   AssertionError: assert '2025-01-01' == '2024-05-01'
E
E     - 2024-05-01
E     + 2025-01-01
FAILED tests/test_edgar_flow.py::test_watermark_without_source_column_is_scoped_to_manager - AssertionError: assert '2025-01-01' == '2024-05-01'

  - 2024-05-01
  + 2025-01-01
1 failed, 8 warnings in 0.94s

exit: 1
```

green: [complete output](docs/evidence/issue-1888/mutations/test_watermark_without_source_column_is_scoped_to_manager-green.txt)

```text
cwd: /tmp/issue1888-edgar-mutations
PYTHONPATH= PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.8/x64/bin/python -m pytest tests/test_edgar_flow.py::test_watermark_without_source_column_is_scoped_to_manager -m 'not slow' --tb=short --disable-warnings
PYTHONPATH= PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.8/x64/bin/python -m pytest tests/test_edgar_flow.py::test_watermark_without_source_column_is_scoped_to_manager -m 'not slow' --tb=short --disable-warnings
.                                                                        [100%]
1 passed, 8 warnings in 0.79s

exit: 0
```

</details>

<details>
<summary>tests/test_edgar_flow.py::test_upsert_without_raw_key_leaves_legacy_table_unchanged</summary>

Mutation: [exact diff](docs/evidence/issue-1888/mutations/test_upsert_without_raw_key_leaves_legacy_table_unchanged.diff).

red: [complete output](docs/evidence/issue-1888/mutations/test_upsert_without_raw_key_leaves_legacy_table_unchanged-red.txt)

```text
cwd: /tmp/issue1888-edgar-mutations
PYTHONPATH= PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.8/x64/bin/python -m pytest tests/test_edgar_flow.py::test_upsert_without_raw_key_leaves_legacy_table_unchanged -m 'not slow' --tb=short --disable-warnings
E   AssertionError: assert 99 == 0
E    +  where 99 = <function _upsert_filing_legacy at 0x781131b90720>(<sqlite3.Connection object at 0x7811346533d0>, 100, '13F-HR', '2024-05-01', 'raw.xml')
E    +    where <function _upsert_filing_legacy at 0x781131b90720> = flow._upsert_filing_legacy
E   AssertionError: assert 99 == 0
FAILED tests/test_edgar_flow.py::test_upsert_without_raw_key_leaves_legacy_table_unchanged[filing_id INTEGER PRIMARY KEY] - AssertionError: assert 99 == 0
 +  where 99 = <function _upsert_filing_legacy at 0x781131b90720>(<sqlite3.Connection object at 0x7811346533d0>, 100, '13F-HR', '2024-05-01', 'raw.xml')
 +    where <function _upsert_filing_legacy at 0x781131b90720> = flow._upsert_filing_legacy
FAILED tests/test_edgar_flow.py::test_upsert_without_raw_key_leaves_legacy_table_unchanged[unrelated TEXT] - AssertionError: assert 99 == 0
 +  where 99 = <function _upsert_filing_legacy at 0x781131b90720>(<sqlite3.Connection object at 0x7811319d74c0>, 100, '13F-HR', '2024-05-01', 'raw.xml')
 +    where <function _upsert_filing_legacy at 0x781131b90720> = flow._upsert_filing_legacy
2 failed, 8 warnings in 0.96s

exit: 1
```

green: [complete output](docs/evidence/issue-1888/mutations/test_upsert_without_raw_key_leaves_legacy_table_unchanged-green.txt)

```text
cwd: /tmp/issue1888-edgar-mutations
PYTHONPATH= PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.8/x64/bin/python -m pytest tests/test_edgar_flow.py::test_upsert_without_raw_key_leaves_legacy_table_unchanged -m 'not slow' --tb=short --disable-warnings
PYTHONPATH= PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.8/x64/bin/python -m pytest tests/test_edgar_flow.py::test_upsert_without_raw_key_leaves_legacy_table_unchanged -m 'not slow' --tb=short --disable-warnings
..                                                                       [100%]
2 passed, 8 warnings in 0.79s

exit: 0
```

</details>

<details>
<summary>tests/test_edgar_flow.py::test_upsert_ignored_by_trigger_returns_existing_id_or_zero</summary>

Mutation: [exact diff](docs/evidence/issue-1888/mutations/test_upsert_ignored_by_trigger_returns_existing_id_or_zero.diff).

red: [complete output](docs/evidence/issue-1888/mutations/test_upsert_ignored_by_trigger_returns_existing_id_or_zero-red.txt)

```text
cwd: /tmp/issue1888-edgar-mutations
PYTHONPATH= PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.8/x64/bin/python -m pytest tests/test_edgar_flow.py::test_upsert_ignored_by_trigger_returns_existing_id_or_zero -m 'not slow' --tb=short --disable-warnings
E   AssertionError: assert 99 == ((None or 0))
E    +  where 99 = <function _upsert_filing_legacy at 0x7456beafc720>(<sqlite3.Connection object at 0x7456c144f3d0>, 100, '13F-HR', '2024-05-01', 'raw.xml')
E    +    where <function _upsert_filing_legacy at 0x7456beafc720> = flow._upsert_filing_legacy
E   AssertionError: assert 99 == ((42))
FAILED tests/test_edgar_flow.py::test_upsert_ignored_by_trigger_returns_existing_id_or_zero[None] - AssertionError: assert 99 == ((None or 0))
 +  where 99 = <function _upsert_filing_legacy at 0x7456beafc720>(<sqlite3.Connection object at 0x7456c144f3d0>, 100, '13F-HR', '2024-05-01', 'raw.xml')
 +    where <function _upsert_filing_legacy at 0x7456beafc720> = flow._upsert_filing_legacy
FAILED tests/test_edgar_flow.py::test_upsert_ignored_by_trigger_returns_existing_id_or_zero[42] - AssertionError: assert 99 == ((42))
 +  where 99 = <function _upsert_filing_legacy at 0x7456beafc720>(<sqlite3.Connection object at 0x7456be94b4c0>, 100, '13F-HR', '2024-05-01', 'raw.xml')
 +    where <function _upsert_filing_legacy at 0x7456beafc720> = flow._upsert_filing_legacy
2 failed, 8 warnings in 0.96s

exit: 1
```

green: [complete output](docs/evidence/issue-1888/mutations/test_upsert_ignored_by_trigger_returns_existing_id_or_zero-green.txt)

```text
cwd: /tmp/issue1888-edgar-mutations
PYTHONPATH= PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.8/x64/bin/python -m pytest tests/test_edgar_flow.py::test_upsert_ignored_by_trigger_returns_existing_id_or_zero -m 'not slow' --tb=short --disable-warnings
PYTHONPATH= PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.8/x64/bin/python -m pytest tests/test_edgar_flow.py::test_upsert_ignored_by_trigger_returns_existing_id_or_zero -m 'not slow' --tb=short --disable-warnings
..                                                                       [100%]
2 passed, 8 warnings in 0.80s

exit: 0
```

</details>

<details>
<summary>tests/test_edgar_flow.py::test_holding_insert_without_recognized_columns_preserves_legacy_rows</summary>

Mutation: [exact diff](docs/evidence/issue-1888/mutations/test_holding_insert_without_recognized_columns_preserves_legacy_rows.diff).

red: [complete output](docs/evidence/issue-1888/mutations/test_holding_insert_without_recognized_columns_preserves_legacy_rows-red.txt)

```text
cwd: /tmp/issue1888-edgar-mutations
PYTHONPATH= PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.8/x64/bin/python -m pytest tests/test_edgar_flow.py::test_holding_insert_without_recognized_columns_preserves_legacy_rows -m 'not slow' --tb=short --disable-warnings
E   RuntimeError: schema rejected
FAILED tests/test_edgar_flow.py::test_holding_insert_without_recognized_columns_preserves_legacy_rows - RuntimeError: schema rejected
1 failed, 8 warnings in 0.98s

exit: 1
```

green: [complete output](docs/evidence/issue-1888/mutations/test_holding_insert_without_recognized_columns_preserves_legacy_rows-green.txt)

```text
cwd: /tmp/issue1888-edgar-mutations
PYTHONPATH= PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.8/x64/bin/python -m pytest tests/test_edgar_flow.py::test_holding_insert_without_recognized_columns_preserves_legacy_rows -m 'not slow' --tb=short --disable-warnings
PYTHONPATH= PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.8/x64/bin/python -m pytest tests/test_edgar_flow.py::test_holding_insert_without_recognized_columns_preserves_legacy_rows -m 'not slow' --tb=short --disable-warnings
.                                                                        [100%]
1 passed, 8 warnings in 0.79s

exit: 0
```

</details>

<details>
<summary>tests/test_edgar_flow.py::test_postgres_upsert_preserves_storage_key</summary>

Mutation: [exact diff](docs/evidence/issue-1888/mutations/test_postgres_upsert_preserves_storage_key.diff).

red: [complete output](docs/evidence/issue-1888/mutations/test_postgres_upsert_preserves_storage_key-red.txt)

```text
cwd: /tmp/issue1888-edgar-mutations
PYTHONPATH= PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.8/x64/bin/python -m pytest tests/test_edgar_flow.py::test_postgres_upsert_preserves_storage_key -m 'not slow' --tb=short --disable-warnings
E   AssertionError: assert 'storage_key = EXCLUDED.storage_key' in 'INSERT INTO filings(manager_id, type, filed_date, source, raw_key, parsed_payload, storage_key) VALUES (%s, %s, %s, %...= EXCLUDED.filed_date, parsed_payload = EXCLUDED.parsed_payload, storage_key = filings.storage_key RETURNING filing_id'
E   AssertionError: assert 'storage_key = EXCLUDED.storage_key' in 'INSERT INTO filings(manager_id, type, filed_date, source, raw_key, parsed_payload, storage_key) VALUES (%s, %s, %s, %...= EXCLUDED.filed_date, parsed_payload = EXCLUDED.parsed_payload, storage_key = filings.storage_key RETURNING filing_id'
E   AssertionError: assert 'storage_key = EXCLUDED.storage_key' in 'INSERT INTO filings(manager_id, type, filed_date, source, raw_key, parsed_payload, storage_key) VALUES (%s, %s, %s, %...= EXCLUDED.filed_date, parsed_payload = EXCLUDED.parsed_payload, storage_key = filings.storage_key RETURNING filing_id'
FAILED tests/test_edgar_flow.py::test_postgres_upsert_preserves_storage_key[None-0] - AssertionError: assert 'storage_key = EXCLUDED.storage_key' in 'INSERT INTO filings(manager_id, type, filed_date, source, raw_key, parsed_payload, storage_key) VALUES (%s, %s, %s, %...= EXCLUDED.filed_date, parsed_payload = EXCLUDED.parsed_payload, storage_key = filings.storage_key RETURNING filing_id'
FAILED tests/test_edgar_flow.py::test_postgres_upsert_preserves_storage_key[returned_row1-0] - AssertionError: assert 'storage_key = EXCLUDED.storage_key' in 'INSERT INTO filings(manager_id, type, filed_date, source, raw_key, parsed_payload, storage_key) VALUES (%s, %s, %s, %...= EXCLUDED.filed_date, parsed_payload = EXCLUDED.parsed_payload, storage_key = filings.storage_key RETURNING filing_id'
FAILED tests/test_edgar_flow.py::test_postgres_upsert_preserves_storage_key[returned_row2-42] - AssertionError: assert 'storage_key = EXCLUDED.storage_key' in 'INSERT INTO filings(manager_id, type, filed_date, source, raw_key, parsed_payload, storage_key) VALUES (%s, %s, %s, %...= EXCLUDED.filed_date, parsed_payload = EXCLUDED.parsed_payload, storage_key = filings.storage_key RETURNING filing_id'
3 failed, 8 warnings in 0.99s

exit: 1
```

green: [complete output](docs/evidence/issue-1888/mutations/test_postgres_upsert_preserves_storage_key-green.txt)

```text
cwd: /tmp/issue1888-edgar-mutations
PYTHONPATH= PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.8/x64/bin/python -m pytest tests/test_edgar_flow.py::test_postgres_upsert_preserves_storage_key -m 'not slow' --tb=short --disable-warnings
PYTHONPATH= PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.8/x64/bin/python -m pytest tests/test_edgar_flow.py::test_postgres_upsert_preserves_storage_key -m 'not slow' --tb=short --disable-warnings
...                                                                      [100%]
3 passed, 8 warnings in 0.81s

exit: 0
```

</details>

<details>
<summary>tests/test_edgar_flow.py::test_reverse_order_original_keeps_amendment_current</summary>

Mutation: [exact diff](docs/evidence/issue-1888/mutations/test_reverse_order_original_keeps_amendment_current.diff).

red: [complete output](docs/evidence/issue-1888/mutations/test_reverse_order_original_keeps_amendment_current-red.txt)

```text
cwd: /tmp/issue1888-edgar-mutations
PYTHONPATH= PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.8/x64/bin/python -m pytest tests/test_edgar_flow.py::test_reverse_order_original_keeps_amendment_current -m 'not slow' --tb=short --disable-warnings
E   assert [(10, 250, 0,..., 1, None, 1)] == [(10, 250, 1,...100, 0, 1, 1)]
E
E     At index 0 diff: (10, 250, 0, 1, 1) != (10, 250, 1, None, 1)
E
FAILED tests/test_edgar_flow.py::test_reverse_order_original_keeps_amendment_current - assert [(10, 250, 0,..., 1, None, 1)] == [(10, 250, 1,...100, 0, 1, 1)]

  At index 0 diff: (10, 250, 0, 1, 1) != (10, 250, 1, None, 1)

  Full diff:
    [
        (
            10,
            250,
  +         0,
            1,
  -         None,
            1,
        ),
        (
            11,
            100,
  -         0,
            1,
  +         None,
            1,
        ),
    ]
1 failed, 8 warnings in 0.95s

exit: 1
```

green: [complete output](docs/evidence/issue-1888/mutations/test_reverse_order_original_keeps_amendment_current-green.txt)

```text
cwd: /tmp/issue1888-edgar-mutations
PYTHONPATH= PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.8/x64/bin/python -m pytest tests/test_edgar_flow.py::test_reverse_order_original_keeps_amendment_current -m 'not slow' --tb=short --disable-warnings
PYTHONPATH= PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.8/x64/bin/python -m pytest tests/test_edgar_flow.py::test_reverse_order_original_keeps_amendment_current -m 'not slow' --tb=short --disable-warnings
.                                                                        [100%]
1 passed, 8 warnings in 0.79s

exit: 0
```

</details>


### Follow-up publication blocker

The follow-up `git add tests/test_edgar_flow.py docs/evidence/issue-1888` failed:
`Unable to create .git/index.lock: Read-only file system`. No commit can be created
in this checkout. Updating PR #1889 with this prepared body was rejected with:
`MCP tool call requires approval, but approval policy is never`. The live PR was
verified open with draft=false; its body has not been updated. The baseline,
CI-artifact reduction and CI pytest acceptance checks remain open. The local
verified checkmarks in this prepared body are not a claim of live PR publication.
