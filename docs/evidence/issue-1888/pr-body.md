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
- [ ] Cover the largest remaining gaps among the 149 units left after #1808: api/search.py (52), ui/daily_report.py (35), api/signals.py (27). Prefer behaviour-level tests over helper-level ones.
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

## Verified implementation

Adds 13 test functions (21 cases) for EDGAR recovery/schema/ingestion boundaries and search, signals, and report behavior. Two existing functions now assert exact digest errors and escaped headline output; search guards have their own test. Runtime code is unchanged. No negative-cache misuse occurs in the inspected runtime surfaces; search deduplication only stores concrete SearchResult objects.

- Final affected suites: **130 passed, 1 existing nightly skip**. Branch coverage: EDGAR 94%, search 95%, signals 94%, report 92%.
- Full local candidate: **2606 passed, 26 skipped, 3 failed**. Existing failures are the chat dependency timeout assertion, EDGAR parser writing to the read-only home directory, and WASM server socket permissions. CI success remains unverified.
- All 13 new functions and both strengthened functions fail under a real production mutation and pass after restoring the source. Full command/output pairs appear below.
- Black repository check: **451 files unchanged**. Ruff passes for changed Python files. The temporary asyncio polling shim is described in [the evidence](docs/evidence/issue-1888/README.md).

### Coverage accounting and remaining acceptance gaps

Current main is `a7f4e626bb4b8359a64c56a56211218e41b6020a`. Its [successful CI run](https://github.com/stranske/Manager-Database/actions/runs/41702885246) has Python 3.13 artifact `gate-coverage-3.13-1`, ID `13265760076`, SHA-256 `0ad415416368e7718da8435c0c541d7ea66aa8e02f70812701b83032b193475a`. The connector returned a download reference, but transfer to this sandbox failed with URLError. **Exact current-main CI uncovered units and a CI after-artifact comparison remain blocked. Historical issue estimates are not current-main artifact measurements.**

Provisional 500-source-commit repair-history ranking: EDGAR (19 repair-subject matches, 41 touches), report (4/14), signals (3/10), search (2/9), digest (0/4), ASIC (0/2). This proxy does not establish escaped-defect counts. The required artifact-based ranking remains open.

| Target | Local main statements+branches | Local candidate statements+branches | Local reduction |
| --- | ---: | ---: | ---: |
| etl/edgar_flow.py | 16+16 = 32 | 9+7 = 16 | 16 |
| ui/daily_report.py | 20+15 = 35 | 16+15 = 31 | 4 |
| api/signals.py | 9+4 = 13 | 4+2 = 6 | 7 |
| api/search.py | 25+27 = 52 | 10+12 = 22 | 30 |
| Total | 132 | 75 | 57 |

**These are local measurements, not CI artifact credit.** The archived baseline has four extra Git-metadata-dependent failures; both runs share the three environment/timing failures listed above. The local base across these four targets contains only 132 units, so a 180-unit reduction from current main cannot be achieved within them. This PR does not claim that quantitative acceptance criterion.

Per-target remaining misses and limitations are documented in [README.md](docs/evidence/issue-1888/README.md): EDGAR compatibility guards/insert fallback, lower-authority history and entry point; search optional imports, vector recovery and schema branches; signals scalar/date/offline-import guards; report error handling, page filters/sorting and entry point. The remaining testable cases are explicitly pending, not falsely described as impossible. Raw local missing-line/branch data and all mutation patches are retained under `docs/evidence/issue-1888/`.

### Individual mutation commands and output

The runner writes the adjacent documented `.diff` to an isolated copy, runs the red command, restores exact original bytes in a `finally` block, and runs the identical green command. [Receipts](docs/evidence/issue-1888/mutations/receipts.json) record matching restored source hashes. The following captures are complete.

<details>
<summary>tests/test_edgar_flow.py::test_manager_lookup_rejects_missing_or_deleted_manager</summary>

Mutation: [exact source patch](docs/evidence/issue-1888/mutations/test_manager_lookup_rejects_missing_or_deleted_manager.diff).

```text
cwd: /tmp/issue1888-mutations
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_edgar_flow.py::test_manager_lookup_rejects_missing_or_deleted_manager -m 'not slow' --tb=short --disable-warnings
F.....                                                                   [100%]
=================================== FAILURES ===================================
______ test_manager_lookup_rejects_missing_or_deleted_manager[None-rows0] ______
tests/test_edgar_flow.py:38: in test_manager_lookup_rejects_missing_or_deleted_manager
    assert flow._manager_id_for_cik(conn, "0") is None
E   AssertionError: assert 99 is None
E    +  where 99 = <function _manager_id_for_cik at 0x7119adc6c9e0>(<sqlite3.Connection object at 0x7119b0727790>, '0')
E    +    where <function _manager_id_for_cik at 0x7119adc6c9e0> = flow._manager_id_for_cik
=========================== short test summary info ============================
FAILED tests/test_edgar_flow.py::test_manager_lookup_rejects_missing_or_deleted_manager[None-rows0] - AssertionError: assert 99 is None
 +  where 99 = <function _manager_id_for_cik at 0x7119adc6c9e0>(<sqlite3.Connection object at 0x7119b0727790>, '0')
 +    where <function _manager_id_for_cik at 0x7119adc6c9e0> = flow._manager_id_for_cik
1 failed, 5 passed, 8 warnings in 1.70s

exit: 1
```

```text
cwd: /tmp/issue1888-mutations
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_edgar_flow.py::test_manager_lookup_rejects_missing_or_deleted_manager -m 'not slow' --tb=short --disable-warnings
......                                                                   [100%]
6 passed, 8 warnings in 1.37s

exit: 0
```

</details>

<details>
<summary>tests/test_edgar_flow.py::test_watermark_unavailable_without_filing_history</summary>

Mutation: [exact source patch](docs/evidence/issue-1888/mutations/test_watermark_unavailable_without_filing_history.diff).

```text
cwd: /tmp/issue1888-mutations
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_edgar_flow.py::test_watermark_unavailable_without_filing_history -m 'not slow' --tb=short --disable-warnings
FF.                                                                      [100%]
=================================== FAILURES ===================================
___________ test_watermark_unavailable_without_filing_history[None] ____________
tests/test_edgar_flow.py:60: in test_watermark_unavailable_without_filing_history
    assert flow._latest_filed_date_for_cik("missing") is None
E   AssertionError: assert '2000-01-01' is None
E    +  where '2000-01-01' = <function _latest_filed_date_for_cik at 0x7476d6065380>('missing')
E    +    where <function _latest_filed_date_for_cik at 0x7476d6065380> = flow._latest_filed_date_for_cik
____ test_watermark_unavailable_without_filing_history[manager_id INTEGER] _____
tests/test_edgar_flow.py:60: in test_watermark_unavailable_without_filing_history
    assert flow._latest_filed_date_for_cik("missing") is None
E   AssertionError: assert '2000-01-01' is None
E    +  where '2000-01-01' = <function _latest_filed_date_for_cik at 0x7476d6065380>('missing')
E    +    where <function _latest_filed_date_for_cik at 0x7476d6065380> = flow._latest_filed_date_for_cik
=========================== short test summary info ============================
FAILED tests/test_edgar_flow.py::test_watermark_unavailable_without_filing_history[None] - AssertionError: assert '2000-01-01' is None
 +  where '2000-01-01' = <function _latest_filed_date_for_cik at 0x7476d6065380>('missing')
 +    where <function _latest_filed_date_for_cik at 0x7476d6065380> = flow._latest_filed_date_for_cik
FAILED tests/test_edgar_flow.py::test_watermark_unavailable_without_filing_history[manager_id INTEGER] - AssertionError: assert '2000-01-01' is None
 +  where '2000-01-01' = <function _latest_filed_date_for_cik at 0x7476d6065380>('missing')
 +    where <function _latest_filed_date_for_cik at 0x7476d6065380> = flow._latest_filed_date_for_cik
2 failed, 1 passed, 8 warnings in 1.67s

exit: 1
```

```text
cwd: /tmp/issue1888-mutations
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_edgar_flow.py::test_watermark_unavailable_without_filing_history -m 'not slow' --tb=short --disable-warnings
...                                                                      [100%]
3 passed, 8 warnings in 1.61s

exit: 0
```

</details>

<details>
<summary>tests/test_edgar_flow.py::test_holdings_resolution_failure_preserves_raw_rows</summary>

Mutation: [exact source patch](docs/evidence/issue-1888/mutations/test_holdings_resolution_failure_preserves_raw_rows.diff).

```text
cwd: /tmp/issue1888-mutations
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_edgar_flow.py::test_holdings_resolution_failure_preserves_raw_rows -m 'not slow' --tb=short --disable-warnings
F                                                                        [100%]
=================================== FAILURES ===================================
_____________ test_holdings_resolution_failure_preserves_raw_rows ______________
tests/test_edgar_flow.py:71: in test_holdings_resolution_failure_preserves_raw_rows
    flow._replace_holdings_for_filing(
etl/edgar_flow.py:376: in _replace_holdings_for_filing
    _run_in_transaction(conn, _work)
etl/edgar_flow.py:308: in _run_in_transaction
    return work()
           ^^^^^^
etl/edgar_flow.py:353: in _work
    resolve_holding_identifiers(conn, rows, filing_id=filing_id, source="edgar")
/opt/hostedtoolcache/Python/3.14.9/x64/lib/python3.14/unittest/mock.py:1176: in __call__
    return self._mock_call(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
/opt/hostedtoolcache/Python/3.14.9/x64/lib/python3.14/unittest/mock.py:1180: in _mock_call
    return self._execute_mock_call(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
/opt/hostedtoolcache/Python/3.14.9/x64/lib/python3.14/unittest/mock.py:1241: in _execute_mock_call
    raise effect
E   RuntimeError: resolver unavailable
=========================== short test summary info ============================
FAILED tests/test_edgar_flow.py::test_holdings_resolution_failure_preserves_raw_rows - RuntimeError: resolver unavailable
1 failed, 8 warnings in 2.51s

exit: 1
```

```text
cwd: /tmp/issue1888-mutations
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_edgar_flow.py::test_holdings_resolution_failure_preserves_raw_rows -m 'not slow' --tb=short --disable-warnings
.                                                                        [100%]
1 passed, 8 warnings in 1.39s

exit: 0
```

</details>

<details>
<summary>tests/test_edgar_flow.py::test_empty_holdings_do_not_delete_existing_rows</summary>

Mutation: [exact source patch](docs/evidence/issue-1888/mutations/test_empty_holdings_do_not_delete_existing_rows.diff).

```text
cwd: /tmp/issue1888-mutations
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_edgar_flow.py::test_empty_holdings_do_not_delete_existing_rows -m 'not slow' --tb=short --disable-warnings
F                                                                        [100%]
=================================== FAILURES ===================================
_______________ test_empty_holdings_do_not_delete_existing_rows ________________
tests/test_edgar_flow.py:99: in test_empty_holdings_do_not_delete_existing_rows
    assert conn.execute("SELECT cusip FROM holdings").fetchall() == [("KEEP",)]
E   AssertionError: assert [] == [('KEEP',)]
E     
E     Right contains one more item: ('KEEP',)
E     
E     Full diff:
E     + []
E     - [
E     -     (
E     -         'KEEP',
E     -     ),
E     - ]
----------------------------- Captured stderr call -----------------------------
00:53:02.677 | WARNING | etl.edgar_flow - Identifier resolution failed; inserting raw holdings
Traceback (most recent call last):
  File "/tmp/issue1888-mutations/etl/edgar_flow.py", line 353, in _work
    resolve_holding_identifiers(conn, rows, filing_id=filing_id, source="edgar")
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.14.9/x64/lib/python3.14/unittest/mock.py", line 1176, in __call__
    return self._mock_call(*args, **kwargs)
           ~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.14.9/x64/lib/python3.14/unittest/mock.py", line 1180, in _mock_call
    return self._execute_mock_call(*args, **kwargs)
           ~~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.14.9/x64/lib/python3.14/unittest/mock.py", line 1241, in _execute_mock_call
    raise effect
AssertionError: empty holdings must not resolve
{"asctime": "2026-11-04 00:53:02,677", "levelname": "WARNING", "name": "etl.edgar_flow", "service": "ingest_flow", "message": "Identifier resolution failed; inserting raw holdings", "exc_info": "Traceback (most recent call last):\n  File \"/tmp/issue1888-mutations/etl/edgar_flow.py\", line 353, in _work\n    resolve_holding_identifiers(conn, rows, filing_id=filing_id, source=\"edgar\")\n    ~~~~~~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n  File \"/opt/hostedtoolcache/Python/3.14.9/x64/lib/python3.14/unittest/mock.py\", line 1176, in __call__\n    return self._mock_call(*args, **kwargs)\n           ~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^\n  File \"/opt/hostedtoolcache/Python/3.14.9/x64/lib/python3.14/unittest/mock.py\", line 1180, in _mock_call\n    return self._execute_mock_call(*args, **kwargs)\n           ~~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^\n  File \"/opt/hostedtoolcache/Python/3.14.9/x64/lib/python3.14/unittest/mock.py\", line 1241, in _execute_mock_call\n    raise effect\nAssertionError: empty holdings must not resolve"}
------------------------------ Captured log call -------------------------------
WARNING  etl.edgar_flow:edgar_flow.py:355 Identifier resolution failed; inserting raw holdings
Traceback (most recent call last):
  File "/tmp/issue1888-mutations/etl/edgar_flow.py", line 353, in _work
    resolve_holding_identifiers(conn, rows, filing_id=filing_id, source="edgar")
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.14.9/x64/lib/python3.14/unittest/mock.py", line 1176, in __call__
    return self._mock_call(*args, **kwargs)
           ~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.14.9/x64/lib/python3.14/unittest/mock.py", line 1180, in _mock_call
    return self._execute_mock_call(*args, **kwargs)
           ~~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.14.9/x64/lib/python3.14/unittest/mock.py", line 1241, in _execute_mock_call
    raise effect
AssertionError: empty holdings must not resolve
=========================== short test summary info ============================
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

```text
cwd: /tmp/issue1888-mutations
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_edgar_flow.py::test_empty_holdings_do_not_delete_existing_rows -m 'not slow' --tb=short --disable-warnings
.                                                                        [100%]
1 passed, 8 warnings in 1.88s

exit: 0
```

</details>

<details>
<summary>tests/test_edgar_flow.py::test_bitemporal_unowned_filing_rerun_preserves_version</summary>

Mutation: [exact source patch](docs/evidence/issue-1888/mutations/test_bitemporal_unowned_filing_rerun_preserves_version.diff).

```text
cwd: /tmp/issue1888-mutations
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_edgar_flow.py::test_bitemporal_unowned_filing_rerun_preserves_version -m 'not slow' --tb=short --disable-warnings
F                                                                        [100%]
=================================== FAILURES ===================================
____________ test_bitemporal_unowned_filing_rerun_preserves_version ____________
tests/test_edgar_flow.py:120: in test_bitemporal_unowned_filing_rerun_preserves_version
    assert [
E   AssertionError: assert [('AAA', 12, ...4.0, 2, None)] == [('AAA', 12, 34.0, 1, None)]
E     
E     At index 0 diff: ('AAA', 12, 34.0, 1, '2026-11-04T00:53:15Z') != ('AAA', 12, 34.0, 1, None)
E     Left contains one more item: ('AAA', 12, 34.0, 2, None)
E     
E     Full diff:
E       [
E           (
E               'AAA',
E               12,
E               34.0,
E               1,
E     +         '2026-11-04T00:53:15Z',
E     +     ),
E     +     (
E     +         'AAA',
E     +         12,
E     +         34.0,
E     +         2,
E               None,
E           ),
E       ]
=========================== short test summary info ============================
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

```text
cwd: /tmp/issue1888-mutations
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_edgar_flow.py::test_bitemporal_unowned_filing_rerun_preserves_version -m 'not slow' --tb=short --disable-warnings
.                                                                        [100%]
1 passed, 8 warnings in 1.43s

exit: 0
```

</details>

<details>
<summary>tests/test_edgar_flow.py::test_deletion_during_second_download_keeps_first_filing</summary>

Mutation: [exact source patch](docs/evidence/issue-1888/mutations/test_deletion_during_second_download_keeps_first_filing.diff).

```text
cwd: /tmp/issue1888-mutations
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_edgar_flow.py::test_deletion_during_second_download_keeps_first_filing -m 'not slow' --tb=short --disable-warnings
F                                                                        [100%]
=================================== FAILURES ===================================
___________ test_deletion_during_second_download_keeps_first_filing ____________
tests/test_edgar_flow.py:150: in test_deletion_during_second_download_keeps_first_filing
    assert rows == [{"nameOfIssuer": "Corp", "cusip": "AAA", "value": 1, "sshPrnamt": 1}]
E   AssertionError: assert [] == [{'nameOfIssu...shPrnamt': 1}]
E     
E     Right contains one more item: {'nameOfIssuer': 'Corp', 'cusip': 'AAA', 'value': 1, 'sshPrnamt': 1}
E     
E     Full diff:
E     + []
E     - [
E     -     {
E     -         'nameOfIssuer': 'Corp',
E     -         'cusip': 'AAA',
E     -         'value': 1,
E     -         'sshPrnamt': 1,
E     -     },
E     - ]
----------------------------- Captured stderr call -----------------------------
00:53:28.668 | WARNING | etl.edgar_flow - Manager deletion started during EDGAR download; stopping ingestion
{"asctime": "2026-11-04 00:53:28,668", "levelname": "WARNING", "name": "etl.edgar_flow", "service": "ingest_flow", "message": "Manager deletion started during EDGAR download; stopping ingestion", "manager_id": 100, "cik": "0"}
------------------------------ Captured log call -------------------------------
WARNING  etl.edgar_flow:edgar_flow.py:426 Manager deletion started during EDGAR download; stopping ingestion
=========================== short test summary info ============================
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

```text
cwd: /tmp/issue1888-mutations
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_edgar_flow.py::test_deletion_during_second_download_keeps_first_filing -m 'not slow' --tb=short --disable-warnings
.                                                                        [100%]
1 passed, 8 warnings in 1.42s

exit: 0
```

</details>

<details>
<summary>tests/test_edgar_flow.py::test_binary_filing_without_manager_skips_text_index</summary>

Mutation: [exact source patch](docs/evidence/issue-1888/mutations/test_binary_filing_without_manager_skips_text_index.diff).

```text
cwd: /tmp/issue1888-mutations
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_edgar_flow.py::test_binary_filing_without_manager_skips_text_index -m 'not slow' --tb=short --disable-warnings
F                                                                        [100%]
=================================== FAILURES ===================================
_____________ test_binary_filing_without_manager_skips_text_index ______________
tests/test_edgar_flow.py:178: in test_binary_filing_without_manager_skips_text_index
    objects.assert_called_once_with(
/opt/hostedtoolcache/Python/3.14.9/x64/lib/python3.14/unittest/mock.py:998: in assert_called_once_with
    return self.assert_called_with(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
/opt/hostedtoolcache/Python/3.14.9/x64/lib/python3.14/unittest/mock.py:986: in assert_called_with
    raise AssertionError(_error_message()) from cause
E   AssertionError: expected call not found.
E   Expected: mock(Bucket='filings', Key='raw/edgar/6eb820e0f9762c61_unknown.xml', Body=b'<xml/>', ServerSideEncryption='AES256')
E     Actual: mock(Bucket='filings', Key='raw/edgar/6eb820e0f9762c61_incorrect.xml', Body=b'<xml/>', ServerSideEncryption='AES256')
=========================== short test summary info ============================
FAILED tests/test_edgar_flow.py::test_binary_filing_without_manager_skips_text_index - AssertionError: expected call not found.
Expected: mock(Bucket='filings', Key='raw/edgar/6eb820e0f9762c61_unknown.xml', Body=b'<xml/>', ServerSideEncryption='AES256')
  Actual: mock(Bucket='filings', Key='raw/edgar/6eb820e0f9762c61_incorrect.xml', Body=b'<xml/>', ServerSideEncryption='AES256')
1 failed, 8 warnings in 1.78s

exit: 1
```

```text
cwd: /tmp/issue1888-mutations
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_edgar_flow.py::test_binary_filing_without_manager_skips_text_index -m 'not slow' --tb=short --disable-warnings
.                                                                        [100%]
1 passed, 8 warnings in 2.28s

exit: 0
```

</details>

<details>
<summary>tests/test_issue_1806_coverage.py::test_search_guards_return_before_accessing_database</summary>

Mutation: [exact source patch](docs/evidence/issue-1888/mutations/test_search_guards_return_before_accessing_database.diff).

```text
cwd: /tmp/issue1888-mutations
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1806_coverage.py::test_search_guards_return_before_accessing_database -m 'not slow' --tb=short --disable-warnings
F                                                                        [100%]
=================================== FAILURES ===================================
_____________ test_search_guards_return_before_accessing_database ______________
tests/test_issue_1806_coverage.py:90: in test_search_guards_return_before_accessing_database
    assert search.universal_search(" ", object()) == []
E   assert [None] == []
E     
E     Left contains one more item: None
E     
E     Full diff:
E     - []
E     + [
E     +     None,
E     + ]
=========================== short test summary info ============================
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

```text
cwd: /tmp/issue1888-mutations
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1806_coverage.py::test_search_guards_return_before_accessing_database -m 'not slow' --tb=short --disable-warnings
.                                                                        [100%]
1 passed, 8 warnings in 2.10s

exit: 0
```

</details>

<details>
<summary>tests/test_issue_1806_coverage.py::test_digest_delivery_configuration_guards</summary>

Mutation: [exact source patch](docs/evidence/issue-1888/mutations/test_digest_delivery_configuration_guards.diff).

```text
cwd: /tmp/issue1888-mutations
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1806_coverage.py::test_digest_delivery_configuration_guards -m 'not slow' --tb=short --disable-warnings
F                                                                        [100%]
=================================== FAILURES ===================================
__________________ test_digest_delivery_configuration_guards ___________________
tests/test_issue_1806_coverage.py:40: in test_digest_delivery_configuration_guards
    assert missing_sender.error_message == "DIGEST_EMAIL_FROM is not configured"
E   AssertionError: assert 'DIGEST_EMAIL...t configured!' == 'DIGEST_EMAIL...ot configured'
E     
E     - DIGEST_EMAIL_FROM is not configured
E     + DIGEST_EMAIL_FROM is not configured!
E     ?                                    +
=========================== short test summary info ============================
FAILED tests/test_issue_1806_coverage.py::test_digest_delivery_configuration_guards - AssertionError: assert 'DIGEST_EMAIL...t configured!' == 'DIGEST_EMAIL...ot configured'
  
  - DIGEST_EMAIL_FROM is not configured
  + DIGEST_EMAIL_FROM is not configured!
  ?                                    +
1 failed, 8 warnings in 1.96s

exit: 1
```

```text
cwd: /tmp/issue1888-mutations
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1806_coverage.py::test_digest_delivery_configuration_guards -m 'not slow' --tb=short --disable-warnings
.                                                                        [100%]
1 passed, 8 warnings in 1.92s

exit: 0
```

</details>

<details>
<summary>tests/test_issue_1806_coverage.py::test_daily_report_formatters_cover_empty_and_directional_values</summary>

Mutation: [exact source patch](docs/evidence/issue-1888/mutations/test_daily_report_formatters_cover_empty_and_directional_values.diff).

```text
cwd: /tmp/issue1888-mutations
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1806_coverage.py::test_daily_report_formatters_cover_empty_and_directional_values -m 'not slow' --tb=short --disable-warnings
F                                                                        [100%]
=================================== FAILURES ===================================
_______ test_daily_report_formatters_cover_empty_and_directional_values ________
tests/test_issue_1806_coverage.py:112: in test_daily_report_formatters_cover_empty_and_directional_values
    assert daily_report.headline_markdown("<x>", None) == "&lt;x&gt;"
E   AssertionError: assert '&lt;x&gt;!' == '&lt;x&gt;'
E     
E     - &lt;x&gt;
E     + &lt;x&gt;!
E     ?          +
=========================== short test summary info ============================
FAILED tests/test_issue_1806_coverage.py::test_daily_report_formatters_cover_empty_and_directional_values - AssertionError: assert '&lt;x&gt;!' == '&lt;x&gt;'
  
  - &lt;x&gt;
  + &lt;x&gt;!
  ?          +
1 failed, 8 warnings in 1.95s

exit: 1
```

```text
cwd: /tmp/issue1888-mutations
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1806_coverage.py::test_daily_report_formatters_cover_empty_and_directional_values -m 'not slow' --tb=short --disable-warnings
.                                                                        [100%]
1 passed, 8 warnings in 1.99s

exit: 0
```

</details>

<details>
<summary>tests/test_issue_1888_coverage.py::test_search_legacy_holdings_return_filing_groups_and_holdings</summary>

Mutation: [exact source patch](docs/evidence/issue-1888/mutations/test_search_legacy_holdings_return_filing_groups_and_holdings.diff).

```text
cwd: /tmp/issue1888-mutations
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_search_legacy_holdings_return_filing_groups_and_holdings -m 'not slow' --tb=short --disable-warnings
F                                                                        [100%]
=================================== FAILURES ===================================
________ test_search_legacy_holdings_return_filing_groups_and_holdings _________
tests/test_issue_1888_coverage.py:27: in test_search_legacy_holdings_return_filing_groups_and_holdings
    assert [
E   AssertionError: assert [('filing', 1...'2024-05-01')] == [('filing', 1...'2024-05-01')]
E     
E     At index 0 diff: ('filing', 1, '123', 'Filing needle-1', '3 holdings; filed 2024-05-01', '2024-05-01') != ('filing', 1, '123', 'Filing needle-1', '2 holdings; filed 2024-05-01', '2024-05-01')
E     
E     Full diff:
E       [
E           (
E               'filing',
E               1,
E               '123',
E               'Filing needle-1',
E     -         '2 holdings; filed 2024-05-01',
E     ?          ^
E     +         '3 holdings; filed 2024-05-01',
E     ?          ^
E               '2024-05-01',
E           ),
E           (
E               'holding',
E               1,
E               '123',
E               'needle Corp (AAA)',
E               'Accession needle-1 filed 2024-05-01',
E               '2024-05-01',
E           ),
E       ]
=========================== short test summary info ============================
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

```text
cwd: /tmp/issue1888-mutations
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_search_legacy_holdings_return_filing_groups_and_holdings -m 'not slow' --tb=short --disable-warnings
.                                                                        [100%]
1 passed, 6 warnings in 0.82s

exit: 0
```

</details>

<details>
<summary>tests/test_issue_1888_coverage.py::test_search_vector_documents_keep_shared_managers_and_deduplicate</summary>

Mutation: [exact source patch](docs/evidence/issue-1888/mutations/test_search_vector_documents_keep_shared_managers_and_deduplicate.diff).

```text
cwd: /tmp/issue1888-mutations
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_search_vector_documents_keep_shared_managers_and_deduplicate -m 'not slow' --tb=short --disable-warnings
F                                                                        [100%]
=================================== FAILURES ===================================
______ test_search_vector_documents_keep_shared_managers_and_deduplicate _______
tests/test_issue_1888_coverage.py:71: in test_search_vector_documents_keep_shared_managers_and_deduplicate
    assert [(r.entity_id, r.manager_name, r.headline, r.snippet) for r in results] == [
E   AssertionError: assert [(1, None, 'm...only result')] == [(1, None, 'm...only result')]
E     
E     At index 1 diff: (2, 'Alpha / Beta', 'vector only result', 'vector only result') != (2, 'Alpha, Beta', 'vector only result', 'vector only result')
E     
E     Full diff:
E       [
E           (
E               1,
E               None,
E               'memo',
E               'needle memo',
E           ),
E           (
E               2,
E     -         'Alpha, Beta',
E     ?               ^
E     +         'Alpha / Beta',
E     ?               ^^
E               'vector only result',
E               'vector only result',
E           ),
E       ]
=========================== short test summary info ============================
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

```text
cwd: /tmp/issue1888-mutations
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_search_vector_documents_keep_shared_managers_and_deduplicate -m 'not slow' --tb=short --disable-warnings
.                                                                        [100%]
1 passed, 6 warnings in 0.83s

exit: 0
```

</details>

<details>
<summary>tests/test_issue_1888_coverage.py::test_search_postgres_activism_preserves_filing_context</summary>

Mutation: [exact source patch](docs/evidence/issue-1888/mutations/test_search_postgres_activism_preserves_filing_context.diff).

```text
cwd: /tmp/issue1888-mutations
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_search_postgres_activism_preserves_filing_context -m 'not slow' --tb=short --disable-warnings
F                                                                        [100%]
=================================== FAILURES ===================================
____________ test_search_postgres_activism_preserves_filing_context ____________
tests/test_issue_1888_coverage.py:101: in test_search_postgres_activism_preserves_filing_context
    assert results[0].model_dump() == {
E   AssertionError: assert {'entity_type... (5.5%)', ...} == {'entity_type... (5.5%)', ...}
E     
E     Omitting 7 identical items, use -vv to show
E     Differing items:
E     {'snippet': 'SC 13D | Wrong: CUSIP | Filed: 2024-05-01'} != {'snippet': 'SC 13D | CUSIP: CUSIP | Filed: 2024-05-01'}
E     
E     Full diff:
E       {
E           'entity_type': 'filing',
E           'entity_id': 12,
E           'manager_name': 'Alpha',
E           'headline': '13D Filing: Alpha -> Target (5.5%)',
E     -     'snippet': 'SC 13D | CUSIP: CUSIP | Filed: 2024-05-01',
E     ?                               -------
E     +     'snippet': 'SC 13D | Wrong: CUSIP | Filed: 2024-05-01',
E     ?                         +++++++
E     -     'relevance': 0.8666666667 ± 8.7e-07,
E     ?                             ^^^^^^^^^^
E     +     'relevance': 0.8666666666666667,
E     ?                             ^^^^^^
E           'url': 'https://example.test/12',
E           'timestamp': '2024-05-01',
E       }
=========================== short test summary info ============================
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

```text
cwd: /tmp/issue1888-mutations
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_search_postgres_activism_preserves_filing_context -m 'not slow' --tb=short --disable-warnings
.                                                                        [100%]
1 passed, 6 warnings in 0.89s

exit: 0
```

</details>

<details>
<summary>tests/test_issue_1888_coverage.py::test_conviction_annotation_survives_unavailable_holdings</summary>

Mutation: [exact source patch](docs/evidence/issue-1888/mutations/test_conviction_annotation_survives_unavailable_holdings.diff).

```text
cwd: /tmp/issue1888-mutations
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_conviction_annotation_survives_unavailable_holdings -m 'not slow' --tb=short --disable-warnings
F                                                                        [100%]
=================================== FAILURES ===================================
___________ test_conviction_annotation_survives_unavailable_holdings ___________
tests/test_issue_1888_coverage.py:134: in test_conviction_annotation_survives_unavailable_holdings
    assert [row.model_dump() for row in rows] == [
E   AssertionError: assert [{'cusip': 'A...': 50.0, ...}] == [{'cusip': 'A...': 50.0, ...}]
E     
E     At index 0 diff: {'cusip': 'AAA', 'name_of_issuer': 'Issuer', 'value_usd': 300.0, 'conviction_pct': 50.0, 'portfolio_weight': 0.5, 'insider_net_direction': None, 'short_interest_pct': None, 'short_interest_report_date': None, 'short_interest_source': None} != {'cusip': 'AAA', 'name_of_issuer': 'Issuer', 'value_usd': 300.0, 'conviction_pct': 50.0, 'portfolio_weight': 0.5, 'insider_net_direction': 'BUY', 'short_interest_pct': None, 'short_interest_report_date': None, 'short_interest_source': None}
E     
E     Full diff:
E       [
E           {
E               'cusip': 'AAA',
E               'name_of_issuer': 'Issuer',
E               'value_usd': 300.0,
E               'conviction_pct': 50.0,
E               'portfolio_weight': 0.5,
E     -         'insider_net_direction': 'BUY',
E     ?                                  ^^^^^
E     +         'insider_net_direction': None,
E     ?                                  ^^^^
E               'short_interest_pct': None,
E               'short_interest_report_date': None,
E               'short_interest_source': None,
E           },
E       ]
=========================== short test summary info ============================
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

```text
cwd: /tmp/issue1888-mutations
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_conviction_annotation_survives_unavailable_holdings -m 'not slow' --tb=short --disable-warnings
.                                                                        [100%]
1 passed, 6 warnings in 0.98s

exit: 0
```

</details>

<details>
<summary>tests/test_issue_1888_coverage.py::test_report_signal_query_failure_returns_empty_and_closes</summary>

Mutation: [exact source patch](docs/evidence/issue-1888/mutations/test_report_signal_query_failure_returns_empty_and_closes.diff).

```text
cwd: /tmp/issue1888-mutations
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_report_signal_query_failure_returns_empty_and_closes -m 'not slow' --tb=short --disable-warnings
FF                                                                       [100%]
=================================== FAILURES ===================================
_ test_report_signal_query_failure_returns_empty_and_closes[load_crowded_trades-query_crowded_trades] _
tests/test_issue_1888_coverage.py:167: in test_report_signal_query_failure_returns_empty_and_closes
    assert load("2024-05-01").empty
           ^^^^^^^^^^^^^^^^^^
/opt/hostedtoolcache/Python/3.14.9/x64/lib/python3.14/site-packages/streamlit/runtime/caching/cache_utils.py:281: in __call__
    return self._get_or_create_cached_value(args, kwargs, spinner_message)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
/opt/hostedtoolcache/Python/3.14.9/x64/lib/python3.14/site-packages/streamlit/runtime/caching/cache_utils.py:326: in _get_or_create_cached_value
    return self._handle_cache_miss(cache, value_key, func_args, func_kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
/opt/hostedtoolcache/Python/3.14.9/x64/lib/python3.14/site-packages/streamlit/runtime/caching/cache_utils.py:385: in _handle_cache_miss
    computed_value = self._info.func(*func_args, **func_kwargs)
                     ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
ui/daily_report.py:121: in load_crowded_trades
    rows = query_crowded_trades(
/opt/hostedtoolcache/Python/3.14.9/x64/lib/python3.14/unittest/mock.py:1176: in __call__
    return self._mock_call(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
/opt/hostedtoolcache/Python/3.14.9/x64/lib/python3.14/unittest/mock.py:1180: in _mock_call
    return self._execute_mock_call(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
/opt/hostedtoolcache/Python/3.14.9/x64/lib/python3.14/unittest/mock.py:1241: in _execute_mock_call
    raise effect
E   sqlite3.OperationalError: database unavailable
----------------------------- Captured stderr call -----------------------------
2026-11-04 00:55:39.377 No runtime found, using MemoryCacheStorageManager
------------------------------ Captured log call -------------------------------
WARNING  streamlit.runtime.caching.cache_data_api:cache_data_api.py:372 No runtime found, using MemoryCacheStorageManager
_ test_report_signal_query_failure_returns_empty_and_closes[load_contrarian_signals-query_contrarian_signals] _
tests/test_issue_1888_coverage.py:167: in test_report_signal_query_failure_returns_empty_and_closes
    assert load("2024-05-01").empty
           ^^^^^^^^^^^^^^^^^^
/opt/hostedtoolcache/Python/3.14.9/x64/lib/python3.14/site-packages/streamlit/runtime/caching/cache_utils.py:281: in __call__
    return self._get_or_create_cached_value(args, kwargs, spinner_message)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
/opt/hostedtoolcache/Python/3.14.9/x64/lib/python3.14/site-packages/streamlit/runtime/caching/cache_utils.py:326: in _get_or_create_cached_value
    return self._handle_cache_miss(cache, value_key, func_args, func_kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
/opt/hostedtoolcache/Python/3.14.9/x64/lib/python3.14/site-packages/streamlit/runtime/caching/cache_utils.py:385: in _handle_cache_miss
    computed_value = self._info.func(*func_args, **func_kwargs)
                     ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
ui/daily_report.py:138: in load_contrarian_signals
    rows = query_contrarian_signals(
/opt/hostedtoolcache/Python/3.14.9/x64/lib/python3.14/unittest/mock.py:1176: in __call__
    return self._mock_call(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
/opt/hostedtoolcache/Python/3.14.9/x64/lib/python3.14/unittest/mock.py:1180: in _mock_call
    return self._execute_mock_call(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
/opt/hostedtoolcache/Python/3.14.9/x64/lib/python3.14/unittest/mock.py:1241: in _execute_mock_call
    raise effect
E   sqlite3.OperationalError: database unavailable
----------------------------- Captured stderr call -----------------------------
2026-11-04 00:55:39.526 No runtime found, using MemoryCacheStorageManager
------------------------------ Captured log call -------------------------------
WARNING  streamlit.runtime.caching.cache_data_api:cache_data_api.py:372 No runtime found, using MemoryCacheStorageManager
=========================== short test summary info ============================
FAILED tests/test_issue_1888_coverage.py::test_report_signal_query_failure_returns_empty_and_closes[load_crowded_trades-query_crowded_trades] - sqlite3.OperationalError: database unavailable
FAILED tests/test_issue_1888_coverage.py::test_report_signal_query_failure_returns_empty_and_closes[load_contrarian_signals-query_contrarian_signals] - sqlite3.OperationalError: database unavailable
2 failed, 6 warnings in 1.17s

exit: 1
```

```text
cwd: /tmp/issue1888-mutations
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_report_signal_query_failure_returns_empty_and_closes -m 'not slow' --tb=short --disable-warnings
..                                                                       [100%]
2 passed, 6 warnings in 0.90s

exit: 0
```

</details>

### Publication blocker

Local staging fails because `.git/index.lock` is on a read-only filesystem. The GitHub tree write also failed: `MCP tool call requires approval, but approval policy is never`. PR-body update, needs-human labeling, and blocker-comment tools were also denied by the same approval policy. Source changes and this prepared body remain local until an authorized write environment can publish them. No workflow or protected file was edited.

