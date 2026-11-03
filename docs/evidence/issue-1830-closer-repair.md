# Issue #1830: closer acceptance evidence (repair of #1874)

Checked 2026-11-03 (America/Chicago). Refs
[#1830](https://github.com/stranske/Manager-Database/issues/1830). Repair of merged
[#1874](https://github.com/stranske/Manager-Database/pull/1874); production code
unchanged in this PR.

## Provenance

| Role | SHA / run | Notes |
| --- | --- | --- |
| PR #1874 merge on `main` | `6f728a470fa0c1968a391cd00cdd083a6a448735` | Adds four tests in `tests/test_issue_1830_coverage.py` |
| Immediate pre-#1874 parent | `38844a0441a16693d212ee6c779b227b2ae948dd` | No Gate run artifact at this exact SHA |
| Retrieved Gate baseline (proxy) | [41091476402](https://github.com/stranske/Manager-Database/actions/runs/41091476402), head `e198f65` | Last successful Gate on PR #1873 branch immediately before #1874; closest downloadable Gate pair |
| Retrieved Gate head (#1874) | [41111923417](https://github.com/stranske/Manager-Database/actions/runs/41111923417), head `473e6bc` | Final successful Gate on `cursor/issue-1830-coverage-round8` |
| This repair branch | `closer/issue-1830` from current `main` | Evidence + mutation receipts only |

Gate command (Python 3.13 job log, both runs):

```text
pytest -p ci_test_growth -m "not nightly" --junitxml=pytest-junit.xml --cov --cov-config=pyproject.toml --cov-report=xml:coverage.xml --cov-report=term-missing --cov-report=json:coverage.json -n auto --dist=loadgroup
```

Coverage scope: `pyproject.toml` `source = ["adapters", "api", "etl", "ui"]`, `branch = true`,
omit `["*/tests/*", "*/__pycache__/*"]`.

Uncovered unit definition: `U = missing_lines + missing_branches` per file summary in
`coverage.json`.

## Gate coverage comparison (listed #1830 targets)

Artifacts: `gate-baseline-pr1873-e198f65-coverage-3.13.json`,
`gate-head-pr1874-473e6bc-coverage-3.13.json`.

| Target | Baseline U (ml+mb) | Head U (ml+mb) | Reduction |
| --- | ---: | ---: | ---: |
| `adapters/edgar.py` | 24 (11+13) | 24 (11+13) | 0 |
| `api/activism.py` | 22 (13+9) | 16 (10+6) | 6 |
| `etl/activism_flow.py` | 32 (19+13) | 32 (19+13) | 0 |
| `api/_compat.py` | 12 (7+5) | 2 (1+1) | 10 |
| `etl/summariser_flow.py` | 7 (5+2) | 7 (5+2) | 0 |
| `adapters/mas.py` | 0 | 0 | 0 |
| `ui/launch.py` | 2 (1+1) | 2 (1+1) | 0 |
| `adapters/uk.py` | 124 (75+49) | 124 (75+49) | 0 |
| **Sum (listed targets)** | **223** | **207** | **16** |

**Measured Gate reduction:** 16 uncovered statements+branches across the issue’s ranked
target list between the retrieved baseline and head Gate artifacts.

**Shortfall vs ~180 goal:** `max(0, 180 − 16) = 164`.

Gate pytest summaries from job logs:

| Run | Python 3.13 summary |
| --- | --- |
| Baseline 41091476402 | `2538 passed, 26 skipped, 342 warnings in 138.98s` |
| Head 41111923417 | `2542 passed, 26 skipped, 362 warnings in 140.56s` |

Head Gate overall coverage (index): **92.68%** (`gate-head-pr1874-index.ndjson`).

### Deferred targets (why ~180 was not reached)

- **`adapters/uk.py` (124 U):** ranked last as uncovered mass; not attempted in #1874
  (external/registry-heavy surface; issue allows deferral with explanation).
- **`adapters/edgar.py` / `etl/activism_flow.py`:** #1874 tests exercise retry,
  helpers, and SQLite bootstrap paths, but whole-Gate `U` for these files did not
  move between the retrieved baseline and head (remaining branches/lines stay in live
  SEC/network and Postgres paths not hit by the added tests).
- **`etl/summariser_flow.py`, `adapters/mas.py`, `ui/launch.py`:** earlier #1831
  tranche already covered the small ranked contracts; Gate `U` unchanged in #1874.

## Relationship to repository coverage-gap ranking

Issue #1830’s ordered list (escaped-defect priority, then churn, then uncovered mass)
is the selection contract:

1. `adapters/edgar.py` (58 U at dispatch estimate)
2. `api/activism.py` (40)
3. `etl/activism_flow.py` (34)
4. `api/_compat.py` (12)
5. `etl/summariser_flow.py` (7)
6. `adapters/mas.py` (1)
7. `ui/launch.py` (2)
8. `adapters/uk.py` (124)

#1874’s four added tests align with ranks 1–4 (activism query/helpers, offline
`_compat`, activism_flow idempotency/index, EDGAR retry). Measured Gate credit
concentrates on **`api/_compat.py`** (−10 U) and **`api/activism.py`** (−6 U) in
the retrieved pair; the chunk remains partial versus the ~180-unit round goal.

## Break-and-revert evidence (#1874’s four added tests)

Raw console captures under `mutation-run/`.

| Test | Deliberate production break | Broken run | Restored run |
| --- | --- | --- | --- |
| `test_offline_api_imports_require_ui_offline_flag` | Disable `UI_OFFLINE` guard in `api/_compat.py` | `offline-red.txt` — `Failed: DID NOT RAISE ModuleNotFoundError` | `offline-green.txt` — 1 passed |
| `test_activism_date_helpers_and_filtered_queries` | Drop `upper()` on activism filing CUSIP filter | `activism-filter-red.txt` — `assert [] == [1]` on mixed-case CUSIP | `activism-filter-green.txt` — 1 passed |
| `test_activism_flow_ensure_sqlite_table_is_idempotent` | Skip creating `idx_activism_date` on SQLite bootstrap | `activism-idempotent-red.txt` — `assert 'idx_activism_date' in names` | `activism-idempotent-green.txt` — 1 passed |
| `test_edgar_request_with_retry_succeeds_after_transient_error` | Set `max_retries` default to `1` | `edgar-retry-red.txt` — `RequestError` after one attempt | `edgar-retry-green.txt` — 1 passed |

Recovered from [#1874 PR body](https://github.com/stranske/Manager-Database/pull/1874)
(alternate mutation on the same test): breaking `activism._to_float` to always return
`None` failed on `assert match.ownership_pct == 6.5`; revert passed. The CUSIP-filter
mutation above is the independently re-run proof retained in `activism-filter-*.txt`.

## Focused module validation (current `main` test file)

```text
$ /opt/anaconda3/bin/python3 -m pytest -q tests/test_issue_1830_coverage.py
8 passed (see focused-pytest.txt)
```
