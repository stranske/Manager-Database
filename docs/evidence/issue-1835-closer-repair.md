# Issue #1835: closer acceptance evidence (repair of #1836)

Checked 2026-11-03 (America/Chicago). Refs
[#1835](https://github.com/stranske/Manager-Database/issues/1835). Repair of merged
[#1836](https://github.com/stranske/Manager-Database/pull/1836); production code
unchanged in this PR.

## Provenance

| Role | SHA / run | Notes |
| --- | --- | --- |
| PR #1836 merge on `main` | `7f6cc1af9c35cd412fd8b2eb780ce6f033b88ac1` | Adds `tests/test_coverage_round_1835.py` (eight tests) |
| Parent before merge | `475ba0e81908160a53275274ed2450143554ff2e` | First parent of merge commit |
| PR #1836 final Gate head | `d322683eaa5ec8a45ed34cab1dd8497a17fa5cd3` | Same tree as merge for coverage targets |
| Earlier repair | `98b63bd693cea338a9a24dca519ba62841db45cb` on `closer/issue-1835` | Squash-merged as `e6405b61d563597e8e675b255a7da6fa5a57f56d` (#1882); tree matches that main commit |
| This repair branch | `closer-r3/issue-1835` from main `e6405b61d563597e8e675b255a7da6fa5a57f56d` | Merged the earlier local branch to retain its history; evidence only |
| Test module SHA-256 | `bba5b9ed4c968df1818f5e6b3812f50d6121f9f33731a54984c515a3bc90714e` | Matches merge content on `main` |

Gate command (Python 3.13 job log, historical PR Gate and local reproduction):

```text
pytest -p ci_test_growth -m "not nightly" --junitxml=pytest-junit.xml --cov --cov-config=pyproject.toml --cov-report=xml:coverage.xml --cov-report=term-missing --cov-report=json:coverage.json -n auto --dist=loadgroup
```

Local comparable pair (same `-m "not nightly"`, branch coverage, `pyproject.toml`
scope; omits workflow-generated `ci_test_growth` only):

```text
pytest -m "not nightly" --cov --cov-config=pyproject.toml --cov-report=json:<path> -q
```

Coverage scope: `source = ["adapters", "api", "etl", "ui"]`, `branch = true`, omit
tests and `__pycache__`. Uncovered unit: `U = len(missing_lines) + len(missing_branches)`
per file in `coverage.json`.

Artifacts: `issue-1835-closer-repair/gate-baseline-475ba0e-coverage.json`,
`issue-1835-closer-repair/gate-head-7f6cc1a-coverage.json`.

## Gate coverage comparison (issue #1835 ranked targets)

| Target | Baseline U (ml+mb) | Head U (ml+mb) | Reduction |
| --- | ---: | ---: | ---: |
| `ui/alerts.py` | 60 (35+25) | 53 (32+21) | 7 |
| `etl/news_flow.py` | 47 (21+26) | 28 (9+19) | 19 |
| `api/alerts.py` | 45 (19+26) | 45 (19+26) | 0 |
| `etl/logging_setup.py` | 8 (7+1) | 2 (1+1) | 6 |
| `ui/search.py` | 11 (5+6) | 8 (3+5) | 3 |
| `api/data.py` | 4 (4+0) | 4 (4+0) | 0 |
| `api/handler.py` | 4 (3+1) | 0 (0+0) | 4 |
| `api/parser.py` | 6 (3+3) | 0 (0+0) | 6 |
| **Sum (listed targets)** | **185** | **140** | **45** |

**Measured reduction:** 45 uncovered statements+branches on the eight ranked targets
between parent `475ba0e` and merge `7f6cc1a` under matching local Gate marker/coverage
settings.

**Shortfall vs ~180 goal:** `max(0, 180 − 45) = 135`.

Historical PR Gate job log at `d322683` (Python 3.13) reports the same target rows for
the merge tree (`2443 passed, 26 skipped`). Parent push CI
[39221909097](https://github.com/stranske/Manager-Database/actions/runs/39221909097)
(Python 3.13) reported `2435 passed, 28 skipped` before the eight tests existed.

### Deferred / zero-credit targets

- **`api/alerts.py` (45 U unchanged):** tests exercise `_history_where` and
  `_normalize_actor` only; remaining misses stay in HTTP handlers, DB paths, and branch
  guards not hit by the focused module tests.
- **`api/data.py` (4 U unchanged):** the added test covers the upstream 502 path; the
  four remaining statement misses on lines 21–24 stay unreachable without a broader
  data-endpoint harness.
- **`ui/alerts.py` (partial):** inbox/main/error paths improved but Streamlit rule
  builder, acknowledge flows, and filter branches remain (53 U after merge).

## Target ranking

Issue
[#1835](https://github.com/stranske/Manager-Database/issues/1835) lists Orchestrator
ranking: escaped-defect priority, then churn, then uncovered mass. Ordered targets and
dispatch estimates (~185 U total): `ui/alerts.py` (60), `etl/news_flow.py` (47),
`api/alerts.py` (45), `etl/logging_setup.py` (8), `ui/search.py` (11), `api/data.py`
(4), `api/handler.py` (4), `api/parser.py` (6). #1836’s eight tests map to those files
in that priority order; measured whole-suite credit is partial (45 U, not ~180).

## Break-and-revert evidence (eight tests)

Acceptance outcome (check date 2026-11-03): eight individually mutated tests must
fail at their documented check, then pass after restoring the source; the complete
focused module must pass with no production or test changes remaining.

Stop rule: at most two pytest attempts per mutation, or 45 minutes of repair work;
stop at the first unresolved step. Each pytest invocation has a 110-second timeout.
The only test runs in this repair are the individual commands below and one final
focused-module run; no full suite or release/verification pipeline was run.

The earlier committed red/green captures were recovered from #1882, but lacked
exact mutation diffs, commands in each capture, and exit-status receipts. A bounded
search of local temporary captures found no more complete originals. This repair
reproduces each documented mutation in the isolated disposable checkout
`/private/tmp/manager-db-1835-mutation-checkout`, detached at main `e6405b6`.
The old trimmed logs are superseded by full fresh captures at the same paths;
their originals remain inspectable in [#1882](https://github.com/stranske/Manager-Database/pull/1882/files).
The coverage accounting, shortfall explanation, and eight tests are retained.
No prior source or test work was discarded.

Each mutation changes one production file in the disposable checkout only. Its
original bytes are restored before the green invocation, and all production paths
(`adapters`, `api`, `etl`, `ui`) are checked for a clean diff after every restore.
`PYTHONDONTWRITEBYTECODE=1` prevents stale bytecode during these runs. The exact
patches below can be replayed with `git apply <name>.diff` at the recorded base;
restore the affected file before running the identical command again.

Raw captures preserve combined stdout/stderr without trimming, with command,
checkout, base SHA, UTC start, duration, and actual exit status. The
[machine-readable receipts](issue-1835-closer-repair/mutation-run/receipts.json)
also map all eight nodes to their patches and logs, record original/restored source
SHA-256 values, and capture the final clean tracked state. Command template:

```text
PYTHONPATH=. /opt/anaconda3/bin/python3 -m pytest tests/test_coverage_round_1835.py::<node> -q --tb=short --disable-warnings
```

| Test | Exact production mutation | Named failure (exit 1) | Restored (exit 0) |
| --- | --- | --- | --- |
| `test_news_helpers_normalize_aliases_timestamps_and_sources` | [Return `None` for valid ISO input](issue-1835-closer-repair/mutation-run/news_helpers.diff) | [AssertionError: `None == datetime(...)`](issue-1835-closer-repair/mutation-run/news_helpers-red.txt) | [Pass](issue-1835-closer-repair/mutation-run/news_helpers-green.txt) |
| `test_news_watermark_fallback_and_update_paths` | [Wrong MAX timestamp](issue-1835-closer-repair/mutation-run/news_watermark.diff) | [AssertionError: `1970-01-01... == 2026-01-03...`](issue-1835-closer-repair/mutation-run/news_watermark-red.txt) | [Pass](issue-1835-closer-repair/mutation-run/news_watermark-green.txt) |
| `test_news_fallback_handles_missing_news_table` | [Return `"not-none"` on missing table](issue-1835-closer-repair/mutation-run/news_missing_table.diff) | [AssertionError: `'not-none' is None`](issue-1835-closer-repair/mutation-run/news_missing_table-red.txt) | [Pass](issue-1835-closer-repair/mutation-run/news_missing_table-green.txt) |
| `test_logging_cloudwatch_idempotence_and_emit_failure` | [Swallow CloudWatch emit errors](issue-1835-closer-repair/mutation-run/logging_cloudwatch.diff) | [AttributeError at `assert record.handled is True`](issue-1835-closer-repair/mutation-run/logging_cloudwatch-red.txt) | [Pass](issue-1835-closer-repair/mutation-run/logging_cloudwatch-green.txt) |
| `test_parser_handler_and_data_cover_success_and_failure_paths` | [Break successful parse](issue-1835-closer-repair/mutation-run/parser_handler_data.diff) | [AssertionError: `None == [1]`](issue-1835-closer-repair/mutation-run/parser_handler_data-red.txt) | [Pass](issue-1835-closer-repair/mutation-run/parser_handler_data-green.txt) |
| `test_alert_filter_helpers_cover_boolean_and_actor_boundaries` | [Skip actor strip](issue-1835-closer-repair/mutation-run/alert_filter.diff) | [AssertionError: `' ui ' == 'ui'`](issue-1835-closer-repair/mutation-run/alert_filter-red.txt) | [Pass](issue-1835-closer-repair/mutation-run/alert_filter-green.txt) |
| `test_search_main_handles_empty_and_renders_grouped_results` | [Change empty-state copy](issue-1835-closer-repair/mutation-run/search_main.diff) | [AssertionError on `empty_ui.infos`](issue-1835-closer-repair/mutation-run/search_main-red.txt) | [Pass](issue-1835-closer-repair/mutation-run/search_main-green.txt) |
| `test_alert_ui_main_and_inbox_error_paths` | [Change bulk-ack error copy](issue-1835-closer-repair/mutation-run/alert_ui.diff) | [AssertionError on `fake.errors`](issue-1835-closer-repair/mutation-run/alert_ui-red.txt) | [Pass](issue-1835-closer-repair/mutation-run/alert_ui-green.txt) |

The logging failure is an `AttributeError` while evaluating the named assertion,
not an `AssertionError`; the mutation removes the attribute set by `handleError`.
The remaining seven failures are assertion mismatches, not collection failures.

## Focused pytest

Historical capture: `issue-1835-closer-repair/focused-pytest.txt`. Fresh complete
module output and exit status:
[focused-pytest.txt](issue-1835-closer-repair/mutation-run/focused-pytest.txt).
The repository's configured `-q` combines with the requested `-q`, so pytest prints
eight dots and `[100%]` instead of a prose count; the receipt records exit 0.

The explicitly requested `/opt/anaconda3/bin/python3` is Python 3.12.2, while
`pyproject.toml` declares Python >=3.13. These are local receipts under that exact
interpreter, not a substitute for CI on the supported Python version. Historical
coverage results above were retained without rerunning whole-suite coverage.

## CI pytest citations

| Run | Role | Python job | Summary from log |
| --- | --- | --- | --- |
| [39229244125](https://github.com/stranske/Manager-Database/actions/runs/39229244125) / [job 117983127743](https://github.com/stranske/Manager-Database/actions/runs/39229244125/job/117983127743) | PR #1836 Gate | 3.13 | `2443 passed, 26 skipped, 359 warnings in 126.71s` |
| [39230296318](https://github.com/stranske/Manager-Database/actions/runs/39230296318) / [job 117986464896](https://github.com/stranske/Manager-Database/actions/runs/39230296318/job/117986464896) | Post-merge CI on `7f6cc1a` | 3.12 | `2443 passed, 28 skipped, 89 warnings in 93.65s` (3.13 job failed unrelated to these tests) |
| [41641679703](https://github.com/stranske/Manager-Database/actions/runs/41641679703) | Current `main` CI | 3.13 | `2588 passed, 28 skipped, 362 warnings in 136.28s` |

Repository workflows: `.github/workflows/ci.yml` (push CI) and `pr-00-gate.yml` →
`stranske/Workflows/.github/workflows/reusable-10-ci-python.yml@main`.
