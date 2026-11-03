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
| This repair branch | `closer/issue-1835` from current `main` | Evidence + receipts only |
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

Raw captures under `issue-1835-closer-repair/mutation-run/`. Command template:

```text
PYTHONPATH=. /opt/anaconda3/bin/python3 -m pytest tests/test_coverage_round_1835.py::<node> -q --tb=short --disable-warnings
```

| Test | Production mutation | Broken assertion (exit 1) | Restored (exit 0) |
| --- | --- | --- | --- |
| `test_news_helpers_normalize_aliases_timestamps_and_sources` | Force `_parse_iso_timestamp` to return `None` for valid ISO input | `_parse_iso_timestamp("2026-01-01T00:00:00Z")` | `news_helpers-green.txt` |
| `test_news_watermark_fallback_and_update_paths` | Wrong MAX timestamp from `_fallback_source_since_from_news` | `_fallback_source_since_from_news(...) == "2026-01-03..."` | `news_watermark-green.txt` |
| `test_news_fallback_handles_missing_news_table` | Return `"not-none"` on missing table | `... is None` | `news_missing_table-green.txt` |
| `test_logging_cloudwatch_idempotence_and_emit_failure` | Swallow CloudWatch `emit` errors | `record.handled is True` | `logging_cloudwatch-green.txt` |
| `test_parser_handler_and_data_cover_success_and_failure_paths` | Break successful `parse_response` | `.data == [1]` | `parser_handler_data-green.txt` |
| `test_alert_filter_helpers_cover_boolean_and_actor_boundaries` | Skip strip in `_normalize_actor` | `_normalize_actor(" ui ") == "ui"` | `alert_filter-green.txt` |
| `test_search_main_handles_empty_and_renders_grouped_results` | Change empty-state copy | `infos == ["No results found."]` | `search_main-green.txt` |
| `test_alert_ui_main_and_inbox_error_paths` | Change bulk-ack error string | `errors == ["Unable to acknowledge alerts: nope"]` | `alert_ui-green.txt` |

## Focused pytest (current `main`)

See `issue-1835-closer-repair/focused-pytest.txt` (`8 passed`).

## CI pytest citations

| Run | Role | Python job | Summary from log |
| --- | --- | --- | --- |
| [39229244125](https://github.com/stranske/Manager-Database/actions/runs/39229244125) / [job 117983127743](https://github.com/stranske/Manager-Database/actions/runs/39229244125/job/117983127743) | PR #1836 Gate | 3.13 | `2443 passed, 26 skipped, 359 warnings in 126.71s` |
| [39230296318](https://github.com/stranske/Manager-Database/actions/runs/39230296318) / [job 117986464896](https://github.com/stranske/Manager-Database/actions/runs/39230296318/job/117986464896) | Post-merge CI on `7f6cc1a` | 3.12 | `2443 passed, 28 skipped, 89 warnings in 93.65s` (3.13 job failed unrelated to these tests) |
| [41641679703](https://github.com/stranske/Manager-Database/actions/runs/41641679703) | Current `main` CI | 3.13 | `2588 passed, 28 skipped, 362 warnings in 136.28s` |

Repository workflows: `.github/workflows/ci.yml` (push CI) and `pr-00-gate.yml` →
`stranske/Workflows/.github/workflows/reusable-10-ci-python.yml@main`.
