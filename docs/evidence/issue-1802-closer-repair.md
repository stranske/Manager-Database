# Issue #1802: closer acceptance evidence (repair of #1805)

Checked 2026-11-03 (America/Chicago). Refs
[#1802](https://github.com/stranske/Manager-Database/issues/1802). Repair of merged
[#1805](https://github.com/stranske/Manager-Database/pull/1805); production code
unchanged in this PR.

## Prior local branch

`closer/issue-1802` pointed at stale history (`e5830db`) diverged from current
`main` and carried no #1802 closer evidence beyond the already-merged #1805 test
module. **Kept:** nothing from that branch. **Discarded:** the stale branch tip
(unrelated intermediate work; not merged).

## Provenance

| Role | SHA / run | Notes |
| --- | --- | --- |
| PR #1805 merge on `main` | `04855e5565fa94142b12ba5ebb2850c3d906bf6b` | Adds `tests/test_issue_1802_coverage.py` (seven tests) |
| Parent before merge | `95827f9a9564193af5cd2b3eec70c0bfac2ac542` | Pre-#1805 tree |
| Evidence base | `e6405b61d563597e8e675b255a7da6fa5a57f56d` | Current `main` at repair time |
| Test module SHA-256 | `d8c706ddedabce56ee2330d427f3de9fc762011807d6731c83646ee681e1c830` | Matches merge content on `main` |

Gate command (from `.github/workflows/pr-00-gate.yml` → `reusable-10-ci-python.yml`,
Python 3.13 job log pattern):

```text
pytest -p ci_test_growth -m "not nightly" --junitxml=pytest-junit.xml --cov --cov-config=pyproject.toml --cov-report=xml:coverage.xml --cov-report=term-missing --cov-report=json:coverage.json -n auto --dist=loadgroup
```

Local comparable pair (same `-m "not nightly"`, `pyproject.toml` scope; omits
workflow-generated `ci_test_growth` and xdist only):

```text
PYTHONPATH=. pytest -m "not nightly" --cov --cov-config=pyproject.toml --cov-report=json:<path> -q
```

Baseline excludes only `tests/test_issue_1802_coverage.py`; final includes all
tests. Interpreter: **Python 3.12.2** locally (CI Gate uses 3.13/3.14); target
`U` deltas are from identical local settings on the same tree.

Coverage scope: `source = ["adapters", "api", "etl", "ui"]`, `branch = true`.
Uncovered unit: `U = len(missing_lines) + len(missing_branches)` per file in
`coverage.json`.

Artifacts:

- `issue-1802-closer-repair/gate-baseline-exclude-1802-module-coverage.json`
- `issue-1802-closer-repair/gate-final-with-1802-module-coverage.json`
- `issue-1802-closer-repair/coverage-baseline-log.txt`, `coverage-final-log.txt`

## Gate coverage comparison (#1802 ranked targets)

| Target | Baseline U (ml+mb) | Final U (ml+mb) | Reduction |
| --- | ---: | ---: | ---: |
| `etl/digest_flow.py` | 13 (8+5) | 3 (1+2) | 10 |
| `ui/daily_report.py` | 100 (69+31) | 35 (20+15) | 65 |
| `etl/ingest_flow.py` | 108 (61+47) | 97 (55+42) | 11 |
| **Sum (listed targets)** | **221** | **135** | **86** |

**Measured incremental reduction:** 86 uncovered statements+branches on the three
ranked targets attributable to `tests/test_issue_1802_coverage.py` under matching
local Gate marker/coverage settings (baseline suite minus that module vs full suite).

**Shortfall vs ~180 goal:** `max(0, 180 − 86) = 94`.

### Meaningful coverage limits (remaining U)

- **`etl/ingest_flow.py` (97 U after final):** the seven tests exercise helpers,
  SQLite authority/supersede paths, and the injectable `fetcher` loop; remaining
  misses stay in live adapter I/O, Postgres transactions, and default fetcher
  branches not hit without broader ETL harnesses.
- **`ui/daily_report.py` (35 U after final):** Streamlit `main()` is partially
  mocked; remaining misses are layout branches, download paths, and DB-backed
  loaders not fully exercised.
- **`etl/digest_flow.py` (3 U after final):** near-complete for ranked surface;
  residual branches are defensive/network edge paths.

## Break-and-revert evidence (seven tests)

Command template:

```text
PYTHONPATH=. /opt/anaconda3/bin/python3 -m pytest tests/test_issue_1802_coverage.py::<node> -q --tb=short --disable-warnings
```

Raw captures under `issue-1802-closer-repair/mutation-run/`.

| Test | Production mutation | Broken assertion (exit ≠ 0) | Restored (exit 0) |
| --- | --- | --- | --- |
| `test_digest_rendering_email_and_csv_helpers_cover_empty_and_populated_paths` | Replace news headline with `BROKEN` in `render_digest_plain_text` | `assert "B: News" in ...` | `digest_rendering-green.txt` |
| `test_digest_delivery_covers_sendgrid_statuses_smtp_and_flow_artifact` | Accept only HTTP 200 from SendGrid | `assert ...success is True` on 202 | `digest_delivery-green.txt` |
| `test_daily_report_helpers_and_missing_source_paths` | Return `["broken"]` for `parse_topics(None)` | `parse_topics(None) == []` | `daily_helpers-green.txt` |
| `test_daily_report_dates_skip_absent_tables_and_choose_nearest` | Use `min(candidates)` in `latest_report_date` | `latest_report_date() == date(2026, 1, 4)` | `daily_dates-green.txt` |
| `test_daily_report_main_covers_populated_secondary_tabs` | Early `return` in `main()` | `assert any("Linked Headlines" in ...)` | `daily_main-green.txt` |
| `test_ingest_helpers_cover_authority_hashes_and_canonical_holdings` | Return `1` for invalid `MAX_RESULTS_IN_MEMORY` | `== 100000` | `ingest_helpers-green.txt` |
| `test_ingest_flow_collects_rows_handles_per_identifier_failures_and_rejects_bad_fetcher` | Write `[]` to `parsed.json` | `read_text() == '[{"identifier": "ok"}]'` | `ingest_flow-green.txt` |

## Focused pytest

See `issue-1802-closer-repair/focused-pytest.txt` (`7 passed`).

## CI pytest (main)

| Run | Role | Python 3.13 summary |
| --- | --- | --- |
| [41658526647](https://github.com/stranske/Manager-Database/actions/runs/41658526647) | CI on `e6405b6` | `2588 passed, 28 skipped, 362 warnings in 91.74s` |

#1805 merge commit `04855e5` delivered the test module; post-merge CI on current
`main` includes these seven tests in the Gate/`not nightly` suite.
