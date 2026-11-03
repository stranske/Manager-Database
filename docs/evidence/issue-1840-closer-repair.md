# Issue #1840: closer acceptance evidence (repair of #1875)

Checked 2026-11-03 (America/Chicago). Refs
[#1840](https://github.com/stranske/Manager-Database/issues/1840). Repair of merged
[#1875](https://github.com/stranske/Manager-Database/pull/1875); production code
unchanged in this PR.

## Prior local branches

- `closer-r1/issue-1840` and `closer-r2/issue-1840` both pointed at current `main`
  (`e6405b6`); no extra commits were merged or kept.
- No partial closer work was discarded beyond that stale branch name.

## Provenance

| Role | SHA / run | Notes |
| --- | --- | --- |
| PR #1875 merge on `main` | `3a587e9cb9b6bd5d89fe1c6e1cccf246af788f83` | Adds `tests/test_chat_coverage_round_1840.py` (41 tests) |
| Parent before merge | `987164d6c13544f4a31060e66f21b5debda8d38f` | First parent of merge commit |
| PR #1875 Gate (Python 3.13) | [41245423372](https://github.com/stranske/Manager-Database/actions/runs/41245423372) / [job 124667852176](https://github.com/stranske/Manager-Database/actions/runs/41245423372/job/124667852176) | `2583 passed, 26 skipped` |
| Test module SHA-256 | `cfe2f9d7e5396981a3a121aaf1aa5b9593294989c3095ddee9291d0d681ea62b` | Matches merge content on `main` |

Gate command (Python 3.13 job log, PR #1875 Gate and local reproduction):

```text
pytest -p ci_test_growth -m "not nightly" --junitxml=pytest-junit.xml --cov --cov-config=pyproject.toml --cov-report=xml:coverage.xml --cov-report=term-missing --cov-report=json:coverage.json -n auto --dist=loadgroup
```

Local comparable pair (same `-m "not nightly"`, branch coverage, `pyproject.toml`
scope; omits workflow-generated `ci_test_growth` only):

```text
PYTHONPATH=. /opt/anaconda3/bin/python3 -m pytest -m "not nightly" --cov --cov-config=pyproject.toml --cov-report=json:<path> -q
```

Coverage scope: `source = ["adapters", "api", "etl", "ui"]`, `branch = true`, omit
tests and `__pycache__`. Uncovered unit: `U = len(missing_lines) + len(missing_branches)`
per file in `coverage.json`.

Artifacts:

- `issue-1840-closer-repair/gate-baseline-987164d-coverage.json` (local, Python 3.12.2)
- `issue-1840-closer-repair/gate-head-3a587e9-coverage.json` (Gate artifact
  `gate-coverage-3.13-1` from run 41245423372, coverage.py 7.17.0)

Local baseline run at `987164d` exited non-zero because
`tests/test_chat_api.py::test_auto_filing_summary_without_filing_id_falls_back_to_rag_search`
failed once on this machine; `coverage.json` was still emitted. Head counts use the
hosted Python 3.13 artifact only.

## Collected tests (41)

All nodes in `tests/test_chat_coverage_round_1840.py`:

1. `test_resolve_chain_name_auto_filing_requires_context_filing_id`
2. `test_context_filter_requested_detects_empty_and_non_empty_values`
3. `test_normalize_sources_and_extract_chain_payload_object_paths`
4. `test_get_health_executor_recreates_after_shutdown`
5. `test_record_chat_fleet_safe_swallows_observability_failures`
6. `test_run_chain_rag_campaign_snapshot_unavailable`
7. `test_run_chain_rejects_invalid_holdings_as_of`
8. `test_run_chain_requires_filing_id_for_filing_summary`
9. `test_run_chain_awaits_coroutine_results`
10. `test_build_chain_uses_fallback_after_generic_import_error`
11. `test_chat_chain_health_when_llm_zone_disabled`
12. `test_enforce_chat_rate_limit_raises_when_budget_exceeded`
13. `test_direct_chat_wrappers_delegate_to_chat_api`
14. `test_resolve_chain_name_falls_back_when_classifier_returns_unknown`
15. `test_enforce_chat_rate_limit_session_budget_after_ip_passes`
16. `test_run_chain_holdings_analysis_formats_payload_with_as_of`
17. `test_run_chain_rejects_non_string_holdings_as_of`
18. `test_run_chain_uses_invoke_when_run_missing`
19. `test_run_chain_raises_when_chain_lacks_run_and_invoke`
20. `test_run_chain_filing_summary_formats_structured_payload`
21. `test_chat_api_disabled_zone_returns_notice`
22. `test_chat_api_requires_llm_client`
23. `test_chat_api_success_path_records_fleet`
24. `test_chat_api_maps_chain_unavailable_to_503`
25. `test_chat_api_maps_unexpected_errors_to_500`
26. `test_submit_feedback_persists_and_forwards`
27. `test_build_chain_instantiates_imported_classes`
28. `test_rate_limiter_clear_resets_window`
29. `test_parse_date_range_and_empty_manager_name_edges`
30. `test_fallback_filing_summary_chain_optional_filing_id`
31. `test_build_chain_unavailable_without_fallback_and_ctor_closes_db`
32. `test_build_chat_client_info_calls_callable_builder`
33. `test_manager_id_column_falls_back_when_helper_returns_none`
34. `test_chat_chain_health_payload_import_success_and_failure`
35. `test_run_dependency_check_disabled_and_success_paths`
36. `test_run_health_summary_checks_success_and_timeout`
37. `test_chat_api_prompt_injection_joins_list_reasons`
38. `test_attach_langsmith_feedback_success_path`
39. `test_liveness_aliases_and_shutdown_hooks`
40. `test_configure_default_executor_and_memory_profiler_shutdown`
41. `test_health_app_marks_unhealthy_dependencies`

## Gate coverage comparison (`api/chat.py` only)

| Target | Baseline U (ml+mb) | Head U (ml+mb) | Reduction |
| --- | ---: | ---: | ---: |
| `api/chat.py` | 4 (0+4) | 4 (0+4) | 0 |

**Measured reduction:** 0 uncovered statements+branches on `api/chat.py` between parent
`987164d` and merge `3a587e9` under comparable Gate marker/coverage settings.

**Shortfall vs ~180 goal:** `max(0, 180 − 0) = 180`.

Whole-repo blended coverage (same scope): baseline **90.24%**, head **90.27%** (+0.03
points). The issue dispatch listed **97 U** on `api/chat.py`; at the pre-merge parent
that file already had **0** missing statements and **4** missing branch arcs in this
comparable pair—other suites had already exercised most chat paths before #1875 landed.

### 93% vs 100% (and 99% terminal rows)

- **100% statements:** `api/chat.py` has 917/917 statements executed in both baseline
  and head JSON (`missing_lines: []`).
- **Not 100% branches:** four partial arcs remain: `519->523`, `825->839`, `884->886`,
  `1354->1356` (optional client builder path, filing/holdings formatter edge branches,
  dependency-check disabled/enabled split). Gate’s `term-missing` row therefore shows
  **99%** on the file, not 100%.
- **~90% whole-repo figure:** blended statement+branch coverage across
  `adapters`/`api`/`etl`/`ui` stays near **90%**, below the fleet **90%** initiative
  target, because large uncovered mass remains in other modules—not because #1875 failed
  to add tests, but because those tests largely re-cover paths already hit elsewhere at
  whole-suite scope.

### Targets that could not meaningfully add whole-suite credit here

The four remaining `api/chat.py` arcs need focused fixtures (registry client builder
failure, empty formatter sub-branches, health dependency `enabled=False` vs breaker
open) without widening into live LLM/registry integration; this repair documents rather
than expanding scope to chase the ~180 U quota on an already statement-complete file.

## Target ranking (why `api/chat.py`)

Issue
[#1840](https://github.com/stranske/Manager-Database/issues/1840) autopilot round 12
(lane B) ranked a **single** target using Orchestrator order (escaped-defect priority,
then churn, then uncovered mass):

| Rank | Target | Issue-listed U | Churn |
| ---: | --- | ---: | ---: |
| 1 | `api/chat.py` | 97 | 3 |

No other files were in scope for this round. The dispatch **97 U** figure came from an
older blended CI snapshot; the comparable pre-merge parent in this repair already
measured **4 U** on the same file.

## Break-and-revert evidence (41 tests)

Driver: `issue-1840-closer-repair/run_mutations.py` (semantic `api/chat.py` edits,
single-node pytest, restore). Raw logs: `issue-1840-closer-repair/mutation-run/<test>-red.txt`
and `-green.txt`. Summary: `mutation-run/summary.txt` (`all 41 mutations ok`).

Per-node command template:

```text
PYTHONPATH=. /opt/anaconda3/bin/python3 -m pytest tests/test_chat_coverage_round_1840.py::<node> -q --tb=short --disable-warnings
```

Each red log ends with `FAILED` and an `AssertionError`, `HTTPException`, or
`RuntimeError` on the behavior under test; each green log restores exit 0.

## Focused pytest (current `main`)

See `issue-1840-closer-repair/focused-pytest.txt` (`41 passed`).

## CI pytest citation (#1875)

| Run | Role | Python job | Summary from log |
| --- | --- | --- | --- |
| [41245423372](https://github.com/stranske/Manager-Database/actions/runs/41245423372) / [124667852176](https://github.com/stranske/Manager-Database/actions/runs/41245423372/job/124667852176) | PR #1875 Gate | 3.13 | `2583 passed, 26 skipped, 362 warnings in 134.67s` |

Repository workflows: `.github/workflows/ci.yml` and `pr-00-gate.yml` →
`stranske/Workflows/.github/workflows/reusable-10-ci-python.yml@main`.
