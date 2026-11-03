# Issue #1842: closer acceptance evidence (repair of #1876)

Checked 2026-11-03. Refs [#1842](https://github.com/stranske/Manager-Database/issues/1842).
Repair PR [#1881](https://github.com/stranske/Manager-Database/pull/1881) continues
merged #1876; production code is unchanged.

## Operational outcome and bounds

Outcome: all five focused tests pass; each independently fails an assertion when its
production behavior is mutated, then passes after restoration. Command:

```sh
PYTHONPATH=. /opt/anaconda3/bin/python3 -m pytest tests/test_coverage_round_1842_followup.py -m 'not slow' -q
```

Observed output: `..... [100%]` (exit 0; five tests). Local interpreter is Python 3.12.2,
not hosted Python 3.13. Stop rule: two failed attempts at a required validation step or
45 minutes. No production exception handling changes. The offline guard uses a
`MagicMock` client constructor, asserts zero calls after return, and clears the
Streamlit cache before the call and in `finally`.

## Mutation receipts

Full commands and output for each broken/restored node are in PR #1881's body.
Every command uses the interpreter above, `PYTHONPATH=.`, and
`-m pytest tests/test_coverage_round_1842_followup.py::<node> -m 'not slow' -q --tb=short --disable-warnings`.

| Node (test_ prefix omitted) | Separate production mutation | Mutated | Restored |
| --- | --- | --- | --- |
| api_base_url_prefers_alerts_env | Bypass ALERTS_API_BASE_URL precedence | AssertionError: api.example != alerts.example; exit 1 | exit 0 |
| load_unacknowledged_alert_count_offline_mode | Remove UI_OFFLINE early return | AssertionError: client call count 1 != 0; exit 1 | exit 0 |
| render_active_campaigns_widget_empty_snapshot | Replace empty-snapshot caption | AssertionError: caption list differs; exit 1 | exit 0 |
| chat_normalizers_and_chain_context_name_resolution | Return None at start of _parse_date_range | AssertionError: None != valid date tuple; exit 1 | exit 0 |
| universe_schema_quality_flags_on_partial_legacy_sqlite_table | Omit quality_flags ALTER TABLE in _ensure_universe_schema | AssertionError: quality_flags absent; exit 1 | exit 0 |

An initial schema mutation hit the earlier `_ensure_manager_table` helper and passed.
That was not evidence for this test: it stubs that helper. The corrected mutation above
hits `_ensure_universe_schema` specifically and fails at the expected assertion.

## Comparable Gate coverage

U = missing statements + missing branch arcs, on the four issue targets only.
Both full-suite runs use Python 3.13 with branch coverage enabled.

- Before #1876: final #1843 head `65cfea696bf49100f8c8c6aea222661b15fb37bf`,
  [Gate 39373355227, Python 3.13 job 118466002079](https://github.com/stranske/Manager-Database/actions/runs/39373355227/job/118466002079).
  Retained job log: **2494 passed, 26 skipped**, 140.73 seconds.
- After #1876 and the offline guard repair: `b705ec6b6b57924d2359366c2884b84ded365bba`,
  [Gate 41629651073, Python 3.13 job 125895911705](https://github.com/stranske/Manager-Database/actions/runs/41629651073/job/125895911705).
  **2588 passed, 26 skipped**, 144.40 seconds. Python 3.14 also passed.
  Artifact `gate-coverage-3.13-1` (13226519529), `runtimes/3.13/coverage.json`,
  metadata `branch_coverage: true`, coverage.py 7.17.0.

| Target | Before U (ml+mb) | After U (ml+mb) | Reduction |
| --- | ---: | ---: | ---: |
| etl/daily_diff_flow.py | 2 (1+1) | 2 (1+1) | 0 |
| api/managers.py | 13 (0+13) | 13 (0+13) | 0 |
| api/chat.py | 4 (0+4) | 4 (0+4) | 0 |
| ui/dashboard.py | 31 (19+12) | 31 (19+12) | 0 |
| **Sum** | **50** | **50** | **0** |

The old baseline artifact 12242174268 has expired (HTTP 410). Counts are reconstructed
from its retained branch-enabled Gate terminal report below: all four source files are
byte-identical between baseline and repair head (`git diff 65cfea69 HEAD -- <four targets>`
is empty); their complete missing-line/arc reports are identical between the two jobs.
The head JSON confirms the missing-arc counts above, with no completely unexecuted
branch source lines. `BrPart` alone is not generally a missing-arc count; the identical
source and complete missing reports establish comparability here.

Baseline terminal report (head job prints identical target rows):

```text
Name                             Stmts   Miss Branch BrPart  Cover   Missing
api/chat.py                        917      0    262      4    99%   519->523, 825->839, 884->886, 1354->1356
api/managers.py                    826      0    300     13    99%   51->55, 1102->1104, 1247->1249, 1367->1434, 1428->1434, 1434->1449, 1446->1449, 1527->1530, 1552->1555, 1643->1645, 1793->1796, 1863->1866, 1900->1902
etl/daily_diff_flow.py             183      1     54      1    99%   440
ui/dashboard.py                    634     19    166     12    96%   329->333, 369->376, 373-374, 389-390, 1014->1021, 1067-1075, 1088->1093, 1097->1099, 1102->1064, 1117-1118, 1126, 1211-1219, 1234, 1264, 1271
```

**Shortfall:** 180 - 0 = **180 U** against the approximately-180-unit goal when measured
from the final #1843 baseline to this follow-up. The five tests are discriminating
regressions, but add no measured whole-suite coverage on these targets.
The former 49 U reduction / 71 U shortfall claims are withdrawn: they credited an
intermediate #1843 revision and mixed Python 3.12 and 3.13. Historical dispatch counts
are not substituted for a comparable baseline. No additional quota work is included.

## Ranking and target-specific deferrals

Ranking is the issue's escaped-defect priority, then churn, then uncovered mass;
[#1842](https://github.com/stranske/Manager-Database/issues/1842) orders:
`etl/daily_diff_flow.py` (churn 4, issue-listed 5 U), `api/managers.py` (churn 3, 16 U),
`api/chat.py` (churn 3, 14 U), `ui/dashboard.py` (churn 2, 120 U).
The issue lists 155 U total; the old Python 3.12 PR evidence instead counted daily_diff
as 5 lines + 4 arcs = 9 U (159 total). Neither is this repair's comparable baseline.

- **etl/daily_diff_flow.py:** remaining line 440 / arc 439->440 launches the Prefect
  flow under `__main__`. Deferred because exercising the live launcher is outside this
  focused helper-test repair; no new daily-diff test or quota is claimed.
- **api/managers.py:** remaining 13 arcs include optional Postgres import fallback and
  endpoint connection/finally/bulk-import paths (`create_manager`, `list_managers`,
  `bulk_import_managers`, `import_manager_universe`, stats/get/patch/delete).
  Forcing these requires separate dependency/connection scenarios; this repair only
  verifies the existing partial SQLite migration regression.
- **api/chat.py:** four remaining arcs are in `_build_chat_client_info`,
  `_format_filing_summary_payload`, `_response_id_from_trace_url`, and
  `_run_dependency_check`. These need additional payload/dependency fixtures beyond
  the requested date-normalizer and lookup mutation; deferred to a separate chunk.
- **ui/dashboard.py:** remaining 19 lines / 12 arcs are QC loaders, manager-summary
  loaders/renderers, authenticated dashboard/main paths and the app entry point.
  Streamlit/session/DB fixtures and launcher execution would broaden scope; deferred.
  Existing offline, URL precedence and empty-caption regressions are verified here.
