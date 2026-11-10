Refs #1888. Evidence reconciliation checked **2026-11-10**.

PR #1889 merged at **2026-11-10T15:50:24Z**, commit `48ef1e750fbbf6fafe9938b094a3f1ceed579ae8`.
Its behavior tests, digest/escaping assertions, separated search guards and all mutation files are preserved.
This replaces historical acceptance/publication claims; it does not declare issue #1888 complete.

- [x] Per-function mutation commands and red/green excerpts retained below, with commit-pinned complete logs.
- [x] Comparable CI coverage accounting and all six target shortfalls verified below.
- [x] Stale review-blocker claims replaced by verified merged state and finding dispositions.
- [ ] Approximately 180 units of CI coverage reduction: **unmet**. Verified reduction is **116**; no owner-approved scope revision is recorded.

Exact final-head Gate [42842046706](https://github.com/stranske/Manager-Database/actions/runs/42842046706)
succeeded at `bcddbdcc113a6330b9c2aef9af1fd18ce2b8576f`. Python 3.14 job
[129919965700](https://github.com/stranske/Manager-Database/actions/runs/42842046706/job/129919965700)
and Python 3.13 job
[129919965756](https://github.com/stranske/Manager-Database/actions/runs/42842046706/job/129919965756)
each report `Pytest (unit tests with coverage): success`. This confirms pytest at the merged PR's final head;
it is distinct from the earlier candidate used for the coverage comparison below.

## Artifact-bound coverage

Both `gate-coverage-3.13-1` artifacts downloaded on attempt **1 of 2**, within ten minutes.
Commands (both exited **0**, no stdout):

```sh
gh run download 41702885246 --repo stranske/Manager-Database --name gate-coverage-3.13-1 --dir /tmp/issue1888-repair-baseline
gh run download 41839886184 --repo stranske/Manager-Database --name gate-coverage-3.13-1 --dir /tmp/issue1888-repair-candidate
```

- Baseline [run 41702885246](https://github.com/stranske/Manager-Database/actions/runs/41702885246), head `a7f4e626bb4b8359a64c56a56211218e41b6020a`; artifact **13265760076**.
- Candidate [run 41839886184](https://github.com/stranske/Manager-Database/actions/runs/41839886184), head `64fcbc71d7fc93c292aff58a57d19d63b2de2b98`; artifact **13329305286**.

Baseline API-reported archive digest: `sha256:0ad415416368e7718da8435c0c541d7ea66aa8e02f70812701b83032b193475a`.
Downloaded `runtimes/3.13/coverage.json` SHA-256: `29b76f2943425e5ccc881f86053c8f6f9f83a290571c6f905dd1bc333db0841a`.

Candidate API-reported archive digest: `sha256:92bb75a418efaba2c7a89aeae813f9eac4a950170e456b9b1ad8e4ad5fda583b`.
Downloaded `runtimes/3.13/coverage.json` SHA-256: `347361e8ca673e33696d90a20c67847f373fe23f1a9390554121ab7a79fcf4d6`.

Archive digests above are GitHub metadata, not independently rehashed ZIPs. The downloaded JSON was parsed directly.
Both reports use coverage.py **7.17.0**, branch coverage, Python **3.13**, and the same **56 source-file keys**.
The six target blobs and `pyproject.toml` are identical at the two run SHAs. Statement and branch denominators match per target.
No Python 3.14 artifact was retrieved in this repair; cross-version coverage equality is not asserted.

Unit = **missing statements + missing branch arcs**; each cell below keeps those components separate.
Priority is the recorded 500-source-commit repair-subject proxy / touches from
[ranking.json](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/ranking.json), not a verified escaped-defect count. Its digest/ASIC nulls are filled here from retrieved JSON. Its stored totals 130 and 117 are arithmetic errors: the four rows sum to 129 and 116.

| Target | Proxy/touches | Baseline statements+arcs | Candidate statements+arcs | Reduction |
| --- | ---: | ---: | ---: | ---: |
| `etl/edgar_flow.py` | 19/41 | 15+14=29 | 2+1=3 | 26 |
| `ui/daily_report.py` | 4/14 | 20+15=35 | 1+1=2 | 33 |
| `api/signals.py` | 3/10 | 9+4=13 | 2+0=2 | 11 |
| `api/search.py` | 2/9 | 25+27=52 | 4+2=6 | 46 |
| `etl/digest_flow.py` | 0/4 | 1+2=3 | 1+2=3 | 0 |
| `adapters/asic.py` | 0/2 | 0+1=1 | 0+1=1 | 0 |
| **Four original targets** | | **69+60=129** | **9+4=13** | **116** |
| **All six ranked targets** | | **70+63=133** | **10+7=17** | **116** |

The four-target maximum possible reduction is 129; the six-target maximum is 133.
Neither establishes approximately 180. Digest and ASIC are included transparently and contribute zero reduction;
this is accounting, not a scope revision. No incremental local coverage deltas are added to these CI numbers.
This historical candidate comparison does not measure every later addition at final head `bcddbdc`.

Candidate artifact shortfalls (line/arc references use the unchanged target blobs at the run SHAs):

- `etl/edgar_flow.py`: statements **534, 536**; branch arcs **533→534**.
- `ui/daily_report.py`: statements **569**; branch arcs **568→569**.
- `api/signals.py`: statements **398, 399**; branch arcs **none**.
- `api/search.py`: statements **28, 29, 31, 93**; branch arcs **23→26, 92→93**.
- `etl/digest_flow.py`: statements **440**; branch arcs **103→135, 439→440**.
- `adapters/asic.py`: statements **none**; branch arcs **42→47**.

EDGAR and daily-report misses are CLI entry wiring; signals misses are an optional import fallback;
search misses include optional import fallback and blank-score behavior; digest and ASIC retain the listed branches.
These are residuals, not declarations that coverage is impossible. PostgreSQL behavior tests model SQL and connections;
no live PostgreSQL validation is claimed. Documentation repair does not authorize coverage expansion.

## Review finding dispositions

Six inline findings were re-read against merged content, including resolved threads:

- Broken nested evidence links (thread `PRRT_kwDOO4LcjM6zZvD6`): obsolete. Merged body uses absolute commit-pinned links; this compact body retains them. Eligible for controller resolution after readback.
- Baseline archived candidate HEAD (`PRRT_kwDOO4LcjM6zZvD-`): already resolved. Merged README pins `a7f4e626`; this repair uses the named baseline CI artifact instead of a local archive.
- Combined coverage mislabeled branch-only (`PRRT_kwDOO4LcjM6zZvEB`): already resolved. This document reports statements and branch arcs separately and avoids ambiguous percentages.
- Approximately-180 reduction (`PRRT_kwDOO4LcjM6zZvEH`): **still substantive; leave unresolved**. Artifacts now reproduce 116 and expose all six targets, but no approved revised criterion exists.
- Follow-up replay lacked 15 seeded receipts (`PRRT_kwDOO4LcjM6zbK-5`): already resolved. Merged README supplies a full replay alternative; partial historical commands are retired here.
- Boundary replay lacked 21 seeded receipts (`PRRT_kwDOO4LcjM6zcAoW`): already resolved. Merged README seeds a separate output directory. Partial replay instructions are omitted from this replacement.

The earlier merge-guard snapshot is superseded by the verified merge. Outdated thread flags alone do not dispose of substantive findings.
Historical read-only Git/publication failures and local-suite failures are retained in the pinned historical documents/logs;
they are not current claims about PR state, and local runs do not override successful exact-head CI.

## Mutation evidence: 37 added functions, 39 receipts

AST definition audit against merge parent: `tests/test_edgar_flow.py` **13** added,
`tests/test_issue_1806_coverage.py` **1** added, `tests/test_issue_1888_coverage.py` **23** added.
All **37** added functions match receipts; the other **2** receipts strengthen pre-existing digest and formatter tests.
All **39** receipts record red=1 / green=0, and their source hashes match the restored merged runtime bytes.

[Complete receipts](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/receipts.json) and [mutation runner/source edits](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/replay.py)
retain exact interpreter, environment, working directory, diff and full outputs.
The following are archived commands and verbatim excerpts, **not reruns in this documentation repair**.
The `PYTHONPATH=/tmp/issue1888-runtime` shim and hosted Linux Python paths belong to the recorded environment;
current macOS runs must use the selected repository interpreter or `/opt/anaconda3/bin/python3`.
Each red command ran after the corresponding source mutation, then the identical green command ran after restoration.
Some parametrized cases pass under mutation; each red run still contains a named failure.

### 1. `test_manager_lookup_rejects_missing_or_deleted_manager`

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_edgar_flow.py::test_manager_lookup_rejects_missing_or_deleted_manager -m 'not slow' --tb=short --disable-warnings
```

```text
RED: 1 failed, 5 passed, 8 warnings in 1.70s
exit: 1
GREEN: 6 passed, 8 warnings in 1.37s
exit: 0
```

Complete logs: [red](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_manager_lookup_rejects_missing_or_deleted_manager-red.txt), [green](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_manager_lookup_rejects_missing_or_deleted_manager-green.txt).

### 2. `test_watermark_unavailable_without_filing_history`

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_edgar_flow.py::test_watermark_unavailable_without_filing_history -m 'not slow' --tb=short --disable-warnings
```

```text
RED: 2 failed, 1 passed, 8 warnings in 1.67s
exit: 1
GREEN: 3 passed, 8 warnings in 1.61s
exit: 0
```

Complete logs: [red](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_watermark_unavailable_without_filing_history-red.txt), [green](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_watermark_unavailable_without_filing_history-green.txt).

### 3. `test_holdings_resolution_failure_preserves_raw_rows`

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_edgar_flow.py::test_holdings_resolution_failure_preserves_raw_rows -m 'not slow' --tb=short --disable-warnings
```

```text
RED: 1 failed, 8 warnings in 2.51s
exit: 1
GREEN: 1 passed, 8 warnings in 1.39s
exit: 0
```

Complete logs: [red](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_holdings_resolution_failure_preserves_raw_rows-red.txt), [green](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_holdings_resolution_failure_preserves_raw_rows-green.txt).

### 4. `test_empty_holdings_do_not_delete_existing_rows`

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_edgar_flow.py::test_empty_holdings_do_not_delete_existing_rows -m 'not slow' --tb=short --disable-warnings
```

```text
RED: 1 failed, 8 warnings in 1.84s
exit: 1
GREEN: 1 passed, 8 warnings in 1.88s
exit: 0
```

Complete logs: [red](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_empty_holdings_do_not_delete_existing_rows-red.txt), [green](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_empty_holdings_do_not_delete_existing_rows-green.txt).

### 5. `test_bitemporal_unowned_filing_rerun_preserves_version`

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_edgar_flow.py::test_bitemporal_unowned_filing_rerun_preserves_version -m 'not slow' --tb=short --disable-warnings
```

```text
RED: 1 failed, 8 warnings in 2.36s
exit: 1
GREEN: 1 passed, 8 warnings in 1.43s
exit: 0
```

Complete logs: [red](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_bitemporal_unowned_filing_rerun_preserves_version-red.txt), [green](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_bitemporal_unowned_filing_rerun_preserves_version-green.txt).

### 6. `test_deletion_during_second_download_keeps_first_filing`

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_edgar_flow.py::test_deletion_during_second_download_keeps_first_filing -m 'not slow' --tb=short --disable-warnings
```

```text
RED: 1 failed, 8 warnings in 1.98s
exit: 1
GREEN: 1 passed, 8 warnings in 1.42s
exit: 0
```

Complete logs: [red](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_deletion_during_second_download_keeps_first_filing-red.txt), [green](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_deletion_during_second_download_keeps_first_filing-green.txt).

### 7. `test_binary_filing_without_manager_skips_text_index`

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_edgar_flow.py::test_binary_filing_without_manager_skips_text_index -m 'not slow' --tb=short --disable-warnings
```

```text
RED: 1 failed, 8 warnings in 1.78s
exit: 1
GREEN: 1 passed, 8 warnings in 2.28s
exit: 0
```

Complete logs: [red](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_binary_filing_without_manager_skips_text_index-red.txt), [green](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_binary_filing_without_manager_skips_text_index-green.txt).

### 8. `test_search_guards_return_before_accessing_database`

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1806_coverage.py::test_search_guards_return_before_accessing_database -m 'not slow' --tb=short --disable-warnings
```

```text
RED: 1 failed, 8 warnings in 1.97s
exit: 1
GREEN: 1 passed, 8 warnings in 2.10s
exit: 0
```

Complete logs: [red](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_search_guards_return_before_accessing_database-red.txt), [green](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_search_guards_return_before_accessing_database-green.txt).

### 9. `test_digest_delivery_configuration_guards` (strengthened existing)

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1806_coverage.py::test_digest_delivery_configuration_guards -m 'not slow' --tb=short --disable-warnings
```

```text
RED: 1 failed, 8 warnings in 1.96s
exit: 1
GREEN: 1 passed, 8 warnings in 1.92s
exit: 0
```

Complete logs: [red](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_digest_delivery_configuration_guards-red.txt), [green](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_digest_delivery_configuration_guards-green.txt).

### 10. `test_daily_report_formatters_cover_empty_and_directional_values` (strengthened existing)

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1806_coverage.py::test_daily_report_formatters_cover_empty_and_directional_values -m 'not slow' --tb=short --disable-warnings
```

```text
RED: 1 failed, 8 warnings in 1.95s
exit: 1
GREEN: 1 passed, 8 warnings in 1.99s
exit: 0
```

Complete logs: [red](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_daily_report_formatters_cover_empty_and_directional_values-red.txt), [green](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_daily_report_formatters_cover_empty_and_directional_values-green.txt).

### 11. `test_search_legacy_holdings_return_filing_groups_and_holdings`

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_search_legacy_holdings_return_filing_groups_and_holdings -m 'not slow' --tb=short --disable-warnings
```

```text
RED: 1 failed, 6 warnings in 0.81s
exit: 1
GREEN: 1 passed, 6 warnings in 0.82s
exit: 0
```

Complete logs: [red](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_search_legacy_holdings_return_filing_groups_and_holdings-red.txt), [green](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_search_legacy_holdings_return_filing_groups_and_holdings-green.txt).

### 12. `test_search_vector_documents_keep_shared_managers_and_deduplicate`

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_search_vector_documents_keep_shared_managers_and_deduplicate -m 'not slow' --tb=short --disable-warnings
```

```text
RED: 1 failed, 6 warnings in 0.82s
exit: 1
GREEN: 1 passed, 6 warnings in 0.83s
exit: 0
```

Complete logs: [red](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_search_vector_documents_keep_shared_managers_and_deduplicate-red.txt), [green](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_search_vector_documents_keep_shared_managers_and_deduplicate-green.txt).

### 13. `test_search_postgres_activism_preserves_filing_context`

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_search_postgres_activism_preserves_filing_context -m 'not slow' --tb=short --disable-warnings
```

```text
RED: 1 failed, 6 warnings in 0.81s
exit: 1
GREEN: 1 passed, 6 warnings in 0.89s
exit: 0
```

Complete logs: [red](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_search_postgres_activism_preserves_filing_context-red.txt), [green](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_search_postgres_activism_preserves_filing_context-green.txt).

### 14. `test_conviction_annotation_survives_unavailable_holdings`

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_conviction_annotation_survives_unavailable_holdings -m 'not slow' --tb=short --disable-warnings
```

```text
RED: 1 failed, 6 warnings in 0.91s
exit: 1
GREEN: 1 passed, 6 warnings in 0.98s
exit: 0
```

Complete logs: [red](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_conviction_annotation_survives_unavailable_holdings-red.txt), [green](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_conviction_annotation_survives_unavailable_holdings-green.txt).

### 15. `test_report_signal_query_failure_returns_empty_and_closes`

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_report_signal_query_failure_returns_empty_and_closes -m 'not slow' --tb=short --disable-warnings
```

```text
RED: 2 failed, 6 warnings in 1.17s
exit: 1
GREEN: 2 passed, 6 warnings in 0.90s
exit: 0
```

Complete logs: [red](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_report_signal_query_failure_returns_empty_and_closes-red.txt), [green](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_report_signal_query_failure_returns_empty_and_closes-green.txt).

### 16. `test_watermark_without_source_column_is_scoped_to_manager`

```sh
PYTHONPATH= PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.8/x64/bin/python -m pytest tests/test_edgar_flow.py::test_watermark_without_source_column_is_scoped_to_manager -m 'not slow' --tb=short --disable-warnings
```

```text
RED: 1 failed, 8 warnings in 0.94s
exit: 1
GREEN: 1 passed, 8 warnings in 0.79s
exit: 0
```

Complete logs: [red](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_watermark_without_source_column_is_scoped_to_manager-red.txt), [green](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_watermark_without_source_column_is_scoped_to_manager-green.txt).

### 17. `test_upsert_without_raw_key_leaves_legacy_table_unchanged`

```sh
PYTHONPATH= PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.8/x64/bin/python -m pytest tests/test_edgar_flow.py::test_upsert_without_raw_key_leaves_legacy_table_unchanged -m 'not slow' --tb=short --disable-warnings
```

```text
RED: 2 failed, 8 warnings in 0.96s
exit: 1
GREEN: 2 passed, 8 warnings in 0.79s
exit: 0
```

Complete logs: [red](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_upsert_without_raw_key_leaves_legacy_table_unchanged-red.txt), [green](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_upsert_without_raw_key_leaves_legacy_table_unchanged-green.txt).

### 18. `test_upsert_ignored_by_trigger_returns_existing_id_or_zero`

```sh
PYTHONPATH= PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.8/x64/bin/python -m pytest tests/test_edgar_flow.py::test_upsert_ignored_by_trigger_returns_existing_id_or_zero -m 'not slow' --tb=short --disable-warnings
```

```text
RED: 2 failed, 8 warnings in 0.96s
exit: 1
GREEN: 2 passed, 8 warnings in 0.80s
exit: 0
```

Complete logs: [red](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_upsert_ignored_by_trigger_returns_existing_id_or_zero-red.txt), [green](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_upsert_ignored_by_trigger_returns_existing_id_or_zero-green.txt).

### 19. `test_holding_insert_without_recognized_columns_preserves_legacy_rows`

```sh
PYTHONPATH= PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.8/x64/bin/python -m pytest tests/test_edgar_flow.py::test_holding_insert_without_recognized_columns_preserves_legacy_rows -m 'not slow' --tb=short --disable-warnings
```

```text
RED: 1 failed, 8 warnings in 0.98s
exit: 1
GREEN: 1 passed, 8 warnings in 0.79s
exit: 0
```

Complete logs: [red](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_holding_insert_without_recognized_columns_preserves_legacy_rows-red.txt), [green](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_holding_insert_without_recognized_columns_preserves_legacy_rows-green.txt).

### 20. `test_postgres_upsert_preserves_storage_key`

```sh
PYTHONPATH= PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.8/x64/bin/python -m pytest tests/test_edgar_flow.py::test_postgres_upsert_preserves_storage_key -m 'not slow' --tb=short --disable-warnings
```

```text
RED: 3 failed, 8 warnings in 0.99s
exit: 1
GREEN: 3 passed, 8 warnings in 0.81s
exit: 0
```

Complete logs: [red](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_postgres_upsert_preserves_storage_key-red.txt), [green](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_postgres_upsert_preserves_storage_key-green.txt).

### 21. `test_reverse_order_original_keeps_amendment_current`

```sh
PYTHONPATH= PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.8/x64/bin/python -m pytest tests/test_edgar_flow.py::test_reverse_order_original_keeps_amendment_current -m 'not slow' --tb=short --disable-warnings
```

```text
RED: 1 failed, 8 warnings in 0.95s
exit: 1
GREEN: 1 passed, 8 warnings in 0.79s
exit: 0
```

Complete logs: [red](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_reverse_order_original_keeps_amendment_current-red.txt), [green](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_reverse_order_original_keeps_amendment_current-green.txt).

### 22. `test_report_page_sorts_value_and_filters_news_exports`

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.10/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_report_page_sorts_value_and_filters_news_exports -m 'not slow' --tb=short --disable-warnings
```

```text
RED: 2 failed, 12 warnings in 1.33s
exit: 1
GREEN: 2 passed, 12 warnings in 1.32s
exit: 0
```

Complete logs: [red](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_report_page_sorts_value_and_filters_news_exports-red.txt), [green](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_report_page_sorts_value_and_filters_news_exports-green.txt).

### 23. `test_report_diff_unexpected_error_propagates_and_closes`

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.10/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_report_diff_unexpected_error_propagates_and_closes -m 'not slow' --tb=short --disable-warnings
```

```text
RED: 1 failed, 6 warnings in 0.58s
exit: 1
GREEN: 1 passed, 6 warnings in 0.54s
exit: 0
```

Complete logs: [red](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_report_diff_unexpected_error_propagates_and_closes-red.txt), [green](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_report_diff_unexpected_error_propagates_and_closes-green.txt).

### 24. `test_report_latest_date_ignores_missing_sources`

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.10/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_report_latest_date_ignores_missing_sources -m 'not slow' --tb=short --disable-warnings
```

```text
RED: 1 failed, 6 warnings in 0.62s
exit: 1
GREEN: 1 passed, 6 warnings in 0.53s
exit: 0
```

Complete logs: [red](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_report_latest_date_ignores_missing_sources-red.txt), [green](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_report_latest_date_ignores_missing_sources-green.txt).

### 25. `test_crowded_query_preserves_native_dates_and_tolerates_invalid_ids`

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.10/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_crowded_query_preserves_native_dates_and_tolerates_invalid_ids -m 'not slow' --tb=short --disable-warnings
```

```text
RED: 3 failed, 1 passed, 6 warnings in 0.54s
exit: 1
GREEN: 4 passed, 6 warnings in 0.52s
exit: 0
```

Complete logs: [red](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_crowded_query_preserves_native_dates_and_tolerates_invalid_ids-red.txt), [green](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_crowded_query_preserves_native_dates_and_tolerates_invalid_ids-green.txt).

### 26. `test_search_sqlite_vector_failure_preserves_text_results`

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.10/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_search_sqlite_vector_failure_preserves_text_results -m 'not slow' --tb=short --disable-warnings
```

```text
RED: 1 failed, 1 passed, 6 warnings in 0.59s
exit: 1
GREEN: 2 passed, 6 warnings in 0.52s
exit: 0
```

Complete logs: [red](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_search_sqlite_vector_failure_preserves_text_results-red.txt), [green](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_search_sqlite_vector_failure_preserves_text_results-green.txt).

### 27. `test_search_postgres_ignores_vector_hits_without_document_id`

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.10/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_search_postgres_ignores_vector_hits_without_document_id -m 'not slow' --tb=short --disable-warnings
```

```text
RED: 1 failed, 6 warnings in 0.54s
exit: 1
GREEN: 1 passed, 6 warnings in 0.53s
exit: 0
```

Complete logs: [red](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_search_postgres_ignores_vector_hits_without_document_id-red.txt), [green](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_search_postgres_ignores_vector_hits_without_document_id-green.txt).

### 28. `test_report_postgres_news_caches_results_by_date_and_closes`

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.8/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_report_postgres_news_caches_results_by_date_and_closes -m 'not slow' --tb=short --disable-warnings
```

```text
RED: 3 failed, 6 warnings in 0.57s
exit: 1
GREEN: 3 passed, 6 warnings in 0.55s
exit: 0
```

Complete logs: [red](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_report_postgres_news_caches_results_by_date_and_closes-red.txt), [green](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_report_postgres_news_caches_results_by_date_and_closes-green.txt).

### 29. `test_report_whitespace_news_url_keeps_escaped_headline`

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.8/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_report_whitespace_news_url_keeps_escaped_headline -m 'not slow' --tb=short --disable-warnings
```

```text
RED: 2 failed, 6 warnings in 0.54s
exit: 1
GREEN: 2 passed, 6 warnings in 0.53s
exit: 0
```

Complete logs: [red](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_report_whitespace_news_url_keeps_escaped_headline-red.txt), [green](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_report_whitespace_news_url_keeps_escaped_headline-green.txt).

### 30. `test_report_date_navigation_keeps_display_and_export_in_sync`

```sh
PYTHONPATH= PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_report_date_navigation_keeps_display_and_export_in_sync -m 'not slow' --tb=short --disable-warnings
```

```text
RED: 2 failed, 12 warnings in 2.38s
exit: 1
GREEN: 2 passed, 12 warnings in 2.42s
exit: 0
```

Complete logs: [red](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_report_date_navigation_keeps_display_and_export_in_sync-red.txt), [green](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_report_date_navigation_keeps_display_and_export_in_sync-green.txt).

### 31. `test_report_login_stop_prevents_database_access`

```sh
PYTHONPATH= PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_report_login_stop_prevents_database_access -m 'not slow' --tb=short --disable-warnings
```

```text
RED: 1 failed, 6 warnings in 1.10s
exit: 1
GREEN: 1 passed, 6 warnings in 0.93s
exit: 0
```

Complete logs: [red](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_report_login_stop_prevents_database_access-red.txt), [green](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_report_login_stop_prevents_database_access-green.txt).

### 32. `test_report_nearest_date_caches_empty_and_populated_results`

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.10/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_report_nearest_date_caches_empty_and_populated_results -m 'not slow' --tb=short --disable-warnings
```

```text
RED: 2 failed, 2 passed, 6 warnings in 0.99s
exit: 1
GREEN: 4 passed, 6 warnings in 0.95s
exit: 0
```

Complete logs: [red](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_report_nearest_date_caches_empty_and_populated_results-red.txt), [green](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_report_nearest_date_caches_empty_and_populated_results-green.txt).

### 33. `test_report_latest_date_query_errors_close_connections_and_are_retried`

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.10/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_report_latest_date_query_errors_close_connections_and_are_retried -m 'not slow' --tb=short --disable-warnings
```

```text
RED: 1 failed, 6 warnings in 0.95s
exit: 1
GREEN: 1 passed, 6 warnings in 0.88s
exit: 0
```

Complete logs: [red](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_report_latest_date_query_errors_close_connections_and_are_retried-red.txt), [green](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_report_latest_date_query_errors_close_connections_and_are_retried-green.txt).

### 34. `test_search_partial_sqlite_schema_keeps_filings_searchable`

```sh
PYTHONPATH= PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.8/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_search_partial_sqlite_schema_keeps_filings_searchable -m 'not slow' --tb=short --disable-warnings
```

```text
RED: 1 failed, 6 warnings in 0.57s
exit: 1
GREEN: 1 passed, 6 warnings in 0.55s
exit: 0
```

Complete logs: [red](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_search_partial_sqlite_schema_keeps_filings_searchable-red.txt), [green](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_search_partial_sqlite_schema_keeps_filings_searchable-green.txt).

### 35. `test_search_activism_without_manager_table_preserves_subject_context`

```sh
PYTHONPATH= PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.8/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_search_activism_without_manager_table_preserves_subject_context -m 'not slow' --tb=short --disable-warnings
```

```text
RED: 1 failed, 6 warnings in 0.57s
exit: 1
GREEN: 1 passed, 6 warnings in 0.55s
exit: 0
```

Complete logs: [red](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_search_activism_without_manager_table_preserves_subject_context-red.txt), [green](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_search_activism_without_manager_table_preserves_subject_context-green.txt).

### 36. `test_conviction_nullable_ticker_lookup_preserves_annotations_without_requery`

```sh
PYTHONPATH=/tmp/issue1888-runtime PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_conviction_nullable_ticker_lookup_preserves_annotations_without_requery -m 'not slow' --tb=short --disable-warnings
```

```text
RED: 1 failed, 6 warnings in 0.92s
exit: 1
GREEN: 1 passed, 6 warnings in 0.94s
exit: 0
```

Complete logs: [red](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_conviction_nullable_ticker_lookup_preserves_annotations_without_requery-red.txt), [green](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_conviction_nullable_ticker_lookup_preserves_annotations_without_requery-green.txt).

### 37. `test_search_postgres_holdings_without_identifiers_keep_cusip_context`

```sh
PYTHONPATH= PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_search_postgres_holdings_without_identifiers_keep_cusip_context -m 'not slow' --tb=short --disable-warnings
```

```text
RED: 2 failed, 6 warnings in 0.80s
exit: 1
GREEN: 2 passed, 6 warnings in 0.78s
exit: 0
```

Complete logs: [red](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_search_postgres_holdings_without_identifiers_keep_cusip_context-red.txt), [green](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_search_postgres_holdings_without_identifiers_keep_cusip_context-green.txt).

### 38. `test_report_page_preserves_unknown_deltas_and_skips_incomplete_signals`

```sh
PYTHONPATH= PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_report_page_preserves_unknown_deltas_and_skips_incomplete_signals -m 'not slow' --tb=short --disable-warnings
```

```text
RED: 3 failed, 14 warnings in 1.93s
exit: 1
GREEN: 3 passed, 14 warnings in 1.75s
exit: 0
```

Complete logs: [red](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_report_page_preserves_unknown_deltas_and_skips_incomplete_signals-red.txt), [green](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_report_page_preserves_unknown_deltas_and_skips_incomplete_signals-green.txt).

### 39. `test_report_missing_activism_table_caches_empty_result_and_closes`

```sh
PYTHONPATH= PYTHONDONTWRITEBYTECODE=1 /opt/hostedtoolcache/Python/3.14.9/x64/bin/python -m pytest tests/test_issue_1888_coverage.py::test_report_missing_activism_table_caches_empty_result_and_closes -m 'not slow' --tb=short --disable-warnings
```

```text
RED: 1 failed, 6 warnings in 0.84s
exit: 1
GREEN: 1 passed, 6 warnings in 0.73s
exit: 0
```

Complete logs: [red](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_report_missing_activism_table_caches_empty_result_and_closes-red.txt), [green](https://github.com/stranske/Manager-Database/blob/48ef1e750fbbf6fafe9938b094a3f1ceed579ae8/docs/evidence/issue-1888/mutations/test_report_missing_activism_table_caches_empty_result_and_closes-green.txt).

## Documentation validation and publication boundary

`/opt/anaconda3/bin/python3 /tmp/issue1888-doc-check.py` checks the actual test definitions,
receipt/log contents, commit-pinned link targets, artifact accounting and documentation-only diff.
No pytest, mutation replay, full suite, or release/verify pipeline was run locally for this documentation repair.
The two changed Markdown files have no focused executable test suite.

Publication protocol: the installed closer controller's GitHub helper replaces PR #1889's body, followed
by byte-for-byte readback. Resolve only the obsolete evidence-link thread; keep the approximately-180 thread
unresolved. The repair PR records observed publication/readback status separately.

Documentation-check output (2026-11-10; under two minutes, no pytest execution):

```text
ADDED_FUNCTIONS=37 RECEIPTS=39 MISSING=0; commands, red/green excerpts and source hashes match
PINNED_LINKS=OK; every linked log/document exists at merged commit
CI_UNITS four=129->13 six=133->17 reduction=116; all six rows and shortfalls match artifacts
DOCS_ONLY=2; final-head pytest and six finding dispositions verified
DOCUMENTATION_EVIDENCE=PASS
```
