# PR #1921 acceptance sync (paste into PR body when orchestrator can call `gh pr edit`)

<!-- deliberate-break: test=tests/test_insider_flow.py::test_local_form4_command_persists_and_annotates test-file=tests/test_insider_flow.py break-file=etl/insider_flow.py -->

Refs #1917 (CLI, as-of issuer resolution via `resolved_ticker`/CUSIP string, summary counters, acceptance + deliberate-break evidence). Issue #1917 stays open for follow-up: repository CUSIP→CIK/ticker mapping, `unmatched_issuers` accounting, and `test_local_form4_cusip_only_issuer_resolution`.

**Progress:** 7/7 tasks complete (salvage scope)

### Acceptance Criteria

The PR is complete when ALL of these are satisfied:

- [x] Named test `tests/test_insider_flow.py::test_local_form4_command_persists_and_annotates` (stub fetcher, seeded SQLite -> rows written + full summary counters; api/signals insider_net_direction readback; failed issuer counted; rerun no duplicates) passes: `python3 -m pytest tests/test_insider_flow.py::test_local_form4_command_persists_and_annotates -q`
- [x] Deliberate break → revert, shown in the PR: Drop the ingestion call from the CLI (or the per-issuer try/except) -> rows absent / one failure aborts run, test fails; record that the named test fails, then revert and record that it passes.
- [x] The whole test file still passes: `python3 -m pytest tests/test_insider_flow.py -q`

Evidence: `docs/verification/issue-1917-local-form4-deliberate-break.md` (break `6aedec2`, restore `f981e20`).