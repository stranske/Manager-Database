# PR #1922 acceptance sync (paste into PR body when orchestrator can call `gh pr edit`)

<!-- deliberate-break: test=tests/test_insider_flow.py::test_local_form4_command_persists_and_annotates test-file=tests/test_insider_flow.py break-file=etl/insider_flow.py -->

**Progress:** 7/7 tasks complete

### Acceptance Criteria

The PR is complete when ALL of these are satisfied:

- [x] Named test `tests/test_insider_flow.py::test_local_form4_command_persists_and_annotates` (stub fetcher, seeded SQLite -> rows written + full summary counters; api/signals insider_net_direction readback; failed issuer counted; rerun no duplicates) passes: `python3 -m pytest tests/test_insider_flow.py::test_local_form4_command_persists_and_annotates -q`
- [x] Deliberate break → revert, shown in the PR: Drop the ingestion call from the CLI (or the per-issuer try/except) -> rows absent / one failure aborts run, test fails; record that the named test fails, then revert and record that it passes.
- [x] The whole test file still passes: `python3 -m pytest tests/test_insider_flow.py -q`

Evidence: `docs/verification/issue-1917-local-form4-deliberate-break.md` (break `27fe8b0`, restore `dc64881`).