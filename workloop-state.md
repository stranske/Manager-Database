## 2026-06-11T00:18Z - opener (codex) issue #1142 -> PR #1143

- Repo: `stranske/Manager-Database`
- Issue: `#1142` (`Add a scheduled EDGAR filing-ingestion deployment contract`)
- Branch: `codex/issue-1142-edgar-deployment`
- Scope: added `edgar_deployment = edgar_flow.to_deployment(...)` with nightly cron `0 4 * * *`, default `cik_list`/`since` parameters, a focused deployment contract test, and README operator guidance for `prefect deployment serve etl/edgar_flow.py:edgar_deployment`.
- Validation:
  - `pytest tests/test_edgar_flow.py::test_edgar_deployment_has_daily_schedule -q` passed.
  - Deliberate break changed cron to `0 0 1 * *`; the focused test failed on `assert schedule.cron == "0 4 * * *"`, then cron was restored.
  - `pytest tests/test_edgar_flow.py::test_edgar_deployment_has_daily_schedule tests/test_edgar_flow.py::test_fetch_and_store_fires_new_filing_alerts tests/test_edgar_flow.py::test_fetch_and_store_uses_postgres_safe_persistence -q` passed.
  - `pytest tests/test_edgar_flow.py -q` passed (`12 passed, 1 skipped`).
  - `rg "edgar_deployment" etl/edgar_flow.py` found the deployment object.
  - `git diff --check` passed.
- PR: `#1143` (`Add EDGAR nightly deployment contract`), non-draft, closes `#1142`.
- Routing: applied `agent:codex`, `agents:keepalive`, `autofix`, `repo-review-approved`, and `priority:normal`; after initial cancelled workflow evidence, added `agent:retry` and dispatched `agents-81-gate-followups.yml` with `force_retry=true` (`27314764280`). Cap-health then reported `#1143` as `draining` with active Gate evidence and `non_drainable_count=0`.
- Next action: wait for keepalive/Gate to finish; closer should drain once checks and verifier state are ready.
