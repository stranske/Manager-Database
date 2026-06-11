## 2026-06-11T00:18Z - opener (codex) issue #1142 -> PR pending

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
- Next action: push branch, open ready-for-review PR, and hand off to keepalive.
