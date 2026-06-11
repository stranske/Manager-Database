## 2026-06-11T03:14Z - opener (codex) issue #1147

- Repo: `stranske/Manager-Database`
- Issue: `#1147` (`Add Postgres-backed integration test for alert dispatch path and wire to postgres-integration CI job`)
- Branch: `codex/issue-1147-alert-postgres`
- State: implementation in progress; new Postgres alert integration tests and CI collection update staged for PR.
- Changes: added `tests/test_alert_postgres_integration.py` covering Postgres alert table DDL, pending alert insert, delivery success/error persistence, new-filing evaluation, and streamlit dispatch; added the new test file to the existing `postgres-integration` CI job.
- Validation:
  - `pytest tests/test_alert_integration.py -q` passed (5).
  - `pytest tests/test_alert_postgres_integration.py -v` collected 4 tests and skipped locally because `MGRDB_PG_TEST_URL` was unset.
  - `rg "test_alert_postgres_integration" .github/workflows/ci.yml` returned the updated CI command.
  - `git diff --check` passed.
  - `python -m ruff check tests/test_alert_postgres_integration.py` passed.
  - Attempted live local Postgres validation with Docker, but the Docker daemon socket `/Users/teacher/.orbstack/run/docker.sock` was unavailable; CI's Postgres service is expected to run these tests non-skipped.

## 2026-06-11T01:16Z - opener (codex) issue #1145

- Repo: `stranske/Manager-Database`
- Issue: `#1145` (`Align API design guidelines with the shipped chat-only rate limiter`)
- PR: `#1146` (`Align API rate-limit guideline scope`)
- Branch: `codex/issue-1145-rate-limit-guidelines`
- State: ready-for-review PR opened; waiting for keepalive/Gate.
- Changes: scoped `docs/api_design_guidelines.md` rate-limit language to the chat write paths documented in `api_rate_limiting.md`; added `test_api_design_guidelines_do_not_claim_global_rate_limiting`.
- Validation:
  - `pytest tests/test_rate_limit_contract.py::test_api_design_guidelines_do_not_claim_global_rate_limiting -q` passed.
  - Deliberate break restored the old all-endpoints sentence; the new test failed with `AssertionError: api_design_guidelines.md must delegate rate-limit scope to api_rate_limiting.md instead of claiming all endpoints are limited.`
  - Restored corrected docs and `pytest tests/test_rate_limit_contract.py -q` passed (14).
  - `rg -i "all.*endpoint.*rate limit" docs/api_design_guidelines.md` returned no matches.
  - `git diff --check` passed.
