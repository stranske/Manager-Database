## 2026-06-11T03:14Z - opener (codex) issue #1147

- Repo: `stranske/Manager-Database`
- Issue: `#1147` (`Add Postgres-backed integration test for alert dispatch path and wire to postgres-integration CI job`)
- PR: `#1148` (`Add Postgres alert integration coverage`)
- Branch: `codex/issue-1147-alert-postgres`
- State: ready-for-review PR opened; waiting for keepalive/Gate.
- Changes: added `tests/test_alert_postgres_integration.py` covering Postgres alert table DDL, pending alert insert, delivery success/error persistence, new-filing evaluation, and streamlit dispatch; added the new test file to the existing `postgres-integration` CI job.
- Validation:
  - `pytest tests/test_alert_integration.py -q` passed (5).
  - `pytest tests/test_alert_postgres_integration.py -v` collected 4 tests and skipped locally because `MGRDB_PG_TEST_URL` was unset.
  - `rg "test_alert_postgres_integration" .github/workflows/ci.yml` returned the updated CI command.
  - `git diff --check` passed.
  - `python -m ruff check tests/test_alert_postgres_integration.py` passed.
  - Attempted live local Postgres validation with Docker, but the Docker daemon socket `/Users/teacher/.orbstack/run/docker.sock` was unavailable; CI's Postgres service is expected to run these tests non-skipped.
  - PR #1148 verified non-draft with labels `agent:codex`, `agents:keepalive`, `autofix`, `agent:retry`, `repo-review-approved`, and `priority:normal`; cap-health reported it as `draining` with active Gate evidence.

## 2026-06-11T04:08Z - opener (codex) issue #1149

- Repo: `stranske/Manager-Database`
- Issue: `#1149` (`Audit and remediate design-doc behavioral claims that are unimplemented or contradicted across the codebase`)
- Branch: `codex/issue-1149-doc-claims-audit`
- State: audit report and regression test implemented; PR not opened yet.
- Changes: added `docs/reports/design-doc-behavioral-claims-audit.md` with classifications for current operator-facing behavioral claims; filed follow-up issues `#1150` and `#1151` for unimplemented database snapshot and parser snapshot-regression contracts; added `tests/test_design_doc_claims_audit.py` to keep the report anchored to current claim refs and follow-up links.
- Validation:
  - `python -m pytest tests/test_design_doc_claims_audit.py -q` passed (2).
  - `rg "implemented-and-verified|unimplemented|contradicted" docs/reports/design-doc-behavioral-claims-audit.md` returned the summary and classified rows.
  - `git diff --check` passed.

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
