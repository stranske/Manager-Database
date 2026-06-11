## 2026-06-11T05:02Z - opener (codex) issue #1150

- Repo: `stranske/Manager-Database`
- Issue: `#1150` (`Add nightly database snapshot and restore-provisioning contract`)
- Branch: `codex/issue-1150-db-snapshot-restore`
- Worktree: `/Users/teacher/.codex/automations/pd-workloop-resume/worktrees/manager-db-1150-db-snapshot-restore`
- State: implemented backup/restore script, scheduled workflow, runbook, internal hosting cross-link, and dry-run tests.
- Validation:
  - `python -m pytest tests/test_db_snapshot_restore.py -q` passed (5; existing FastAPI/LangSmith deprecation warnings only).
  - Deliberate-break gate: temporarily removed the workflow `backup --dry-run` command and `test_database_snapshot_workflow_runs_dry_run_and_conditional_live_backup` failed on the missing dry-run assertion; restored the command.
  - `python -m ruff check scripts/db_snapshot_restore.py tests/test_db_snapshot_restore.py` passed.
  - `git diff --check` passed.
  - `DB_SNAPSHOT_DATABASE_URL=postgresql://user:pw@db:5432/manager DB_SNAPSHOT_S3_URI=s3://manager-db-backups/prod python scripts/db_snapshot_restore.py backup --dry-run` printed a masked plan with encrypted S3 upload.

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
