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
