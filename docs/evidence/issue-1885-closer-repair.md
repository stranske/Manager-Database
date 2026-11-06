# Issue #1885 closer repair (PR #1893)

Checked 2026-11-05 (America/Chicago). Closes
[#1885](https://github.com/stranske/Manager-Database/issues/1885).

## Validator digest

| Field | SHA-256 |
| --- | --- |
| `docs/evidence/issue-1885/previous-validator.py.txt` (archived) | `0d5880682352899c8341a02dd4b1dc6fa5d0d3c4b8b72efb965679abaa42cc34` |
| `tests/test_dashboard_read_failure_evidence.py` (current) | `2d12b36382deb8f4b89095498e8e5df6ae00d40b247db84e9932ac178e18102c` |

Recorded in `docs/evidence/issue-1885/synced-tool-pin-successor.json` (`current_validator_sha256` only; `previous_validator_sha256` unchanged).

## Dependency setup validation

Command: `python scripts/validate_dependency_test_setup.py`

Receipt: [validate_dependency_test_setup.txt](issue-1885/validate_dependency_test_setup.txt) — ends with `✓ All validation checks passed!`

## Focused pytest

Command:

```text
python -m pytest tests/test_validate_dependency_test_setup.py tests/test_dashboard_read_failure_evidence.py -q
```

Receipt: [focused-pytest.txt](issue-1885/focused-pytest.txt) — exit 0.

## Deliberate break → restore (`test_review_recovery_accepts_synced_dev_tool_pin_bumps`)

Temporarily restored formatter-only normalization (`black`, `ruff`, `coverage` only), aligned `synced_dev_tools` in the successor JSON, and refreshed `current_validator_sha256` for that variant.

| Phase | Receipt | Exit | Failure locus |
| --- | --- | ---: | --- |
| RED (formatter-only) | [deliberate-break-pin-bump-red.txt](issue-1885/deliberate-break-pin-bump-red.txt) | 1 | `validate_review_recovery_current_inputs` → `assert current == historical` |
| GREEN (restored six-tool validator) | [deliberate-break-pin-bump-green.txt](issue-1885/deliberate-break-pin-bump-green.txt) | 0 | — |

## Full repository test command (local, product deps)

Gate-comparable command (omits generated `ci_test_growth` plugin only):

```text
python -m pip install -e ".[dev,app]"
python -m pytest -m "not nightly" -q
```

Environment: `.venv313` with `/opt/homebrew/bin/python3.13`.

Receipt: [full-pytest-not-nightly-py313.txt](issue-1885/full-pytest-not-nightly-py313.txt).

First remaining failure on this machine (acceptance attempt 2/2): `tests/test_dialect_portability_gate.py::test_dialect_gate_accepts_current_audited_repo_state` — unrelated to #1885; CI runs the authoritative full gate on the PR.

## Regenerated sync PR / sync-core (not completed in this push)

Issue acceptance requires `maint-68-sync-consumer-repos.yml` to regenerate closed sync PR #1884 after this fix merges, then `gh run view <run-id>` on the named Python CI job for that successor PR, and a sync-core round with the pin-conflict finding absent. That workflow reads consumer `main`; it cannot be satisfied on branch `claude/issue-1885` alone.
