Continuing the fleet coverage initiative -- this round was opened automatically by coverage-autopilot.py after the previous round on this repo closed.

Same contract as every round in this initiative:
1. Pick the next target by the repo's own coverage-gap ranking (escaped-defect priority, then
   churn, then uncovered mass), not by file size.
2. Every new test must FAIL when the code it covers is deliberately broken and PASS when
   reverted -- run that break/revert for real and report it, don't assert it.
3. Real bugs found while writing tests get fixed with a minimal, targeted diff in the same PR,
   not worked around. Watch for the negative-cache pattern (cache.get(key) is None used as a 'not yet cached' check, which can't tell 'never queried' from 'queried and got nothing') -- it has recurred independently four separate times fleet-wide already.
4. Low blast radius only -- test-only or tightly-scoped fixes, no refactors.
5. One PR per logical chunk of work.

The goal is to improve the code, not a metric. If you measure this repo at or above 90% before
starting, say so in a comment on this issue and close it without opening a PR -- do not
manufacture a fix just because a round was requested.


## Why

Continue the accepted coverage initiative after merged write-outage chunk #1753 and raw-proof #1754. Fresh main1be82283 baseline is86.65485471296952%, with unchanged24 UI-auth failures and21 skips. Preserve the original initiative and historical provider verdicts.

## Scope

One bounded test-only chunk in api/managers.py, ranked first by repair-subject proxy9, churn16 and67uncovered statements over the current last500commits. Protect stats HTTP compatibility with current SQLite schemas that omit legacy jurisdiction, legacy scalar fallback only for empty arrays, and non-retry/sanitized503 handling for unrelated database failures. Repair-history proxy is explicitly not an escaped-incident count.

## Tasks

- [ ] Add tests/test_manager_stats_schema_fallback.py HTTP cases using real physical current and legacy SQLite schemas and one isolated database-failure connection double.
- [ ] Prove aggregate totals, normalized/deduplicated jurisdictions and tags, precedence of populated modern arrays, sanitized503 and exactly-once acquired connection cleanup without treating unrelated database errors as schema absence.
- [ ] Actually mutate api/managers.py missing-column fallback, legacy scalar fallback and unrelated-error classification; run every named new node RED, restore byte-identical source and require GREEN. Retain commands, raw output, hashes and receipts in docs/evidence/issue-1750-stats-schema/.
- [ ] Run identical full-suite coverage commands before/after, retain pre-existing UI-auth failures and record scope/counts/delta in docs/evidence/issue-1750-stats-schema/README.md.

## Acceptance Criteria

- [ ] /opt/anaconda3/bin/python3 -m pytest tests/test_manager_stats_schema_fallback.py -q exits0 with four passing cases. Real HTTP responses preserve aggregate JSON and legacy fallback, and unrelated failures return503 with only Database unavailable. Capture raw output and JUnit in docs/evidence/issue-1750-stats-schema/.
- [ ] Every new named case fails for an actual production mutation and passes after byte-identical source restoration; retain exact mutation driver, logs, exits and SHA256 in docs/evidence/issue-1750-stats-schema/.
- [ ] /opt/anaconda3/bin/python3 -m pytest tests -q --cov --cov-report=json:coverage.json has identical source/configuration scope and no new failing nodes versus the fresh baseline. Preserve baseline failures and skips and capture console/coverage/JUnit in docs/evidence/issue-1750-stats-schema/.
- [ ] Focused manager API/cache/stats tests, Black/Ruff and git diff --check pass. Production api/managers.py bytes remain unchanged unless a real defect is independently reproduced.

## Non-Goals

No workflow changes, refactor, exclusion/threshold changes, deployment claim, live PostgreSQL claim or whole-repository90% completion. Broader initiative remainsOPEN; related PR only.

## Implementation Notes

Matching registry keepalive owns PR checks/review after opening. Reviewed Repo Merge Verify Closer owns unchanged-head expected topology, full active review threads, seven-minute review floor, guarded merge and actual comparison/chunk disposition. Unrelated UI-auth failure remains separately owned, and original provider verdicts are preserved.
