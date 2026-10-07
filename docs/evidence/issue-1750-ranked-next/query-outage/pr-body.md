## Summary

Protect manager similarity HTTP results when stored scores are infinite or the database fails. Six test-only regressions prove nonfinite selected/secondary scores are filtered before limit1, failures return sanitized503, and acquired connections close exactly once. Related to #1750; this logical chunk keeps the broader coverage initiative open.

## Tasks and acceptance

- [x] Add four real SQLite infinity-score HTTP cases and two isolated database-outage cases in tests/test_manager_similarity_boundaries.py.
- [x] Execute each new named node under an actual api/managers.py production mutation: every REDexit1, exact restoration, then GREENexit0. Twelve complete console/JUnit/argv/cwd/hash phase receipts retained.
- [x] Compare matched complete baseline/candidate:1800→1806PASS; identical24existingUI-auth failures and21skips. Both full commands actually exit1, not full-suitePASS.
- [x] Same14110statements,82excludedlines/source universe and no covered-line regression. Coverage86.75407512402552%→86.7611622962438% (+1line). Finite-score/cleanup controls also protect already executed decisions.
- [x] Black/Ruff, netdiff whitespace and committed artifact hash checks pass. Production bytes match base1fc1474d exactly; no application bug or deployment/Postgres validation claimed.

## Acceptance Criteria

- [x] **Tests**
  - [x] Added HTTP-level checks that exclude infinite similarity scores before applying result limits and rank finite results according to the selected metric.
  - [x] Added checks for sanitized service-unavailable responses to database errors and connection cleanup after query failures.
- [x] **Documentation**
  - [x] Added evidence covering test results, coverage comparisons, mutation checks, and artifact integrity.
  - [x] Recorded existing UI authentication failures and clarified that the results do not establish deployment readiness or complete the broader coverage initiative.

Reconciled against `ecae68c`: all 6 focused cases pass; all 108 committed evidence artifacts match stored and decoded hashes; the original full-suite comparison and 24 prior mutation phases were independently verified. Application bytes match base `1fc1474d`. The retained full-suite failures remain visible; no new full-suite run is claimed.

Latest query-outage follow-up: the existing query case also fails the similarity-results query after successful manager lookup, checking sanitized 503 and a single close. All 6 focused and 76 related cases pass with slow tests excluded. A mutation exposing details only after successful lookup fails the added assertion, then passes after exact application-byte restoration. Targeted manager coverage remains diagnostic at 510/821 statements (62.119366626065776%); the retained complete-suite comparison remains authoritative. Receipts are under `docs/evidence/issue-1750-ranked-next/query-outage/`.

## Validation and retained proof

Candidate: `/opt/anaconda3/bin/python3 -m pytest tests -q --cov --cov-report=json:docs/evidence/issue-1750-ranked-next/candidate-coverage.json --junitxml=docs/evidence/issue-1750-ranked-next/candidate-junit.xml`.

Authoritative baseline repeats the same suite with `--ignore=tests/test_manager_similarity_boundaries.py`; complete argv/cwd/actualexit1, raw compressed console/JUnit/coverage, exact failing-node inventories and comparison are committed under docs/evidence/issue-1750-ranked-next/. Initial baseline captures remain preserved; their raw exit was not retained and is not asserted. The later captured baseline resolves that limitation.

Portable replay: `python3 docs/evidence/issue-1750-ranked-next/replay.py --output /tmp/a-new-similarity-proof-directory`. It derives checkout/interpreter, reserves a fresh output, bounds every child and restores source in finally. Actual existing-output refusal preserved every previous proof/source hash. All50retained artifacts independently match committed blob hashes, including decoded compressed bytes.

Fresh500commit ranking selects api/managers.py (9repair-subjectproxy/16touches/53uncoveredstatements). History proxy is not a verified escaped-incident count. Coverage delta is modest and this PR does not claim the repository90% goal or supersede historical provider dispositions. No thresholds, exclusions, production or workflow files changed.

## Handoff

Matching Codex keepalive owns asynchronous hosted CI/review. Reviewed Repo Merge Verify Closer owns full current expected-check topology, unchanged head, complete active-thread bodies/zero-active gate, seven-minute review floor, guarded merge and actual verify:compare/chunk disposition. Keep broad issue #1750 OPEN below90%; related chunk only. Existing unrelated UI-auth failures stay visible and separately owned.


<!-- This is an auto-generated comment: release notes by coderabbit.ai -->
## Summary by CodeRabbit

* **Tests**
  * Added HTTP-level checks that exclude infinite similarity scores before applying result limits and rank finite results according to the selected metric.
  * Added checks for sanitized service-unavailable responses to database errors and connection cleanup after query failures.
* **Documentation**
  * Added evidence covering test results, coverage comparisons, mutation checks, and artifact integrity.
  * Recorded existing UI authentication failures and clarified that the results do not establish deployment readiness or complete the broader coverage initiative.
<!-- end of auto-generated comment: release notes by coderabbit.ai -->