A bulk import can commit successfully and then fail while restoring autocommit. Add one regression proving the endpoint reports503, logs that cleanup exception and closes the connection. Production code is unchanged.

Related #1750. This is one ranked coverage chunk; the broader initiative remains open.

- [x] Add the named in-process HTTP cleanup regression with a recording PostgreSQL connection.
- [x] Prove actual production error-propagation mutation RED and byte-identical restoration GREEN, in standard and optimized replay; refuse reused output without mutation.
- [x] Retain matched full-suite coverage/outcomes and hash-bound raw evidence.

Validation:1956→1957PASS, unchanged24UI-authFAIL/21SKIP; both full commands exit1. All2001 old outcomes unchanged. Coverage12446→12447 of14309statements (86.98022224→86.98721085%), same117files/82exclusions/no covered-line regression. Manager824→825of826; unreachable schema-add sibling remains honestly uncovered.

Actual production mutation changes the selected HTTP response from503 to200 and fails the named assertion; restoration passes. FullBlack406files, focusedRuff and diff checks pass. Python3.12.2; no live PostgreSQL, deployment, hosted parity, providerPASS or90% claim. Existing provider CONCERNS retained.

Commands, console, JUnit, coverageJSON, both replay modes, reuse refusal, source snapshots and hashes: docs/evidence/issue-1750-cleanup-propagation/validation.tar.gz, manifest.json and README.md.

Keepalive owns hosted CI/review after this ready PR opens. Closer owns unchanged-head complete expected checks, all review-thread pages/zero-active gate, seven-minute floor, guarded merge and actual compare. A generated closing reference must not close broad1750 below90%.


### Acceptance Criteria

- [x] **Tests**
  - [x] Added regression coverage for bulk imports when restoring database settings fails after a commit, including the service response, operation order, and error reporting.
  - [x] Added a replay check that verifies the regression test detects the cleanup failure.
- [x] **Documentation**
  - [x] Added reproducible evidence and comparison results, including test outcomes and coverage details.
  - [x] Clarified that the results do not represent broader issue resolution or live deployment validation.

Reconciled against commits 1d34ed3 and 97363be: the HTTP regression passes; original and follow-up archives pass all 69 member hash checks, and follow-up source/test/driver inputs match this checkout. Follow-up evidence verifies actual standard/optimized pytest RED1 and restored GREEN0, output reuse refusal, and 308 passing manager tests with targeted manager statement coverage 819/826 (99.152542%). Historical full-suite results above are retained, not rerun in the follow-up. Production code remains unchanged; broader #1750 remains open.


Automated replay follow-up: tests/test_manager_cleanup_replay.py runs actual standard/optimized production mutation and restoration, checks the named HTTP 200-versus-503 failure, verifies -O reaches child pytest, and refuses reused output while preserving all seven proof files and caller identities. All 310 manager tests pass; targeted manager statement coverage remains 819/826 (99.152542%). Black checks all 407 Python files; focused Ruff and diff checks pass. New raw receipts and current input snapshots are hash-bound in docs/evidence/issue-1750-cleanup-propagation/automated/validation.tar.gz and automated/manifest.json. Production code remains unchanged; no live database/deployment claim or broader #1750 closure.

<!-- This is an auto-generated comment: release notes by coderabbit.ai -->
## Summary by CodeRabbit

* **Tests**
  * Added regression coverage for bulk imports when restoring database settings fails after a commit, including the service response, operation order, and error reporting.
  * Added a replay check that verifies the regression test detects the cleanup failure.
* **Documentation**
  * Added reproducible evidence and comparison results, including test outcomes and coverage details.
  * Clarified that the results do not represent broader issue resolution or live deployment validation.
<!-- end of auto-generated comment: release notes by coderabbit.ai -->
