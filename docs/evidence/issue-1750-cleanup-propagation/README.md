# Committed bulk-import cleanup error — source #1750

One new HTTP regression covers the remaining reachable manager cleanup-error line.
After a successful commit and cache invalidation, restoring PostgreSQL autocommit
raises: the endpoint must return 503 with `Database unavailable`, log the same
exception and still close the connection. A recording connection and in-process
ASGI request exercise production routing; this is not a live PostgreSQL claim.
Production code is unchanged.

Current last-500-commit ranking selects `api/managers.py`: 8 whole-word repair-subject
proxy matches, 17 touches and 2 uncovered statements. This proxy is not an escaped
incident count. Line1441 is the selected reachable error path. Line330 is behind
`_ensure_manager_table`, which already adds that column; bypassing that helper would
manufacture a normal-path precondition. It remains honestly uncovered.

## Historical matched full-suite results

- Complete matched suite: 1956→1957 PASS, identical24UI-auth FAIL and21SKIP. Both
  commands exit1. All2001 old node outcomes are unchanged; one new node passes.
- Covered statements12446→12447 of14309:86.98022224%→86.98721085%. Identical117-file
  source universe and82excluded lines; no covered-line regression. Manager824→825
  of826 statements. The broader initiative remains OPEN below90%.
- Actual function-scoped production mutation removes only error propagation after
  autocommit restoration: the named case fails with HTTP200 instead of503 (REDexit1).
  Byte-identical source restoration passes (GREENexit0). Standard and optimized-driver
  replay both enforce named JUnit failures, zero errors/skips and exact source hashes.
  These historical optimized receipts did not pass `-O` to the pytest subprocess;
  the acceptance follow-up below corrects that limitation.
- Reusing the standard proof directory fails before mutation and preserves all7
  existing files. Replay uses a private git archive and preserves caller source/test.
- Full repository Black, focused Ruff and `git diff --check` pass. Actual interpreter
  Python3.12.2; hosted dependency parity is not asserted. No deployment/provider PASS.

## Acceptance verification follow-up

The same regression now records the insert and verifies it uses the same connection
with autocommit disabled and `commit=False`. Its complete event sequence proves one
commit, manager-cache invalidation, failed restoration, and one close, without a
rollback after commit. Structured log assertions check the warning followed by the
API error, the exact cleanup exception object, its type, and its retained traceback.

On Python 3.14.8, all 308 manager tests pass. Targeted `api/managers.py` statement
coverage is 819/826 (99.152542%, displayed as 99%); line 1441 is covered. This smaller
test selection is not comparable to the historical full-suite coverage above.
The standard and optimized replays each fail with HTTP 200 after the production
propagation mutation and pass after byte-identical restoration. The optimized
receipts now include `-O` in both pytest commands. Reusing an output directory exits
1 while preserving all seven proof files and the caller's source/test hashes.
Focused Ruff and `git diff --check` pass. Black checked all 406 discovered Python
files individually, then the required repository-wide command passed with those
verified cache entries in a writable temporary directory. This avoids an observed
stall in Black's multiple-file worker path in this sandbox; no formatting checks
were skipped.

`followup/validation.tar.gz` and `followup/manifest.json` retain and hash-bind these
new commands, exits, console output, JUnit, coverage JSON, and current input snapshots.
The original archive and manifest remain unchanged and describe the historical test
and driver snapshots. Their member hashes and full-suite outcome totals were
reverified; the historical full suite was not rerun during this follow-up.

These checks use an in-process HTTP request and a recording PostgreSQL connection.
They do not validate a live database, hosted service, deployment, or provider. The
broader issue #1750 remains open; this completes only its cleanup-error coverage chunk.

Verified acceptance checklist:

- [x] Tests
- [x] Regression checks the cleanup-failure response, operation order, and error reporting.
- [x] Replay verifies the regression detects the cleanup failure.
- [x] Documentation
- [x] Reproducible evidence includes test outcomes, comparison results, and coverage details.
- [x] Results explicitly exclude broader issue resolution and live deployment validation.

## Automated replay acceptance check

`tests/test_manager_cleanup_replay.py` runs the actual replay driver in standard
and optimized modes. Each case checks the named HTTP 200-versus-503 JUnit failure,
the restored passing result, mutation/restoration hashes, child interpreter flags,
and caller source/test preservation. It then retries the same output path and
requires refusal with all seven proof files unchanged. No subprocess is mocked.

The current manager selection passes all 310 tests on Python 3.14.8. Targeted
manager statement coverage remains 819/826 (99.152542%, displayed as 99%), with
line 1441 covered. These targeted results do not replace the historical matched
full-suite comparison. Black passes for all 407 Python files after serial
verification populated its temporary cache; focused Ruff and diff checks pass.

`automated/validation.tar.gz` and `automated/manifest.json` retain the current
targeted command, console, coverage JSON, JUnit, both modes' actual replay receipts,
formatting results, and input snapshots. Original and follow-up evidence remains
unchanged. `pr-body.md` contains the reconciled acceptance checklist: updating the
remote PR body was blocked because the GitHub connector requires approval and
this run's approval policy is `never`. The PR was verified open and ready for review.
Broader #1750 remains open; no live database or deployment was validated.

## Reproduce

```sh
python -m pytest tests/test_manager_bulk_cleanup_failure.py -m "not slow" -q -o addopts=
python -m pytest tests/test_manager_cleanup_replay.py -m "not slow" -q -o addopts=
python -m pytest tests/test_manager*.py -m "not slow" -q -o addopts= --cov=api.managers --cov-report=term-missing --cov-report=json:targeted-coverage.json --cov-fail-under=0
python docs/evidence/issue-1750-cleanup-propagation/replay_cleanup.py --output /tmp/new-cleanup-proof
python -O docs/evidence/issue-1750-cleanup-propagation/replay_cleanup.py --output /tmp/new-cleanup-proof-optimized
python -m pytest tests -m "not slow" -q --cov --cov-report=json:coverage.json
```

Use fresh replay output paths. `validation.tar.gz` retains complete baseline/candidate
console, coverageJSON, JUnit and exact commands/exits, both final mutation phase
receipts, reuse refusal, source/test/driver snapshots and ranking. `manifest.json`
binds every archive member and its historical inputs. Historical JSON/dialect chunk receipts
and provider CONCERNS are preserved. Matching keepalive owns hosted CI/review; closer
owns complete exact-head topology, all review threads, the seven-minute floor,
guarded merge and actual compare. This PR is related to1750 and does not close it.
